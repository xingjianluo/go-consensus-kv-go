package kv

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// pending tracks one in-flight client request. Multiple concurrent
// callers using the same idempotency key share one proposal and all
// receive the same terminal result.
type pending struct {
	kind     string // "put" or "session*"
	cmd      Command
	deadline time.Time
	waiters  []chan Result
}

func (p *pending) deliver(r Result) {
	for _, ch := range p.waiters {
		select {
		case ch <- r:
		default:
		}
	}
	p.waiters = nil
}

// Service wires a raft.Node and a Store into a request handler with
// at-most-once semantics. It consumes the node's Ready channel and its
// event stream; exactly one goroutine must drive Apply per node, which
// the Service provides via Run.
type Service struct {
	mu    sync.Mutex
	node  *raft.Node
	store *Store
	clock func() time.Time

	// sessionID -> requestID -> pending
	puts   map[string]map[string]*pending
	others map[raft.ReqID]*pending
	reads  map[raft.ReqID]chan Result

	events chan raft.Event
	stop   chan struct{}

	// MessageHook, if set, observes every raft message the node emits
	// (used by the server driver to forward onto its transport).
	MessageHook func(raft.Message)
	// OnConfCommit, if set, is called when a membership entry commits.
	OnConfCommit func(raft.Entry)
}

// NewService constructs a Service.
func NewService(n *raft.Node, st *Store) *Service {
	s := &Service{
		node:   n,
		store:  st,
		clock:  time.Now,
		puts:   map[string]map[string]*pending{},
		others: map[raft.ReqID]*pending{},
		reads:  map[raft.ReqID]chan Result{},
		events: make(chan raft.Event, 256),
		stop:   make(chan struct{}),
	}
	// Auto-register so callers do not have to thread the sink through
	// raft.Options (which would preclude other observers such as logs).
	n.AddEventListener(s.EventSink())
	return s
}

// EventSink returns the callback to pass into raft.Options.Events.
func (s *Service) EventSink() func(raft.Event) {
	return func(e raft.Event) {
		select {
		case s.events <- e:
		case <-s.stop:
		}
	}
}

// SetClock overrides the wall clock (deterministic tests).
func (s *Service) SetClock(f func() time.Time) { s.clock = f }

func newSessionID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "s-" + hex.EncodeToString(b)
}

// Run pumps raft Ready batches and events until Close.
func (s *Service) Run() {
	for {
		select {
		case <-s.stop:
			return
		case r, ok := <-s.node.Ready():
			if !ok {
				return
			}
			s.handleReady(r)
		case e := <-s.events:
			s.handleEvent(e)
		}
	}
}

func (s *Service) handleReady(r raft.Ready) {
	for _, m := range r.Messages {
		if s.MessageHook != nil {
			s.MessageHook(m)
		}
	}
	if r.Snapshot != nil {
		if err := s.store.Restore(r.Snapshot.Data); err != nil {
			return
		}
		s.node.Applied(r.Snapshot.Index)
		return
	}
	var last raft.Index
	for _, e := range r.Committed {
		if e.Kind == raft.EntryConf && s.OnConfCommit != nil {
			s.OnConfCommit(e)
		}
		res, ok := s.store.Apply(e)
		if !ok {
			last = e.Index // no-op entries still advance app position
			continue
		}
		last = e.Index
		s.correlate(e, res)
	}
	if last > 0 {
		s.node.Applied(last)
	}
}

// correlate matches a committed entry back to its waiter and returns
// the state-machine-derived verdict.
func (s *Service) correlate(e raft.Entry, res Result) {
	cmd, err := Decode(e.Data)
	if err != nil {
		return
	}
	_ = err
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Type {
	case CmdPut:
		if m := s.puts[cmd.SessionID]; m != nil {
			if p := m[cmd.RequestID]; p != nil {
				res.Leader = s.node.Leader()
				p.deliver(res)
				delete(m, cmd.RequestID)
			}
		}
	case CmdSessionOpen, CmdSessionClose:
		rid := raft.ReqID(cmd.SessionID + "/" + cmd.RequestID)
		if p := s.others[rid]; p != nil {
			res.Leader = s.node.Leader()
			p.deliver(res)
			delete(s.others, rid)
		}
	}
}

func (s *Service) handleEvent(e raft.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch e.Type {
	case raft.EvtProposalResult:
		if e.Status == raft.PropCommitted {
			return // resolved via committed-entry correlation
		}
		if e.Reason == raft.RejectDuplicate {
			// In-flight coalescing: the shared proposal's terminal result
			// arrives via committed-entry correlation; keep waiting.
			return
		}
		// Explicit rejection (not leader / stale term / not a voter).
		if p := s.others[e.ReqID]; p != nil {
			p.deliver(Result{Verdict: VerdictNotLeader, Leader: e.Leader,
				Reason: e.Reason.String(), Index: e.Index})
			delete(s.others, e.ReqID)
		}
		// Put requests are keyed by session/request; raft ReqID is the
		// combined key, so scan puts for that pair.
		for sid, m := range s.puts {
			for rid, p := range m {
				if raft.ReqID(sid+"/"+rid) == e.ReqID {
					p.deliver(Result{Verdict: VerdictNotLeader, Leader: e.Leader,
						Reason: e.Reason.String()})
					delete(m, rid)
				}
			}
		}
	case raft.EvtReadResult:
		if ch := s.reads[e.ReqID]; ch != nil {
			v := VerdictOK
			reason := ""
			if e.Reason != raft.RejectNone {
				v = VerdictNotLeader
				reason = e.Reason.String()
			}
			ch <- Result{Verdict: v, Leader: e.Leader, Reason: reason, Index: e.Index}
			delete(s.reads, e.ReqID)
		}
	}
}

// proposePut submits a write with an explicit timeout. Registration of
// the waiter is atomic so concurrent duplicates share one proposal.
func (s *Service) proposePut(cmd Command, timeout time.Duration) Result {
	if s.node.Role() != raft.RoleLeader {
		return Result{Verdict: VerdictNotLeader, Leader: s.node.Leader(),
			Reason: raft.RejectNotLeader.String()}
	}
	rid := raft.ReqID(cmd.SessionID + "/" + cmd.RequestID)
	ch, joined := s.joinOrRegisterPut(cmd)
	if !joined {
		data, _ := Encode(cmd)
		s.node.Propose(rid, data)
	}
	select {
	case res := <-ch:
		return res
	case <-time.After(timeout):
		// CRITICAL: timeout means "outcome unknown", never success. The
		// entry may still commit; later duplicate requests reconcile via
		// the state machine's per-request reply cache. Remove the pending
		// registration so a retry can issue a fresh proposal.
		s.forgetPut(cmd.SessionID, cmd.RequestID)
		return Result{Verdict: VerdictTimeout, Reason: "commit-not-confirmed-in-time"}
	case <-s.stop:
		return Result{Verdict: VerdictNotLeader, Reason: "closing"}
	}
}

func (s *Service) forgetPut(sessionID, requestID string) {
	s.mu.Lock()
	if m := s.puts[sessionID]; m != nil {
		delete(m, requestID)
	}
	s.mu.Unlock()
}

// OpenSession creates a session and returns its id.
func (s *Service) OpenSession(ttlMillis int64) (string, Result) {
	id := newSessionID()
	reqID := "open"
	cmd := Command{Type: CmdSessionOpen, SessionID: id, RequestID: reqID,
		TTLMillis: ttlMillis, CreateTime: s.clock().UnixMilli()}
	if s.node.Role() != raft.RoleLeader {
		return "", Result{Verdict: VerdictNotLeader, Leader: s.node.Leader()}
	}
	rid := raft.ReqID(id + "/" + reqID)
	ch := make(chan Result, 1)
	s.mu.Lock()
	s.others[rid] = &pending{waiters: []chan Result{ch}}
	s.mu.Unlock()
	data, _ := Encode(cmd)
	s.node.Propose(rid, data)
	res := <-ch
	if res.Verdict != VerdictOK {
		return "", res
	}
	return id, res
}

// CloseSession explicitly closes a session.
func (s *Service) CloseSession(sessionID string) Result {
	cmd := Command{Type: CmdSessionClose, SessionID: sessionID, RequestID: "close"}
	rid := raft.ReqID(sessionID + "/close")
	ch := make(chan Result, 1)
	s.mu.Lock()
	s.others[rid] = &pending{waiters: []chan Result{ch}}
	s.mu.Unlock()
	data, _ := Encode(cmd)
	s.node.Propose(rid, data)
	return <-ch
}

// Put proposes a write. A VerdictTimeout result MUST be treated as
// "outcome unknown", never as committed success.
func (s *Service) Put(sessionID, requestID, key string, value []byte) Result {
	// Redirect first: only the leader may propose, regardless of whether
	// this follower's (possibly lagging) store knows the session.
	if s.node.Role() != raft.RoleLeader {
		return Result{Verdict: VerdictNotLeader, Leader: s.node.Leader(),
			Reason: raft.RejectNotLeader.String()}
	}
	known, closed := s.store.HasSession(sessionID)
	if !known || closed {
		return Result{Verdict: VerdictSessionExpired, Reason: "session-unknown-or-closed"}
	}
	// Fast path: same (session, request) already applied => return the
	// original verdict without touching raft.
	if rep, ok := s.store.PendingPut(sessionID, requestID); ok {
		return Result{Verdict: VerdictDuplicate, Value: rep.Value, Index: rep.AppliedAt,
			Reason: "request-already-applied"}
	}
	cmd := Command{Type: CmdPut, SessionID: sessionID, RequestID: requestID,
		Key: key, Value: value, CreateTime: s.clock().UnixMilli()}
	return s.proposePut(cmd, 2*time.Second)
}

// joinOrRegisterPut atomically joins an in-flight request or registers
// a new one, closing the window where concurrent callers could each
// create their own proposal for the same idempotency key.
func (s *Service) joinOrRegisterPut(cmd Command) (<-chan Result, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.puts[cmd.SessionID]
	if m == nil {
		m = map[string]*pending{}
		s.puts[cmd.SessionID] = m
	}
	ch := make(chan Result, 1)
	if p := m[cmd.RequestID]; p != nil {
		p.waiters = append(p.waiters, ch)
		return ch, true
	}
	p := &pending{kind: "put", cmd: cmd, deadline: s.clock().Add(2 * time.Second),
		waiters: []chan Result{ch}}
	m[cmd.RequestID] = p
	return ch, false
}

// Get performs a linearizable read via Raft ReadIndex.
func (s *Service) Get(sessionID, requestID, key string) Result {
	if s.node.Role() != raft.RoleLeader {
		return Result{Verdict: VerdictNotLeader, Leader: s.node.Leader(),
			Reason: raft.RejectNotLeader.String()}
	}
	known, closed := s.store.HasSession(sessionID)
	if !known || closed {
		return Result{Verdict: VerdictSessionExpired, Reason: "session-unknown-or-closed"}
	}
	rid := raft.ReqID(sessionID + "/get/" + requestID)
	ch := make(chan Result, 1)
	// Register the waiter BEFORE issuing the read: for a single-node
	// cluster the ReadIndex barrier completes synchronously inside
	// ProposeRead, so the event can arrive before the call returns.
	s.mu.Lock()
	s.reads[rid] = ch
	s.mu.Unlock()
	s.node.ProposeRead(rid, []byte(key))
	var res Result
	select {
	case res = <-ch:
	case <-time.After(s.readTimeout()):
		return Result{Verdict: VerdictTimeout, Reason: "read-barrier-not-confirmed-in-time"}
	case <-s.stop:
		return Result{Verdict: VerdictNotLeader, Reason: "closing"}
	}
	if res.Verdict != VerdictOK {
		return res
	}
	r := s.store.Read(sessionID, key, s.clock().UnixMilli())
	r.Index = res.Index
	r.Leader = s.node.Leader()
	return r
}

// AddMember proposes adding a voter.
func (s *Service) AddMember(id raft.NodeID, addr string) (raft.RejectReason, bool) {
	return s.node.ProposeConfChange(raft.ConfChange{
		Type: raft.ConfAddNode, Node: id, Addr: addr,
	})
}

// RemoveMember proposes removing a voter.
func (s *Service) RemoveMember(id raft.NodeID) (raft.RejectReason, bool) {
	return s.node.ProposeConfChange(raft.ConfChange{
		Type: raft.ConfRemoveNode, Node: id,
	})
}

// readTimeout bounds waiting on the ReadIndex barrier.
func (s *Service) readTimeout() time.Duration { return 2 * time.Second }

// Close stops the service pump.
func (s *Service) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
}
