package raft

import (
	"sync"
	"time"
)

// Options configures a Node.
type Options struct {
	ID                NodeID
	Storage           Storage
	ElectionTimeout   int                    // ticks before a follower starts an election
	HeartbeatTick     int                    // leader heartbeat interval in ticks
	MaxSizePerMsg     int                    // soft cap on entries per MsgApp
	SnapshotThreshold Index                  // compact after live log exceeds this
	Applied           Index                  // last index the application has applied
	Rand              func(salt int64) int64 // deterministic jitter (nil => 0)
	Events            func(Event)            // observability hook
}

// Ready is a batch of work for the host application.
type Ready struct {
	Messages  []Message
	Committed []Entry
	Snapshot  *Snapshot // non-nil => restore state machine from this
}

// Node is a Raft state-machine instance. All exported methods are
// serialized by a single mutex; the deterministic harness therefore
// never observes torn internal state.
type Node struct {
	mu  sync.Mutex
	opt Options

	role Role
	term Term
	vote NodeID

	// Entries live in Storage. We additionally remember the compacted
	// snapshot in-memory for fast access.
	snap *Snapshot

	commit  Index
	applied Index

	leader         NodeID
	leadTransferee NodeID

	elapsed      int // ticks since last meaningful contact
	heartElapsed int

	log0 Index // compacted base index (snap.Index, 0 initially)

	prs map[NodeID]*progress // only meaningful for leader

	// election bookkeeping
	votes     map[NodeID]bool
	elections int // number of elections started (per-run, deterministic)

	// pending client work
	props map[ReqID]*propState
	reads map[ReqID]struct{}
	// read-index barriers awaiting a quorum heartbeat: ctx id -> pending
	readBar      map[uint64]map[NodeID]bool
	readBarFor   map[uint64][]ReqID
	readBarID    uint64
	readConfirm  map[uint64]bool // barrier confirmed by quorum
	readAckIndex Index           // index the leader must hold applied at read issue

	// membership change
	pendingConf bool

	out  []Message // buffered messages to flush into Ready
	rd   chan Ready
	done chan struct{}

	listeners []func(Event)
}

type propState struct {
	term     Term
	index    Index // index of the appended entry carrying the request
	notified bool  // committed event already emitted
}

// NewNode constructs a node from storage. The storage must have been
// Bootstrapped for a brand-new cluster, or contain prior state for a
// restarting/compacted node.
func NewNode(opt Options) *Node {
	if opt.ElectionTimeout <= 1 {
		opt.ElectionTimeout = 10
	}
	if opt.HeartbeatTick == 0 {
		opt.HeartbeatTick = 1
	}
	if opt.MaxSizePerMsg == 0 {
		opt.MaxSizePerMsg = 16
	}
	if opt.SnapshotThreshold == 0 {
		opt.SnapshotThreshold = 64
	}
	n := &Node{
		opt:         opt,
		role:        RoleFollower,
		prs:         map[NodeID]*progress{},
		votes:       map[NodeID]bool{},
		props:       map[ReqID]*propState{},
		reads:       map[ReqID]struct{}{},
		readBar:     map[uint64]map[NodeID]bool{},
		readBarFor:  map[uint64][]ReqID{},
		readConfirm: map[uint64]bool{},
		rd:          make(chan Ready, 64),
		done:        make(chan struct{}),
	}
	if opt.Events != nil {
		n.listeners = append(n.listeners, opt.Events)
	}
	n.load()
	return n
}

// AddEventListener registers an additional observability callback. It
// is invoked while the node lock is held, so callbacks must not call
// back into the node.
func (n *Node) AddEventListener(f func(Event)) {
	n.mu.Lock()
	n.listeners = append(n.listeners, f)
	n.mu.Unlock()
}

// load restores persisted state.
func (n *Node) load() {
	hard, conf, hadSnap := n.opt.Storage.InitialState()
	n.term = hard.Term
	n.vote = hard.Vote
	n.commit = hard.Commit
	n.applied = n.opt.Applied
	if hadSnap {
		sn, _ := n.opt.Storage.Snapshot()
		n.snap = sn
		n.log0 = sn.Index
	}
	_ = conf
	// Rebuild progress/peer list lazily from active config once needed.
}

func (n *Node) emit(e Event) {
	for _, f := range n.listeners {
		f(e)
	}
}

func (n *Node) send(m Message) {
	m.From = n.opt.ID
	if m.Term == 0 {
		m.Term = n.term
	}
	n.out = append(n.out, m)
}

// flush packages pending messages and newly committed entries into Ready.
func (n *Node) flush() {
	var committed []Entry
	if n.applied < n.commit {
		lo := n.applied + 1
		ents, err := n.opt.Storage.Entries(lo, n.commit+1)
		if err != nil {
			// Entries compacted before the application consumed them:
			// the host must restore the snapshot first.
			sn, _ := n.opt.Storage.Snapshot()
			n.rd <- Ready{Messages: n.out, Snapshot: sn}
			n.out = nil
			return
		}
		committed = ents
	}
	if len(n.out) == 0 && len(committed) == 0 {
		return
	}
	select {
	case n.rd <- Ready{Messages: n.out, Committed: committed}:
		n.out = nil
		if len(committed) > 0 {
			last := committed[len(committed)-1].Index
			n.applied = last
			n.onCommitAdvanced(committed)
		}
	default:
		// Host has not consumed the previous batch yet; retry next tick.
	}
}

// onCommitAdvanced releases client proposals whose entries committed,
// finalizes membership changes and removes leader authority for nodes
// excluded by the newest config.
func (n *Node) onCommitAdvanced(ents []Entry) {
	var lastConf *Entry
	for i := range ents {
		e := ents[i]
		if e.Kind == EntryConf {
			lastConf = &ents[i]
		}
	}
	if lastConf != nil {
		conf, err := DecodeConfig(lastConf.Data)
		if err == nil {
			n.pendingConf = false
			n.emit(Event{Type: EvtConfChange, Term: n.term, Role: n.role,
				Index: lastConf.Index})
			if n.role == RoleLeader {
				// Drop progress for removed members; stop sending late
				// messages that would now be ignored.
				for id := range n.prs {
					if !n.isMember(id, conf) {
						delete(n.prs, id)
					}
				}
				if !n.isMember(n.opt.ID, conf) {
					n.becomeFollower(n.term, "")
					return
				}
			} else if !n.isMember(n.opt.ID, conf) {
				n.applyConfigIfRemoved()
				return
			}
		}
	}
	// Any pending proposal whose entry landed in this span is committed.
	// Correlation by ReqID is encoded inside the data payload by the kv
	// layer; raft itself marks proposals via the callback registry keyed
	// on append order tracked in props.
	n.resolveCommittedProps(ents)
	n.emit(Event{Type: EvtEntriesCommitted, Term: n.term, Role: n.role,
		Index: ents[len(ents)-1].Index, Entries: ents})
	n.maybeCompact()
}

func (n *Node) resolveCommittedProps(ents []Entry) {
	if len(n.props) == 0 {
		return
	}
	// Report commit but KEEP tracking so an immediate retry observes
	// "already committed" and the host can correlate. Entries are pruned
	// when leadership changes or a newer term supersedes them.
	for id, st := range n.props {
		if !st.notified && st.index > 0 && st.index <= ents[len(ents)-1].Index &&
			st.index >= ents[0].Index && st.term == n.term {
			st.notified = true
			n.emit(Event{Type: EvtProposalResult, Term: n.term, Role: n.role,
				ReqID: id, Status: PropCommitted, Reason: RejectNone,
				Index: st.index})
		}
	}
}

// ID returns the node identity.
func (n *Node) ID() NodeID { return n.opt.ID }

// Ready returns the channel of work batches.
func (n *Node) Ready() <-chan Ready { return n.rd }

// Term returns the current term (for tests/assertions).
func (n *Node) Term() Term {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

// Role returns the current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Leader returns the last known leader.
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// CommitIndex returns the current commit index.
func (n *Node) CommitIndex() Index {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commit
}

func (n *Node) persistHard() {
	_ = n.opt.Storage.SetHard(HardState{Term: n.term, Vote: n.vote, Commit: n.commit})
}

func (n *Node) becomeFollower(term Term, leader NodeID) {
	if term < n.term {
		return
	}
	changed := n.role != RoleFollower || n.term != term
	if term > n.term {
		n.vote = ""
	}
	n.role = RoleFollower
	n.term = term
	n.leader = leader
	n.leadTransferee = ""
	n.elapsed = 0
	n.prs = map[NodeID]*progress{}
	n.abortPending(RejectTermStale)
	n.persistHard()
	if changed {
		n.emit(Event{Type: EvtRoleChange, Term: term, Role: RoleFollower, Leader: leader})
	}
}

// abortPending rejects queued client work that can no longer succeed.
func (n *Node) abortPending(reason RejectReason) {
	for id := range n.props {
		n.finishProposal(id, PropRejected, reason)
	}
	for id := range n.reads {
		delete(n.reads, id)
		n.emit(Event{Type: EvtReadResult, Term: n.term, Role: n.role, ReqID: id, Reason: reason})
	}
	n.readBar = map[uint64]map[NodeID]bool{}
	n.readBarFor = map[uint64][]ReqID{}
	n.readConfirm = map[uint64]bool{}
}

func (n *Node) finishProposal(id ReqID, st ProposalStatus, reason RejectReason) {
	if _, ok := n.props[id]; !ok {
		return
	}
	delete(n.props, id)
	n.emit(Event{Type: EvtProposalResult, Term: n.term, Role: n.role, ReqID: id, Status: st, Reason: reason})
}

// Tick advances the logical clock by one tick.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tickElection()
	n.checkPropDeadlines()
	n.flush()
}

func (n *Node) tickElection() {
	n.elapsed++
	if n.role == RoleLeader {
		n.heartElapsed++
		if n.heartElapsed >= n.opt.HeartbeatTick {
			n.heartElapsed = 0
			n.broadcastHeartbeat()
		}
		return
	}
	if n.elapsed >= n.electionTimeout() {
		n.step(Message{Type: MsgHup})
	}
}

func (n *Node) electionTimeout() int {
	t := n.opt.ElectionTimeout
	if n.opt.Rand != nil {
		// Jitter is keyed by the election attempt so two followers that
		// timed out together do not collide on every retry. With an
		// identity-hashed Rand the whole schedule stays reproducible.
		j := n.opt.Rand(int64(n.elections)) % int64(t)
		t += int(j)
	}
	return t
}

func (n *Node) checkPropDeadlines() {}

// Step injects one local or network message.
func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.step(m)
	n.flush()
}

// Propose enqueues a client proposal.
func (n *Node) Propose(id ReqID, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.step(Message{Type: MsgProp, ReqID: id, Data: data})
	n.flush()
}

// ProposeRead enqueues a linearizable read.
func (n *Node) ProposeRead(id ReqID, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.step(Message{Type: MsgRead, ReqID: id, Data: data})
	n.flush()
}

// Applied reports application progress.
func (n *Node) Applied(idx Index) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if idx > n.applied {
		n.applied = idx
	}
	n.maybeCompact()
	n.flush()
}

// SnapshotBuilt hands a built snapshot to the node for compaction.
func (n *Node) SnapshotBuilt(idx Index, term Term, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	conf := n.activeConfig()
	sn := Snapshot{Index: idx, Term: term, Conf: conf, Data: data}
	if err := n.opt.Storage.Compact(sn); err != nil {
		return err
	}
	n.snap = &sn
	n.log0 = idx
	if n.applied < idx {
		n.applied = idx
	}
	return nil
}

// ActiveConfig exposes the latest configuration for tests/host.
func (n *Node) ActiveConfig() Config {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.activeConfig()
}

func (n *Node) activeConfig() Config {
	if n.snap != nil {
		// Snapshot carries conf at snap.Index; later committed conf
		// entries override it.
	}
	conf := Config{}
	sn, _ := n.opt.Storage.Snapshot()
	if sn != nil {
		conf = sn.Conf
	}
	// Prefer committed config entries; fall back to uncommitted ones
	// only for the bootstrapped index-1 config before any commit has been
	// observed, and for a leader's own just-appended pending change.
	scan := n.commit
	ents, err := n.opt.Storage.Entries(n.log0+1, scan+1)
	if err == nil {
		for _, e := range ents {
			if e.Kind == EntryConf {
				if c, err := DecodeConfig(e.Data); err == nil {
					conf = c
				}
			}
		}
	}
	if len(conf.Voters) == 0 {
		all, aerr := n.opt.Storage.Entries(n.log0+1, n.lastIndex()+1)
		if aerr == nil {
			for i := len(all) - 1; i >= 0; i-- {
				if all[i].Kind == EntryConf {
					if c, err := DecodeConfig(all[i].Data); err == nil {
						return c
					}
				}
			}
		}
	}
	return conf
}

// Run drives the node with a real ticker until Stop.
func (n *Node) Run(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-t.C:
			n.Tick()
		}
	}
}

// Stop terminates the node.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	select {
	case <-n.done:
	default:
		close(n.done)
	}
}
