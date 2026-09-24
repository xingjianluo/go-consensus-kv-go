package raft

import (
	"math/rand"
	"sort"
	"time"
)

// Node is the deterministic Raft core. One goroutine (or one simulation
// turn) must call its methods at a time.
type Node struct {
	id string

	role Role
	term int
	vote string
	lead string

	log    Storage
	sm     StateMachine
	env    Env
	cl     clock
	timers TimerHost

	commitIndex int
	lastApplied int

	// applied voter configuration
	conf ConfState
	// uncommitted membership change
	pendingConf  *ConfState
	pendingIndex int

	// leader-only progress, indexed by voter id
	pr map[string]*progress
	// in-flight client proposals: log index -> reqID
	pendingReqs map[int]string
	// reqID -> log index (guards duplicate in-flight proposals)
	inflight map[string]int
	// in-flight linearizable reads
	reads   map[string]*readState
	readSeq int
	// committed client result cache for exact replays on this leader.
	done map[string]Outcome

	// candidate-only tally
	votes map[string]bool

	cfg Config
	rng *rand.Rand

	stopped bool
}

type clock interface{ Now() time.Time }

type progress struct {
	next  int
	match int
}

type readState struct {
	reqID   string
	session string
	key     string
	startAt time.Time
	index   int // leader commit index at read start
	acks    map[string]bool
}

type proposal struct {
	req ClientRequest
}

// Config parameterizes a Node.
type Config struct {
	ID                string
	ElectionMin       time.Duration
	ElectionMax       time.Duration
	Heartbeat         time.Duration
	RNGSeed           int64
	SnapshotThreshold int // entries between snapshot points; 0 = default 100
}

// StateMachine is the replicated application.
type StateMachine interface {
	Apply(e Entry, ctx ApplyCtx) ApplyResult
	Snapshot() []byte
	Restore(data []byte) error
}

// ApplyCtx supplies non-deterministic-free context for Apply.
type ApplyCtx struct {
	Now time.Time
}

// ApplyResult lets the SM express idempotent/session decisions.
type ApplyResult struct {
	OK        bool
	Code      ErrorCode
	Reason    string
	Value     string
	Duplicate bool
}

// NewNode builds a node and restores any durable state. A fresh node must
// still be initialized with Bootstrap.
func NewNode(cfg Config, st Storage, sm StateMachine, env Env, timers TimerHost, cl clock) *Node {
	if cfg.ElectionMin <= 0 {
		cfg.ElectionMin = 150 * time.Millisecond
	}
	if cfg.ElectionMax <= cfg.ElectionMin {
		cfg.ElectionMax = 2 * cfg.ElectionMin
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = cfg.ElectionMin / 2
	}
	if cfg.SnapshotThreshold <= 0 {
		cfg.SnapshotThreshold = 100
	}
	n := &Node{
		id:          cfg.ID,
		log:         st,
		sm:          sm,
		env:         env,
		cl:          cl,
		timers:      timers,
		cfg:         cfg,
		rng:         rand.New(rand.NewSource(cfg.RNGSeed)),
		pendingReqs: map[int]string{},
		inflight:    map[string]int{},
		reads:       map[string]*readState{},
		done:        map[string]Outcome{},
	}
	// Restore hard state.
	hard := st.InitialState()
	n.term = hard.Term
	n.vote = hard.Vote
	// Restore snapshot / applied state.
	if snap := st.Snapshot(); snap != nil {
		if err := sm.Restore(snap.Data); err == nil {
			n.conf = snap.Conf
			n.commitIndex = snap.LastIndex
			n.lastApplied = snap.LastIndex
		}
	}
	return n
}

// StartTimers arms the election timer after construction/restore.
func (n *Node) StartTimers() {
	if n.conf.size() > 0 {
		n.timers.ResetElection(n.term)
	}
}

// Bootstrap initializes a brand-new cluster: every member starts as a
// follower in term 0 with the initial voter set recorded at snapshot
// index 0. Only valid on an empty store.
func (n *Node) Bootstrap(voters ConfState) error {
	if n.conf.size() > 0 {
		return errUnavailable
	}
	meta := SnapshotMeta{
		LastIndex: 0,
		LastTerm:  0,
		Conf:      voters,
		Data:      n.sm.Snapshot(),
	}
	if err := n.log.ApplySnapshot(meta); err != nil {
		return err
	}
	n.conf = voters
	n.commitIndex = 0
	n.lastApplied = 0
	n.timers.ResetElection(0)
	n.emit(EvRoleChange, 0, "初始引导完成，配置投票节点="+joinIDs(voters.Voters))
	return nil
}

// ElectionTimeout returns this node's deterministic randomized election
// timeout for the next term (used by timer hosts).
func (n *Node) ElectionTimeout() time.Duration {
	span := int64(n.cfg.ElectionMax - n.cfg.ElectionMin)
	d := int64(n.cfg.ElectionMin)
	if span > 0 {
		d += n.rng.Int63n(span)
	}
	return time.Duration(d)
}

// MarkUnconfigured marks a brand-new node that has no configuration yet.
// It will not start elections until it receives a snapshot/conf.
func (n *Node) MarkUnconfigured() { n.timers.StopTimers() }

// Stop halts timers.
func (n *Node) Stop() {
	if n.stopped {
		return
	}
	n.stopped = true
	n.timers.StopTimers()
}

// ID returns the node id.
func (n *Node) ID() string { return n.id }

// State returns a snapshot for assertions/logging.
type State struct {
	ID          string
	Role        Role
	Term        int
	Leader      string
	VotedFor    string
	CommitIndex int
	LastApplied int
	LastIndex   int
	Voters      []string
}

func (n *Node) State() State {
	v := append([]string(nil), n.conf.Voters...)
	return State{
		ID: n.id, Role: n.role, Term: n.term, Leader: n.lead,
		VotedFor: n.vote, CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied, LastIndex: n.log.LastIndex(), Voters: v,
	}
}

// Conf returns the applied voter configuration.
func (s State) Conf() ConfState { return ConfState{Voters: s.Voters} }

// ---- state transitions ----

func (n *Node) persistHard() {
	_ = n.log.SaveHardState(HardState{Term: n.term, Vote: n.vote})
}

// becomeFollower drops all leader/candidate volatile state. Pending
// proposals are only failed when the term actually advances; a candidate
// that recognizes a same-term leader still holds valid in-flight entries.
func (n *Node) becomeFollower(term int, leader string) {
	termAdvanced := term > n.term
	n.role = Follower
	n.term = term
	n.vote = ""
	n.lead = leader
	n.pr = nil
	n.votes = nil
	if termAdvanced {
		n.failPending("leader 变更且 term 前进，提交状态未知；请以相同 ReqID 重试")
	}
	n.persistHard()
	n.timers.ResetElection(term)
	n.emit(EvRoleChange, n.log.LastIndex(),
		"转为 follower，term="+itoa(term)+leaderPart(leader)+
			tern(termAdvanced, "（term 前进，清理在途提案）", "（同 term 承认 leader，保留在途日志）"))
}

func tern(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func (n *Node) becomeCandidate() {
	n.role = Candidate
	n.term++
	n.vote = n.id
	n.lead = ""
	n.pr = nil
	n.votes = map[string]bool{n.id: true}
	n.failPending("开始新一轮选举，旧提案提交状态未知")
	n.persistHard()
	n.timers.ResetElection(n.term)
	n.emit(EvElection, n.log.LastIndex(), "选举超时，成为 candidate 并自投，term="+itoa(n.term))
	if n.conf.size() == 1 && n.conf.contains(n.id) {
		n.becomeLeader()
		return
	}
	lastTerm, _ := n.log.TermAt(n.log.LastIndex())
	for _, id := range n.conf.Voters {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type: MsgRequestVote, From: n.id, To: id, Term: n.term,
			LogTerm: lastTerm, Index: n.log.LastIndex(),
		})
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.lead = n.id
	n.votes = nil
	n.pr = map[string]*progress{}
	last := n.log.LastIndex()
	for _, id := range n.conf.Voters {
		if id == n.id {
			continue
		}
		n.pr[id] = &progress{next: last + 1, match: 0}
	}
	n.persistHard()
	n.timers.ResetHeartbeat(n.term)
	n.emit(EvElection, last, "获得多数票，成为 leader，term="+itoa(n.term))
	// The no-op commits the leader's term: entries from prior terms are
	// never committed directly by counting replicas (Raft §8).
	n.appendEntries([]Entry{{Term: n.term, Kind: EntryNoop}}, nil)
	n.bcastAppend()
}

// failPending answers every unresolved proposal/ read with an explicit
// "commit status unknown" outcome, never claiming success or commit.
func (n *Node) failPending(reason string) {
	for idx, reqID := range n.pendingReqs {
		n.respond(reqID, Outcome{
			OK: false, Code: ErrTimeout, Reason: reason,
			Leader: n.lead, Committed: false, Index: idx, Term: n.term,
		})
	}
	n.pendingReqs = map[int]string{}
	n.inflight = map[string]int{}
	for id, r := range n.reads {
		n.respond(r.reqID, Outcome{
			OK: false, Code: ErrTimeout, Reason: reason,
			Leader: n.lead, Term: n.term,
		})
		delete(n.reads, id)
	}
}

// ---- small helpers ----

func (n *Node) send(m Message) { n.env.Send(m) }

func (n *Node) respond(reqID string, o Outcome) {
	if reqID == "" {
		return
	}
	n.env.Respond(reqID, o)
}

func (n *Node) emit(k EventKind, index int, detail string) {
	n.env.Emit(Event{
		At: n.cl.Now(), Node: n.id, Kind: k, Term: n.term,
		Role: n.role, Index: index, Detail: detail,
	})
}

func (n *Node) lastTerm() int {
	t, err := n.log.TermAt(n.log.LastIndex())
	if err != nil {
		return 0
	}
	return t
}

func (n *Node) snapLastIndex() int {
	if s := n.log.Snapshot(); s != nil {
		return s.LastIndex
	}
	return 0
}

// appendEntries stores new entries (caller assigns indices) and returns
// them with indices filled.
func (n *Node) appendEntries(entries []Entry, conf *ConfState) int {
	li := n.log.LastIndex()
	for i := range entries {
		li++
		entries[i].Index = li
		if conf != nil && entries[i].Kind == EntryConf {
			entries[i].Conf = conf
		}
	}
	_ = n.log.Append(entries)
	return li
}

func sortedVoters(c ConfState) []string {
	v := append([]string(nil), c.Voters...)
	sort.Strings(v)
	return v
}

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += id
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [24]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

func leaderPart(id string) string {
	if id == "" {
		return ""
	}
	return "，leader=" + id
}
