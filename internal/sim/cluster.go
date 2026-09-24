// Package sim provides a deterministic in-process Raft cluster with
// controllable partitions, crashes, restarts, latency and clocks.
//
// Determinism: one goroutine drives everything. Pending network messages
// and one-shot timers are held in a single time-ordered queue with fixed
// tie-breaks, so Advance() with a given seed always replays identically.
package sim

import (
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/clock"
	"github.com/xingjianluo/go-consensus-kv-go/internal/kvsm"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// Options configures a cluster.
type Options struct {
	Nodes           []string
	Seed            int64
	StartTime       time.Time
	ElectionMin     time.Duration
	ElectionMax     time.Duration
	Heartbeat       time.Duration
	SnapshotEntries int
	NetLatency      time.Duration
}

type envelope struct {
	seq   int64
	at    time.Time
	from  string
	to    string
	msg   raft.Message
	kind  uint8 // 0=raft, 1=response
	out   raft.Outcome
	reqID string
}

type runtime struct {
	id      string
	node    *raft.Node
	store   *raft.MemStorage // kept across crash/restart
	kv      *kvsm.KV
	env     *simEnv
	alive   bool
	timerE  clock.TimerID
	timerH  clock.TimerID
	tagTerm int
}

// Cluster is the simulated network and node set.
type Cluster struct {
	mu      sync.Mutex
	opts    Options
	sched   *clock.Scheduler
	nodes   map[string]*runtime
	order   []string
	queue   []envelope
	seq     int64
	events  []raft.Event
	results map[string]chan raft.Outcome

	// topology: a node can only deliver to members of the same partition
	// group. nil groups = fully connected.
	groups [][]string

	latency time.Duration

	// driveMu serializes the whole deterministic state machine (node
	// steps + event loop) when many real goroutines act as clients.
	driveMu sync.Mutex
}

// NewCluster creates nodes as a bootstrapped term-0 cluster.
func NewCluster(opts Options) *Cluster {
	if opts.StartTime.IsZero() {
		opts.StartTime = time.Unix(1_700_000_000, 0)
	}
	if opts.ElectionMin <= 0 {
		opts.ElectionMin = 150 * time.Millisecond
	}
	if opts.ElectionMax <= 0 {
		opts.ElectionMax = 300 * time.Millisecond
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 50 * time.Millisecond
	}
	if opts.SnapshotEntries <= 0 {
		opts.SnapshotEntries = 100
	}
	if opts.NetLatency <= 0 {
		opts.NetLatency = time.Millisecond
	}
	ids := append([]string(nil), opts.Nodes...)
	sort.Strings(ids)
	c := &Cluster{
		opts:    opts,
		sched:   clock.NewScheduler(opts.StartTime),
		nodes:   map[string]*runtime{},
		order:   ids,
		results: map[string]chan raft.Outcome{},
		latency: opts.NetLatency,
	}
	voters := raft.ConfState{Voters: ids}
	for _, id := range ids {
		c.startNode(id, voters)
	}
	return c
}

func (c *Cluster) startNode(id string, voters raft.ConfState) {
	st := raft.NewMemStorage()
	kv := kvsm.New()
	rt := &runtime{id: id, store: st, kv: kv, alive: true}
	env := &simEnv{c: c, rt: rt}
	seed := c.opts.Seed*1000 + int64(hashID(id))
	cfg := raft.Config{
		ID:                id,
		ElectionMin:       c.opts.ElectionMin,
		ElectionMax:       c.opts.ElectionMax,
		Heartbeat:         c.opts.Heartbeat,
		RNGSeed:           seed,
		SnapshotThreshold: c.opts.SnapshotEntries,
	}
	rt.env = env
	rt.node = raft.NewNode(cfg, st, kv, env, &simTimers{c: c, rt: rt}, c.sched)
	_ = rt.node.Bootstrap(voters)
	rt.node.StartTimers()
	c.nodes[id] = rt
}

func hashID(s string) int {
	h := int(0)
	for _, b := range []byte(s) {
		h = h*31 + int(b)
	}
	if h < 0 {
		h = -h
	}
	return h
}

// EventLog returns all structured events emitted so far.
func (c *Cluster) EventLog() []raft.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]raft.Event, len(c.events))
	copy(out, c.events)
	return out
}

// Members returns the applied voter set as seen by the current leader
// (falls back to sorted union).
func (c *Cluster) Members() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l := c.lockedLeader(); l != "" {
		st := c.nodes[l].node.State()
		return append([]string(nil), st.Voters...)
	}
	return c.order
}

func (c *Cluster) lockedLeader() string {
	for _, id := range c.order {
		rt := c.nodes[id]
		if rt.alive && rt.node.State().Role == raft.Leader {
			return id
		}
	}
	return ""
}

// Leader returns the current leader id or "".
func (c *Cluster) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lockedLeader()
}

// State returns the raft core state of a node.
func (c *Cluster) State(id string) raft.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id].node.State()
}

// ---- result plumbing ----

func (c *Cluster) registerResult(reqID string) chan raft.Outcome {
	ch := make(chan raft.Outcome, 1)
	c.mu.Lock()
	c.results[reqID] = ch
	c.mu.Unlock()
	return ch
}

// Env implementation -----------------------------------------------------

type simEnv struct {
	c  *Cluster
	rt *runtime
}

func (e *simEnv) Now() time.Time { return e.c.sched.Now() }

func (e *simEnv) Send(m raft.Message) {
	e.c.route(raftEnvelope(e.rt.id, m))
}

func (e *simEnv) Emit(ev raft.Event) {
	e.c.mu.Lock()
	e.c.events = append(e.c.events, ev)
	e.c.mu.Unlock()
}

func (e *simEnv) Respond(reqID string, out raft.Outcome) {
	e.c.mu.Lock()
	ch, ok := e.c.results[reqID]
	if ok {
		// Single-shot channel: unregister so a later distinct request
		// can never observe a stale buffered outcome.
		delete(e.c.results, reqID)
	}
	e.c.mu.Unlock()
	if ok {
		ch <- out
	}
}

func (e *simEnv) ApplySnapshotData(data []byte) error {
	return e.rt.kv.Restore(data)
}

type simTimers struct {
	c  *Cluster
	rt *runtime
}

func (t *simTimers) clear() {
	if t.rt.timerE != 0 {
		t.c.sched.Cancel(t.rt.timerE)
		t.rt.timerE = 0
	}
	if t.rt.timerH != 0 {
		t.c.sched.Cancel(t.rt.timerH)
		t.rt.timerH = 0
	}
}

func (t *simTimers) ResetElection(term int) {
	t.clear()
	if !t.rt.alive {
		return
	}
	d := t.rt.node.ElectionTimeout()
	t.rt.timerE = t.c.sched.Add(t.c.sched.Now().Add(d), "election", t.rt.id)
	t.rt.tagTerm = term
}

func (t *simTimers) ResetHeartbeat(term int) {
	t.clear()
	if !t.rt.alive {
		return
	}
	t.rt.timerH = t.c.sched.Add(t.c.sched.Now().Add(t.c.opts.Heartbeat), "heartbeat", t.rt.id)
	t.rt.tagTerm = term
}

func (t *simTimers) StopTimers() { t.clear() }
