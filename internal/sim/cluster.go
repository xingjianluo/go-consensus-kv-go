package sim

import (
	"hash/fnv"
	"sync"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// LoggedEvent is an observability event tagged with its source node.
type LoggedEvent struct {
	Node raft.NodeID
	raft.Event
}

// Cluster is a deterministic group of raft nodes sharing one Network.
// Persistent MemStorage objects survive Crash/Restart; only the volatile
// raft.Node is rebuilt.
type Cluster struct {
	mu      sync.Mutex
	net     *Network
	nodes   map[raft.NodeID]*raft.Node
	stores  map[raft.NodeID]*raft.MemStorage
	applied map[raft.NodeID]raft.Index
	events  []LoggedEvent
	addrs   map[raft.NodeID]string
	ranks   map[raft.NodeID]int
	ticks   int

	// OnCommit, if set, is invoked (in deterministic node order) with
	// each batch of committed entries delivered to the application.
	OnCommit func(id raft.NodeID, ents []raft.Entry)
	// OnSnapshot, if set, is invoked when a node must restore app state.
	OnSnapshot func(id raft.NodeID, sn *raft.Snapshot)
	// ManualDrain, when true, leaves Ready batches for an external driver.
	manual bool
}

// ClusterOptions describes a cluster to create.
type ClusterOptions struct {
	IDs               []raft.NodeID
	ElectionTicks     int
	HeartbeatTicks    int
	SnapshotThreshold int
	// Addrs optionally maps ids to dial addresses (carried in configs).
	Addrs map[raft.NodeID]string
	// ManualDrain makes Tick only deliver network messages and advance
	// node timers; Ready batches are left for an external driver (e.g. a
	// kv.Service Run loop) to consume. The caller is also responsible for
	// applying committed entries and reporting Applied positions.
	ManualDrain bool
}

// NewCluster bootstraps a cluster where every listed id is a voter with
// a shared EntryConf sentinel at index 1.
func NewCluster(opt ClusterOptions) *Cluster {
	if opt.ElectionTicks == 0 {
		opt.ElectionTicks = 10
	}
	if opt.HeartbeatTicks == 0 {
		opt.HeartbeatTicks = 1
	}
	c := &Cluster{
		net:     NewNetwork(),
		nodes:   map[raft.NodeID]*raft.Node{},
		stores:  map[raft.NodeID]*raft.MemStorage{},
		applied: map[raft.NodeID]raft.Index{},
		addrs:   map[raft.NodeID]string{},
		ranks:   map[raft.NodeID]int{},
		manual:  opt.ManualDrain,
	}
	for k, v := range opt.Addrs {
		c.addrs[k] = v
	}
	peers := make([]raft.NodeID, 0, len(opt.IDs))
	peers = append(peers, opt.IDs...)
	for rank, id := range opt.IDs {
		c.ranks[id] = rank
		st := raft.NewMemStorage()
		if err := raft.Bootstrap(st, id, peers); err != nil {
			panic(err)
		}
		c.stores[id] = st
		c.applied[id] = 1 // sentinel config entry treated as applied
		c.startNode(id, opt.ElectionTicks, opt.HeartbeatTicks, opt.SnapshotThreshold)
	}
	return c
}

func (c *Cluster) startNode(id raft.NodeID, elect, heart int, snapThresh int) {
	st := c.stores[id]
	n := raft.NewNode(raft.Options{
		ID:                id,
		Storage:           st,
		ElectionTimeout:   elect,
		HeartbeatTick:     heart,
		SnapshotThreshold: raft.Index(snapThresh),
		Applied:           c.applied[id],
		// Deterministic jitter: hash(identity, election attempt).
		// Identical inputs reproduce identical election ordering.
		Rand: func(salt int64) int64 {
			h := fnv.New32a()
			_, _ = h.Write([]byte(id))
			var b [8]byte
			for i := 0; i < 8; i++ {
				b[i] = byte(salt >> (i * 8))
			}
			_, _ = h.Write(b[:])
			return int64(h.Sum32())
		},
		Events: func(e raft.Event) {
			c.mu.Lock()
			c.events = append(c.events, LoggedEvent{Node: id, Event: e})
			c.mu.Unlock()
		},
	})
	c.nodes[id] = n
}

// Node returns a member node (nil if crashed/absent).
func (c *Cluster) Node(id raft.NodeID) *raft.Node { return c.nodes[id] }

// Network returns the shared simulated network.
func (c *Cluster) Network() *Network { return c.net }

// Store returns a member's persistent storage (survives crashes).
func (c *Cluster) Store(id raft.NodeID) *raft.MemStorage { return c.stores[id] }

// Events returns a copy of the event log so far.
func (c *Cluster) Events() []LoggedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]LoggedEvent, len(c.events))
	copy(out, c.events)
	return out
}

// Leader returns the current leader id or "". During the brief split-brain
// window more than one node may still claim leadership (a deposed leader
// has not yet observed the newer term); the claimant in the HIGHEST term
// is returned, ties broken by lowest id, so the choice is deterministic
// across runs and matches what a term-aware client would retry against.
func (c *Cluster) Leader() raft.NodeID {
	var best raft.NodeID
	var bestTerm raft.Term
	for _, id := range c.orderedAlive() {
		n := c.nodes[id]
		if n == nil || n.Role() != raft.RoleLeader {
			continue
		}
		if best == "" || n.Term() > bestTerm ||
			(n.Term() == bestTerm && id < best) {
			best, bestTerm = id, n.Term()
		}
	}
	return best
}

// LeaderTerm returns the current leader's term, or 0 with empty id.
func (c *Cluster) LeaderTerm() (raft.NodeID, raft.Term) {
	l := c.Leader()
	if l == "" {
		return "", 0
	}
	return l, c.nodes[l].Term()
}

// Tick performs one deterministic round:
//  1. due network messages are delivered (ordered by time/enqueue seq);
//  2. every live node advances one tick;
//  3. all Ready batches are drained in id order and routed to network.
func (c *Cluster) Tick() {
	c.ticks++
	// 1. Deliver due messages.
	for _, m := range c.net.Tick() {
		n := c.nodes[m.To]
		if n == nil {
			c.net.Drops = append(c.net.Drops, DropRecord{
				Tick: c.net.Now(), From: m.From, To: m.To,
				Reason: raft.RejectDroppedQueue, Kind: m.Type,
			})
			continue
		}
		n.Step(m)
	}
	// 2. Node timers (deterministic id order).
	for _, id := range c.orderedAlive() {
		c.nodes[id].Tick()
	}
	if c.manual {
		return
	}
	// 3. Drain Ready batches.
	for _, id := range c.orderedAlive() {
		c.drain(id)
	}
}

func (c *Cluster) orderedAlive() []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	// Stable lexical order.
	sortIDs(ids)
	return ids
}

func sortIDs(ids []raft.NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}

func (c *Cluster) drain(id raft.NodeID) {
	n := c.nodes[id]
	for {
		var r raft.Ready
		select {
		case r = <-n.Ready():
		default:
			return
		}
		c.processReady(id, n, r)
	}
}

// processReady handles one Ready batch completely, including batches
// emitted re-entrantly by n.Applied. Committed entries are never
// silently dropped: the same handler owns both messages and application.
func (c *Cluster) processReady(id raft.NodeID, n *raft.Node, r raft.Ready) {
	for _, m := range r.Messages {
		if m.From == "" {
			m.From = id
		}
		c.net.Send(m, -1)
	}
	if r.Snapshot != nil {
		if r.Snapshot.Index > c.applied[id] {
			c.applied[id] = r.Snapshot.Index
		}
		if c.OnSnapshot != nil {
			c.OnSnapshot(id, r.Snapshot)
		}
		n.Applied(r.Snapshot.Index)
		// Applied may emit a follow-up Ready; pump it too.
		c.drain(id)
		return
	}
	if len(r.Committed) > 0 {
		last := r.Committed[len(r.Committed)-1].Index
		if c.OnCommit != nil {
			c.OnCommit(id, r.Committed)
		}
		c.applied[id] = last
		n.Applied(last)
		// n.Applied may synchronously emit another Ready (more entries,
		// compaction). Drain it in this same pass so nothing is lost.
		c.drain(id)
	}
}

// RunUntil ticks until cond is true or maxTicks rounds elapse.
func (c *Cluster) RunUntil(maxTicks int, cond func(*Cluster) bool) bool {
	for i := 0; i < maxTicks; i++ {
		if cond(c) {
			return true
		}
		c.Tick()
	}
	return cond(c)
}

// drainForTest pumps pending Ready batches (test helper).
// DrainForTest pumps pending Ready batches (test helper).
func (c *Cluster) DrainForTest(id raft.NodeID) { c.drain(id) }

// Crash simulates a hard crash: the node is stopped and volatile state
// dropped; its MemStorage is retained for Restart.
func (c *Cluster) Crash(id raft.NodeID) {
	delete(c.nodes, id)
	c.net.SetDown(id, true)
}

// Restart rebuilds a crashed node from persisted storage. The network
// crash flag is cleared so messages flow again.
func (c *Cluster) Restart(id raft.NodeID) {
	if c.stores[id] == nil {
		return
	}
	c.net.SetDown(id, false)
	c.startNode(id, 10, 1, 0)
}

// AddNewNode registers storage for an identity not yet in the config.
// The node runs with no voters, so it neither campaigns nor votes until
// a ConfAddNode change commits and it receives log/snapshot traffic.
func (c *Cluster) AddNewNode(id raft.NodeID, addr string) {
	c.addrs[id] = addr
	c.ranks[id] = len(c.ranks)
	c.stores[id] = raft.NewMemStorage()
	c.applied[id] = 0
	c.net.SetDown(id, false)
	c.startNode(id, 10, 1, 0)
}

// RemoveNode permanently forgets a removed member: its volatile node is
// deleted; its storage is retained until Wipe for inspection in tests.
func (c *Cluster) RemoveNode(id raft.NodeID) {
	if n := c.nodes[id]; n != nil {
		n.Stop()
	}
	delete(c.nodes, id)
	c.net.SetDown(id, true)
}
