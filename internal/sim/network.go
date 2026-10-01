package sim

import (
	"sort"
	"sync"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// queuedMsg is a message waiting to be delivered.
type queuedMsg struct {
	at  int64
	seq int64
	msg raft.Message
}

// DropRecord explains one refusal.
type DropRecord struct {
	Tick   int64
	From   raft.NodeID
	To     raft.NodeID
	Reason raft.RejectReason
	Kind   raft.MsgType
}

// Network is the simulated switch. Every Send is queued with a delivery
// tick; Tick() pops due messages and applies fault rules *at delivery
// time*, so a partition injected while a message is in flight is
// faithfully modeled. Every refusal is recorded with a distinguishable
// raft.RejectReason.
//
// All operations are mutex-guarded: like a real transport, Send may be
// invoked from many per-node goroutines while one pump drives Tick.
type Network struct {
	mu       sync.Mutex
	now      int64
	seq      int64
	queue    []queuedMsg
	down     map[raft.NodeID]bool
	blocked  map[[2]raft.NodeID]bool
	latency  int // fixed one-way latency in ticks (default 0)
	dropNext int // optional: drop the next N deliveries
	Drops    []DropRecord
}

// NewNetwork returns an empty network.
func NewNetwork() *Network {
	return &Network{
		down:    map[raft.NodeID]bool{},
		blocked: map[[2]raft.NodeID]bool{},
	}
}

// SetLatency sets the fixed one-way latency in simulated ticks.
func (n *Network) SetLatency(ticks int) {
	if ticks < 0 {
		ticks = 0
	}
	n.mu.Lock()
	n.latency = ticks
	n.mu.Unlock()
}

// SetDown marks a node crashed (queued messages to/from it are dropped
// until it comes back via Restart).
func (n *Network) SetDown(id raft.NodeID, down bool) {
	n.mu.Lock()
	n.down[id] = down
	n.mu.Unlock()
}

// IsDown reports crash state.
func (n *Network) IsDown(id raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.down[id]
}

// Partition blocks every message between a and b in both directions.
func (n *Network) Partition(a, b raft.NodeID) {
	n.mu.Lock()
	n.blocked[[2]raft.NodeID{a, b}] = true
	n.blocked[[2]raft.NodeID{b, a}] = true
	n.mu.Unlock()
}

// Isolate blocks every message between node and the rest of the cluster.
func (n *Network) Isolate(node raft.NodeID, peers []raft.NodeID) {
	n.mu.Lock()
	for _, p := range peers {
		if p == node {
			continue
		}
		n.blocked[[2]raft.NodeID{node, p}] = true
		n.blocked[[2]raft.NodeID{p, node}] = true
	}
	n.mu.Unlock()
}

// Heal removes all partition rules.
func (n *Network) Heal() {
	n.mu.Lock()
	n.blocked = map[[2]raft.NodeID]bool{}
	n.mu.Unlock()
}

// IsPartitioned reports whether a->b is currently blocked.
func (n *Network) IsPartitioned(a, b raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.blocked[[2]raft.NodeID{a, b}]
}

// Send enqueues a message. Delivery happens after latency ticks
// (overridable with delayTicks >= 0).
func (n *Network) Send(m raft.Message, delayTicks int) {
	n.mu.Lock()
	d := n.latency
	if delayTicks >= 0 {
		d = delayTicks
	}
	n.seq++
	n.queue = append(n.queue, queuedMsg{at: n.now + int64(d), seq: n.seq, msg: m})
	n.mu.Unlock()
}

// Pending reports queued (not-yet-due-or-fault-checked) messages.
func (n *Network) Pending() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.queue)
}

// Now returns the simulated tick.
func (n *Network) Now() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

// Tick advances network time by one and returns messages now due that
// pass the fault checks. Refusals are appended to n.Drops.
func (n *Network) Tick() []raft.Message {
	n.mu.Lock()
	n.now++
	out := n.dueAtLocked(n.now)
	n.mu.Unlock()
	return out
}

func (n *Network) dueAt(now int64) []raft.Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dueAtLocked(now)
}

// dueAtLocked returns every message due by now, stable-ordered by
// scheduled tick then enqueue sequence so scheduling is deterministic
// across repeated runs. The caller must hold n.mu.
func (n *Network) dueAtLocked(now int64) []raft.Message {
	sort.SliceStable(n.queue, func(i, j int) bool {
		if n.queue[i].at != n.queue[j].at {
			return n.queue[i].at < n.queue[j].at
		}
		return n.queue[i].seq < n.queue[j].seq
	})
	var out []raft.Message
	rest := n.queue[:0]
	for _, q := range n.queue {
		if q.at > now {
			rest = append(rest, q)
			continue
		}
		m := q.msg
		if reason := n.faultCheckLocked(m); reason != raft.RejectNone {
			n.Drops = append(n.Drops, DropRecord{
				Tick: now, From: m.From, To: m.To, Reason: reason, Kind: m.Type,
			})
			continue
		}
		out = append(out, m)
	}
	n.queue = rest
	return out
}

func (n *Network) faultCheckLocked(m raft.Message) raft.RejectReason {
	if n.down[m.To] {
		return raft.RejectDroppedQueue
	}
	if n.blocked[[2]raft.NodeID{m.From, m.To}] {
		return raft.RejectDroppedPartition
	}
	return raft.RejectNone
}

// Drain delivers every currently queued message ignoring schedule but
// still applying fault checks.
func (n *Network) Drain() []raft.Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dueAtLocked(n.now + 1<<40)
}
