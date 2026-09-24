package sim

import (
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kvsm"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// Partition splits the network into disconnected groups. Nodes absent
// from all groups are isolated.
func (c *Cluster) Partition(groups ...[]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groups = groups
	// Drop every queued message that crosses the partition. Old messages
	// arriving after a heal that were sent during the partition stay
	// dropped (they are re-sent by heartbeats anyway).
	kept := c.queue[:0]
	for _, e := range c.queue {
		if c.lockedReachable(e.from, e.to) {
			kept = append(kept, e)
		}
	}
	c.queue = kept
	c.sortQueue()
}

// Heal removes all partitions.
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groups = nil
}

// Crash stops a node, discarding only volatile state: the durable store
// and its snapshot/log survive, as does the KV snapshot payload.
func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rt := c.nodes[id]
	if rt == nil || !rt.alive {
		return
	}
	rt.node.Stop()
	rt.alive = false
	rt.timerE, rt.timerH = 0, 0
	// Drop messages addressed to or from this node.
	kept := c.queue[:0]
	for _, e := range c.queue {
		if e.to != id && e.from != id {
			kept = append(kept, e)
		}
	}
	c.queue = kept
}

// Restart restores a crashed node from durable storage.
func (c *Cluster) Restart(id string) {
	c.mu.Lock()
	old := c.nodes[id]
	c.mu.Unlock()
	if old == nil || old.alive {
		return
	}
	c.rebuild(id, old.store, old.kv, true)
}

// StartUnconfiguredMember boots a brand-new empty node that is not yet a
// voter (used before an AddMember membership change).
func (c *Cluster) StartUnconfiguredMember(id string) {
	st := raft.NewMemStorage()
	kv := kvsm.New()
	c.rebuild(id, st, kv, false)
	c.mu.Lock()
	found := false
	for _, x := range c.order {
		if x == id {
			found = true
		}
	}
	if !found {
		c.order = append(c.order, id)
	}
	c.mu.Unlock()
}

func (c *Cluster) rebuild(id string, st *raft.MemStorage, kv *kvsm.KV, bootstrapped bool) {
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
	if bootstrapped {
		rt.node.StartTimers()
	} else {
		// Pre-voter node: no conf, does not time out into elections.
		rt.node.MarkUnconfigured()
	}
	c.mu.Lock()
	c.nodes[id] = rt
	c.mu.Unlock()
}

// AddMember proposes adding id through the current leader.
func (c *Cluster) AddMember(reqID, id string) raft.Outcome {
	return c.confChange(reqID, id, true)
}

// RemoveMember proposes removing id through the current leader.
func (c *Cluster) RemoveMember(reqID, id string) raft.Outcome {
	return c.confChange(reqID, id, false)
}

func (c *Cluster) confChange(reqID, id string, add bool) raft.Outcome {
	c.driveMu.Lock()
	defer c.driveMu.Unlock()
	for attempt := 0; attempt < 5; attempt++ {
		leader := c.Leader()
		if leader == "" {
			c.runFor(c.opts.ElectionMax + c.opts.Heartbeat)
			continue
		}
		ch := c.registerResult(reqID)
		rt := c.nodes[leader]
		var err error
		if add {
			err = rt.node.AddMember(reqID, id)
		} else {
			err = rt.node.RemoveMember(reqID, id)
		}
		if err != nil {
			continue
		}
		out, ok := c.waitResult(ch, 3*time.Second)
		if ok && (out.OK || out.Code == raft.ErrUnknownNode) {
			return out
		}
	}
	return raft.Outcome{OK: false, Code: raft.ErrTimeout, Reason: "成员变更未能在限定时间内确认"}
}

func (c *Cluster) mustLeader() string {
	l := c.Leader()
	if l == "" {
		c.runFor(c.opts.ElectionMax + c.opts.Heartbeat)
		l = c.Leader()
	}
	if l == "" {
		panic("sim: no leader available")
	}
	return l
}
