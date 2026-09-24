package sim

import (
	"sort"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

const (
	envRaft = iota
)

func raftEnvelope(from string, m raft.Message) envelope {
	return envelope{from: from, to: m.To, msg: m, at: time.Time{}}
}

// route enqueues an outbound raft message subject to topology.
func (c *Cluster) route(e envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.to != e.from && !c.lockedReachable(e.from, e.to) {
		return // partitioned or crashed target group: silently dropped
	}
	c.seq++
	e.seq = c.seq
	e.at = c.sched.Now().Add(c.latency)
	c.queue = append(c.queue, e)
	c.sortQueue()
}

func (c *Cluster) sortQueue() {
	sort.SliceStable(c.queue, func(i, j int) bool {
		if !c.queue[i].at.Equal(c.queue[j].at) {
			return c.queue[i].at.Before(c.queue[j].at)
		}
		return c.queue[i].seq < c.queue[j].seq
	})
}

// lockedReachable reports whether the two live nodes share a partition group.
func (c *Cluster) lockedReachable(from, to string) bool {
	if rt := c.nodes[to]; rt == nil || !rt.alive {
		return false
	}
	if rt := c.nodes[from]; rt != nil && !rt.alive {
		return false
	}
	if c.groups == nil {
		return true
	}
	gf, gt := -2, -2 // -2 = not listed anywhere (isolated)
	for gi, g := range c.groups {
		for _, id := range g {
			if id == from {
				gf = gi
			}
			if id == to {
				gt = gi
			}
		}
	}
	return gf == gt
}

func (c *Cluster) nextDeadline() (time.Time, bool) {
	var t time.Time
	set := false
	if len(c.queue) > 0 {
		t, set = c.queue[0].at, true
	}
	if nt, ok := c.sched.Peek(); ok {
		if !set || nt.Before(t) {
			t, set = nt, true
		}
	}
	return t, set
}

// step advances to the next event and processes it. Returns false when idle.
func (c *Cluster) step(until time.Time) bool {
	t, ok := c.nextDeadline()
	if !ok {
		return false
	}
	if t.After(until) {
		return false
	}
	// Fire due timers first (deterministic), then deliver due messages.
	fired := c.sched.AdvanceTo(t)
	for _, f := range fired {
		rt := c.nodes[f.Node]
		if rt == nil || !rt.alive || rt.node == nil {
			continue
		}
		func() {
			defer func() { _ = recover() }()
			_ = rt.node.Tick(f.Tag)
		}()
	}
	for len(c.queue) > 0 && !c.queue[0].at.After(c.sched.Now()) {
		e := c.queue[0]
		c.queue = c.queue[1:]
		rt := c.nodes[e.to]
		if rt == nil || !rt.alive {
			continue
		}
		if e.to != e.from && !c.lockedReachable(e.from, e.to) {
			continue // healed state is irrelevant; this one was already enqueued
		}
		_ = rt.node.Step(e.msg)
	}
	return true
}

// runFor pumps the simulation for up to maxDur of simulated time or until
// the queue and timers are empty.
func (c *Cluster) runFor(maxDur time.Duration) {
	deadline := c.sched.Now().Add(maxDur)
	for c.sched.Now().Before(deadline) {
		if !c.step(deadline) {
			// idle: jump clock to deadline to honor timeouts/waits
			c.sched.AdvanceTo(deadline)
			return
		}
	}
}

func (c *Cluster) now() time.Time { return c.sched.Now() }

// waitResult pumps until reqID gets its outcome or timeout of sim time.
func (c *Cluster) waitResult(ch <-chan raft.Outcome, maxWait time.Duration) (raft.Outcome, bool) {
	deadline := c.sched.Now().Add(maxWait)
	for {
		select {
		case o := <-ch:
			return o, true
		default:
		}
		if !c.sched.Now().Before(deadline) {
			break
		}
		if !c.step(deadline) {
			c.sched.AdvanceTo(deadline)
			break
		}
	}
	select {
	case o := <-ch:
		return o, true
	default:
		return raft.Outcome{OK: false, Code: raft.ErrTimeout,
			Reason: "模拟时间内未收到结论：不能据此判定已提交", Committed: false}, false
	}
}

// submit sends a client proposal to a specific node and waits for outcome.
func (c *Cluster) submit(to string, req raft.ClientRequest, maxWait time.Duration) (raft.Outcome, bool) {
	ch := c.registerResult(req.ReqID)
	c.driveMu.Lock()
	defer c.driveMu.Unlock()
	c.mu.Lock()
	rt := c.nodes[to]
	c.mu.Unlock()
	if rt == nil || !rt.alive {
		return raft.Outcome{OK: false, Code: raft.ErrStopped, Reason: "目标节点不可用"}, false
	}
	_ = rt.node.Propose(req)
	return c.waitResult(ch, maxWait)
}

// Pump exposes the event loop to tests for a fixed duration.
func (c *Cluster) Pump(d time.Duration) { c.runFor(d) }
