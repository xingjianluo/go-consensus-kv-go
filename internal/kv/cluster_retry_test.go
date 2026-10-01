package kv_test

// This file holds the multi-node, real-Service harness used by the
// retry-convergence tests: three raft nodes driven by a wall-clock ticker,
// linked by an in-memory fault-injecting network, each with its own
// kv.Service + kv.Store.

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// memTransport is a tiny in-process transport shared by all test nodes.
// Sent messages are queued with a one-tick delay and delivered by tick;
// partitions are evaluated at delivery time, like the sim network.
type memTransport struct {
	mu      sync.Mutex
	down    map[raft.NodeID]bool
	blocked map[[2]raft.NodeID]bool
	queues  map[raft.NodeID]chan raft.Message
}

func newMemTransport(ids []raft.NodeID) *memTransport {
	tr := &memTransport{
		down:    map[raft.NodeID]bool{},
		blocked: map[[2]raft.NodeID]bool{},
		queues:  map[raft.NodeID]chan raft.Message{},
	}
	for _, id := range ids {
		tr.queues[id] = make(chan raft.Message, 1024)
	}
	return tr
}

func (t *memTransport) Send(m raft.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.down[m.To] || t.blocked[[2]raft.NodeID{m.From, m.To}] {
		return
	}
	select {
	case t.queues[m.To] <- m:
	default: // slow consumer: raft retries on the next heartbeat
	}
}

func (t *memTransport) Recv(id raft.NodeID) <-chan raft.Message { return t.queues[id] }

func (t *memTransport) Isolate(node raft.NodeID, peers []raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range peers {
		if p == node {
			continue
		}
		t.blocked[[2]raft.NodeID{node, p}] = true
		t.blocked[[2]raft.NodeID{p, node}] = true
	}
}

func (t *memTransport) Heal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked = map[[2]raft.NodeID]bool{}
}

// svcNode is one full stack under test.
type svcNode struct {
	id    raft.NodeID
	node  *raft.Node
	store *kv.Store
	svc   *kv.Service
}

// svcCluster is a running 3-node Service cluster with controllable
// networking and a fast ticker.
type svcCluster struct {
	t     *testing.T
	tr    *memTransport
	nodes map[raft.NodeID]*svcNode
	stop  chan struct{}
	tick  time.Duration
	wg    sync.WaitGroup
}

func newSvcCluster(t *testing.T, writeTimeout time.Duration) *svcCluster {
	t.Helper()
	ids := []raft.NodeID{"n1", "n2", "n3"}
	tr := newMemTransport(ids)
	c := &svcCluster{t: t, tr: tr, nodes: map[raft.NodeID]*svcNode{},
		stop: make(chan struct{}), tick: 10 * time.Millisecond}
	for _, id := range ids {
		st := raft.NewMemStorage()
		if err := raft.Bootstrap(st, id, ids); err != nil {
			t.Fatal(err)
		}
		store := kv.NewStore()
		node := raft.NewNode(raft.Options{
			ID: id, Storage: st, ElectionTimeout: 10, HeartbeatTick: 2,
		})
		svc := kv.NewService(node, store)
		svc.SetWriteTimeout(writeTimeout)
		go svc.Run()
		c.nodes[id] = &svcNode{id: id, node: node, store: store, svc: svc}
	}
	// Cross-wire messages and drive every node. Each node's ticker is
	// phase-offset so the three driver goroutines do not wake in lockstep
	// and saturate one core together under tight -count=N loops.
	phase := 0
	for _, n := range c.nodes {
		n := n
		n.svc.MessageHook = func(m raft.Message) { tr.Send(m) }
		c.wg.Add(1)
		go func(offset time.Duration) {
			defer c.wg.Done()
			select {
			case <-c.stop:
				return
			case <-time.After(offset):
			}
			tk := time.NewTicker(c.tick)
			defer tk.Stop()
			for {
				select {
				case <-c.stop:
					return
				case <-tk.C:
					n.node.Tick()
				case m := <-tr.Recv(n.id):
					n.node.Step(m)
				}
			}
		}(time.Duration(phase) * c.tick / 4)
		phase++
	}
	t.Cleanup(c.shutdown)
	return c
}

// shutdown stops every raft node FIRST (so no more Ready/messages are
// produced), closes the service pumps, drains leftover transport queues
// (unblocking drivers parked on Recv), and only then waits for the driver
// goroutines. This guarantees a previous cluster cannot keep ticking and
// starve the next test's cluster under tight -count=N loops.
func (c *svcCluster) shutdown() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	for _, n := range c.nodes {
		n.node.Stop()
		n.svc.Close()
	}
	for id := range c.nodes {
	drainLoop:
		for {
			select {
			case <-c.tr.Recv(id):
			default:
				break drainLoop
			}
		}
	}
	c.wg.Wait()
}

// waitLeader polls until some node reports itself leader. Wall-clock
// scheduling under heavy -count=N load can delay tick delivery, so the
// wait budget is generous.
func (c *svcCluster) waitLeader(maxWait time.Duration) raft.NodeID {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.node.Role() == raft.RoleLeader && n.node.Leader() == id {
				return id
			}
		}
		time.Sleep(c.tick)
	}
	c.t.Fatal("no leader in time")
	return ""
}

// leaderOf returns the service currently believing it is leader.
func (c *svcCluster) anyLeader() raft.NodeID {
	for id, n := range c.nodes {
		if n.node.Role() == raft.RoleLeader {
			return id
		}
	}
	return ""
}
