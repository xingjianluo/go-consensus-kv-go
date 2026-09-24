package raft

import "testing"

func newTestNode(t *testing.T, id NodeID, peers []NodeID) (*Node, *MemStorage) {
	t.Helper()
	st := NewMemStorage()
	if err := Bootstrap(st, id, peers); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	n := NewNode(Options{ID: id, Storage: st, ElectionTimeout: 5, HeartbeatTick: 1})
	return n, st
}

func drain(n *Node) Ready {
	select {
	case r := <-n.rd:
		return r
	default:
		return Ready{}
	}
}

func TestSingleNodeElectionAndCommit(t *testing.T) {
	n, _ := newTestNode(t, "n1", []NodeID{"n1"})
	for i := 0; i < 6; i++ {
		n.Tick()
	}
	if n.Role() != RoleLeader {
		t.Fatalf("role=%s, want leader", n.Role())
	}
	r := drain(n)
	if len(r.Messages) != 0 {
		t.Fatalf("unexpected msgs: %d", len(r.Messages))
	}
	// no-op entry must commit via flush
	drain(n)
	n.Propose("r1", []byte("x"))
	drain(n)
	if n.CommitIndex() < 2 {
		t.Fatalf("commit=%d, want >=2", n.CommitIndex())
	}
}

func TestThreeNodeElection(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
	nodes := map[NodeID]*Node{}
	for _, id := range ids {
		n, _ := newTestNode(t, id, ids)
		nodes[id] = n
	}
	// Tick all until n1 campaigns at timeout 5.
	for i := 0; i < 6; i++ {
		for _, id := range ids {
			nodes[id].Tick()
			r := drain(nodes[id])
			for _, m := range r.Messages {
				if m.To != "" {
					nodes[m.To].Step(m)
				}
			}
		}
	}
	leaders := 0
	for _, n := range nodes {
		if n.Role() == RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders=%d, want 1", leaders)
	}
}
