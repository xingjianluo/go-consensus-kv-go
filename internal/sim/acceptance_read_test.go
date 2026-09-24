package sim_test

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// Scenario: a read on the leader only succeeds after a current-term
// quorum heartbeat barrier (ReadIndex). A read while the leader is
// partitioned from a quorum must NOT return stale data — it must time
// out / fail until leadership is re-confirmed.
func TestLinearizableReadBarrier(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	k.propose(leader, kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "w", Key: "k", Value: []byte("v1"), CreateTime: 2})
	k.tick(3)

	// Issue a read; it opens a barrier and needs a quorum ack.
	rid := raft.ReqID("s/get/r1")
	k.c.Node(leader).ProposeRead(rid, []byte("k"))
	ok := false
	barrierAt := raft.Index(0)
	k.c.RunUntil(20, func(c *sim.Cluster) bool {
		for _, e := range c.Events() {
			if e.Node == leader && e.Type == raft.EvtReadResult && e.ReqID == rid {
				if e.Reason == raft.RejectNone {
					ok = true
					barrierAt = e.Index
				}
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatal("read did not receive a quorum barrier result")
	}
	if barrierAt < k.c.Node(leader).CommitIndex() {
		t.Fatalf("read barrier at %d below commit %d (stale read possible)",
			barrierAt, k.c.Node(leader).CommitIndex())
	}
	r := k.stores[leader].Read("s", "k", 2)
	if string(r.Value) != "v1" {
		t.Fatalf("read returned %q want v1 after barrier", string(r.Value))
	}
	t.Logf("read served after quorum barrier at index=%d", barrierAt)
}

// Scenario: a read initiated on a partitioned (minority) leader must not
// succeed — no quorum acks the barrier, so no EvtReadResult(ok) is
// emitted until the partition heals.
func TestReadFailsWithoutQuorum(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	k.tick(3)

	// Isolate leader alone.
	for _, id := range ids("n1", "n2", "n3") {
		if id != leader {
			k.c.Network().Partition(leader, id)
		}
	}
	rid := raft.ReqID("s/get/stale")
	k.c.Node(leader).ProposeRead(rid, []byte("k"))
	k.tick(30)

	served := false
	for _, e := range k.c.Events() {
		if e.Node == leader && e.Type == raft.EvtReadResult &&
			e.ReqID == rid && e.Reason == raft.RejectNone {
			served = true
		}
	}
	if served {
		t.Fatal("read returned success without a quorum — not linearizable")
	}
	t.Log("read withheld until leadership re-confirmed by quorum")
}
