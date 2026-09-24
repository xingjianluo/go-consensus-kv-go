package sim_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// autoSnapCluster builds a raft snapshot from the local kv.Store as
// soon as raft requests one (EvtSnapshotRequired), then keeps ticking.
func autoSnap(t *testing.T, k *kvCluster, rounds int) {
	for r := 0; r < rounds; r++ {
		k.c.Tick()
		for _, e := range k.c.Events() {
			if e.Type != raft.EvtSnapshotRequired {
				continue
			}
			node := k.c.Node(e.Node)
			if node == nil {
				continue
			}
			// Skip requests already fulfilled (events log is cumulative).
			if fi := k.c.Store(e.Node).FirstIndex(); e.Index < fi {
				continue
			}
			term, err := k.c.Store(e.Node).Term(e.Index)
			if err != nil {
				t.Fatalf("term at snapshot idx %d on %s: %v", e.Index, e.Node, err)
			}
			data, err := k.stores[e.Node].Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err := node.SnapshotBuilt(e.Index, term, data); err != nil {
				t.Fatalf("SnapshotBuilt: %v", err)
			}
		}
	}
}

// Scenario: a long-down follower is brought back only via
// InstallSnapshot because the leader has compacted the entries it is
// missing; after restoring, trailing entries still converge.
func TestSnapshotInstallAndConverge(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 6)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	victim := raft.NodeID("n1")
	if victim == leader {
		victim = "n2"
	}
	k.c.Crash(victim)

	for i := 0; i < 25; i++ {
		cmd := kv.Command{Type: kv.CmdPut, SessionID: "s",
			RequestID: fmt.Sprintf("w%02d", i), Key: fmt.Sprintf("k%02d", i),
			Value: []byte(fmt.Sprintf("v%02d", i)), CreateTime: int64(10 + i)}
		if _, ok := k.propose(leader, cmd); !ok {
			t.Fatalf("write %d not committed", i)
		}
		autoSnap(t, k, 2)
	}
	autoSnap(t, k, 10)

	leaderFirst := k.c.Store(leader).FirstIndex()
	if leaderFirst <= 1 {
		t.Fatalf("leader log was not compacted, first=%d", leaderFirst)
	}
	t.Logf("leader compacted: first=%d last=%d commit=%d",
		leaderFirst, k.c.Store(leader).LastIndex(), k.c.Node(leader).CommitIndex())

	// Restart victim with stale storage; it must receive a snapshot.
	k.c.Restart(victim)
	ok := false
	for r := 0; r < 200; r++ {
		autoSnap(t, k, 1)
		if k.c.Node(victim) != nil &&
			k.c.Node(victim).CommitIndex() >= k.c.Node(leader).CommitIndex() {
			ok = true
			break
		}
	}
	if !ok {
		dumpKeyEvents(t, k.c)
		t.Fatalf("victim did not converge via snapshot: commit=%d leader=%d",
			k.c.Node(victim).CommitIndex(), k.c.Node(leader).CommitIndex())
	}
	// After convergence the restored store must serve all values.
	for i := 0; i < 25; i++ {
		r := k.stores[victim].Read("s", fmt.Sprintf("k%02d", i), 100)
		if r.Verdict != kv.VerdictOK || string(r.Value) != fmt.Sprintf("v%02d", i) {
			t.Fatalf("victim missing k%02d after snapshot: verdict=%s", i, r.Verdict)
		}
	}
	k.assertConverged(ids("n1", "n2", "n3"))
	t.Logf("victim restored via snapshot then caught up to index=%d",
		k.c.Node(victim).CommitIndex())
}
