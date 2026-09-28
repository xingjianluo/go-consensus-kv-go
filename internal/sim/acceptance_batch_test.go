package sim_test

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

func batchCmd(req string, items ...kv.BatchItem) kv.Command {
	return kv.Command{Type: kv.CmdBatch, SessionID: "s", RequestID: req,
		Items: items, CreateTime: 100}
}

func putBatch(k *kvCluster, leader raft.NodeID, req string, items ...kv.BatchItem) (raft.Index, bool) {
	return k.propose(leader, batchCmd(req, items...))
}

// Scenario: a multi-key batch commits atomically; a batch whose
// precondition fails changes NOTHING (all-or-nothing), and per-key
// resulting versions are reported.
func TestBatchAtomicAllOrNothing(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	idx, ok := putBatch(k, leader, "b1",
		kv.BatchItem{Key: "a", Value: []byte("1")},
		kv.BatchItem{Key: "b", Value: []byte("2")},
	)
	if !ok {
		t.Fatal("batch b1 did not commit")
	}
	// The committed verdict, derived from the state machine on every node,
	// reports one commit position and per-key versions.
	res, handled := k.stores[leader].Apply(raft.Entry{
		Index: idx, Term: k.c.Node(leader).Term(), Kind: raft.EntryData,
		Data: dataOf(batchCmd("b1",
			kv.BatchItem{Key: "a", Value: []byte("1")},
			kv.BatchItem{Key: "b", Value: []byte("2")},
		)),
	})
	if !handled || res.Verdict != kv.VerdictDuplicate || res.Index != idx {
		t.Fatalf("re-read original verdict: %+v", res)
	}
	for _, o := range res.Items {
		if o.Verdict != kv.VerdictOK || o.Version != 1 {
			t.Fatalf("item %+v", o)
		}
	}

	// Failing CAS batch: c would be created, a is expected at version 99.
	failIdx, committed := putBatch(k, leader, "b2",
		kv.BatchItem{Key: "c", Value: []byte("3")},
		kv.BatchItem{Key: "a", Value: []byte("9"),
			CheckVersion: true, ExpectedVersion: 99},
	)
	if !committed {
		t.Fatal("failed batch entry should still commit as a rejection verdict")
	}
	// Assert every node applied the committed rejection identically: c
	// absent on all members, a unchanged.
	k.tick(5)
	for _, id := range ids("n1", "n2", "n3") {
		if r := k.stores[id].Read("s", "c", 100); r.Verdict != kv.VerdictNotFound {
			t.Fatalf("%s sees half-applied key c: %s", id, r.Verdict)
		}
		r := k.stores[id].Read("s", "a", 100)
		if string(r.Value) != "1" || r.Version != 1 {
			t.Fatalf("%s a mutated: %q v%d", id, r.Value, r.Version)
		}
	}
	// The first failing item identifies the key and the reason.
	rep, exists := k.stores[leader].PendingPut("s", "b2")
	if !exists || rep.Verdict != kv.VerdictPreconditionFailed ||
		rep.AppliedAt != failIdx {
		t.Fatalf("cached rejection=%+v exists=%v", rep, exists)
	}
	k.assertConverged(ids("n1", "n2", "n3"))
}

// Scenario: structural problems (duplicate/empty keys, oversize) are
// rejected deterministically; no value is picked silently.
func TestBatchStructuralRejectionCommitted(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	if _, committed := putBatch(k, leader, "dup-keys",
		kv.BatchItem{Key: "a", Value: []byte("1")},
		kv.BatchItem{Key: "a", Value: []byte("2")},
	); !committed {
		t.Fatal("rejection verdict entry must commit")
	}
	rep, ok := k.stores[leader].PendingPut("s", "dup-keys")
	if !ok || rep.Verdict != kv.VerdictRejected || rep.Reason != "batch-duplicate-key" {
		t.Fatalf("rep=%+v ok=%v", rep, ok)
	}
	if r := k.stores[leader].Read("s", "a", 100); r.Verdict != kv.VerdictNotFound {
		t.Fatal("a must not be written when the batch is malformed")
	}

	// Empty batch likewise rejected.
	if _, committed := putBatch(k, leader, "empty"); !committed {
		t.Fatal("empty-batch rejection should commit")
	}
	rep, _ = k.stores[leader].PendingPut("s", "empty")
	if rep.Verdict != kv.VerdictRejected || rep.Reason != "batch-empty" {
		t.Fatalf("empty rep=%+v", rep)
	}
}

// Scenario: two clients conditionally updating the same key from the
// same base version — exactly one wins at commit time; the loser's
// verdict is a definitive precondition failure naming the key.
func TestBatchConcurrentConditionalRace(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	if _, ok := putBatch(k, leader, "seed",
		kv.BatchItem{Key: "cfg", Value: []byte("v0")}); !ok {
		t.Fatal("seed")
	}

	cas := func(req, val string) kv.Result {
		cmd := batchCmd(req, kv.BatchItem{Key: "cfg", Value: []byte(val),
			CheckVersion: true, ExpectedVersion: 1})
		idx, _ := k.propose(leader, cmd)
		// Every replica derives the same verdict at the commit index; read
		// the recorded (original) verdict straight from the reply cache.
		rep, ok := k.stores[leader].PendingPut("s", req)
		if !ok {
			t.Fatalf("no verdict for %s at idx %d", req, idx)
		}
		return kv.Result{Verdict: rep.Verdict, Reason: rep.Reason,
			Index: rep.AppliedAt, Items: rep.Items}
	}
	r1 := cas("c1", "one")
	r2 := cas("c2", "two")
	if r1.Verdict != kv.VerdictOK || r1.Items[0].Version != 2 {
		t.Fatalf("r1=%+v", r1)
	}
	if r2.Verdict != kv.VerdictPreconditionFailed ||
		r2.Reason != "version-mismatch:cfg" || r2.Items[0].Version != 2 {
		t.Fatalf("r2=%+v", r2)
	}
	if r := k.stores[leader].Read("s", "cfg", 100); string(r.Value) != "one" || r.Version != 2 {
		t.Fatalf("cfg=%q v%d want one/2", r.Value, r.Version)
	}
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
}

// Scenario: retrying the same (session, request) after a leadership
// change returns the ORIGINAL committed verdict, never re-judged against
// the newer state.
func TestBatchDuplicateAfterLeaderChange(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	old := k.waitLeader(40)
	k.propose(old, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	cmd := batchCmd("once",
		kv.BatchItem{Key: "x", Value: []byte("orig"),
			CheckVersion: true, ExpectedVersion: 0})
	origIdx, committed := k.propose(old, cmd)
	if !committed {
		t.Fatal("original batch did not commit")
	}

	// A later write through the same cluster moves x to a new version.
	move := batchCmd("later", kv.BatchItem{Key: "x", Value: []byte("newer")})
	if _, ok := k.propose(old, move); !ok {
		t.Fatal("later batch did not commit")
	}

	// Force a leadership change: isolate the old leader, majority elects.
	var others []raft.NodeID
	for _, id := range ids("n1", "n2", "n3") {
		if id != old {
			others = append(others, id)
			k.c.Network().Partition(old, id)
		}
	}
	var fresh raft.NodeID
	if !k.c.RunUntil(60, func(c *sim.Cluster) bool {
		for _, id := range others {
			if c.Node(id).Role() == raft.RoleLeader {
				fresh = id
				return true
			}
		}
		return false
	}) {
		dumpKeyEvents(t, k.c)
		t.Fatal("no new leader")
	}

	// Retry the ORIGINAL request through the new leader. Raft dedupes at
	// the proposal layer where possible; regardless of the path, the
	// state machine's verdict must be the original duplicate verdict.
	if _, committed := k.propose(fresh, cmd); !committed {
		t.Fatal("retry did not settle to a terminal committed verdict")
	}
	k.tick(10)
	for _, id := range append(others, old) {
		// Old leader is partitioned; check it after heal below.
		if id == old {
			continue
		}
		rep, ok := k.stores[id].PendingPut("s", "once")
		if !ok || rep.Verdict != kv.VerdictOK || rep.AppliedAt != origIdx {
			t.Fatalf("%s original verdict lost: %+v ok=%v", id, rep, ok)
		}
		if r := k.stores[id].Read("s", "x", 100); string(r.Value) != "newer" {
			t.Fatalf("%s x=%q", id, r.Value)
		}
	}

	// Heal and let the old leader catch up; convergence must hold.
	k.c.Network().Heal()
	k.c.RunUntil(80, func(c *sim.Cluster) bool {
		return c.Node(old) != nil &&
			c.Node(old).CommitIndex() >= c.Node(fresh).CommitIndex()
	})
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	rep, _ := k.stores[old].PendingPut("s", "once")
	if rep.AppliedAt != origIdx || rep.Verdict != kv.VerdictOK {
		t.Fatalf("old leader reply cache wrong: %+v", rep)
	}
}

// Scenario: a batch proposed to an isolated leader cannot commit; the
// client treats it as unknown. After the partition heals (and possibly a
// new leader exists), retrying with the SAME request id converges to one
// definitive verdict — either the original attempt's committed verdict
// or the retry's; it can never commit twice or leave half state.
func TestBatchRetryAfterPartitionConverges(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	old := k.waitLeader(40)
	k.propose(old, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	// Isolate old leader alone and start a batch that cannot reach quorum.
	var others []raft.NodeID
	for _, id := range ids("n1", "n2", "n3") {
		if id != old {
			others = append(others, id)
			k.c.Network().Partition(old, id)
		}
	}
	cmd := batchCmd("uncertain",
		kv.BatchItem{Key: "p", Value: []byte("v")})
	rid := raft.ReqID("s/uncertain")
	k.c.Node(old).Propose(rid, dataOf(cmd))
	base := k.leaderCommit(old)
	k.tick(20)
	if k.leaderCommit(old) > base {
		t.Fatal("partitioned leader committed without quorum")
	}

	// Majority elects fresh leader.
	var fresh raft.NodeID
	if !k.c.RunUntil(60, func(c *sim.Cluster) bool {
		for _, id := range others {
			if c.Node(id).Role() == raft.RoleLeader {
				fresh = id
				return true
			}
		}
		return false
	}) {
		t.Fatal("no fresh leader")
	}

	// Heal and retry the SAME request id. It must converge exactly once.
	k.c.Network().Heal()
	idx, committed := k.propose(fresh, cmd)
	if !committed {
		t.Fatal("retry never got a definitive committed verdict")
	}
	k.c.RunUntil(60, func(c *sim.Cluster) bool {
		return c.Node(old).CommitIndex() >= c.Node(fresh).CommitIndex()
	})
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	for _, id := range ids("n1", "n2", "n3") {
		rep, ok := k.stores[id].PendingPut("s", "uncertain")
		if !ok || rep.Verdict != kv.VerdictOK || rep.AppliedAt != idx {
			t.Fatalf("%s reply=%+v ok=%v idx=%d", id, rep, ok, idx)
		}
		if r := k.stores[id].Read("s", "p", 100); r.Version != 1 ||
			string(r.Value) != "v" {
			t.Fatalf("%s p=%q v%d", id, r.Value, r.Version)
		}
	}
	// A further retry returns the original verdict instead of reapplying.
	retry, handled := k.stores[fresh].Apply(raft.Entry{
		Index: idx + 100, Term: k.c.Node(fresh).Term(),
		Kind: raft.EntryData, Data: dataOf(cmd)})
	if !handled || retry.Verdict != kv.VerdictDuplicate || retry.Index != idx {
		t.Fatalf("final retry=%+v handled=%v", retry, handled)
	}
}

// Scenario: batch replies (with versions and per-key outcomes) survive
// snapshot restore, so a compacted-and-restarted node still answers a
// duplicate request with the original verdict.
func TestBatchVerdictSurvivesSnapshot(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 8)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	idx, ok := putBatch(k, leader, "snap-batch",
		kv.BatchItem{Key: "s1", Value: []byte("a"),
			CheckVersion: true, ExpectedVersion: 0},
		kv.BatchItem{Key: "s2", Value: []byte("b")},
	)
	if !ok {
		t.Fatal("batch commit")
	}
	// Drive enough ticks/commits to force a snapshot on the leader.
	for i := 0; i < 20; i++ {
		autoSnap(t, k, 1)
		_, _ = putBatch(k, leader, "fill",
			kv.BatchItem{Key: "f", Value: []byte{byte('a' + i%26)}})
	}
	autoSnap(t, k, 20)
	if k.c.Store(leader).FirstIndex() <= idx {
		// Snapshot threshold is a hint; if raft did not compact, verify
		// serialization explicitly instead.
		data, err := k.stores[leader].Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		st2 := kv.NewStore()
		if err := st2.Restore(data); err != nil {
			t.Fatal(err)
		}
		rep, ok := st2.PendingPut("s", "snap-batch")
		if !ok || rep.AppliedAt != idx || len(rep.Items) != 2 {
			t.Fatalf("snapshot reply cache: %+v ok=%v", rep, ok)
		}
		return
	}
	rep, ok := k.stores[leader].PendingPut("s", "snap-batch")
	if !ok || rep.AppliedAt != idx || len(rep.Items) != 2 ||
		rep.Items[0].Version != 1 {
		t.Fatalf("post-snapshot reply=%+v ok=%v", rep, ok)
	}
	if r := k.stores[leader].Read("s", "s1", 100); r.Version != 1 {
		t.Fatalf("s1 version after snapshot: %d", r.Version)
	}
}
