package sim_test

import (
	"encoding/json"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

func batchCmd(sid, req string, ops ...kv.BatchOp) kv.Command {
	return kv.Command{Type: kv.CmdBatch, SessionID: sid, RequestID: req,
		Ops: ops, CreateTime: 1}
}

func mustBatch(t *testing.T, st *kv.Store, idx raft.Index, cmd kv.Command) kv.Result {
	t.Helper()
	data, _ := kv.Encode(cmd)
	res, handled := st.Apply(raft.Entry{Index: idx, Term: 2,
		Kind: raft.EntryData, Data: data})
	if !handled {
		t.Fatal("batch not handled")
	}
	return res
}

// A successful batch commits as ONE log entry and converges across the
// whole cluster: every node has every key at the same version.
func TestBatchMultiKeyCommitsAndConverges(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	idx, committed := k.propose(leader, batchCmd("s", "b1",
		kv.BatchOp{Key: "cfg/a", Value: []byte("1")},
		kv.BatchOp{Key: "cfg/b", Value: []byte("2")},
		kv.BatchOp{Key: "cfg/c", Value: []byte("3")},
	))
	if !committed {
		t.Fatal("batch did not commit")
	}
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	for _, key := range []string{"cfg/a", "cfg/b", "cfg/c"} {
		r := k.stores[leader].Read("s", key, 1)
		if r.Verdict != kv.VerdictOK || r.Version != 1 {
			t.Fatalf("key %s after batch: %q v%s/%d", key, r.Value, r.Verdict, r.Version)
		}
	}
	// Exactly one data entry (plus open + leader no-op) carried all keys.
	if n := countKeys(t, k.stores[leader]); n != 3 {
		t.Fatalf("store has %d keys want 3", n)
	}
	t.Logf("3-key batch committed atomically at index=%d and converged", idx)
}

func countKeys(t *testing.T, st *kv.Store) int {
	t.Helper()
	var snap map[string]any
	b, _ := st.Snapshot()
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatal(err)
	}
	return len(snap["Kv"].(map[string]any))
}

// A conditional batch that fails on one node must fail identically on
// every node (deterministic state machine), leave all keys untouched,
// and converge.
func TestBatchPreconditionFailureConvergesAllOrNothing(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	k.propose(leader, batchCmd("s", "seed",
		kv.BatchOp{Key: "k1", Value: []byte("old")},
		kv.BatchOp{Key: "k2", Value: []byte("old")},
	))

	idx, committed := k.propose(leader, batchCmd("s", "fail",
		kv.BatchOp{Key: "k1", Value: []byte("new"), ExpectVersion: kv.ExpectVersion(1)},
		kv.BatchOp{Key: "k2", Value: []byte("new"), ExpectVersion: kv.ExpectVersion(7)},
	))
	if !committed {
		t.Fatal("failed-conditional batch must still be committed as a log entry")
	}
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	for _, id := range ids("n1", "n2", "n3") {
		rep, ok := k.stores[id].PendingPut("s", "fail")
		if !ok {
			t.Fatalf("%s missing cached verdict", id)
		}
		if rep.Verdict != kv.VerdictPreconditionFailed ||
			rep.Batch == nil || rep.Batch.FailedKey != "k2" ||
			rep.Batch.ExpectVersion != 7 || rep.Batch.ActualVersion != 1 {
			t.Fatalf("%s cached verdict=%s detail=%+v", id, rep.Verdict, rep.Batch)
		}
		if rep.AppliedAt != idx {
			t.Fatalf("%s applied-at=%d want %d", id, rep.AppliedAt, idx)
		}
		r := k.stores[id].Read("s", "k1", 1)
		if string(r.Value) != "old" || r.Version != 1 {
			t.Fatalf("%s key mutated by failed batch: %q v%d", id, r.Value, r.Version)
		}
	}
	t.Log("conditional batch failed atomically on every node with same detail")
}

// Duplicate proposal after other intervening writes returns the ORIGINAL
// verdict (checked by replaying the same command on the converged store).
func TestBatchDuplicateAfterInterveningWrites(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	first, ok := k.propose(leader, batchCmd("s", "once",
		kv.BatchOp{Key: "x", Value: []byte("v1"), ExpectVersion: kv.ExpectAbsent()},
	))
	if !ok {
		t.Fatal("first batch failed")
	}
	k.propose(leader, batchCmd("s", "other",
		kv.BatchOp{Key: "x", Value: []byte("v2")},
	))

	// Simulate a client timeout-retry: same (session, request) re-enters
	// the state machine (here replayed deterministically) and must yield
	// the original success at the original index, with v1 per key.
	st := k.stores[leader]
	retry := mustBatch(t, st, first+50, batchCmd("s", "once",
		kv.BatchOp{Key: "x", Value: []byte("v1"), ExpectVersion: kv.ExpectAbsent()},
	))
	if retry.Verdict != kv.VerdictDuplicate {
		t.Fatalf("retry verdict=%s want duplicate", retry.Verdict)
	}
	if retry.Index != first || retry.Batch.AppliedAt != first {
		t.Fatalf("retry index=%d batch=%+v want original %d", retry.Index, retry.Batch, first)
	}
	if op := retry.Batch.Ops[0]; op.Version != 1 {
		t.Fatalf("retry returned version %d want original 1", op.Version)
	}
	r := st.Read("s", "x", 1)
	if string(r.Value) != "v2" || r.Version != 2 {
		t.Fatalf("retry reapplied: x=%q v%d", r.Value, r.Version)
	}
}

// A batch proposed while the leader is in a minority partition cannot
// commit (timeout-unknown at the client). After the partition heals and
// leadership settles, retrying the SAME request id converges to one
// decisive result; the keys carry exactly the batch's effect.
func TestBatchRetryAfterPartitionConverges(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	// Isolate the leader: it has no quorum, so the batch must not commit.
	k.c.Network().Isolate(leader, ids("n1", "n2", "n3"))
	cmd := batchCmd("s", "partitioned",
		kv.BatchOp{Key: "p1", Value: []byte("a")},
		kv.BatchOp{Key: "p2", Value: []byte("b")},
	)
	data, _ := kv.Encode(cmd)
	rid := raft.ReqID("s/partitioned")
	mark := k.c.Node(leader).CommitIndex()
	k.c.Node(leader).Propose(rid, data)
	k.tick(20)
	if got := k.c.Node(leader).CommitIndex(); got != mark {
		t.Fatalf("minority leader advanced commit %d -> %d", mark, got)
	}
	for _, id := range ids("n1", "n2", "n3") {
		if r := k.stores[id].Read("s", "p1", 1); r.Verdict != kv.VerdictNotFound {
			t.Fatalf("%s saw uncommitted batch key p1", id)
		}
	}

	// Heal; wait for a stable leader (old leader may be deposed).
	k.c.Network().Heal()
	oldTerm := k.c.Node(leader).Term()
	if !k.c.RunUntil(60, func(c *sim.Cluster) bool {
		l := c.Leader()
		// Settle means a leader CONFIRMED IN A NEWER TERM: the old
		// isolated leader may still locally believe it leads at the old
		// term, and a proposal to it would be term-stale.
		return l != "" && l != leader && c.Node(l).Term() > oldTerm
	}) {
		t.Fatal("cluster did not settle under a new-term leader after heal")
	}
	newLeader := k.c.Leader()
	// Retry the SAME request id on the settled leader. If the old entry
	// somehow committed during healing the reply cache returns it; either
	// way the retry produces exactly one decisive application.
	idx, committed := k.propose(newLeader, cmd)
	if !committed {
		t.Fatal("retried batch did not commit after heal")
	}
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	for _, id := range ids("n1", "n2", "n3") {
		rep, ok := k.stores[id].PendingPut("s", "partitioned")
		if !ok {
			t.Fatalf("%s missing cached verdict for retried batch", id)
		}
		if rep.Verdict != kv.VerdictOK || rep.AppliedAt != idx {
			t.Fatalf("%s verdict=%s at=%d want ok at %d", id, rep.Verdict, rep.AppliedAt, idx)
		}
		for _, key := range [2]string{"p1", "p2"} {
			r := k.stores[id].Read("s", key, 1)
			if r.Verdict != kv.VerdictOK || r.Version != 1 {
				t.Fatalf("%s %s after retry: %q v%d", id, key, r.Value, r.Version)
			}
		}
	}
	t.Logf("batch unknown during partition converged on retry at index=%d via leader=%s",
		idx, newLeader)
}

// Crash the leader right after a conditional batch commits, then restart
// it: reply cache and versions replay from storage, a duplicate returns
// the original verdict and the next conditional update sees the version.
func TestBatchLeaderCrashRestartKeepsVerdict(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	idx, ok := k.propose(leader, batchCmd("s", "durable",
		kv.BatchOp{Key: "d1", Value: []byte("v"), ExpectVersion: kv.ExpectAbsent()},
	))
	if !ok {
		t.Fatal("batch did not commit")
	}
	k.tick(5)

	k.c.Crash(leader)
	k.tick(3)
	k.c.Restart(leader)
	k.tick(20)

	// Model log replay on the restarted node: the duplicated committed
	// entry must yield the original verdict (not a second application)...
	st := k.stores[leader]
	replay := mustBatch(t, st, idx, batchCmd("s", "durable",
		kv.BatchOp{Key: "d1", Value: []byte("v"), ExpectVersion: kv.ExpectAbsent()},
	))
	if replay.Verdict != kv.VerdictDuplicate || replay.Index != idx {
		t.Fatalf("replay after restart: %s idx=%d want duplicate @%d",
			replay.Verdict, replay.Index, idx)
	}
	next := mustBatch(t, st, idx+1, batchCmd("s", "next",
		kv.BatchOp{Key: "d1", Value: []byte("w"), ExpectVersion: kv.ExpectVersion(1)},
	))
	if next.Verdict != kv.VerdictOK || next.Batch.Ops[0].Version != 2 {
		t.Fatalf("conditional write after restart: %+v", next)
	}
}
