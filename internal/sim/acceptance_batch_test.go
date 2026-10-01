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

	// Heal; wait for a leader. The deposed isolated node may briefly still
	// report itself leader in an old term, so retry across leader changes
	// (a term-stale proposal simply yields a later election) until the
	// same-request retry yields a committed verdict, like a real client.
	k.c.Network().Heal()
	var (
		newLeader raft.NodeID
		idx       raft.Index
		converged bool
	)
	for attempt := 0; attempt < 40; attempt++ {
		if !k.c.RunUntil(30, func(c *sim.Cluster) bool { return c.Leader() != "" }) {
			continue
		}
		newLeader = k.c.Leader()
		if i, ok := k.proposeCommittedOrCached(newLeader, cmd, "s", "partitioned"); ok {
			idx, converged = i, true
			break
		}
		k.tick(2)
	}
	if !converged {
		t.Fatal("retried batch did not converge to a committed verdict after heal")
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

// Deterministic counterpart of the service-level retry tests: while the
// leader is in a minority partition, repeated proposals with the SAME
// request id (fresh Propose calls, as real clients do) never advance the
// commit point on ANY node; after healing, every retry converges to the
// single committed verdict at one position.
func TestBatchRepeatedSameRequestDuringPartitionNeverCommits(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	cmd := batchCmd("s", "storm",
		kv.BatchOp{Key: "q1", Value: []byte("a")},
		kv.BatchOp{Key: "q2", Value: []byte("b")},
	)
	data, _ := kv.Encode(cmd)
	rid := raft.ReqID("s/storm")

	k.c.Network().Isolate(leader, ids("n1", "n2", "n3"))
	// The isolated leader must not advance its own commit point. The
	// majority side may legally elect a new leader and commit its own
	// term no-ops, but it cannot contain the batch's entries, so assert
	// the batch itself is absent there (no cache, no keys) rather than
	// claiming the commit index can never move.
	leaderMark := k.c.Node(leader).CommitIndex()
	// Many same-id retries while isolated: none may commit anywhere.
	for i := 0; i < 12; i++ {
		k.c.Node(leader).Propose(rid, data)
		k.tick(2)
	}
	if got := k.c.Node(leader).CommitIndex(); got != leaderMark {
		t.Fatalf("isolated leader advanced commit %d -> %d", leaderMark, got)
	}
	for _, id := range ids("n1", "n2", "n3") {
		if _, ok := k.stores[id].PendingPut("s", "storm"); ok {
			t.Fatalf("%s cached verdict for an uncommitted partitioned batch", id)
		}
		for _, key := range [2]string{"q1", "q2"} {
			if r := k.stores[id].Read("s", key, 1); r.Verdict != kv.VerdictNotFound {
				t.Fatalf("%s saw partitioned key %s", id, key)
			}
		}
	}

	// Heal; settle on a quorum-valid leader; same-id retry converges once.
	// Right after heal, Leader() may briefly still name the deposed
	// isolated node (term-stale proposals); retry across leader changes
	// until a committed verdict appears, as a real client would.
	k.c.Network().Heal()
	var (
		nl        raft.NodeID
		idx       raft.Index
		converged bool
	)
	for attempt := 0; attempt < 30; attempt++ {
		if !k.c.RunUntil(30, func(c *sim.Cluster) bool { return c.Leader() != "" }) {
			continue
		}
		nl = k.c.Leader()
		if i, ok := k.proposeCommittedOrCached(nl, cmd, "s", "storm"); ok {
			idx, converged = i, true
			break
		}
		k.tick(2)
	}
	if !converged {
		t.Fatal("retry after heal did not converge to a committed verdict")
	}
	// Several more same-id retries: the raft layer answers
	// already-committed duplicate without a second entry; every node's
	// cache keeps the single original verdict.
	for i := 0; i < 5; i++ {
		k.c.Node(nl).Propose(rid, data)
		k.tick(2)
	}
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	for _, id := range ids("n1", "n2", "n3") {
		rep, ok := k.stores[id].PendingPut("s", "storm")
		if !ok || rep.Verdict != kv.VerdictOK || rep.AppliedAt != idx {
			t.Fatalf("%s cache: ok=%v %+v want ok @%d", id, ok, rep, idx)
		}
		for _, key := range [2]string{"q1", "q2"} {
			r := k.stores[id].Read("s", key, 1)
			if r.Verdict != kv.VerdictOK || r.Version != 1 {
				t.Fatalf("%s %s = %q v%d want v1", id, key, r.Value, r.Version)
			}
		}
	}
}

// A conditional batch whose precondition failed, replayed with the same
// request id AFTER other writes moved the version (and after a leadership
// cycle), returns the ORIGINAL failure with the ORIGINAL actual version —
// it never re-evaluates against the newest state.
func TestBatchConditionalFailureRetryKeepsOriginalDetail(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	idx, committed := k.propose(leader, batchCmd("s", "failx",
		kv.BatchOp{Key: "k", Value: []byte("new"), ExpectVersion: kv.ExpectVersion(5)},
	))
	if !committed {
		t.Fatal("failing batch must still commit as a log entry")
	}
	k.tick(3)
	// Move the key to version 1.
	k.propose(leader, batchCmd("s", "other",
		kv.BatchOp{Key: "k", Value: []byte("actual")},
	))
	k.tick(3)

	// Replay the failed request at a later index (model of a retry): must
	// be a duplicate carrying the original failure at the original index,
	// with actual_version 0 (key absent at first evaluation), not 1.
	retry := mustBatch(t, k.stores[leader], idx+50, batchCmd("s", "failx",
		kv.BatchOp{Key: "k", Value: []byte("new"), ExpectVersion: kv.ExpectVersion(5)},
	))
	if retry.Verdict != kv.VerdictDuplicate {
		t.Fatalf("retry verdict=%s want duplicate", retry.Verdict)
	}
	b := retry.Batch
	if b.AppliedAt != idx || b.FailedKey != "k" ||
		b.ExpectVersion != 5 || b.ActualVersion != 0 {
		t.Fatalf("retry lost original failure detail: %+v", b)
	}
	// And the key keeps its only successful write at version 1.
	r := k.stores[leader].Read("s", "k", 1)
	if string(r.Value) != "actual" || r.Version != 1 {
		t.Fatalf("retry of failed batch mutated state: %q v%d", r.Value, r.Version)
	}
}
