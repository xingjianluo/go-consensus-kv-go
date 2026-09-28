package kv

import (
	"strings"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// batchEntry builds a committed data entry carrying cmd at index idx.
func batchEntry(cmd Command, idx raft.Index) raft.Entry {
	data, _ := Encode(cmd)
	return raft.Entry{Index: idx, Term: 1, Kind: raft.EntryData, Data: data}
}

func applyBatch(t *testing.T, st *Store, sid, req string, idx raft.Index, ops ...BatchOp) Result {
	t.Helper()
	cmd := Command{Type: CmdBatch, SessionID: sid, RequestID: req,
		Ops: ops, CreateTime: idx2time(idx)}
	res, handled := st.Apply(batchEntry(cmd, idx))
	if !handled {
		t.Fatal("batch entry not handled")
	}
	return res
}

func idx2time(idx raft.Index) int64 { return int64(idx) * 100 }

func openSM(t *testing.T, st *Store) {
	t.Helper()
	res, _ := st.Apply(batchEntry(Command{Type: CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1}, 1))
	if res.Verdict != VerdictOK {
		t.Fatalf("open session: %s/%s", res.Verdict, res.Reason)
	}
}

// Successful multi-key batch applies every key at one commit index with
// monotonically assigned versions, visible atomically.
func TestBatchAppliesAllKeysAtomically(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	res := applyBatch(t, st, "s", "b1", 2,
		BatchOp{Key: "a", Value: []byte("1")},
		BatchOp{Key: "b", Value: []byte("2")},
		BatchOp{Key: "c", Value: []byte("3")},
	)
	if res.Verdict != VerdictOK {
		t.Fatalf("verdict=%s reason=%s", res.Verdict, res.Reason)
	}
	if res.Batch == nil || len(res.Batch.Ops) != 3 {
		t.Fatal("missing per-key results")
	}
	if res.Batch.AppliedAt != 2 {
		t.Fatalf("applied-at=%d want 2", res.Batch.AppliedAt)
	}
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range want {
		r := st.Read("s", k, 200)
		if r.Verdict != VerdictOK || string(r.Value) != v {
			t.Fatalf("key %s = %q/%s want %q", k, r.Value, r.Verdict, v)
		}
		if r.Version != 1 {
			t.Fatalf("key %s version=%d want 1", k, r.Version)
		}
	}
	for i, op := range res.Batch.Ops {
		if op.Index != 2 || op.Version != 1 {
			t.Fatalf("op %d index/version=%d/%d want 2/1", i, op.Index, op.Version)
		}
	}

	// A second batch bumps each touched key's version independently.
	res2 := applyBatch(t, st, "s", "b2", 3,
		BatchOp{Key: "a", Value: []byte("11")},
		BatchOp{Key: "c", Value: []byte("33")},
	)
	if res2.Verdict != VerdictOK {
		t.Fatalf("second batch: %s/%s", res2.Verdict, res2.Reason)
	}
	gotVersions := map[string]int64{}
	for _, op := range res2.Batch.Ops {
		gotVersions[op.Key] = op.Version
	}
	if gotVersions["a"] != 2 || gotVersions["c"] != 2 {
		t.Fatalf("versions after second batch: %v want a=2 c=2", gotVersions)
	}
	if v := st.VersionOf("b"); v != 1 {
		t.Fatalf("untouched key b version=%d want 1", v)
	}
}

// When one item's precondition fails, NOTHING in the batch may apply —
// including items earlier in the request and the offending key itself.
func TestBatchPreconditionFailureAllOrNothing(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	// seed: a@1, b@1
	if r := applyBatch(t, st, "s", "seed", 2,
		BatchOp{Key: "a", Value: []byte("old")},
		BatchOp{Key: "b", Value: []byte("old")},
	); r.Verdict != VerdictOK {
		t.Fatal(r.Verdict, r.Reason)
	}

	res := applyBatch(t, st, "s", "cond", 3,
		BatchOp{Key: "a", Value: []byte("new"), ExpectVersion: ExpectVersion(1)},
		BatchOp{Key: "b", Value: []byte("new"), ExpectVersion: ExpectVersion(5)}, // stale
		BatchOp{Key: "c", Value: []byte("x")},                                    // must not be applied either
	)
	if res.Verdict != VerdictPreconditionFailed {
		t.Fatalf("verdict=%s want precondition-failed", res.Verdict)
	}
	if res.Reason != "version-mismatch" {
		t.Fatalf("reason=%s want version-mismatch", res.Reason)
	}
	if res.Batch.FailedKey != "b" || res.Batch.ExpectVersion != 5 ||
		res.Batch.ActualVersion != 1 {
		t.Fatalf("failure detail=%+v want key=b expect=5 actual=1", res.Batch)
	}
	if r := st.Read("s", "c", 300); r.Verdict != VerdictNotFound {
		t.Fatalf("third item applied despite failed batch: %q", r.Value)
	}
	for _, k := range []string{"a", "b"} {
		r := st.Read("s", k, 300)
		if string(r.Value) != "old" || r.Version != 1 {
			t.Fatalf("key %s mutated by failed batch: %q v%d", k, r.Value, r.Version)
		}
	}
}

// ExpectVersion(0) means "must not exist"; an existing key rejects the
// whole batch with key-already-exists, and a missing key creates at v1.
func TestBatchCreateIfAbsent(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	ok1 := applyBatch(t, st, "s", "create", 2,
		BatchOp{Key: "new", Value: []byte("v"), ExpectVersion: ExpectAbsent()},
	)
	if ok1.Verdict != VerdictOK || ok1.Batch.Ops[0].Version != 1 {
		t.Fatalf("create absent: %+v", ok1)
	}
	bad := applyBatch(t, st, "s", "create-again", 3,
		BatchOp{Key: "new", Value: []byte("v2"), ExpectVersion: ExpectAbsent()},
	)
	if bad.Verdict != VerdictPreconditionFailed || bad.Reason != "key-already-exists" {
		t.Fatalf("recreate: %s/%s", bad.Verdict, bad.Reason)
	}
	if bad.Batch.FailedKey != "new" || bad.Batch.ActualVersion != 1 {
		t.Fatalf("recreate detail=%+v", bad.Batch)
	}
	missing := applyBatch(t, st, "s", "expect-existing", 4,
		BatchOp{Key: "ghost", Value: []byte("x"), ExpectVersion: ExpectVersion(3)},
	)
	if missing.Verdict != VerdictPreconditionFailed ||
		missing.Reason != "key-not-exists" {
		t.Fatalf("expect-existing on missing key: %s/%s", missing.Verdict, missing.Reason)
	}
	if v := st.VersionOf("new"); v != 1 {
		t.Fatalf("new mutated after failures: v%d", v)
	}
}

// Structural rejections never apply anything and carry distinct reasons.
func TestBatchStructuralValidation(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	bigValue := make([]byte, MaxValueLen+1)
	cases := []struct {
		name   string
		ops    []BatchOp
		reason string
	}{
		{"empty", nil, "empty-batch"},
		{"empty-key", []BatchOp{{Key: "", Value: []byte("x")}}, "empty-key"},
		{"duplicate-key", []BatchOp{
			{Key: "k", Value: []byte("a")},
			{Key: "k", Value: []byte("b")},
		}, "duplicate-key"},
		{"value-too-large", []BatchOp{{Key: "k", Value: bigValue}}, "value-too-long"},
		{"too-many-items", makeMany(MaxBatchItems + 1), "too-many-items"},
		// Five items each under MaxValueLen but together over MaxBatchPayload.
		{"payload-too-large", []BatchOp{
			{Key: "k1", Value: make([]byte, 900<<10)},
			{Key: "k2", Value: make([]byte, 900<<10)},
			{Key: "k3", Value: make([]byte, 900<<10)},
			{Key: "k4", Value: make([]byte, 900<<10)},
			{Key: "k5", Value: make([]byte, 900<<10)},
		}, "payload-too-large"},
	}
	for i, tc := range cases {
		res := applyBatch(t, st, "s", "bad-"+tc.name, raft.Index(10+i), tc.ops...)
		if res.Verdict != VerdictInvalidBatch {
			t.Fatalf("%s: verdict=%s want invalid-batch", tc.name, res.Verdict)
		}
		if res.Reason != tc.reason {
			t.Fatalf("%s: reason=%s want %s", tc.name, res.Reason, tc.reason)
		}
	}
	if len(st.kv) != 0 {
		t.Fatalf("invalid batches mutated store: %v", st.kv)
	}
}

func makeMany(n int) []BatchOp {
	ops := make([]BatchOp, n)
	for i := range ops {
		ops[i] = BatchOp{Key: "k" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Value: []byte("x")}
	}
	return ops
}

// A repeated (session, request) returns the FIRST verdict in full, even
// when (a) other writes changed the keys afterwards, and (b) the
// original request's conditions would now fail.
func TestBatchDuplicateReturnsOriginalVerdict(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	first := applyBatch(t, st, "s", "once", 2,
		BatchOp{Key: "a", Value: []byte("v1"), ExpectVersion: ExpectAbsent()},
		BatchOp{Key: "b", Value: []byte("v1")},
	)
	if first.Verdict != VerdictOK {
		t.Fatal(first.Verdict, first.Reason)
	}
	// Other writes advance the keys.
	if r := applyBatch(t, st, "s", "other", 3,
		BatchOp{Key: "a", Value: []byte("v2")},
		BatchOp{Key: "b", Value: []byte("v2")},
	); r.Verdict != VerdictOK {
		t.Fatal(r.Verdict, r.Reason)
	}
	// Retry the ORIGINAL request: its create-if-absent condition is now
	// false, but the cached verdict must be the original success.
	retry := applyBatch(t, st, "s", "once", 99,
		BatchOp{Key: "a", Value: []byte("v1"), ExpectVersion: ExpectAbsent()},
		BatchOp{Key: "b", Value: []byte("v1")},
	)
	if retry.Verdict != VerdictDuplicate {
		t.Fatalf("retry verdict=%s want duplicate", retry.Verdict)
	}
	if retry.Index != 2 || retry.Batch == nil || retry.Batch.AppliedAt != 2 {
		t.Fatalf("retry lost original position: %+v", retry)
	}
	for _, op := range retry.Batch.Ops {
		if op.Version != 1 {
			t.Fatalf("retry returned new versions: %+v", retry.Batch.Ops)
		}
	}
	if r := st.Read("s", "a", 400); string(r.Value) != "v2" || r.Version != 2 {
		t.Fatalf("retry mutated state: %q v%d", r.Value, r.Version)
	}

	// A rejected batch is likewise cached: same id gives duplicate with
	// the original failure detail; changing state does not re-evaluate.
	fail := applyBatch(t, st, "s", "fail", 4,
		BatchOp{Key: "a", Value: []byte("x"), ExpectVersion: ExpectVersion(99)},
	)
	if fail.Verdict != VerdictPreconditionFailed {
		t.Fatal(fail.Verdict, fail.Reason)
	}
	// Make the condition now hold-ish (a reaches some other version, but
	// we retry identical request) and verify the original failure wins.
	retryFail := applyBatch(t, st, "s", "fail", 100,
		BatchOp{Key: "a", Value: []byte("x"), ExpectVersion: ExpectVersion(99)},
	)
	if retryFail.Verdict != VerdictDuplicate ||
		retryFail.Batch.FailedKey != "a" || retryFail.Index != 4 {
		t.Fatalf("retry of failed batch: %+v", retryFail)
	}
}

// Versions and batch reply cache survive snapshot/restore, so a crash
// recovery keeps conditional-update semantics and duplicate verdicts.
func TestBatchSnapshotRestorePreservesVersionsAndReplies(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	if r := applyBatch(t, st, "s", "b", 2,
		BatchOp{Key: "k", Value: []byte("v"), ExpectVersion: ExpectAbsent()},
	); r.Verdict != VerdictOK {
		t.Fatal(r.Verdict, r.Reason)
	}
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	st2 := NewStore()
	if err := st2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if v := st2.VersionOf("k"); v != 1 {
		t.Fatalf("version after restore=%d want 1", v)
	}
	// Duplicate after restore returns original verdict from cache.
	retry := applyBatch(t, st2, "s", "b", 50,
		BatchOp{Key: "k", Value: []byte("v"), ExpectVersion: ExpectAbsent()},
	)
	if retry.Verdict != VerdictDuplicate || retry.Index != 2 {
		t.Fatalf("duplicate after restore: %s idx=%d", retry.Verdict, retry.Index)
	}
	// Next conditional write on restored store sees v1.
	next := applyBatch(t, st2, "s", "c", 51,
		BatchOp{Key: "k", Value: []byte("w"), ExpectVersion: ExpectVersion(1)},
	)
	if next.Verdict != VerdictOK || next.Batch.Ops[0].Version != 2 {
		t.Fatalf("conditional write after restore: %+v", next)
	}

	// Old snapshot format (no Versions field) restores with v1 per key.
	oldJSON := []byte(`{"Kv":{"legacy":"MQ=="},"Sessions":{}}`)
	st3 := NewStore()
	if err := st3.Restore(oldJSON); err != nil {
		t.Fatal(err)
	}
	if v := st3.VersionOf("legacy"); v != 1 {
		t.Fatalf("legacy snapshot version=%d want 1", v)
	}
	if !strings.Contains(string(mustSnap(t, st3)), "Versions") {
		t.Fatal("re-serialized snapshot missing Versions")
	}
}

func mustSnap(t *testing.T, st *Store) []byte {
	t.Helper()
	b, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return b
}
