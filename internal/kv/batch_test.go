package kv

import (
	"encoding/json"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// newBatchStore applies an open-session entry and returns the store.
func newBatchStore(t *testing.T) *Store {
	t.Helper()
	st := NewStore()
	apply := func(idx raft.Index, cmd Command) Result {
		data, err := Encode(cmd)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := st.Apply(raft.Entry{Index: idx, Kind: raft.EntryData, Data: data})
		return r
	}
	r := apply(1, Command{Type: CmdSessionOpen, SessionID: "s",
		RequestID: "open", TTLMillis: 0, CreateTime: 1})
	if r.Verdict != VerdictOK {
		t.Fatalf("open: %s %s", r.Verdict, r.Reason)
	}
	return st
}

func batchEntry(idx raft.Index, req string, items []BatchItem) raft.Entry {
	data, _ := Encode(Command{Type: CmdBatch, SessionID: "s", RequestID: req,
		Items: items, CreateTime: 100})
	return raft.Entry{Index: idx, Term: 1, Kind: raft.EntryData, Data: data}
}

func TestBatchAtomicSuccessAndVersions(t *testing.T) {
	st := newBatchStore(t)
	items := []BatchItem{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}
	res, _ := st.Apply(batchEntry(2, "r1", items))
	if res.Verdict != VerdictOK {
		t.Fatalf("batch verdict=%s reason=%s", res.Verdict, res.Reason)
	}
	if len(res.Items) != 2 || res.Items[0].Version != 1 || res.Items[1].Version != 1 {
		t.Fatalf("items=%+v want both version 1", res.Items)
	}
	if got := st.Read("s", "a", 100); string(got.Value) != "1" || got.Version != 1 {
		t.Fatalf("read a value=%q ver=%d", got.Value, got.Version)
	}
	// A second batch touching "a" bumps only its version.
	res2, _ := st.Apply(batchEntry(3, "r2", []BatchItem{{Key: "a", Value: []byte("11")}}))
	if res2.Items[0].Version != 2 {
		t.Fatalf("a version=%d want 2", res2.Items[0].Version)
	}
	if got := st.Read("s", "b", 100); got.Version != 1 {
		t.Fatalf("b version changed: %d", got.Version)
	}
}

func TestBatchAllOrNothingOnPreconditionFailure(t *testing.T) {
	st := newBatchStore(t)
	// Seed a at version 3 via three committed writes.
	for i, v := range []string{"a0", "a1", "a2"} {
		_, _ = st.Apply(batchEntry(raft.Index(2+i), "seed-"+v,
			[]BatchItem{{Key: "a", Value: []byte(v)}}))
	}
	// Batch: b (unconditional) + a must stay at version 1 — fails.
	res, _ := st.Apply(batchEntry(5, "cas1", []BatchItem{
		{Key: "b", Value: []byte("b0")},
		{Key: "a", Value: []byte("x"), CheckVersion: true, ExpectedVersion: 1},
	}))
	if res.Verdict != VerdictPreconditionFailed {
		t.Fatalf("verdict=%s want precondition-failed", res.Verdict)
	}
	if res.Reason != "version-mismatch:a" {
		t.Fatalf("reason=%q want version-mismatch:a", res.Reason)
	}
	if res.Items[0].Verdict != VerdictNotApplied ||
		res.Items[1].Verdict != VerdictPreconditionFailed {
		t.Fatalf("items=%+v", res.Items)
	}
	if res.Items[1].Version != 3 {
		t.Fatalf("observed version=%d want 3", res.Items[1].Version)
	}
	// Nothing changed: b absent, a still version 3 / value a2.
	if r := st.Read("s", "b", 100); r.Verdict != VerdictNotFound {
		t.Fatalf("b leaked: %s", r.Verdict)
	}
	if r := st.Read("s", "a", 100); r.Version != 3 || string(r.Value) != "a2" {
		t.Fatalf("a mutated: ver=%d val=%q", r.Version, r.Value)
	}
}

func TestBatchCreateIfNotExistsGuard(t *testing.T) {
	st := newBatchStore(t)
	ok := []BatchItem{{Key: "cfg", Value: []byte("v1"),
		CheckVersion: true, ExpectedVersion: 0}}
	if res, _ := st.Apply(batchEntry(2, "c1", ok)); res.Verdict != VerdictOK {
		t.Fatalf("first create: %s %s", res.Verdict, res.Reason)
	}
	// Second creator must fail definitively and report current version 1.
	res, _ := st.Apply(batchEntry(3, "c2",
		[]BatchItem{{Key: "cfg", Value: []byte("v2"),
			CheckVersion: true, ExpectedVersion: 0}}))
	if res.Verdict != VerdictPreconditionFailed ||
		res.Reason != "must-not-exist-but-exists:cfg" ||
		res.Items[0].Version != 1 {
		t.Fatalf("second create res=%+v reason=%s", res.Items, res.Reason)
	}
	if r := st.Read("s", "cfg", 100); string(r.Value) != "v1" || r.Version != 1 {
		t.Fatalf("value changed on failed create: %q v%d", r.Value, r.Version)
	}
}

func TestBatchDuplicateRequestReturnsOriginalVerdict(t *testing.T) {
	st := newBatchStore(t)
	items := []BatchItem{
		{Key: "a", Value: []byte("1"), CheckVersion: true, ExpectedVersion: 0},
		{Key: "b", Value: []byte("2")},
	}
	first, _ := st.Apply(batchEntry(2, "dup", items))
	if first.Verdict != VerdictOK {
		t.Fatal(first.Verdict, first.Reason)
	}
	// Move state forward: a is now version 1, and an unrelated write lands.
	_, _ = st.Apply(batchEntry(3, "other",
		[]BatchItem{{Key: "a", Value: []byte("99")}}))

	// Retry the SAME request id: must return the original verdict at the
	// original index with original versions, never re-judge or re-apply.
	retry, _ := st.Apply(batchEntry(4, "dup", items))
	if retry.Verdict != VerdictDuplicate {
		t.Fatalf("retry verdict=%s want duplicate", retry.Verdict)
	}
	if retry.Index != 2 {
		t.Fatalf("retry index=%d want 2", retry.Index)
	}
	if len(retry.Items) != 2 || retry.Items[0].Version != 1 ||
		retry.Items[1].Version != 1 {
		t.Fatalf("retry items=%+v want original versions", retry.Items)
	}
	if r := st.Read("s", "a", 100); string(r.Value) != "99" || r.Version != 2 {
		t.Fatalf("retry reapplied: a=%q v%d", r.Value, r.Version)
	}
}

func TestBatchDuplicateFailedRequestStaysFailed(t *testing.T) {
	st := newBatchStore(t)
	fail := []BatchItem{
		{Key: "a", Value: []byte("1"), CheckVersion: true, ExpectedVersion: 5},
	}
	first, _ := st.Apply(batchEntry(2, "f1", fail))
	if first.Verdict != VerdictPreconditionFailed {
		t.Fatalf("first=%s", first.Verdict)
	}
	// Even after a reaches version 5, the original request keeps its verdict.
	for i := 0; i < 5; i++ {
		_, _ = st.Apply(batchEntry(raft.Index(3+i), "grow",
			[]BatchItem{{Key: "a", Value: []byte("v")}}))
	}
	retry, _ := st.Apply(batchEntry(8, "f1", fail))
	if retry.Verdict != VerdictDuplicate || retry.Index != 2 {
		t.Fatalf("retry verdict=%s index=%d, want duplicate@2", retry.Verdict, retry.Index)
	}
	if len(retry.Items) != 1 || retry.Items[0].Version != 0 ||
		retry.Items[0].Verdict != VerdictPreconditionFailed {
		t.Fatalf("retry lost original outcome: %+v", retry.Items)
	}
}

func TestBatchStructuralRejections(t *testing.T) {
	cases := []struct {
		name   string
		items  []BatchItem
		reason string
	}{
		{"empty", nil, "batch-empty"},
		{"duplicate key", []BatchItem{{Key: "a"}, {Key: "a"}}, "batch-duplicate-key"},
		{"empty key", []BatchItem{{Key: ""}}, "batch-empty-key"},
		{"key too long", []BatchItem{{Key: string(make([]byte, MaxKeyLength+1))}}, "key-too-long"},
		{"value too large", []BatchItem{{Key: "k", Value: make([]byte, MaxValueSize+1)}}, "value-too-large"},
		{"negative expected", []BatchItem{{Key: "k", CheckVersion: true, ExpectedVersion: -1}}, "bad-expected-version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newBatchStore(t)
			res, _ := st.Apply(batchEntry(2, "req-"+tc.name, tc.items))
			if res.Verdict != VerdictRejected || res.Reason != tc.reason {
				t.Fatalf("verdict=%s reason=%q want rejected/%s",
					res.Verdict, res.Reason, tc.reason)
			}
			// Retry is cached as the original rejection.
			retry, _ := st.Apply(batchEntry(3, "req-"+tc.name, tc.items))
			if retry.Verdict != VerdictDuplicate || retry.Reason != "request-already-applied" {
				t.Fatalf("retry=%s/%s", retry.Verdict, retry.Reason)
			}
		})
	}
	// Too many items: rejected without per-item payloads.
	st := newBatchStore(t)
	many := make([]BatchItem, MaxBatchItems+1)
	for i := range many {
		many[i] = BatchItem{Key: "k"}
	}
	res, _ := st.Apply(batchEntry(2, "many", many))
	if res.Verdict != VerdictRejected || res.Reason != "batch-too-many-items" {
		t.Fatalf("many: %s/%s", res.Verdict, res.Reason)
	}
}

func TestBatchVersionsSurviveSnapshot(t *testing.T) {
	st := newBatchStore(t)
	_, _ = st.Apply(batchEntry(2, "r1", []BatchItem{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}))
	_, _ = st.Apply(batchEntry(3, "r2", []BatchItem{
		{Key: "a", Value: []byte("11")},
	}))
	data, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	st2 := NewStore()
	if err := st2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if r := st2.Read("s", "a", 100); string(r.Value) != "11" || r.Version != 2 {
		t.Fatalf("a after restore: %q v%d", r.Value, r.Version)
	}
	// Duplicate verdict (including items) survives the snapshot too.
	rep, ok := st2.PendingPut("s", "r1")
	if !ok || rep.AppliedAt != 2 || len(rep.Items) != 2 ||
		rep.Items[0].Version != 1 {
		t.Fatalf("reply cache lost: %+v ok=%v", rep, ok)
	}
}

// TestSnapshotLegacyByteFmt verifies pre-versioning snapshots (key maps
// straight to a JSON byte string) still restore as version 1 entries.
func TestSnapshotLegacyByteFmt(t *testing.T) {
	legacy := []byte(`{"Kv":{"a":"MQ=="},"Sessions":{}}`)
	st := NewStore()
	if err := st.Restore(legacy); err != nil {
		t.Fatal(err)
	}
	if e := st.kv["a"]; string(e.Value) != "1" || e.Version != 1 {
		t.Fatalf("legacy entry=%+v", e)
	}
}

// TestConditionalCASContentionInStateMachine models two clients racing
// from the same stale version. Because evaluation is serialized in
// applyLocked, the first commits and the second gets a definitive,
// distinguishable precondition failure (never "unknown").
func TestConditionalCASContentionInStateMachine(t *testing.T) {
	st := newBatchStore(t)
	_, _ = st.Apply(batchEntry(2, "seed", []BatchItem{
		{Key: "cfg", Value: []byte("initial")},
	}))
	r1, _ := st.Apply(batchEntry(3, "c1", []BatchItem{{
		Key: "cfg", Value: []byte("client-1"),
		CheckVersion: true, ExpectedVersion: 1}}))
	r2, _ := st.Apply(batchEntry(4, "c2", []BatchItem{{
		Key: "cfg", Value: []byte("client-2"),
		CheckVersion: true, ExpectedVersion: 1}}))
	if r1.Verdict != VerdictOK || r1.Items[0].Version != 2 {
		t.Fatalf("winner: %+v", r1)
	}
	if r2.Verdict != VerdictPreconditionFailed ||
		r2.Reason != "version-mismatch:cfg" || r2.Items[0].Version != 2 {
		t.Fatalf("loser: %+v reason=%s", r2, r2.Reason)
	}
	if r := st.Read("s", "cfg", 100); string(r.Value) != "client-1" || r.Version != 2 {
		t.Fatalf("state=%q v%d", r.Value, r.Version)
	}
}

func TestBatchEntryJSONShape(t *testing.T) {
	c := Command{Type: CmdBatch, SessionID: "s", RequestID: "r",
		Items: []BatchItem{{Key: "k", Value: []byte("v"),
			CheckVersion: true, ExpectedVersion: 0}}}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var back Command
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Items) != 1 || !back.Items[0].CheckVersion ||
		back.Items[0].ExpectedVersion != 0 || string(back.Items[0].Value) != "v" {
		t.Fatalf("round trip lost item: %+v raw=%s", back.Items, data)
	}
}
