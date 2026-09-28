package kv_test

import (
	"sync"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
)

// Two clients concurrently base a conditional update on the same old
// version. Exactly one may commit; the loser must receive a decisive
// precondition-failed, never a timeout/unknown verdict.
func TestBatchConcurrentConditionalRace(t *testing.T) {
	svc, _, _ := newSingle(t)
	sid, res := svc.OpenSession(0)
	if res.Verdict != kv.VerdictOK {
		t.Fatalf("open: %s/%s", res.Verdict, res.Reason)
	}

	// Seed key "cfg" at version 1.
	seed := svc.Batch(sid, "seed", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v0")},
	})
	if seed.Verdict != kv.VerdictOK {
		t.Fatalf("seed: %s/%s", seed.Verdict, seed.Reason)
	}

	const N = 8
	var wg sync.WaitGroup
	results := make([]kv.Result, N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = svc.Batch(sid, "race-"+req(i), []kv.BatchItem{
				{Key: "cfg", Value: []byte("winner"),
					ExpectVersion: kv.ExpectVersion(1)},
			})
		}(i)
	}
	close(start)
	wg.Wait()

	wins, fails := 0, 0
	for _, r := range results {
		switch r.Verdict {
		case kv.VerdictOK:
			wins++
			if r.Batch == nil || r.Batch.Ops[0].Version != 2 {
				t.Fatalf("winner version detail wrong: %+v", r.Batch)
			}
		case kv.VerdictPreconditionFailed:
			fails++
			if r.Batch == nil || r.Batch.FailedKey != "cfg" ||
				r.Batch.ExpectVersion != 1 || r.Batch.ActualVersion != 2 {
				t.Fatalf("loser detail wrong: %+v", r.Batch)
			}
			if r.Reason != "version-mismatch" {
				t.Fatalf("loser reason=%s want version-mismatch", r.Reason)
			}
		default:
			t.Fatalf("unexpected verdict %s/%s — racers must get a decisive answer",
				r.Verdict, r.Reason)
		}
	}
	if wins != 1 || fails != N-1 {
		t.Fatalf("wins=%d fails=%d want exactly 1 win and %d decisive failures",
			wins, fails, N-1)
	}
}

// A batch retried with the same request id after a timeout returns the
// ORIGINAL complete verdict, and the keys carry exactly one effect.
func TestBatchRetryAfterUnknownReturnsOriginal(t *testing.T) {
	svc, _, _ := newSingle(t)
	sid, _ := svc.OpenSession(0)

	first := svc.Batch(sid, "b1", []kv.BatchItem{
		{Key: "x", Value: []byte("1"), ExpectVersion: kv.ExpectAbsent()},
		{Key: "y", Value: []byte("2")},
	})
	if first.Verdict != kv.VerdictOK {
		t.Fatalf("first: %s/%s", first.Verdict, first.Reason)
	}

	// Interleaving write changes both keys before the "retry".
	moved := svc.Batch(sid, "b2", []kv.BatchItem{
		{Key: "x", Value: []byte("9")},
		{Key: "y", Value: []byte("9")},
	})
	if moved.Verdict != kv.VerdictOK {
		t.Fatalf("moved: %s/%s", moved.Verdict, moved.Reason)
	}

	retry := svc.Batch(sid, "b1", []kv.BatchItem{
		{Key: "x", Value: []byte("1"), ExpectVersion: kv.ExpectAbsent()},
		{Key: "y", Value: []byte("2")},
	})
	if retry.Verdict != kv.VerdictDuplicate {
		t.Fatalf("retry verdict=%s want duplicate", retry.Verdict)
	}
	if retry.Batch == nil || retry.Batch.AppliedAt != first.Index ||
		len(retry.Batch.Ops) != 2 {
		t.Fatalf("retry lost original batch detail: idx=%d batch=%+v", first.Index, retry.Batch)
	}
	for _, op := range retry.Batch.Ops {
		if op.Version != 1 {
			t.Fatalf("retry reported non-original versions: %+v", retry.Batch.Ops)
		}
	}
	if v := svc.Get(sid, "g1", "x"); string(v.Value) != "9" || v.Version != 2 {
		t.Fatalf("retry reapplied the batch: x=%q v%d", v.Value, v.Version)
	}
}

// Invalid batches are rejected before any effect and are themselves
// idempotent under the same request id.
func TestBatchInvalidRejectedDecisively(t *testing.T) {
	svc, _, _ := newSingle(t)
	sid, _ := svc.OpenSession(0)
	r := svc.Batch(sid, "dup-key", []kv.BatchItem{
		{Key: "k", Value: []byte("a")},
		{Key: "k", Value: []byte("b")},
	})
	if r.Verdict != kv.VerdictInvalidBatch || r.Reason != "duplicate-key" {
		t.Fatalf("got %s/%s want invalid-batch/duplicate-key", r.Verdict, r.Reason)
	}
	retry := svc.Batch(sid, "dup-key", []kv.BatchItem{
		{Key: "k", Value: []byte("a")},
		{Key: "k", Value: []byte("b")},
	})
	if retry.Verdict != kv.VerdictDuplicate || retry.Reason != "request-already-applied" {
		t.Fatalf("retry of invalid batch: %s/%s", retry.Verdict, retry.Reason)
	}
	if g := svc.Get(sid, "g", "k"); g.Verdict != kv.VerdictNotFound {
		t.Fatalf("invalid batch wrote key: %q", g.Value)
	}
}
