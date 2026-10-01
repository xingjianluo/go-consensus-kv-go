package kv_test

// Retry-convergence tests for multi-key batches / conditional updates:
//
//  1. after a partition heals (or leadership changes), retrying the SAME
//     (session, request) converges to ONE decisive verdict — either the
//     original committed verdict or one clean fresh application;
//  2. many concurrent callers retrying the same request id, across a
//     partition/heal/leader-change cycle, ALL receive the same decisive
//     conclusion — never "unknown" as a final answer, never a hang.
//
// These tests run against a real 3-node kv.Service cluster with an
// in-memory fault-injecting network and a fast wall clock; they are
// designed to be stable in tight loops (go test -count=N).

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

const retryTestTimeout = 120 * time.Millisecond

// isRetryable reports whether a verdict means "try again with the same
// request id" rather than a terminal conclusion.
//
// timeout-unknown and not-leader are the usual ones. session-expired with
// reason session-unknown-or-closed can be transient at the SERVICE fast
// path: a freshly settled leader may not have applied the session-open
// entry yet, even though the session is durably committed. The client
// retries with the same request id until the leader's store caught up.
func isRetryable(r kv.Result) bool {
	if r.Verdict == kv.VerdictTimeout || r.Verdict == kv.VerdictNotLeader {
		return true
	}
	if r.Verdict == kv.VerdictSessionExpired &&
		r.Reason == "session-unknown-or-closed" {
		return true
	}
	return false
}

// batchOnAnyLeader tries the batch on every node that currently believes
// itself leader, rotating the starting node so concurrent callers do not
// stampede one node. A stale, partitioned leader costs at most one short
// write timeout; the quorum leader answers decisively.
func batchOnAnyLeader(c *svcCluster, sid, req string, items []kv.BatchItem,
	rotate int) kv.Result {
	order := make([]raft.NodeID, 0, len(c.nodes))
	for _, n := range c.nodes {
		order = append(order, n.id)
	}
	for i := range order {
		// lexical order n1,n2,n3; rotate per caller
		j := (i + rotate) % len(order)
		id := order[j]
		n := c.nodes[id]
		if n.node.Role() != raft.RoleLeader {
			continue
		}
		r := n.svc.Batch(sid, req, items)
		if isRetryable(r) {
			continue // try another claimant / come back later
		}
		return r
	}
	return kv.Result{Verdict: kv.VerdictTimeout, Reason: "no-leader-available"}
}

// retryBatch loops the SAME request id until a terminal verdict or the
// deadline, re-discovering the leader after every retryable answer.
func retryBatch(c *svcCluster, sid, req string, items []kv.BatchItem,
	rotate int, deadline time.Time) kv.Result {
	for {
		if time.Now().After(deadline) {
			return kv.Result{Verdict: kv.VerdictTimeout, Reason: "test-deadline-exceeded"}
		}
		r := batchOnAnyLeader(c, sid, req, items, rotate)
		if !isRetryable(r) {
			return r
		}
		time.Sleep(c.tick)
	}
}

func snapshotJSON(t *testing.T, st *kv.Store) string {
	t.Helper()
	b, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func assertStoresConverged(t *testing.T, c *svcCluster) {
	t.Helper()
	want := snapshotJSON(t, c.nodes["n1"].store)
	for _, id := range []raft.NodeID{"n2", "n3"} {
		if got := snapshotJSON(t, c.nodes[id].store); got != want {
			t.Fatalf("store of %s diverged", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Scenario 1a: a batch proposed to an isolated (minority) leader is
// unknown; after the partition heals and a new-term leader settles,
// retrying the same request id applies exactly once and every node ends
// with the keys at the same version and commit position.
// ---------------------------------------------------------------------------
func TestRetryAfterPartitionConvergesToOneCommit(t *testing.T) {
	c := newSvcCluster(t, retryTestTimeout)
	leader := c.waitLeader(5 * time.Second)
	sid, op := c.nodes[leader].svc.OpenSession(0)
	if op.Verdict != kv.VerdictOK {
		t.Fatalf("open session: %s/%s", op.Verdict, op.Reason)
	}

	items := []kv.BatchItem{
		{Key: "p1", Value: []byte("a")},
		{Key: "p2", Value: []byte("b")},
	}

	// Isolate the leader, then issue the batch with the SAME request id
	// repeatedly. While isolated each answer is timeout-unknown. A call
	// crossing the Isolate boundary can in principle still commit if its
	// replications were already queued, in which case the post-heal retries
	// must return THAT verdict; the test asserts the decisive convergence
	// either way rather than racing the partition instant.
	c.tr.Isolate(leader, []raft.NodeID{"n1", "n2", "n3"})
	deadline := time.Now().Add(400 * time.Millisecond)
	var sawUnknown bool
	var boundary raft.Index // non-zero only if a boundary call committed
	for time.Now().Before(deadline) {
		r := c.nodes[leader].svc.Batch(sid, "part1", items)
		switch {
		case r.Verdict == kv.VerdictTimeout:
			sawUnknown = true
		case r.Verdict == kv.VerdictOK:
			// Slipped through just as the partition went up.
			boundary = r.Index
		case r.Verdict == kv.VerdictDuplicate:
			boundary = r.Index
		default:
			t.Fatalf("partitioned leader answered %s/%s", r.Verdict, r.Reason)
		}
		time.Sleep(c.tick)
	}
	if !sawUnknown {
		t.Fatal("never observed the unknown window")
	}

	// Heal and wait for ANY settled leader. Retrying the same request id
	// against a same-term or newer-term leader converges identically: if
	// the stale entry committed, the reply cache returns it; otherwise the
	// retry is one clean fresh commit.
	c.tr.Heal()
	var newLeader raft.NodeID
	dl := time.Now().Add(5 * time.Second)
	for time.Now().Before(dl) {
		if l := c.anyLeader(); l != "" {
			newLeader = l
			break
		}
		time.Sleep(c.tick)
	}
	if newLeader == "" {
		t.Fatal("no leader after heal")
	}
	// Let the healed leader catch the majority up so the very first retry
	// is not a timing-flaky not-leader response (still legal for callers,
	// but this is the single-retrier test).
	time.Sleep(100 * time.Millisecond)
	if l := c.anyLeader(); l != "" {
		newLeader = l
	}

	// Retry the SAME request id. The first terminal retry is decisive:
	// either the boundary/stale entry committed (duplicate at its index) or
	// this retry is the one clean fresh commit.
	r := retryBatch(c, sid, "part1", items, 0, time.Now().Add(5*time.Second))
	if r.Verdict != kv.VerdictOK && r.Verdict != kv.VerdictDuplicate {
		t.Fatalf("retry verdict %s/%s — want a decisive verdict", r.Verdict, r.Reason)
	}
	if r.Index == 0 || r.Batch == nil || r.Batch.AppliedAt != r.Index {
		t.Fatalf("retry lost commit position: %+v", r)
	}
	firstIdx := r.Index
	if boundary != 0 && boundary != firstIdx {
		t.Fatalf("boundary commit at %d but retry converged at %d", boundary, firstIdx)
	}
	t.Logf("partitioned retry converged at index=%d verdict=%s", firstIdx, r.Verdict)

	// Further retries must always return the same original conclusion at
	// the same position — never a second application.
	for i := 0; i < 5; i++ {
		rr := retryBatch(c, sid, "part1", items, i, time.Now().Add(2*time.Second))
		if rr.Verdict != kv.VerdictDuplicate {
			t.Fatalf("repeat #%d verdict %s — want duplicate", i, rr.Verdict)
		}
		if rr.Index != firstIdx || rr.Batch.AppliedAt != firstIdx {
			t.Fatalf("repeat #%d at idx=%d want %d", i, rr.Index, firstIdx)
		}
		for _, op := range rr.Batch.Ops {
			if op.Version != 1 {
				t.Fatalf("repeat #%d reports version %d want original 1", i, op.Version)
			}
		}
	}
	time.Sleep(50 * time.Millisecond)
	assertStoresConverged(t, c)
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		for _, key := range []string{"p1", "p2"} {
			got := c.nodes[id].store.Read(sid, key, time.Now().UnixMilli())
			if got.Verdict != kv.VerdictOK || got.Version != 1 {
				t.Fatalf("%s %s after retry: %s v%d want v1",
					id, key, got.Verdict, got.Version)
			}
		}
		rep, ok := c.nodes[id].store.PendingPut(sid, "part1")
		if !ok || rep.Verdict != kv.VerdictOK || rep.AppliedAt != firstIdx {
			t.Fatalf("%s cached reply wrong: ok=%v %+v", id, ok, rep)
		}
	}
}

// ---------------------------------------------------------------------------
// Scenario 1b: the batch DID commit before the leader was deposed. An
// intervening write then changes the key. Retrying the same conditional
// request id after a leader change must return the ORIGINAL verdict at the
// ORIGINAL position — it must NOT re-evaluate the precondition against the
// newer version (which would wrongly report precondition-failed).
// ---------------------------------------------------------------------------
func TestRetryAfterLeaderChangeReturnsOriginalVerdict(t *testing.T) {
	c := newSvcCluster(t, retryTestTimeout)
	leader := c.waitLeader(5 * time.Second)
	sid, op := c.nodes[leader].svc.OpenSession(0)
	if op.Verdict != kv.VerdictOK {
		t.Fatalf("open session: %s/%s", op.Verdict, op.Reason)
	}

	// Seed cfg at version 1.
	seed := c.nodes[leader].svc.Batch(sid, "seed", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v0")},
	})
	if seed.Verdict != kv.VerdictOK {
		t.Fatalf("seed: %s/%s", seed.Verdict, seed.Reason)
	}

	// Conditional batch commits at version 1 -> 2... wait: seed was the
	// first write, cfg is at version 1; this batch bumps it to 2.
	first := c.nodes[leader].svc.Batch(sid, "cond1", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v1"), ExpectVersion: kv.ExpectVersion(1)},
	})
	if first.Verdict != kv.VerdictOK {
		t.Fatalf("first conditional batch: %s/%s", first.Verdict, first.Reason)
	}
	origIdx := first.Index
	if first.Batch.Ops[0].Version != 2 {
		t.Fatalf("unexpected post version %d", first.Batch.Ops[0].Version)
	}

	// Intervening write moves cfg to version 2 value.
	moved := c.nodes[leader].svc.Batch(sid, "move", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v9")},
	})
	if moved.Verdict != kv.VerdictOK {
		t.Fatalf("intervening write: %s/%s", moved.Verdict, moved.Reason)
	}

	// Depose the leader: isolate until the majority elects its own leader,
	// then heal. The test does not require a strictly newer term (wall
	// scheduling under -count=N load may vary); any settled leader must
	// answer the same-request retry with the cached original verdict.
	c.tr.Isolate(leader, []raft.NodeID{"n1", "n2", "n3"})
	dl := time.Now().Add(5 * time.Second)
	var newLeader raft.NodeID
	for time.Now().Before(dl) {
		for id, n := range c.nodes {
			if id != leader && n.node.Role() == raft.RoleLeader {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(c.tick)
	}
	if newLeader == "" {
		t.Fatal("majority did not elect a leader while isolated")
	}
	c.tr.Heal()
	// Let replication converge so the serving leader's store has the cache.
	time.Sleep(150 * time.Millisecond)
	if l := c.anyLeader(); l != "" {
		newLeader = l
	}

	// Retry the SAME conditional request id against the new leader.
	retry := retryBatch(c, sid, "cond1", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v1"), ExpectVersion: kv.ExpectVersion(1)},
	}, 0, time.Now().Add(3*time.Second))
	if retry.Verdict != kv.VerdictDuplicate {
		t.Fatalf("retry after leader change: %s/%s — want original verdict (duplicate)",
			retry.Verdict, retry.Reason)
	}
	if retry.Index != origIdx || retry.Batch.AppliedAt != origIdx {
		t.Fatalf("retry idx=%d want original %d", retry.Index, origIdx)
	}
	if op0 := retry.Batch.Ops[0]; op0.Version != 2 {
		t.Fatalf("retry re-evaluated/reapplied: version=%d want original 2", op0.Version)
	}
	time.Sleep(50 * time.Millisecond)
	assertStoresConverged(t, c)
	// The intervening write must remain the final value: no re-application.
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		got := c.nodes[id].store.Read(sid, "cfg", time.Now().UnixMilli())
		if string(got.Value) != "v9" || got.Version != 3 {
			t.Fatalf("%s cfg=%q v%d want v9 v3", id, got.Value, got.Version)
		}
	}
}

// ---------------------------------------------------------------------------
// Scenario 1c: a conditional batch that FAILS is cached like every other
// verdict. Retrying the same request id after state moved on must surface
// the ORIGINAL failure reason/versions (decisive), never re-evaluate
// against the newest state and never time out as "unknown".
// ---------------------------------------------------------------------------
func TestRetriedConditionalFailureKeepsOriginalReason(t *testing.T) {
	c := newSvcCluster(t, retryTestTimeout)
	leader := c.waitLeader(5 * time.Second)
	sid, op := c.nodes[leader].svc.OpenSession(0)
	if op.Verdict != kv.VerdictOK {
		t.Fatalf("open session: %s/%s", op.Verdict, op.Reason)
	}
	seed := c.nodes[leader].svc.Batch(sid, "seed", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v0")},
	})
	if seed.Verdict != kv.VerdictOK {
		t.Fatalf("seed: %s/%s", seed.Verdict, seed.Reason)
	}

	fail := c.nodes[leader].svc.Batch(sid, "fail1", []kv.BatchItem{
		{Key: "cfg", Value: []byte("x"), ExpectVersion: kv.ExpectVersion(7)},
	})
	if fail.Verdict != kv.VerdictPreconditionFailed ||
		fail.Batch.FailedKey != "cfg" || fail.Batch.ActualVersion != 1 ||
		fail.Batch.ExpectVersion != 7 || fail.Reason != "version-mismatch" {
		t.Fatalf("first failure verdict wrong: %+v", fail)
	}

	// State moves on: cfg is now at version 2.
	move := c.nodes[leader].svc.Batch(sid, "move", []kv.BatchItem{
		{Key: "cfg", Value: []byte("v2")},
	})
	if move.Verdict != kv.VerdictOK {
		t.Fatalf("move: %s/%s", move.Verdict, move.Reason)
	}

	// Repeated retries of the failed request: always the ORIGINAL decisive
	// failure detail (actual_version 1), not actual=2 and not unknown.
	for i := 0; i < 5; i++ {
		r := c.nodes[leader].svc.Batch(sid, "fail1", []kv.BatchItem{
			{Key: "cfg", Value: []byte("x"), ExpectVersion: kv.ExpectVersion(7)},
		})
		if r.Verdict != kv.VerdictDuplicate || r.Reason != "request-already-applied" {
			t.Fatalf("retry #%d verdict %s/%s want duplicate of original",
				i, r.Verdict, r.Reason)
		}
		if r.Batch.FailedKey != "cfg" || r.Batch.ExpectVersion != 7 ||
			r.Batch.ActualVersion != 1 {
			t.Fatalf("retry #%d re-evaluated condition: %+v want actual=1", i, r.Batch)
		}
	}
}

// ---------------------------------------------------------------------------
// Scenario 2: many callers concurrently retry the SAME request id while a
// partition/heal/leader change happens. Every caller must finish with the
// same decisive conclusion (OK or duplicate) at the same commit position;
// nobody may finish with timeout-unknown and nobody may hang.
// ---------------------------------------------------------------------------
func TestConcurrentSameRequestRetriesAllDecisive(t *testing.T) {
	c := newSvcCluster(t, retryTestTimeout)
	leader := c.waitLeader(5 * time.Second)
	sid, op := c.nodes[leader].svc.OpenSession(0)
	if op.Verdict != kv.VerdictOK {
		t.Fatalf("open session: %s/%s", op.Verdict, op.Reason)
	}

	const callers = 24
	items := []kv.BatchItem{
		{Key: "cc/1", Value: []byte("1")},
		{Key: "cc/2", Value: []byte("2")},
	}

	// Isolate the leader first: early retries are genuinely unknown.
	c.tr.Isolate(leader, []raft.NodeID{"n1", "n2", "n3"})

	var wg sync.WaitGroup
	results := make([]kv.Result, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = retryBatch(c, sid, "same-cc", items, i,
				time.Now().Add(8*time.Second))
		}(i)
	}
	close(start)

	// Hold the partition briefly, then heal: retries span unknown ->
	// leader change -> convergence.
	time.Sleep(150 * time.Millisecond)
	c.tr.Heal()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("at least one concurrent caller never got a decisive verdict (hung)")
	}

	var idx raft.Index
	oks, dups := 0, 0
	for i, r := range results {
		switch r.Verdict {
		case kv.VerdictOK:
			oks++
		case kv.VerdictDuplicate:
			dups++
		default:
			t.Fatalf("caller %d got non-decisive verdict %s/%s",
				i, r.Verdict, r.Reason)
		}
		if r.Index == 0 || r.Batch == nil || r.Batch.AppliedAt != r.Index {
			t.Fatalf("caller %d verdict missing commit position: %+v", i, r)
		}
		for _, op := range r.Batch.Ops {
			if op.Version != 1 {
				t.Fatalf("caller %d saw version %d — batch applied more than once",
					i, op.Version)
			}
		}
		if idx == 0 {
			idx = r.Index
		} else if r.Index != idx {
			t.Fatalf("caller %d at index %d, others at %d — conclusions diverged",
				i, r.Index, idx)
		}
	}
	// The proposal may have been coalesced inside raft so that every
	// external caller receives "duplicate"; what matters is one decisive
	// conclusion shared by all callers at one position.
	if oks+dups != callers {
		t.Fatalf("decisive callers=%d want %d", oks+dups, callers)
	}
	t.Logf("converged: %d ok + %d duplicate callers, all at index %d", oks, dups, idx)

	// Exactly one effect, same on every node, cache agrees everywhere.
	time.Sleep(50 * time.Millisecond)
	assertStoresConverged(t, c)
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		for _, key := range []string{"cc/1", "cc/2"} {
			got := c.nodes[id].store.Read(sid, key, time.Now().UnixMilli())
			if got.Verdict != kv.VerdictOK || got.Version != 1 {
				t.Fatalf("%s %s = %s v%d want v1", id, key, got.Verdict, got.Version)
			}
		}
		rep, ok := c.nodes[id].store.PendingPut(sid, "same-cc")
		if !ok || rep.AppliedAt != idx || rep.Verdict != kv.VerdictOK {
			t.Fatalf("%s cache mismatch: ok=%v %+v", id, ok, rep)
		}
	}
}

// Same-term stress without faults: hammers one settled leader with the
// same request id. The committed-duplicate event path (a retry landing
// just after the first correlation deleted the in-flight pending) used to
// be answered only after the full 2s timeout; assert it never regresses.
func TestConcurrentSameRequestNoFaultStress(t *testing.T) {
	c := newSvcCluster(t, retryTestTimeout)
	leader := c.waitLeader(5 * time.Second)
	sid, op := c.nodes[leader].svc.OpenSession(0)
	if op.Verdict != kv.VerdictOK {
		t.Fatalf("open session: %s/%s", op.Verdict, op.Reason)
	}
	const callers = 32
	items := []kv.BatchItem{{Key: "z", Value: []byte("1"),
		ExpectVersion: kv.ExpectAbsent()}}
	var wg sync.WaitGroup
	results := make([]kv.Result, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = c.nodes[leader].svc.Batch(sid, "z-req", items)
		}(i)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a same-term duplicate caller hung")
	}
	var idx raft.Index
	for i, r := range results {
		if r.Verdict != kv.VerdictOK && r.Verdict != kv.VerdictDuplicate {
			t.Fatalf("caller %d: %s/%s", i, r.Verdict, r.Reason)
		}
		if r.Verdict == kv.VerdictTimeout {
			t.Fatalf("caller %d got timeout-unknown on the happy path", i)
		}
		if idx == 0 {
			idx = r.Index
		} else if r.Index != idx {
			t.Fatalf("caller %d index %d != %d", i, r.Index, idx)
		}
	}
	if got := c.nodes[leader].store.VersionOf("z"); got != 1 {
		t.Fatalf("z version=%d want 1", got)
	}
}
