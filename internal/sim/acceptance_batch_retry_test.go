package sim_test

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// svcNode pairs one raft node with its kv.Service and store.
type svcNode struct {
	node  *raft.Node
	svc   *kv.Service
	store *kv.Store
}

// svcCluster runs a REAL kv.Service per simulated node: services consume
// Ready/event work on their own goroutine (exactly as the production
// driver does), while a high-frequency tick pump drives the deterministic
// network/raft clock. This exercises the service-layer waiter coalescing
// and timeout reconciliation under partitions and leadership changes.
type svcCluster struct {
	c     *sim.Cluster
	nodes map[raft.NodeID]*svcNode
	stop  chan struct{}
	t     *testing.T
}

func newSvcCluster(t *testing.T) *svcCluster {
	t.Helper()
	c := sim.NewCluster(sim.ClusterOptions{
		IDs:            ids("n1", "n2", "n3"),
		ElectionTicks:  8,
		HeartbeatTicks: 1,
		ManualDrain:    true, // services own Ready consumption + Apply
	})
	sc := &svcCluster{c: c, nodes: map[raft.NodeID]*svcNode{},
		stop: make(chan struct{}), t: t}
	for _, id := range ids("n1", "n2", "n3") {
		st := kv.NewStore()
		n := c.Node(id)
		svc := kv.NewService(n, st)
		// Generous relative to the 1ms tick pump: a healthy quorum commit
		// lands within a handful of ticks, so this never fires spuriously,
		// while a minority leader still times out promptly.
		svc.SetWriteTimeout(150 * time.Millisecond)
		svc.MessageHook = func(m raft.Message) {
			if m.From == "" {
				m.From = id
			}
			c.Network().Send(m, -1)
		}
		go svc.Run()
		sc.nodes[id] = &svcNode{node: n, svc: svc, store: st}
	}
	tk := time.NewTicker(time.Millisecond)
	go func() {
		defer tk.Stop()
		for {
			select {
			case <-sc.stop:
				return
			case <-tk.C:
				c.Tick()
			}
		}
	}()
	t.Cleanup(func() {
		close(sc.stop)
		for _, nd := range sc.nodes {
			nd.svc.Close()
		}
	})
	return sc
}

// leaderFor returns a live node that currently believes it is leader;
// when more than one does (the transient split-brain window) the one in
// the highest term wins, so callers never propose to a stale leader.
func (sc *svcCluster) leaderFor() (*svcNode, raft.NodeID) {
	var best *svcNode
	var bestID raft.NodeID
	var bestTerm raft.Term
	for id, nd := range sc.nodes {
		if nd.node.Role() != raft.RoleLeader {
			continue
		}
		if tm := nd.node.Term(); best == nil || tm > bestTerm {
			best, bestID, bestTerm = nd, id, tm
		}
	}
	return best, bestID
}

func (sc *svcCluster) waitLeader(d time.Duration) (*svcNode, raft.NodeID) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if nd, id := sc.leaderFor(); nd != nil {
			return nd, id
		}
		time.Sleep(time.Millisecond)
	}
	sc.t.Fatalf("no leader within %s", d)
	return nil, ""
}

// retryBatch issues the same (session, request) batch, following
// leadership changes and treating timeouts as "unknown, retry", until a
// decisive verdict is obtained or the deadline passes. A decisive
// verdict is any terminal application result; not-leader/timeout are
// transient transport-level outcomes and never returned to the caller.
func (sc *svcCluster) retryBatch(sid, rid string, items []kv.BatchItem,
	deadline time.Time) kv.Result {
	var last kv.Result
	for time.Now().Before(deadline) {
		nd, _ := sc.leaderFor()
		if nd == nil {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		r := nd.svc.Batch(sid, rid, items)
		last = r
		switch r.Verdict {
		case kv.VerdictOK, kv.VerdictDuplicate,
			kv.VerdictPreconditionFailed, kv.VerdictInvalidBatch:
			return r
		case kv.VerdictNotLeader, kv.VerdictTimeout:
			time.Sleep(2 * time.Millisecond)
		default:
			// Session expiry etc. is also decisive, but must not occur in
			// these scenarios.
			return r
		}
	}
	return last
}

func (sc *svcCluster) pump(d time.Duration) { time.Sleep(d) }

func (sc *svcCluster) assertStoresConverged(keys ...string) {
	snap0 := mustSnapshot(sc.t, sc.nodes[ids("n1", "n2", "n3")[0]].store)
	for _, id := range ids("n1", "n2", "n3")[1:] {
		if got := mustSnapshot(sc.t, sc.nodes[id].store); got != snap0 {
			sc.t.Fatalf("store divergence:\nn1=%s\n%s=%s", snap0, id, got)
		}
	}
	for _, key := range keys {
		r := sc.nodes["n1"].store.Read("s", key, 0)
		for _, id := range ids("n1", "n2", "n3")[1:] {
			o := sc.nodes[id].store.Read("s", key, 0)
			if o.Verdict != r.Verdict || string(o.Value) != string(r.Value) ||
				o.Version != r.Version {
				sc.t.Fatalf("key %s diverges on %s: %+v vs %+v", key, id, o, r)
			}
		}
	}
}

func retryIterations() int {
	if v := os.Getenv("KVBATCH_RETRY_ITERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 12
}

// TestBatchRetryConvergenceAcrossPartitionAndLeaderChange repeatedly runs:
//
//  1. a batch whose proposal is stranded on a minority-partitioned
//     leader (caller sees timeout-unknown);
//  2. partition heal + new-term leader;
//  3. retry with the SAME request id -> one decisive verdict.
//
// It also fans out many concurrent callers on the same request id across
// the heal boundary: every caller must end with the SAME decisive
// conclusion (same applied-at index and per-key versions), none may end
// "unknown", none may wait forever, and a conditional update whose
// precondition does not hold must give every loser the exact failure
// cause rather than an unknown verdict.
func TestBatchRetryConvergenceAcrossPartitionAndLeaderChange(t *testing.T) {
	iters := retryIterations()
	for iter := 0; iter < iters; iter++ {
		t.Run("iter", func(t *testing.T) {
			sc := newSvcCluster(t)
			nd, leader := sc.waitLeader(3 * time.Second)
			sid, res := nd.svc.OpenSession(0)
			if res.Verdict != kv.VerdictOK {
				t.Fatalf("open session: %s/%s", res.Verdict, res.Reason)
			}

			// --- Scenario A: single caller, unknown during partition,
			// decisive convergence after heal/new leader. ---
			sc.c.Network().Isolate(leader, ids("n1", "n2", "n3"))
			itemsA := []kv.BatchItem{
				{Key: "p1", Value: []byte("a")},
				{Key: "p2", Value: []byte("b")},
			}
			// First attempt against the stranded leader: it cannot commit,
			// so the honest verdict is timeout-unknown (never success).
			stranded := nd.svc.Batch(sid, "batch-a", itemsA)
			if stranded.Verdict != kv.VerdictTimeout {
				t.Fatalf("[iter %d] stranded proposal verdict=%s/%s want timeout-unknown",
					iter, stranded.Verdict, stranded.Reason)
			}
			// While isolated, the majority elects a new-term leader.
			sc.pump(120 * time.Millisecond)
			sc.c.Network().Heal()
			oldTerm := nd.node.Term()
			deadline := time.Now().Add(5 * time.Second)
			var settled *svcNode
			for time.Now().Before(deadline) {
				if cand, _ := sc.leaderFor(); cand != nil && cand.node.Term() > oldTerm {
					settled = cand
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			if settled == nil {
				t.Fatalf("[iter %d] no new-term leader after heal", iter)
			}
			rA := sc.retryBatch(sid, "batch-a", itemsA, deadline)
			if rA.Verdict != kv.VerdictOK {
				t.Fatalf("[iter %d] retry A verdict=%s/%s want ok",
					iter, rA.Verdict, rA.Reason)
			}
			if rA.Batch == nil || rA.Batch.AppliedAt != rA.Index {
				t.Fatalf("[iter %d] retry A lost commit position: %+v", iter, rA)
			}
			idxA := rA.Batch.AppliedAt
			sc.pump(80 * time.Millisecond)
			sc.assertStoresConverged("p1", "p2")
			for _, id := range ids("n1", "n2", "n3") {
				rep, ok := sc.nodes[id].store.PendingPut(sid, "batch-a")
				if !ok {
					t.Fatalf("[iter %d] %s missing cached verdict for batch-a", iter, id)
				}
				if rep.Verdict != kv.VerdictOK || rep.AppliedAt != idxA {
					t.Fatalf("[iter %d] %s cached %s@%d want ok@%d",
						iter, id, rep.Verdict, rep.AppliedAt, idxA)
				}
			}

			// --- Scenario B: many concurrent callers, SAME request id,
			// started while partitioned and fanning across the heal and
			// the leadership change. ---
			newLeader := sc.c.Leader()
			if newLeader == "" {
				t.Fatalf("[iter %d] no leader for scenario B", iter)
			}
			sc.c.Network().Isolate(newLeader, ids("n1", "n2", "n3"))
			itemsB := []kv.BatchItem{
				{Key: "q1", Value: []byte("1")},
				{Key: "q2", Value: []byte("2")},
			}
			const N = 12
			results := make([]kv.Result, N)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results[i] = sc.retryBatch(sid, "batch-b", itemsB,
						time.Now().Add(6*time.Second))
				}(i)
			}
			close(start)
			// Let roughly half the callers burn attempts inside the
			// partition, then heal; the rest cross the boundary.
			sc.pump(120 * time.Millisecond)
			sc.c.Network().Heal()
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(8 * time.Second):
				t.Fatalf("[iter %d] concurrent retriers did not all converge", iter)
			}
			sc.pump(80 * time.Millisecond)
			sc.assertStoresConverged("q1", "q2")
			var idxB raft.Index
			for i, r := range results {
				if r.Verdict != kv.VerdictOK && r.Verdict != kv.VerdictDuplicate {
					t.Fatalf("[iter %d] caller %d stuck on non-decisive %s/%s",
						iter, i, r.Verdict, r.Reason)
				}
				if r.Batch == nil || r.Batch.AppliedAt == 0 {
					t.Fatalf("[iter %d] caller %d got verdict without commit detail: %+v",
						iter, i, r)
				}
				if i == 0 {
					idxB = r.Batch.AppliedAt
				} else if r.Batch.AppliedAt != idxB {
					t.Fatalf("[iter %d] caller %d applied-at=%d differs from %d",
						iter, i, r.Batch.AppliedAt, idxB)
				}
				if len(r.Batch.Ops) != 2 || r.Batch.Ops[0].Version != 1 ||
					r.Batch.Ops[1].Version != 1 {
					t.Fatalf("[iter %d] caller %d non-original versions: %+v",
						iter, i, r.Batch.Ops)
				}
			}

			// --- Scenario C: conditional update whose precondition can
			// never hold. Across partition/heal every concurrent caller
			// must converge onto the SAME decisive failure with the exact
			// reason — never an unknown verdict. ---
			seed := sc.retryBatch(sid, "seed-cfg", []kv.BatchItem{
				{Key: "cfg", Value: []byte("v1")},
			}, time.Now().Add(5*time.Second))
			if seed.Verdict != kv.VerdictOK {
				t.Fatalf("[iter %d] seed: %s/%s", iter, seed.Verdict, seed.Reason)
			}
			leader2 := sc.c.Leader()
			sc.c.Network().Isolate(leader2, ids("n1", "n2", "n3"))
			itemsC := []kv.BatchItem{
				{Key: "cfg", Value: []byte("v2"), ExpectVersion: kv.ExpectVersion(3)},
			}
			const M = 8
			cres := make([]kv.Result, M)
			var wg2 sync.WaitGroup
			start2 := make(chan struct{})
			for i := 0; i < M; i++ {
				wg2.Add(1)
				go func(i int) {
					defer wg2.Done()
					<-start2
					cres[i] = sc.retryBatch(sid, "batch-c", itemsC,
						time.Now().Add(6*time.Second))
				}(i)
			}
			close(start2)
			sc.pump(120 * time.Millisecond)
			sc.c.Network().Heal()
			done2 := make(chan struct{})
			go func() { wg2.Wait(); close(done2) }()
			select {
			case <-done2:
			case <-time.After(8 * time.Second):
				t.Fatalf("[iter %d] conditional retriers did not all converge", iter)
			}
			sc.pump(80 * time.Millisecond)
			failed, dup := 0, 0
			for i, r := range cres {
				// The first application returns the decisive failure; any
				// caller whose retry lands after that gets Duplicate with
				// the SAME embedded failure detail. Both are terminal, and
				// neither may be a timeout-unknown.
				switch r.Verdict {
				case kv.VerdictPreconditionFailed:
					failed++
					if r.Reason != "version-mismatch" {
						t.Fatalf("[iter %d] conditional caller %d reason=%s want version-mismatch",
							iter, i, r.Reason)
					}
				case kv.VerdictDuplicate:
					dup++
				default:
					t.Fatalf("[iter %d] conditional caller %d: %s/%s want a decisive failure",
						iter, i, r.Verdict, r.Reason)
				}
				if r.Batch == nil || r.Batch.FailedKey != "cfg" ||
					r.Batch.ExpectVersion != 3 || r.Batch.ActualVersion != 1 {
					t.Fatalf("[iter %d] conditional caller %d lost failure detail: reason=%s %+v",
						iter, i, r.Reason, r.Batch)
				}
			}
			if failed == 0 {
				t.Fatalf("[iter %d] no caller observed the decisive precondition failure", iter)
			}
			t.Logf("[iter %d] conditional failure: %d direct failures, %d duplicates of the same verdict",
				iter, failed, dup)

			// Once the failure verdict is cached, any further same-id
			// retry on ANY node's leader hits the fast path: Duplicate
			// carrying the exact original failure, never re-evaluated
			// against current state and never an unknown verdict.
			if nd2, _ := sc.leaderFor(); nd2 != nil {
				rDup := nd2.svc.Batch(sid, "batch-c", itemsC)
				if rDup.Verdict != kv.VerdictDuplicate || rDup.Batch == nil ||
					rDup.Batch.FailedKey != "cfg" || rDup.Batch.ExpectVersion != 3 ||
					rDup.Batch.ActualVersion != 1 {
					t.Fatalf("[iter %d] fast-path retry after cached failure: %s/%s %+v",
						iter, rDup.Verdict, rDup.Reason, rDup.Batch)
				}
			}
			// And a successful batch's fast-path retry returns the original
			// success with the original commit position, even via a
			// different leader than the one that committed it.
			if nd2, _ := sc.leaderFor(); nd2 != nil {
				rDupOK := nd2.svc.Batch(sid, "batch-a", itemsA)
				if rDupOK.Verdict != kv.VerdictDuplicate ||
					rDupOK.Batch == nil || rDupOK.Batch.AppliedAt != idxA {
					t.Fatalf("[iter %d] fast-path retry of batch-a: %s/%s %+v want duplicate@%d",
						iter, rDupOK.Verdict, rDupOK.Reason, rDupOK.Batch, idxA)
				}
			}
			sc.assertStoresConverged("cfg")
			if v := sc.nodes["n1"].store.Read(sid, "cfg", 0); string(v.Value) != "v1" ||
				v.Version != 1 {
				t.Fatalf("[iter %d] failed conditional batch mutated cfg: %q v%d",
					iter, v.Value, v.Version)
			}
		})
	}
}
