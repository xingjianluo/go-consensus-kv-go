package kv_test

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// newSingle runs a full single-node raft+KV service with goroutine
// concurrency. Raft is driven via a tick channel pumped by the test
// (fast, no dependence on wall-clock scheduling), while the service
// still processes Ready/events on its own goroutine.
func newSingle(t *testing.T) (*kv.Service, *raft.Node, chan struct{}) {
	t.Helper()
	st := raft.NewMemStorage()
	if err := raft.Bootstrap(st, "solo", []raft.NodeID{"solo"}); err != nil {
		t.Fatal(err)
	}
	store := kv.NewStore()
	node := raft.NewNode(raft.Options{
		ID: "solo", Storage: st, ElectionTimeout: 3, HeartbeatTick: 1,
	})
	svc := kv.NewService(node, store)
	go svc.Run()
	// A high-frequency wall timer drives raft while the test runs. The
	// interval is far shorter than the 2s client timeout and is only used
	// for these concurrency tests (determinism is covered in sim tests).
	stop := make(chan struct{})
	tk := time.NewTicker(time.Millisecond)
	go func() {
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				node.Tick()
			}
		}
	}()
	// become leader
	for i := 0; i < 200 && node.Role() != raft.RoleLeader; i++ {
		time.Sleep(time.Millisecond)
	}
	if node.Role() != raft.RoleLeader {
		t.Fatal("single node never became leader")
	}
	t.Cleanup(func() { close(stop) })
	return svc, node, nil
}

func TestConcurrentDuplicatesApplyOnce(t *testing.T) {
	svc, node, _ := newSingle(t)
	sessionID, res := svc.OpenSession(0)
	if res.Verdict != kv.VerdictOK {
		t.Fatalf("open session: %s %s", res.Verdict, res.Reason)
	}

	const N = 20
	var wg sync.WaitGroup
	results := make([]kv.Result, N)
	// All goroutines use the SAME request id: at most one apply.
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = svc.Put(sessionID, "same-req", "color", []byte("blue"))
		}(i)
	}
	wg.Wait()

	okOrDup := 0
	for _, r := range results {
		if r.Verdict != kv.VerdictOK && r.Verdict != kv.VerdictDuplicate {
			t.Fatalf("unexpected verdict %s: %s", r.Verdict, r.Reason)
		}
		okOrDup++
	}
	if okOrDup != N {
		t.Fatalf("got %d terminal results want %d", okOrDup, N)
	}

	// Distinct request ids each apply exactly once.
	const M = 10
	var wg2 sync.WaitGroup
	for i := 0; i < M; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			r := svc.Put(sessionID, req(i), "key", []byte("x"))
			if r.Verdict != kv.VerdictOK {
				t.Errorf("put %d verdict %s: %s", i, r.Verdict, r.Reason)
			}
		}(i)
	}
	wg2.Wait()
	if got := node.CommitIndex(); got < raft.Index(M+2) {
		t.Fatalf("commit=%d want at least %d distinct entries", got, M+2)
	}
}

func req(i int) string {
	return "req-" + string(rune('a'+i))
}

func TestBatchServiceAtomicWrite(t *testing.T) {
	svc, _, _ := newSingle(t)
	sessionID, _ := svc.OpenSession(0)

	res := svc.Batch(sessionID, "b1", []kv.BatchItem{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2"), CheckVersion: true, ExpectedVersion: 0},
	})
	if res.Verdict != kv.VerdictOK {
		t.Fatalf("batch: %s %s", res.Verdict, res.Reason)
	}
	if len(res.Items) != 2 || res.Items[0].Version != 1 || res.Items[1].Version != 1 {
		t.Fatalf("items=%+v", res.Items)
	}
	if r := svc.Get(sessionID, "g1", "a"); string(r.Value) != "1" || r.Version != 1 {
		t.Fatalf("get a=%q ver=%d", r.Value, r.Version)
	}

	// Precondition failure: whole batch rejected, c must not appear.
	res2 := svc.Batch(sessionID, "b2", []kv.BatchItem{
		{Key: "c", Value: []byte("3")},
		{Key: "a", Value: []byte("9"), CheckVersion: true, ExpectedVersion: 99},
	})
	if res2.Verdict != kv.VerdictPreconditionFailed || res2.Reason != "version-mismatch:a" {
		t.Fatalf("res2=%s/%s", res2.Verdict, res2.Reason)
	}
	if r := svc.Get(sessionID, "g2", "c"); r.Verdict != kv.VerdictNotFound {
		t.Fatalf("half batch visible: c verdict=%s", r.Verdict)
	}

	// Duplicate request id returns the ORIGINAL verdict and index even
	// though state has moved.
	origIndex := res.Index
	later := svc.Put(sessionID, "p1", "a", []byte("later"))
	if later.Verdict != kv.VerdictOK {
		t.Fatalf("intervening put: %s", later.Verdict)
	}
	dup := svc.Batch(sessionID, "b1", []kv.BatchItem{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2"), CheckVersion: true, ExpectedVersion: 0},
	})
	if dup.Verdict != kv.VerdictDuplicate || dup.Index != origIndex {
		t.Fatalf("dup verdict=%s index=%d want duplicate@%d",
			dup.Verdict, dup.Index, origIndex)
	}
	if len(dup.Items) != 2 || dup.Items[1].Version != 1 {
		t.Fatalf("dup items=%+v", dup.Items)
	}
}

// TestBatchConcurrentCASRace fires two conditional updates based on the
// same observed version; exactly one may win, the loser gets a
// definitive precondition-failed verdict (not timeout/unknown).
func TestBatchConcurrentCASRace(t *testing.T) {
	svc, _, _ := newSingle(t)
	sessionID, _ := svc.OpenSession(0)
	seed := svc.Batch(sessionID, "seed", []kv.BatchItem{{Key: "cfg", Value: []byte("v0")}})
	if seed.Verdict != kv.VerdictOK {
		t.Fatal(seed.Verdict, seed.Reason)
	}

	const N = 8
	var wg sync.WaitGroup
	results := make([]kv.Result, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = svc.Batch(sessionID, "cas-"+req(i), []kv.BatchItem{{
				Key: "cfg", Value: []byte("winner-or-loser"),
				CheckVersion: true, ExpectedVersion: 1,
			}})
		}(i)
	}
	wg.Wait()

	wins, losses := 0, 0
	for i, r := range results {
		switch r.Verdict {
		case kv.VerdictOK:
			wins++
			if r.Items[0].Version != 2 {
				t.Fatalf("winner %d version=%d want 2", i, r.Items[0].Version)
			}
		case kv.VerdictPreconditionFailed:
			losses++
			if r.Reason != "version-mismatch:cfg" || r.Items[0].Version != 2 {
				t.Fatalf("loser %d reason=%s observed=%d", i, r.Reason, r.Items[0].Version)
			}
		default:
			t.Fatalf("client %d unexpected %s/%s", i, r.Verdict, r.Reason)
		}
	}
	if wins != 1 || losses != N-1 {
		t.Fatalf("wins=%d losses=%d want 1/%d", wins, losses, N-1)
	}
	final := svc.Get(sessionID, "gf", "cfg")
	if final.Version != 2 || string(final.Value) != "winner-or-loser" {
		t.Fatalf("final ver=%d val=%q", final.Version, final.Value)
	}
}

// TestBatchConcurrentSameRequestCoalesces: concurrent duplicates of one
// batch request all receive the same original verdict and the batch's
// writes apply exactly once (versions prove it).
func TestBatchConcurrentSameRequestCoalesces(t *testing.T) {
	svc, _, _ := newSingle(t)
	sessionID, _ := svc.OpenSession(0)
	const N = 12
	items := []kv.BatchItem{{Key: "z", Value: []byte("1"),
		CheckVersion: true, ExpectedVersion: 0}}
	var wg sync.WaitGroup
	results := make([]kv.Result, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = svc.Batch(sessionID, "same-batch", items)
		}(i)
	}
	wg.Wait()
	oks, dups := 0, 0
	for _, r := range results {
		switch r.Verdict {
		case kv.VerdictOK:
			oks++
		case kv.VerdictDuplicate:
			dups++
		default:
			t.Fatalf("unexpected %s/%s", r.Verdict, r.Reason)
		}
		if r.Index == 0 {
			t.Fatal("missing commit index")
		}
	}
	if oks+dups != N {
		t.Fatalf("oks=%d dups=%d", oks, dups)
	}
	if r := svc.Get(sessionID, "gz", "z"); r.Version != 1 {
		t.Fatalf("z applied more than once: version=%d", r.Version)
	}
}

func TestExpiredReadRejected(t *testing.T) {
	var svc *kv.Service
	svc, _, _ = newSingle(t)
	sessionID, _ := svc.OpenSession(20) // 20ms TTL
	time.Sleep(40 * time.Millisecond)
	r := svc.Get(sessionID, "r1", "k")
	if r.Verdict != kv.VerdictSessionExpired {
		t.Fatalf("expired read verdict=%s reason=%s", r.Verdict, r.Reason)
	}
}
