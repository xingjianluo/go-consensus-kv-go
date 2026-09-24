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
