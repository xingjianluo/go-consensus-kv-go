package sim

import (
	"sync"
	"testing"
	"time"
)

// TestIdempotentRetry: the same (session, ReqID) applied twice commits once
// and the second result is marked Duplicate; map value is unchanged.
func TestIdempotentRetry(t *testing.T) {
	c := newTestCluster(t, 100)
	awaitLeader(t, c)
	cl := mustRegister(t, c)

	out1 := cl.PutWithID("fixed-req-1", "dup", "first")
	if !out1.OK {
		t.Fatalf("first put: %+v", out1)
	}
	// Exactly one entry for this request must exist on every node.
	count := countReqEntries(c, "fixed-req-1")
	if count != len(c.order) {
		t.Fatalf("expected exactly one entry per node for req, got %d", count)
	}
	out2 := cl.PutWithID("fixed-req-1", "dup", "second-ignored")
	if !out2.OK || !out2.Duplicate {
		t.Fatalf("replay must succeed marked Duplicate, got %+v", out2)
	}
	if count2 := countReqEntries(c, "fixed-req-1"); count2 != len(c.order) {
		t.Fatalf("replay appended another entry: count=%d", count2)
	}
	mustGet(t, cl, "dup", "first")
	t.Logf("重复 ReqID 只提交一次 idx=%d，第二次返回 Duplicate=true，值仍为 first", out1.Index)
}

func countReqEntries(c *Cluster, reqID string) int {
	n := 0
	for _, id := range c.order {
		rt := c.nodes[id]
		first := rt.store.FirstIndex()
		last := rt.store.LastIndex()
		es, err := rt.store.Entries(first, last+1)
		if err != nil {
			continue
		}
		for _, e := range es {
			if e.ReqID == reqID {
				n++
			}
		}
	}
	return n
}

// TestExpiredSessionRejected: writes after TTL expiry are rejected with the
// distinguishable session_expired code; a stale read attempt likewise gets
// an explicit conclusion, never stale data presented as fresh.
func TestExpiredSessionRejected(t *testing.T) {
	c := newTestCluster(t, 100)
	awaitLeader(t, c)
	cl := NewClient(c)
	if err := cl.RegisterSession(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	mustPut(t, cl, "s", "alive")
	c.runFor(500 * time.Millisecond) // let session expire

	out := cl.Put("s", "late")
	t.Logf("expired write: code=%s reason=%q", out.Code, out.Reason)
	if out.Code != "session_expired" {
		t.Fatalf("want session_expired, got %s (%s)", out.Code, out.Reason)
	}
	if out2 := cl.Get("s"); out2.OK {
		t.Fatalf("read with expired session must be rejected, got %+v", out2)
	} else {
		t.Logf("expired read conclusion: code=%s reason=%q", out2.Code, out2.Reason)
	}

	// A fresh session can still operate normally.
	cl2 := mustRegister(t, c)
	mustPut(t, cl2, "s", "new")
	mustGet(t, cl2, "s", "new")
}

// TestConcurrentClients: many goroutine clients write distinct keys; every
// reported success must be durable and committed (queried after settling).
func TestConcurrentClients(t *testing.T) {
	c := newTestCluster(t, 100)
	awaitLeader(t, c)

	const n = 12
	var wg sync.WaitGroup
	errs := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := NewClient(c)
			if err := cl.RegisterSession(defaultTTL); err != nil {
				errs <- err.Error()
				return
			}
			out := cl.Put("k"+itoaTest(i), "v"+itoaTest(i))
			if !out.OK || !out.Committed {
				errs <- "client " + itoaTest(i) + " unsuccessful: " + string(out.Code)
				return
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent client failure: %s", e)
	}
	c.runFor(200 * time.Millisecond)
	for i := 0; i < n; i++ {
		mustGet(t, NewClientMustRegister(t, c), "k"+itoaTest(i), "v"+itoaTest(i))
	}
	t.Logf("%d 个并发客户端的写入全部以多数派提交并可线性一致读出", n)
}

func NewClientMustRegister(t *testing.T, c *Cluster) *Client {
	return mustRegister(t, c)
}

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	b := []byte{}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// TestReadFromNonLeaderRejectedExplainably: contacting a follower yields a
// not_leader outcome with a leader hint rather than possibly stale data.
func TestReadFromNonLeaderRejectedExplainably(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	follow := pickFollower(c, leader)
	ch := c.registerResult("read-follower-1")
	_ = c.nodes[follow].node.Propose(readReq("read-follower-1", "anything"))
	out, ok := c.waitResult(ch, 500*time.Millisecond)
	if !ok || out.OK || out.Code != "not_leader" {
		t.Fatalf("follower read must be not_leader, got ok=%v code=%s", ok, out.Code)
	}
	if out.Leader == "" {
		t.Fatalf("rejection must carry a leader hint")
	}
	t.Logf("follower 读被拒绝：code=not_leader, hint=%s（避免返回可能过期的数据）", out.Leader)
}
