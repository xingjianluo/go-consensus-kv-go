package kv

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// A write (single key) that already has a cached verdict must return that
// original verdict on retry EVEN IF the session has since expired or been
// closed. Returning a fresh session-expired verdict instead would leave
// the caller unable to distinguish "committed" from "failed".
func TestRetryAfterSessionExpiryReturnsOriginalVerdict(t *testing.T) {
	st := NewStore()
	openSM(t, st)
	// Successful conditional batch at index 2.
	ok := applyBatch(t, st, "s", "b1", 2,
		BatchOp{Key: "k", Value: []byte("v"), ExpectVersion: ExpectAbsent()})
	if ok.Verdict != VerdictOK {
		t.Fatalf("first batch: %s/%s", ok.Verdict, ok.Reason)
	}
	// A single-key put, also while alive.
	putOK, handled := st.Apply(batchEntry(Command{Type: CmdPut, SessionID: "s",
		RequestID: "p1", Key: "p", Value: []byte("1"), CreateTime: 1}, 3))
	if !handled || putOK.Verdict != VerdictOK || putOK.Index != 3 || putOK.Version != 1 {
		t.Fatalf("first put: %+v handled=%v", putOK, handled)
	}

	// Then the session goes idle past its TTL / is closed.
	st.mu.Lock()
	st.sessions["s"].Closed = true
	st.mu.Unlock()

	// Batch retry as a fresh log entry, on any node, after expiry: still
	// the original verdict, at the original index, original versions.
	retry := applyBatch(t, st, "s", "b1", 99,
		BatchOp{Key: "k", Value: []byte("v"), ExpectVersion: ExpectAbsent()})
	if retry.Verdict != VerdictDuplicate || retry.Index != 2 {
		t.Fatalf("expired-session retry: %s/%s idx=%d want duplicate@2",
			retry.Verdict, retry.Reason, retry.Index)
	}
	if retry.Batch == nil || retry.Batch.AppliedAt != 2 ||
		retry.Batch.Ops[0].Version != 1 {
		t.Fatalf("expired-session retry lost original detail: %+v", retry.Batch)
	}

	// Put retry after close: original verdict/index/version.
	applyPut := func(req string, idx raft.Index, create int64) Result {
		r, _ := st.Apply(batchEntry(Command{Type: CmdPut, SessionID: "s",
			RequestID: req, Key: "p", Value: []byte("1"), CreateTime: create}, idx))
		return r
	}
	again := applyPut("p1", 100, 1)
	if again.Verdict != VerdictDuplicate || again.Index != 3 || again.Version != 1 {
		t.Fatalf("closed-session put retry: %s/%s idx=%d v%d want duplicate@3 v1",
			again.Verdict, again.Reason, again.Index, again.Version)
	}
	// A genuinely NEW request on the closed session is still rejected.
	fresh := applyPut("p2", 101, 1)
	if fresh.Verdict != VerdictSessionExpired || fresh.Reason != "closed" {
		t.Fatalf("new put on closed session: %s/%s want session-expired/closed",
			fresh.Verdict, fresh.Reason)
	}
}

// decisiveResultFromReply must preserve the ORIGINAL verdict (including a
// precondition failure and its exact reason/detail) when a timed-out
// caller is reconciled against the reply cache.
func TestDecisiveResultFromReplyPreservesOriginal(t *testing.T) {
	br := &BatchResult{AppliedAt: 7, FailedKey: "cfg", ExpectVersion: 3, ActualVersion: 1}
	rep := Reply{Verdict: VerdictPreconditionFailed, Reason: "version-mismatch",
		AppliedAt: 7, Batch: br}
	r := decisiveResultFromReply(rep)
	if r.Verdict != VerdictPreconditionFailed || r.Reason != "version-mismatch" ||
		r.Index != 7 || r.Batch != br {
		t.Fatalf("reconciled verdict lost original: %+v", r)
	}

	// A successful reply reconstructs as a decisive OK at its commit.
	ok := Reply{Verdict: VerdictOK, AppliedAt: 5, Version: 2,
		Batch: &BatchResult{AppliedAt: 5}}
	r2 := decisiveResultFromReply(ok)
	if r2.Verdict != VerdictOK || r2.Index != 5 || r2.Version != 2 {
		t.Fatalf("reconciled ok verdict wrong: %+v", r2)
	}

	// Invalid batch keeps its machine-readable structural reason.
	inv := Reply{Verdict: VerdictInvalidBatch, Reason: "duplicate-key",
		AppliedAt: 9, Batch: &BatchResult{AppliedAt: 9}}
	r3 := decisiveResultFromReply(inv)
	if r3.Verdict != VerdictInvalidBatch || r3.Reason != "duplicate-key" ||
		r3.Index != 9 {
		t.Fatalf("reconciled invalid verdict wrong: %+v", r3)
	}
}
