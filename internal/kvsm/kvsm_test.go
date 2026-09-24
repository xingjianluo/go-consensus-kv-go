package kvsm

import (
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

func baseCtx(ttlSec int) raft.ApplyCtx {
	return raft.ApplyCtx{Now: time.Unix(0, 0)}
}

func TestSessionIdempotencyAndExpiry(t *testing.T) {
	k := New()
	now := time.Unix(100, 0)
	ctx := raft.ApplyCtx{Now: now}
	reg := raft.Entry{Op: "register", ReqID: "s1", TTLNs: int64(10 * time.Second)}
	if r := k.Apply(reg, ctx); !r.OK || r.Value != "s1" {
		t.Fatalf("register: %+v", r)
	}
	put := raft.Entry{Op: "put", Session: "s1", ReqID: "q1", Key: "x", Value: "1"}
	if r := k.Apply(put, ctx); !r.OK {
		t.Fatalf("put1: %+v", r)
	}
	put.Value = "2"
	r := k.Apply(put, ctx)
	if !r.OK || !r.Duplicate {
		t.Fatalf("replay must be duplicate: %+v", r)
	}
	if v, _ := k.Get("x"); v != "1" {
		t.Fatalf("duplicate replay must not apply new value, got %q", v)
	}
	// After TTL the same write is rejected.
	expired := raft.ApplyCtx{Now: now.Add(11 * time.Second)}
	if r := k.Apply(put, expired); r.Code != raft.ErrSessionExpired {
		t.Fatalf("want session_expired, got %+v", r)
	}
	if v, _ := k.Get("x"); v != "1" {
		t.Fatalf("expired write must not mutate, got %q", v)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	k := New()
	ctx := baseCtx(10)
	_ = k.Apply(raft.Entry{Op: "register", ReqID: "s", TTLNs: 100000000000}, ctx)
	_ = k.Apply(raft.Entry{Op: "put", Session: "s", ReqID: "1", Key: "a", Value: "b"}, ctx)
	data := k.Snapshot()
	k2 := New()
	if err := k2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if v, ok := k2.Get("a"); !ok || v != "b" {
		t.Fatalf("restored value %q ok=%v", v, ok)
	}
	if !k2.SessionAlive("s", time.Unix(0, 0).UnixNano()) {
		t.Fatalf("session should survive snapshot restore")
	}
}
