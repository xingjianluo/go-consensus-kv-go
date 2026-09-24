package sim_test

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

func dataOf(c kv.Command) []byte {
	b, _ := kv.Encode(c)
	return b
}

// Scenario: the same (session, request) is applied exactly once. The
// raft layer short-circuits an in-flight duplicate, and the state
// machine dedupes on log replay (e.g. after a follower restart).
func TestIdempotentDuplicateWriteAppliedOnce(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	cmd := kv.Command{Type: kv.CmdPut, SessionID: "s", RequestID: "dup",
		Key: "color", Value: []byte("red"), CreateTime: 100}
	first, ok := k.propose(leader, cmd)
	if !ok {
		t.Fatal("first write failed")
	}

	// In-flight duplicate: raft returns an explicit duplicate verdict.
	_, _ = k.propose(leader, cmd)
	k.tick(5)

	// Replay-level duplicate directly through the state machine.
	dupEntry := raft.Entry{Index: first + 1, Term: k.c.Node(leader).Term(),
		Kind: raft.EntryData, Data: dataOf(cmd)}
	res, handled := k.stores[leader].Apply(dupEntry)
	if !handled {
		t.Fatal("duplicate entry not handled")
	}
	if res.Verdict != kv.VerdictDuplicate {
		t.Fatalf("replayed duplicate verdict=%s want duplicate", res.Verdict)
	}

	rep, exists := k.stores[leader].PendingPut("s", "dup")
	if !exists {
		t.Fatal("reply cache missing the request")
	}
	if rep.AppliedAt != first {
		t.Fatalf("duplicate reapplied at %d, original index %d", rep.AppliedAt, first)
	}
	r := k.stores[leader].Read("s", "color", 100)
	if string(r.Value) != "red" {
		t.Fatalf("value=%q want red", string(r.Value))
	}
	k.assertConverged(ids("n1", "n2", "n3"))
	t.Logf("duplicate request applied once at index=%d; replay returned duplicate", first)
}

// Scenario: an expired session is rejected for reads and writes with an
// explicit reason, never silently served or accepted.
func TestExpiredSessionRejectedExplicitly(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", TTLMillis: 50, CreateTime: 0})
	k.propose(leader, kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "w", Key: "k", Value: []byte("v"), CreateTime: 10})

	r := k.stores[leader].Read("s", "k", 1000)
	if r.Verdict != kv.VerdictSessionExpired || r.Reason == "" {
		t.Fatalf("expected explicit session-expired, got %s/%s", r.Verdict, r.Reason)
	}
	late := kv.Command{Type: kv.CmdPut, SessionID: "s", RequestID: "late",
		Key: "k2", Value: []byte("x"), CreateTime: 1000}
	e := raft.Entry{Index: 100, Term: k.c.Node(leader).Term(),
		Kind: raft.EntryData, Data: dataOf(late)}
	res, _ := k.stores[leader].Apply(e)
	if res.Verdict != kv.VerdictSessionExpired {
		t.Fatalf("late write verdict=%s want session-expired", res.Verdict)
	}
	t.Logf("expired session reads/writes rejected: reason=%s", r.Reason)
}

// Scenario: a proposal on a follower is explicitly rejected as
// not-leader; it can never be mistaken for a commit.
func TestProposalOnFollowerRejected(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	var follower raft.NodeID
	for _, id := range ids("n1", "n2", "n3") {
		if id != leader {
			follower = id
			break
		}
	}
	k.c.Node(follower).Propose("s/x", dataOf(kv.Command{Type: kv.CmdPut,
		SessionID: "s", RequestID: "x", Key: "k", Value: []byte("v")}))
	k.tick(3)
	gotReject := false
	for _, e := range k.c.Events() {
		if e.Node == follower && e.Type == raft.EvtProposalResult &&
			e.ReqID == "s/x" && e.Status == raft.PropRejected &&
			e.Reason == raft.RejectNotLeader {
			gotReject = true
		}
	}
	if !gotReject {
		t.Fatal("follower proposal not explicitly rejected as not-leader")
	}
	t.Log("follower proposal rejected with not-leader and leader hint")
}
