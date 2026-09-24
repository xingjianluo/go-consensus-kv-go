package sim_test

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// Scenario: after a membership change removes node X, a late AppendEntries
// response from X arriving at the leader must be ignored safely (it must
// not update replication progress or affect commit), recorded with the
// distinguishable reason not-a-voter.
func TestLateMessageFromRemovedMemberIgnored(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3", "n4"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	// Remove n4 (not leader).
	victim := raft.NodeID("n4")
	if _, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfRemoveNode, Node: victim,
	}); !ok {
		t.Fatal("remove n4 not accepted")
	}
	k.c.RunUntil(40, func(c *sim.Cluster) bool {
		return len(confOf(t, c, leader).Voters) == 3
	})
	k.tick(5)

	// Inject a late, successful MsgAppResp from the removed node at the
	// current term. It must not alter commit and must be recorded as a
	// not-a-voter drop.
	before := k.c.Node(leader).CommitIndex()
	k.c.Node(leader).Step(raft.Message{
		From: victim, To: leader, Type: raft.MsgAppResp,
		Term: k.c.Node(leader).Term(), Index: 1000,
	})
	k.c.DrainForTest(leader)
	after := k.c.Node(leader).CommitIndex()
	if after != before {
		t.Fatalf("late response from removed member moved commit %d -> %d", before, after)
	}
	found := false
	for _, e := range k.c.Events() {
		if e.Node == leader && e.Type == raft.EvtMessageDropped &&
			e.Reason == raft.RejectNotVoter {
			found = true
		}
	}
	if !found {
		t.Fatal("late removed-member response was not recorded as not-a-voter")
	}
	t.Log("late response from removed member safely ignored (not-a-voter); commit unchanged")
}

// Scenario: a vote request / append from an old-term peer after an
// election is ignored and causes the receiver neither to step back nor
// to grant leadership; the receiver replies with its current term.
func TestOldTermMessageCannotRollBackTerm(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	term := k.c.Node(leader).Term()

	// Force an election in a partition so terms diverge then heal.
	k.c.Network().Isolate(leader, []raft.NodeID{"n1", "n2", "n3"})
	k.tick(25)
	k.c.Network().Heal()
	k.tick(10)

	newTerm := k.c.Node(leader).Term()
	// An old leader (same process if it remained leader in minority) or
	// stale peer sends term < current: deliver directly.
	staleFrom := raft.NodeID("nX")
	old := raft.Message{From: staleFrom, To: leader, Type: raft.MsgApp,
		Term: 0, Index: 0, LogTerm: 0}
	k.c.Node(leader).Step(old)
	if k.c.Node(leader).Term() < newTerm {
		t.Fatalf("term rolled back from %d to %d by stale message", newTerm, k.c.Node(leader).Term())
	}
	if k.c.Node(leader).Role() == raft.RoleFollower && newTerm == term {
		t.Fatal("legitimate leader stepped down due to stale term-0 message")
	}
	t.Logf("stale term-0 append ignored; node kept term=%d role=%s",
		k.c.Node(leader).Term(), k.c.Node(leader).Role())
}
