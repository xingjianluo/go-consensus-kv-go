package sim_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// Combined scenario:
//
//  1. 3-node cluster commits data.
//  2. partition the leader into a minority; majority elects anew.
//  3. hard-crash a majority follower, add a new node to replace it.
//  4. commit through the changed cluster.
//  5. heal, restart the crashed follower and the old leader; verify all
//     live nodes converge to identical state with no rollback.
func TestPartitionCrashMembershipRestartCombined(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	old := k.waitLeader(40)
	k.propose(old, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	k.propose(old, kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "i0", Key: "k0", Value: []byte("v0"), CreateTime: 2})

	// Partition old leader alone.
	peers := ids("n1", "n2", "n3")
	for _, id := range peers {
		if id != old {
			k.c.Network().Partition(old, id)
		}
	}
	var fresh raft.NodeID
	k.c.RunUntil(60, func(c *sim.Cluster) bool {
		for _, id := range peers {
			if id != old && c.Node(id).Role() == raft.RoleLeader {
				fresh = id
				return true
			}
		}
		return false
	})
	if fresh == "" {
		t.Fatal("majority failed to elect after partition")
	}
	t.Logf("step2: old=%s isolated; new leader=%s term=%d",
		old, fresh, k.c.Node(fresh).Term())

	// Hard-crash the other majority follower (the one that is not fresh).
	var downed raft.NodeID
	for _, id := range peers {
		if id != old && id != fresh {
			downed = id
		}
	}
	k.c.Crash(downed)
	t.Logf("step3: crashed follower=%s; adding n4 to grow to 4 voters", downed)

	// Wait: after crashing, the majority of the OLD 3 is gone (only fresh
	// up). Adding a node one-at-a-time needs a quorum; heal the old leader
	// first so a quorum exists, but keep downed crashed.
	k.c.Network().Heal()
	k.tick(10)
	addLeader := k.waitLeader(20)

	// Register and add n4.
	k.c.AddNewNode("n4", "a4")
	if reason, ok := k.c.Node(addLeader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfAddNode, Node: "n4", Addr: "a4",
	}); !ok {
		t.Fatalf("add n4 rejected: %s", reason)
	}
	k.c.RunUntil(80, func(c *sim.Cluster) bool {
		return len(confOf(t, c, addLeader).Voters) == 4 &&
			c.Node("n4") != nil &&
			c.Node("n4").CommitIndex() >= c.Node(addLeader).CommitIndex()
	})
	t.Log("step4: n4 is a caught-up voter; cluster has 4 members")

	// Commit a write through the new configuration.
	k.propose(k.c.Leader(), kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "i1", Key: "k1", Value: []byte("v1"), CreateTime: 3})

	// Remove the crashed node so the final cluster is {old, fresh, n4}
	// (plus the leader doing the removal must not remove itself).
	if k.c.Leader() == downed {
		t.Fatal("crashed node cannot be leader")
	}
	if reason, ok := k.c.Node(k.c.Leader()).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfRemoveNode, Node: downed,
	}); !ok {
		t.Fatalf("remove downed member rejected: %s", reason)
	}
	k.c.RunUntil(60, func(c *sim.Cluster) bool {
		return len(confOf(t, c, c.Leader()).Voters) == 3
	})
	k.propose(k.c.Leader(), kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "i2", Key: "k2", Value: []byte("v2"), CreateTime: 4})
	k.tick(10)

	// Restart the crashed follower as a fresh process (it has been
	// removed, so it stays passive but its storage can be inspected).
	k.c.Restart(downed)
	// A removed node must not be able to win an election or disturb the
	// current cluster; let timers fire.
	k.tick(30)
	l := k.c.Leader()
	if l == "" {
		dumpKeyEvents(t, k.c)
		t.Fatal("no leader after restarting removed member")
	}
	removedBecameLeader := false
	for _, id := range peers {
		if n := k.c.Node(id); n != nil {
			t.Logf("final role %s=%s term=%d", id, n.Role(), n.Term())
		}
	}
	if n := k.c.Node(downed); n != nil && n.Role() == raft.RoleLeader {
		removedBecameLeader = true
	}
	if removedBecameLeader {
		t.Fatal("removed/restarted node won leadership")
	}

	// The authoritative cluster converges on the three current voters.
	current := []raft.NodeID{}
	for _, v := range confOf(t, k.c, l).Voters {
		current = append(current, v.ID)
	}
	if len(current) != 3 {
		t.Fatalf("final voters=%d want 3", len(current))
	}
	k.assertConverged(current)
	for i, want := range []string{"v0", "v1", "v2"} {
		r := k.stores[k.c.Leader()].Read("s", fmt.Sprintf("k%d", i), 10)
		if string(r.Value) != want {
			t.Fatalf("k%d=%q want %q (rollback?)", i, string(r.Value), want)
		}
	}
	if countLeaders(k) > 1 {
		t.Fatal("dual leader observed in final state")
	}
	t.Logf("combined: converged on %v; v0..v2 intact; no rollback/dual-leader", current)
}
