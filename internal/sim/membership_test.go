package sim

import (
	"testing"
	"time"
)

// TestAddMember: a brand-new empty node joins; after the conf entry
// commits it must catch up and serve the pre-existing committed data.
func TestAddMember(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)
	mustPut(t, cl, "before", "join")

	c.StartUnconfiguredMember("n4")
	out := c.AddMember("add-n4-1", "n4")
	if !out.OK {
		t.Fatalf("add member: code=%s reason=%s", out.Code, out.Reason)
	}
	t.Logf("membership add committed at index=%d term=%d", out.Index, out.Term)
	c.runFor(800 * time.Millisecond)

	if got := c.State("n4"); len(got.Voters) != 4 || !containsStr(got.Voters, "n4") {
		t.Fatalf("n4 voters=%v after join", got.Voters)
	}
	if v, ok := c.nodes["n4"].kv.Get("before"); !ok || v != "join" {
		t.Fatalf("joined node missing pre-join data: %q ok=%v", v, ok)
	}
	if l := c.Leader(); l == "" {
		t.Fatalf("no leader after join")
	} else {
		t.Logf("after join leader=%s, voters=%v", l, c.Members())
	}
	// Writes still commit in the 4-node cluster.
	mustPut(t, cl, "after", "join")
	mustGet(t, cl, "after", "join")

	// No term regression: the old leader keeps a single leadership view.
	t.Logf("leader=%s term=%d", leader, c.State(leader).Term)
}

// TestRemoveMember: removing a node shrinks the majority; the removed node
// must not be able to elect itself (no dual leader) and data stays intact.
func TestRemoveMember(t *testing.T) {
	c := newTestCluster(t, 100)
	awaitLeader(t, c)
	cl := mustRegister(t, c)
	mustPut(t, cl, "keep", "me")

	victim := "n3"
	c.StartUnconfiguredMember("n4")
	if out := c.AddMember("add-n4-1", "n4"); !out.OK {
		t.Fatalf("add n4: %s", out.Reason)
	}
	c.runFor(800 * time.Millisecond)

	beforeVoters := len(c.Members())
	out := c.RemoveMember("rm-n3-1", victim)
	if !out.OK {
		t.Fatalf("remove member: code=%s reason=%s", out.Code, out.Reason)
	}
	c.runFor(1200 * time.Millisecond)

	// The authoritative view is the current leader's applied config.
	leader := c.Leader()
	applied := c.State(leader).Voters
	if containsStr(applied, victim) {
		t.Fatalf("removed node still in applied voters %v on leader %s",
			applied, leader)
	}
	if len(applied) != beforeVoters-1 {
		t.Fatalf("voter count=%d want %d on leader %s",
			len(applied), beforeVoters-1, leader)
	}
	// Cluster of 3 keeps committing after shrink.
	mustPut(t, cl, "after", "remove")
	mustGet(t, cl, "keep", "me")
	mustGet(t, cl, "after", "remove")

	// Isolated removed node cannot win an election without the quorum.
	st := c.State(victim)
	t.Logf("removed node: role=%s term=%d voters=%v", st.Role, st.Term, st.Voters)
	c.Partition([]string{victim}, []string{"n1", "n2", "n4"})
	c.runFor(1200 * time.Millisecond)
	c.Heal()
	c.runFor(400 * time.Millisecond)
	l := c.Leader()
	if l == victim {
		t.Fatalf("removed node must never become leader")
	}
	t.Logf("post-remove leader=%s (never %s), 已提交数据完整", l, victim)
}

// TestPartitionDuringAddThenCrash: combine membership change + partition +
// crash/restart. Final state must be one leader and identical committed data.
func TestPartitionDuringAddThenCrash(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)
	mustPut(t, cl, "base", "1")

	c.StartUnconfiguredMember("n4")
	c.Partition([]string{leader}, othersPlusN4(c, leader))
	// Change attempted on the isolated leader cannot commit.
	ch := c.registerResult("add-stuck")
	_ = c.nodes[leader].node.AddMember("add-stuck", "n4")
	stuck, ok := c.waitResult(ch, 500*time.Millisecond)
	if ok && stuck.OK {
		t.Fatalf("membership change in minority partition must not commit")
	}
	c.runFor(600 * time.Millisecond)
	c.Heal()
	c.runFor(600 * time.Millisecond)

	// The legitimate change now goes through the majority leader.
	out := c.AddMember("add-n4-final", "n4")
	if !out.OK {
		t.Fatalf("final add failed: %s %s", out.Code, out.Reason)
	}
	c.runFor(800 * time.Millisecond)

	// Crash the newly added and the old leader, restart both.
	c.Crash("n4")
	c.Crash(leader)
	c.runFor(800 * time.Millisecond)
	c.Restart("n4")
	c.Restart(leader)
	c.runFor(1200 * time.Millisecond)

	for _, id := range []string{"n1", "n2", "n3", "n4", leader} {
		if !containsStr(c.order, id) {
			continue
		}
		v, okv := c.nodes[id].kv.Get("base")
		if !okv || v != "1" {
			t.Fatalf("node %s base=%q ok=%v after combined failures", id, v, okv)
		}
	}
	if l := c.Leader(); l == "" {
		t.Fatalf("no single leader after recovery")
	} else {
		t.Logf("组合故障恢复后唯一 leader=%s voters=%v，base=1 全节点一致",
			l, c.Members())
	}
	t.Log("\n成员变更/组合故障关键事件:\n" + formatEvents(c.EventLog()))
}

func othersPlusN4(c *Cluster, leader string) []string {
	var out []string
	out = append(out, "n4")
	for _, id := range c.order {
		if id != leader && id != "n4" {
			out = append(out, id)
		}
	}
	return out
}
