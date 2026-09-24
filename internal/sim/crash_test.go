package sim

import (
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// TestCrashLeaderReplay: crash the leader right after a successful commit,
// restart it from durable storage; data must survive with no duplicate apply.
func TestCrashLeaderReplay(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)
	out1 := mustPut(t, cl, "a", "1")
	t.Logf("committed a=1 index=%d term=%d on %s", out1.Index, out1.Term, leader)

	c.Crash(leader)
	t.Logf("crashed leader %s (durable log/store kept, volatile state dropped)", leader)
	c.runFor(800 * time.Millisecond)

	c.Restart(leader)
	t.Logf("restarted %s from durable storage", leader)
	c.runFor(600 * time.Millisecond)

	st := c.State(leader)
	t.Logf("after restart: role=%s term=%d lastApplied=%d lastIndex=%d",
		st.Role, st.Term, st.LastApplied, st.LastIndex)
	if st.LastApplied < out1.Index {
		t.Fatalf("restarted node lost committed data: applied=%d < %d",
			st.LastApplied, out1.Index)
	}
	mustGet(t, cl, "a", "1")
}

// TestCrashAllRestartConverges: hard-kill every node simultaneously, then
// restart; the committed prefix must be identical on all nodes.
func TestCrashAllRestartConverges(t *testing.T) {
	c := newTestCluster(t, 100)
	awaitLeader(t, c)
	cl := mustRegister(t, c)
	for i := 0; i < 5; i++ {
		mustPut(t, cl, "key", string(rune('A'+i)))
	}
	last := mustPut(t, cl, "final", "Z")
	t.Logf("final commit index=%d term=%d", last.Index, last.Term)

	for _, id := range c.order {
		c.Crash(id)
	}
	t.Log("all nodes crashed simultaneously")
	for _, id := range c.order {
		c.Restart(id)
	}
	c.runFor(1200 * time.Millisecond)

	for _, id := range c.order {
		st := c.State(id)
		if st.CommitIndex < last.Index {
			t.Fatalf("node %s commit=%d < committed %d after restart",
				id, st.CommitIndex, last.Index)
		}
		v, ok := c.nodes[id].kv.Get("final")
		if !ok || v != "Z" {
			t.Fatalf("node %s KV final=%q ok=%v, want Z", id, v, ok)
		}
		t.Logf("node %s consistent: commit=%d applied=%d final=Z",
			id, st.CommitIndex, st.LastApplied)
	}
}

// TestSnapshotTruncatedRestart: force frequent snapshots, truncate the log,
// then crash a lagging follower; it must recover via InstallSnapshot to the
// same state and report the explainable path.
func TestSnapshotTruncatedRestart(t *testing.T) {
	c := newTestCluster(t, 4) // compact every 4 applied entries
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)

	// Isolate n3, let leader compact well past it.
	iso := pickFollower(c, leader)
	c.Partition([]string{leader, otherFollower(c, leader)}, []string{iso})
	for i := 0; i < 12; i++ {
		mustPut(t, cl, "bulk", string(rune('a'+i)))
	}
	st := c.State(leader)
	t.Logf("leader applied=%d firstIndex=%d snapPoint>=? (log compacted)",
		st.LastApplied, c.nodes[leader].store.FirstIndex())
	if c.nodes[leader].store.FirstIndex() <= 1 {
		t.Fatalf("expected leader log to be compacted, firstIndex=%d",
			c.nodes[leader].store.FirstIndex())
	}

	c.Crash(iso)
	c.Heal()
	c.Restart(iso)
	c.runFor(1200 * time.Millisecond)

	lv, _ := c.nodes[leader].kv.Get("bulk")
	rv, rok := c.nodes[iso].kv.Get("bulk")
	if !rok || rv != lv {
		t.Fatalf("lagging follower recovered wrong value: %q vs leader %q", rv, lv)
	}
	rst := c.State(iso)
	if rst.LastApplied < c.State(leader).LastApplied {
		t.Fatalf("restarted follower applied=%d behind leader=%d",
			rst.LastApplied, c.State(leader).LastApplied)
	}
	t.Logf("%s recovered via snapshot to applied=%d value=%q",
		iso, rst.LastApplied, rv)
	t.Log("\n快照相关事件:\n" + filterEvents(c.EventLog(), raft.EventKind("snapshot_install")))
}
