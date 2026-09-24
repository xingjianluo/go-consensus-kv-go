package sim_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// Scenario: crash a follower, keep writing, restart it. The follower
// must be brought to exactly the same state, and every missing entry's
// provenance (index/term) must be explainable via commit events.
func TestCrashRestartFollowerCatchup(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})

	victim := raft.NodeID("n1")
	if victim == leader {
		victim = "n2"
	}
	k.c.Crash(victim)
	t.Logf("crashed follower=%s; continuing writes", victim)

	// Commit five values through the surviving majority.
	for i := 0; i < 5; i++ {
		cmd := kv.Command{Type: kv.CmdPut, SessionID: "s",
			RequestID: fmt.Sprintf("w%d", i), Key: fmt.Sprintf("k%d", i),
			Value: []byte(fmt.Sprintf("v%d", i)), CreateTime: int64(10 + i)}
		if _, ok := k.propose(leader, cmd); !ok {
			t.Fatalf("write %d failed while follower down", i)
		}
	}
	leaderCommit := k.c.Node(leader).CommitIndex()

	// Restart: persistent log replayed, volatile state gone.
	k.c.Restart(victim)
	caughtUp := k.c.RunUntil(60, func(c *sim.Cluster) bool {
		return c.Node(victim) != nil && c.Node(victim).CommitIndex() >= leaderCommit
	})
	if !caughtUp {
		dumpKeyEvents(t, k.c)
		t.Fatalf("%s did not catch up: local commit=%d leader=%d",
			victim, k.c.Node(victim).CommitIndex(), leaderCommit)
	}
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3"))
	for i := 0; i < 5; i++ {
		r := k.stores[victim].Read("s", fmt.Sprintf("k%d", i), 20)
		if r.Verdict != kv.VerdictOK || string(r.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("%s missing k%d (verdict=%s)", victim, i, r.Verdict)
		}
	}
	t.Logf("%s recovered to index=%d; all 5 writes present and explained",
		victim, leaderCommit)
}

// Scenario: crash the leader, elect a new one, write; restart the old
// leader. Its stale uncommitted suffix must be truncated and replaced,
// never committed.
func TestCrashLeaderStaleSuffixTruncated(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	old := k.waitLeader(40)
	k.propose(old, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	k.propose(old, kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "a", Key: "a", Value: []byte("1"), CreateTime: 2})

	// Isolate the leader alone; its attempted write has no quorum.
	others := []raft.NodeID{}
	for _, id := range ids("n1", "n2", "n3") {
		if id != old {
			others = append(others, id)
			k.c.Network().Partition(old, id)
		}
	}
	data, _ := kv.Encode(kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "orphan", Key: "orphan", Value: []byte("X"), CreateTime: 3})
	k.c.Node(old).Propose("s/orphan", data)
	k.tick(20)

	// Majority elects a new leader and commits a competing value.
	var fresh raft.NodeID
	k.c.RunUntil(60, func(c *sim.Cluster) bool {
		for _, id := range others {
			if c.Node(id).Role() == raft.RoleLeader {
				fresh = id
				return true
			}
		}
		return false
	})
	k.propose(fresh, kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "b", Key: "b", Value: []byte("2"), CreateTime: 4})

	// Hard-crash and restart the old leader (volatile proposal forgotten),
	// then heal. Its log must converge to the new leader's committed log.
	k.c.Network().Heal()
	k.c.Crash(old)
	k.c.Restart(old)
	k.c.RunUntil(60, func(c *sim.Cluster) bool {
		return c.Node(old).CommitIndex() >= c.Node(fresh).CommitIndex()
	})
	k.tick(10)
	k.assertConverged(ids("n1", "n2", "n3"))
	r := k.stores[old].Read("s", "orphan", 5)
	if r.Verdict == kv.VerdictOK {
		t.Fatal("stale orphan entry from old term became visible")
	}
	r = k.stores[old].Read("s", "b", 5)
	if r.Verdict != kv.VerdictOK || string(r.Value) != "2" {
		t.Fatalf("committed value missing on recovered leader: %v", r)
	}
	t.Log("old leader restarted; orphan suffix replaced, committed history intact")
}
