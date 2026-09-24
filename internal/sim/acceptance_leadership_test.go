package sim_test

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// Scenario 1: at most one leader per term, and a leader in a minority
// partition cannot commit; when healed, the stale leader steps down and
// never overwrites committed data.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3", "n4", "n5"}, 0)
	leader := k.waitLeader(40)
	t.Logf("initial leader=%s term=%d commit=%d", leader,
		k.c.Node(leader).Term(), k.c.Node(leader).CommitIndex())

	// Establish an initial committed value.
	open := kv.Command{Type: kv.CmdSessionOpen, SessionID: "s1", RequestID: "open", CreateTime: 1}
	if _, ok := k.propose(leader, open); !ok {
		t.Fatal("session open did not confirm")
	}
	w1 := kv.Command{Type: kv.CmdPut, SessionID: "s1", RequestID: "q1",
		Key: "color", Value: []byte("blue"), CreateTime: 2}
	if _, ok := k.propose(leader, w1); !ok {
		t.Fatal("first write did not confirm")
	}

	// Partition: leader + one peer vs three peers (minority side).
	others := []raft.NodeID{}
	for _, id := range ids("n1", "n2", "n3", "n4", "n5") {
		if id != leader {
			others = append(others, id)
		}
	}
	minorityPeer := others[0]
	majority := others[1:]
	for _, id := range majority {
		k.c.Network().Partition(leader, id)
		k.c.Network().Partition(minorityPeer, id)
	}

	// Let the majority elect a new leader.
	var newLeader raft.NodeID
	k.c.RunUntil(60, func(c *sim.Cluster) bool {
		for _, id := range majority {
			if c.Node(id).Role() == raft.RoleLeader {
				newLeader = id
				return true
			}
		}
		return false
	})
	if newLeader == "" {
		dumpKeyEvents(t, k.c)
		t.Fatal("majority did not elect a replacement leader")
	}
	t.Logf("new majority leader=%s term=%d", newLeader, k.c.Node(newLeader).Term())
	if k.c.Node(newLeader).Term() <= k.c.Node(leader).Term() {
		t.Fatalf("new leader term %d not greater than old %d",
			k.c.Node(newLeader).Term(), k.c.Node(leader).Term())
	}

	// Heal so the old leader sees a higher term, then check it stepped
	// down before any stale proposal is evaluated.
	k.c.Network().Heal()
	k.tick(10)
	if k.c.Node(leader).Role() == raft.RoleLeader &&
		k.c.Node(leader).Term() < k.c.Node(newLeader).Term() {
		t.Fatal("stale leader did not step down after heal")
	}

	// Majority-side committed value survives; old leader must not overwrite.
	w2 := kv.Command{Type: kv.CmdPut, SessionID: "s1", RequestID: "q2",
		Key: "color", Value: []byte("green"), CreateTime: 3}
	curLeader := k.waitLeader(40)
	if _, ok := k.propose(curLeader, w2); !ok {
		t.Fatal("post-heal write did not confirm")
	}
	k.tick(10)
	live := ids("n1", "n2", "n3", "n4", "n5")
	k.assertConverged(live)
	r := k.stores[curLeader].Read("s1", "color", 3)
	if string(r.Value) != "green" {
		dumpKeyEvents(t, k.c)
		t.Fatalf("committed value overwritten: got %q want green", string(r.Value))
	}
	t.Logf("converged: color=green index=%d, single leader=%s",
		k.c.Node(curLeader).CommitIndex(), curLeader)
}

// Scenario 2: a proposal issued on a partitioned minority leader must
// never be reported committed by it (no quorum ack possible).
func TestMinorityWriteRejectedNotSuccess(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	open := kv.Command{Type: kv.CmdSessionOpen, SessionID: "s", RequestID: "open", CreateTime: 1}
	k.propose(leader, open)

	// Isolate the leader alone.
	rest := []raft.NodeID{}
	for _, id := range ids("n1", "n2", "n3") {
		if id != leader {
			rest = append(rest, id)
			k.c.Network().Partition(leader, id)
		}
	}

	data, _ := kv.Encode(kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "qx", Key: "k", Value: []byte("v"), CreateTime: 2})
	k.c.Node(leader).Propose("s/qx", data)

	committed := false
	k.tick(40)
	for _, e := range k.c.Events() {
		if e.Node == leader && e.Type == raft.EvtProposalResult &&
			e.ReqID == "s/qx" && e.Status == raft.PropCommitted {
			committed = true
		}
	}
	if committed {
		t.Fatal("minority leader reported a commit — quorum violated")
	}
	// Follower-side value must remain unset after heal.
	k.c.Network().Heal()
	k.tick(20)
	for _, id := range rest {
		r := k.stores[id].Read("s", "k", 2)
		if r.Verdict == kv.VerdictOK {
			t.Fatalf("minority write leaked to %s", id)
		}
	}
	t.Log("minority write never committed; followers preserved old state")
}

// Scenario 3: repeat determinism — same test run twice yields the same
// leader sequence and final term.
func TestDeterministicRepeatedRun(t *testing.T) {
	run := func() string {
		k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
		k.waitLeader(40)
		l := k.c.Leader()
		// crash leader, wait for replacement
		k.c.Crash(l)
		k.c.RunUntil(60, func(c *sim.Cluster) bool {
			for _, id := range ids("n1", "n2", "n3") {
				if n := c.Node(id); n != nil && n.Role() == raft.RoleLeader && id != l {
					return true
				}
			}
			return false
		})
		l2, term := k.c.LeaderTerm()
		return string(l) + "->" + string(l2) + ":term=" + itoaTerm(term)
	}
	a := run()
	b := run()
	if a != b {
		t.Fatalf("nondeterministic: %q vs %q", a, b)
	}
	t.Logf("identical across runs: %s", a)
}

func itoaTerm(t raft.Term) string {
	if t == 0 {
		return "0"
	}
	var b []byte
	for t > 0 {
		b = append([]byte{byte('0' + t%10)}, b...)
		t /= 10
	}
	return string(b)
}

var _ = sim.NewCluster
