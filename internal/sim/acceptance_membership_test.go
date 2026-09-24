package sim_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// confOf decodes the latest committed EntryConf visible to a node.
func confOf(t *testing.T, c *sim.Cluster, id raft.NodeID) raft.Config {
	t.Helper()
	st := c.Store(id)
	sn, _ := st.Snapshot()
	conf := raft.Config{}
	if sn != nil {
		conf = sn.Conf
	}
	base := raft.Index(0)
	if sn != nil {
		base = sn.Index
	}
	for i := base + 1; i <= st.LastIndex(); i++ {
		ents, err := st.Entries(i, i+1)
		if err != nil || len(ents) == 0 {
			continue
		}
		if ents[0].Kind == raft.EntryConf {
			var cc raft.Config
			if err := json.Unmarshal(ents[0].Data, &cc); err == nil {
				conf = cc
			}
		}
	}
	return conf
}

func voters(conf raft.Config) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(conf.Voters))
	for _, v := range conf.Voters {
		out = append(out, v.ID)
	}
	return out
}

// Scenario: add a brand-new node to a 3-node cluster, commit through
// the new 4-node quorum, then remove a node. Throughout, there must be
// exactly one leader and no committed value rollback.
func TestAddThenRemoveMember(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)
	k.propose(leader, kv.Command{Type: kv.CmdSessionOpen, SessionID: "s",
		RequestID: "open", CreateTime: 1})
	k.propose(leader, kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "w0", Key: "k0", Value: []byte("v0"), CreateTime: 2})

	// Register an empty new node (not a voter yet).
	k.c.AddNewNode("n4", "127.0.0.1:9404")

	// Propose the add on the leader.
	reason, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfAddNode, Node: "n4", Addr: "127.0.0.1:9404",
	})
	if !ok {
		t.Fatalf("add rejected immediately: %s", reason)
	}
	// Wait for the conf entry to commit on the leader AND replicate so
	// that n4 is a fully caught-up voter.
	joined := k.c.RunUntil(80, func(c *sim.Cluster) bool {
		conf := confOf(t, c, leader)
		if len(conf.Voters) != 4 {
			return false
		}
		// n4 must have the committed config and prior data.
		if k.c.Node("n4") == nil {
			return false
		}
		return k.c.Node("n4").CommitIndex() >= c.Node(leader).CommitIndex()
	})
	if !joined {
		dumpKeyEvents(t, k.c)
		t.Fatal("n4 did not join and catch up")
	}
	r := k.stores["n4"].Read("s", "k0", 2)
	if r.Verdict != kv.VerdictOK || string(r.Value) != "v0" {
		t.Fatalf("new member missing pre-join data: verdict=%s", r.Verdict)
	}
	if countLeaders(k) != 1 {
		t.Fatal("non-unique leader during/after join")
	}
	t.Log("n4 joined with history; single leader maintained")

	// Write through the new 4-voter cluster.
	k.propose(k.c.Leader(), kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "w1", Key: "k1", Value: []byte("v1"), CreateTime: 3})
	k.tick(5)
	k.assertConverged(ids("n1", "n2", "n3", "n4"))

	// Remove a non-leader member.
	victim := raft.NodeID("n2")
	if victim == k.c.Leader() {
		victim = "n3"
	}
	if reason, ok := k.c.Node(k.c.Leader()).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfRemoveNode, Node: victim,
	}); !ok {
		t.Fatalf("remove rejected: %s", reason)
	}
	removed := k.c.RunUntil(60, func(c *sim.Cluster) bool {
		return len(confOf(t, c, k.c.Leader()).Voters) == 3
	})
	if !removed {
		t.Fatal("remove membership change did not commit")
	}
	k.tick(8)
	if countLeaders(k) != 1 {
		t.Fatal("non-unique leader after removal")
	}
	// The removed node must have stepped down / gone passive.
	if k.c.Node(victim).Role() == raft.RoleLeader {
		t.Fatal("removed node still believes it is leader")
	}
	k.c.RemoveNode(victim)

	// Remaining 3 voters keep committing; data never rolls back.
	k.propose(k.c.Leader(), kv.Command{Type: kv.CmdPut, SessionID: "s",
		RequestID: "w2", Key: "k2", Value: []byte("v2"), CreateTime: 4})
	k.tick(5)
	k.assertConverged(remainingLive(k, victim))
	for i, want := range []string{"v0", "v1", "v2"} {
		r := k.stores[k.c.Leader()].Read("s", fmt.Sprintf("k%d", i), 5)
		if string(r.Value) != want {
			t.Fatalf("k%d=%q want %q after membership change", i, string(r.Value), want)
		}
	}
	t.Log("membership add+remove completed; no dual leader, no rollback")
}

func countLeaders(k *kvCluster) int {
	n := 0
	for _, id := range ids("n1", "n2", "n3", "n4") {
		if node := k.c.Node(id); node != nil && node.Role() == raft.RoleLeader {
			n++
		}
	}
	return n
}

func remainingLive(k *kvCluster, removed raft.NodeID) []raft.NodeID {
	out := []raft.NodeID{}
	for _, id := range ids("n1", "n2", "n3", "n4") {
		if id != removed {
			out = append(out, id)
		}
	}
	return out
}

// Scenario: overlapping membership changes are explicitly rejected while
// one is uncommitted; removing the current leader is rejected too.
func TestRejectInvalidConfChanges(t *testing.T) {
	k := newKVCluster(t, []string{"n1", "n2", "n3"}, 0)
	leader := k.waitLeader(40)

	// add existing
	if reason, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfAddNode, Node: "n2",
	}); ok {
		t.Fatalf("add of existing member accepted")
	} else if reason != raft.RejectNodeExists {
		t.Fatalf("add existing reason=%s want node-exists", reason)
	}

	// remove unknown
	if reason, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfRemoveNode, Node: "zz",
	}); ok || reason != raft.RejectNodeMissing {
		t.Fatalf("remove unknown reason=%s", reason)
	}

	// remove the leader itself
	if reason, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfRemoveNode, Node: leader,
	}); ok || reason != raft.RejectRemoveLeader {
		t.Fatalf("remove leader reason=%s", reason)
	}

	// busy: add n4, and before it commits add n5.
	k.c.AddNewNode("n4", "a")
	k.c.AddNewNode("n5", "a")
	if _, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfAddNode, Node: "n4", Addr: "a",
	}); !ok {
		t.Fatal("first add should be accepted")
	}
	if reason, ok := k.c.Node(leader).ProposeConfChange(raft.ConfChange{
		Type: raft.ConfAddNode, Node: "n5", Addr: "a",
	}); ok || reason != raft.RejectConfigBusy {
		t.Fatalf("overlapping change reason=%s want config-busy", reason)
	}
	t.Log("invalid/overlapping membership changes rejected with distinct reasons")
}
