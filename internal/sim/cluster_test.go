package sim

import (
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

func TestClusterElectsLeader(t *testing.T) {
	c := NewCluster(ClusterOptions{IDs: []raft.NodeID{"n1", "n2", "n3"}, ElectionTicks: 6})
	ok := c.RunUntil(30, func(c *Cluster) bool { return c.Leader() != "" })
	if !ok {
		t.Fatalf("no leader after ticks; events=%d", len(c.Events()))
	}
	// Exactly one leader.
	leaders := 0
	for _, n := range []raft.NodeID{"n1", "n2", "n3"} {
		if c.Node(n).Role() == raft.RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders=%d", leaders)
	}
}

func TestClusterProposalReplicates(t *testing.T) {
	c := NewCluster(ClusterOptions{IDs: []raft.NodeID{"n1", "n2", "n3"}, ElectionTicks: 6})
	c.RunUntil(30, func(c *Cluster) bool { return c.Leader() != "" })
	leader := c.Leader()
	committed := make(chan raft.Index, 1)
	c.OnCommit = func(id raft.NodeID, ents []raft.Entry) {
		if id == leader {
			for _, e := range ents {
				if string(e.Data) == "hello" {
					committed <- e.Index
				}
			}
		}
	}
	c.Node(leader).Propose("req-1", []byte("hello"))
	ok := c.RunUntil(30, func(c *Cluster) bool {
		select {
		case <-committed:
			return true
		default:
			return false
		}
	})
	if !ok {
		t.Fatalf("proposal never committed; leader=%s events=%d", leader, len(c.Events()))
	}
}
