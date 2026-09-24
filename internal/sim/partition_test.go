package sim

import (
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// TestMinorityPartitionCannotCommit: a leader isolated with no quorum must
// stop committing and must not return success.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)
	mustPut(t, cl, "x", "1")

	var followers []string
	for _, id := range c.order {
		if id != leader {
			followers = append(followers, id)
		}
	}
	t.Logf("partition: leader=%s isolated from majority=%v", leader, followers)
	c.Partition([]string{leader}, followers)
	c.runFor(800 * time.Millisecond) // majority elects new leader

	// Old isolated leader cannot commit: it lacks a quorum.
	ch := c.registerResult("minority-put-1")
	_ = c.nodes[leader].node.Propose(raft.ClientRequest{
		ReqID: "minority-put-1", Session: cl.Session(), Op: "put", Key: "x", Value: "2",
	})
	out, ok := c.waitResult(ch, 600*time.Millisecond)
	t.Logf("minority proposal result: ok=%v committed=%v code=%s reason=%q",
		ok, out.Committed, out.Code, out.Reason)
	if ok && out.OK {
		t.Fatalf("minority leader must not return success, got: %+v", out)
	}

	// Majority side keeps serving and its committed value wins.
	c.Heal()
	c.runFor(500 * time.Millisecond)
	cl2 := NewClient(c)
	_ = cl2.RegisterSession(defaultTTL)
	mustPut(t, cl2, "y", "majority")
	mustGet(t, cl2, "y", "majority")

	// x must remain the majority-committed value "1".
	mustGet(t, cl2, "x", "1")
	t.Logf("after heal, x=1 held; committed index majority=%d",
		c.State(c.Leader()).CommitIndex)
	t.Log("\n关键事件:\n" + formatEvents(c.EventLog()))
}

// TestStaleTermLeaderDoesNotOverwrite: after a partition heals, the old
// leader rolls back its uncommitted suffix and never overwrites committed
// majority entries.
func TestStaleTermLeaderDoesNotOverwrite(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)
	mustPut(t, cl, "k", "committed-v1")

	majority := []string{}
	for _, id := range c.order {
		if id != leader {
			majority = append(majority, id)
		}
	}
	c.Partition([]string{leader}, majority)
	c.runFor(800 * time.Millisecond)
	newLeader := ""
	for _, id := range majority {
		if c.State(id).Role == raft.Leader {
			newLeader = id
			break
		}
	}
	if newLeader == "" {
		t.Fatalf("majority side should elect a new leader (old=%s still leading minority)", leader)
	}
	t.Logf("new majority leader=%s term=%d; old leader=%s partitioned",
		newLeader, c.State(newLeader).Term, leader)

	// Old leader appends an uncommitted entry in its stale term.
	ch := c.registerResult("stale-1")
	_ = c.nodes[leader].node.Propose(raft.ClientRequest{
		ReqID: "stale-1", Session: cl.Session(), Op: "put", Key: "k", Value: "STALE",
	})
	staleOut, _ := c.waitResult(ch, 300*time.Millisecond)
	if staleOut.OK {
		t.Fatalf("stale-term leader must not commit STALE")
	}

	c.Heal()
	c.runFor(500 * time.Millisecond)

	st := c.State(leader)
	t.Logf("old leader after heal: term=%d role=%s lastIdx=%d commitIdx=%d",
		st.Term, st.Role, st.LastIndex, st.CommitIndex)
	if st.Term < c.State(newLeader).Term {
		t.Fatalf("old leader did not catch up to the current term")
	}
	// Deterministic value: stale entry rolled back, committed value kept.
	mustGet(t, NewClientWithSession(t, c, cl.Session()), "k", "committed-v1")
	t.Log("\n关键事件（含旧 term 被忽略/回退）:\n" + formatEvents(c.EventLog()))
}

func NewClientWithSession(t *testing.T, c *Cluster, session string) *Client {
	cl := NewClient(c)
	cl.session = session
	return cl
}
