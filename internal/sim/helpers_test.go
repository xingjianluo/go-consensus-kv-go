package sim

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

const defaultTTL = 20 * time.Second

func newTestCluster(t *testing.T, snap int) *Cluster {
	t.Helper()
	c := NewCluster(Options{
		Nodes:           []string{"n1", "n2", "n3"},
		Seed:            42,
		SnapshotEntries: snap,
	})
	t.Cleanup(func() {
		for _, id := range c.order {
			if rt := c.nodes[id]; rt != nil && rt.alive {
				rt.node.Stop()
			}
		}
	})
	return c
}

func awaitLeader(t *testing.T, c *Cluster) string {
	t.Helper()
	c.runFor(500 * time.Millisecond)
	l := c.Leader()
	if l == "" {
		t.Fatalf("no leader within election window; events:\n%s", formatEvents(c.EventLog()))
	}
	return l
}

func mustRegister(t *testing.T, c *Cluster) *Client {
	t.Helper()
	cl := NewClient(c)
	if err := cl.RegisterSession(defaultTTL); err != nil {
		t.Fatalf("register session: %v", err)
	}
	return cl
}

func mustPut(t *testing.T, cl *Client, k, v string) raft.Outcome {
	t.Helper()
	out := cl.Put(k, v)
	if !out.OK {
		t.Fatalf("put %s=%s rejected: code=%s reason=%s", k, v, out.Code, out.Reason)
	}
	return out
}

func mustGet(t *testing.T, cl *Client, k, want string) {
	t.Helper()
	out := cl.Get(k)
	if !out.OK {
		t.Fatalf("get %s rejected: code=%s reason=%s", k, out.Code, out.Reason)
	}
	if out.Value != want {
		t.Fatalf("get %s = %q, want %q (barrier index=%d term=%d)",
			k, out.Value, want, out.Index, out.Term)
	}
}

// eventSummary prints the decisive lines: role/term/position/reason.
func formatEvents(evs []raft.Event) string {
	var b strings.Builder
	for _, ev := range evs {
		if !interesting(ev.Kind) {
			continue
		}
		fmt.Fprintf(&b, "  [%s] %-8s term=%-2d %-9s idx=%-3d %s\n",
			ev.At.Format("15:04:05.000"), ev.Node, ev.Term, ev.Role, ev.Index, ev.Detail)
	}
	return b.String()
}

func interesting(k raft.EventKind) bool {
	switch k {
	case EvRoleChangeRep, EvElectionRep, EvProposalRep, EvCommitRep,
		EvApplyRep, EvRejectRep, EvStaleRep, EvSnapshotRep,
		EvConfRep, EvSessionRep, EvReadRep, EvTimeoutRep, EvTermRep:
		return true
	}
	return false
}

// raft event kind constants mirrored for filtering convenience.
const (
	EvRoleChangeRep = raft.EventKind("role_change")
	EvElectionRep   = raft.EventKind("election")
	EvProposalRep   = raft.EventKind("proposal")
	EvCommitRep     = raft.EventKind("commit")
	EvApplyRep      = raft.EventKind("apply")
	EvRejectRep     = raft.EventKind("reject")
	EvStaleRep      = raft.EventKind("stale_message")
	EvSnapshotRep   = raft.EventKind("snapshot_install")
	EvConfRep       = raft.EventKind("conf_change")
	EvSessionRep    = raft.EventKind("session")
	EvReadRep       = raft.EventKind("read")
	EvTimeoutRep    = raft.EventKind("timeout")
	EvTermRep       = raft.EventKind("stale_term")
)
