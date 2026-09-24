package sim

import (
	"testing"
	"time"
)

func TestSmokeElectAndReplicate(t *testing.T) {
	c := NewCluster(Options{
		Nodes:           []string{"n1", "n2", "n3"},
		Seed:            1,
		SnapshotEntries: 100,
	})
	defer func() {
		for _, id := range c.order {
			if rt := c.nodes[id]; rt != nil && rt.alive {
				rt.node.Stop()
			}
		}
	}()

	c.runFor(400 * time.Millisecond)
	leader := c.Leader()
	if leader == "" {
		t.Fatalf("expected a leader after election window")
	}
	t.Logf("leader elected: %s term=%d", leader, c.State(leader).Term)

	cl := NewClient(c)
	if err := cl.RegisterSession(10 * time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}
	if out := cl.Put("k1", "v1"); !out.OK {
		t.Fatalf("put failed: %+v", out)
	} else {
		t.Logf("put committed at index=%d term=%d", out.Index, out.Term)
	}
	if out := cl.Get("k1"); !out.OK || out.Value != "v1" {
		t.Fatalf("get: %+v", out)
	} else {
		t.Logf("get k1=%s at barrier index=%d", out.Value, out.Index)
	}
}
