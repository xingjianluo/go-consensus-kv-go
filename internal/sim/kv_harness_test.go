package sim_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

// kvCluster wires a deterministic sim.Cluster to per-node kv.Stores.
type kvCluster struct {
	c      *sim.Cluster
	stores map[raft.NodeID]*kv.Store
	t      *testing.T
}

func newKVCluster(t *testing.T, ids []string, snapThreshold int) *kvCluster {
	raftIDs := make([]raft.NodeID, 0, len(ids))
	for _, id := range ids {
		raftIDs = append(raftIDs, raft.NodeID(id))
	}
	k := &kvCluster{stores: map[raft.NodeID]*kv.Store{}, t: t}
	c := sim.NewCluster(sim.ClusterOptions{
		IDs:               raftIDs,
		ElectionTicks:     8,
		HeartbeatTicks:    1,
		SnapshotThreshold: snapThreshold,
	})
	k.c = c
	c.OnSnapshot = func(id raft.NodeID, sn *raft.Snapshot) {
		if err := k.stores[id].Restore(sn.Data); err != nil {
			t.Fatalf("restore on %s: %v", id, err)
		}
	}
	c.OnCommit = func(id raft.NodeID, ents []raft.Entry) {
		st := k.store(id)
		for _, e := range ents {
			st.Apply(e)
			if e.Kind == raft.EntryConf {
				continue
			}
		}
	}
	for _, id := range raftIDs {
		k.stores[id] = kv.NewStore()
	}
	return k
}

func (k *kvCluster) store(id raft.NodeID) *kv.Store {
	st := k.stores[id]
	if st == nil {
		st = kv.NewStore()
		k.stores[id] = st
	}
	return st
}

func (k *kvCluster) waitLeader(max int) raft.NodeID {
	if !k.c.RunUntil(max, func(c *sim.Cluster) bool { return c.Leader() != "" }) {
		k.t.Fatalf("no leader within %d ticks; events=%d", max, len(k.c.Events()))
	}
	return k.c.Leader()
}

func (k *kvCluster) tick(n int) {
	for i := 0; i < n; i++ {
		k.c.Tick()
	}
}

// propose writes one KV command through the given leader and runs
// until the node emits a terminal proposal verdict for THIS call. It
// returns the committed index (0 if rejected) and whether it committed.
func (k *kvCluster) propose(leader raft.NodeID, cmd kv.Command) (raft.Index, bool) {
	data, err := kv.Encode(cmd)
	if err != nil {
		k.t.Fatal(err)
	}
	rid := raft.ReqID(cmd.SessionID + "/" + cmd.RequestID)
	startEvents := len(k.c.Events())
	k.c.Node(leader).Propose(rid, data)
	var idx raft.Index
	committed := false
	ok := k.c.RunUntil(40, func(c *sim.Cluster) bool {
		for _, e := range c.Events()[startEvents:] {
			if e.Node != leader || e.Type != raft.EvtProposalResult || e.ReqID != rid {
				continue
			}
			if e.Status == raft.PropCommitted {
				idx = e.Index
				committed = true
			}
			return true
		}
		return false
	})
	if !ok {
		return 0, false
	}
	return idx, committed
}

func (k *kvCluster) leaderCommit(id raft.NodeID) raft.Index {
	return k.c.Node(id).CommitIndex()
}

// proposeCommittedOrCached is like propose but also accepts the
// PropCommitted/RejectDuplicate answer (the retry's original entry
// already committed), resolving its position from the reply cache. This
// models a client whose first attempt timed out unknown but whose entry
// committed before the retry.
func (k *kvCluster) proposeCommittedOrCached(leader raft.NodeID, cmd kv.Command,
	sessionID, requestID string) (raft.Index, bool) {
	data, err := kv.Encode(cmd)
	if err != nil {
		k.t.Fatal(err)
	}
	rid := raft.ReqID(cmd.SessionID + "/" + cmd.RequestID)
	startEvents := len(k.c.Events())
	k.c.Node(leader).Propose(rid, data)
	ok := k.c.RunUntil(40, func(c *sim.Cluster) bool {
		for _, e := range c.Events()[startEvents:] {
			if e.Node != leader || e.Type != raft.EvtProposalResult || e.ReqID != rid {
				continue
			}
			if e.Status == raft.PropCommitted {
				return true
			}
		}
		return false
	})
	if ok {
		if rep, ok := k.stores[leader].PendingPut(sessionID, requestID); ok {
			return rep.AppliedAt, true
		}
	}
	return 0, false
}

// assertConverged asserts every live member's KV store is identical.
func (k *kvCluster) assertConverged(live []raft.NodeID) {
	snap0 := mustSnapshot(k.t, k.stores[live[0]])
	for _, id := range live[1:] {
		snapN := mustSnapshot(k.t, k.stores[id])
		if snapN != snap0 {
			k.t.Fatalf("store divergence: %s=%s\n%s=%s", live[0], snap0, id, snapN)
		}
	}
}

func mustSnapshot(t *testing.T, st *kv.Store) string {
	b, err := st.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func sortedEvents(c *sim.Cluster, types ...raft.EventType) []sim.LoggedEvent {
	want := map[raft.EventType]bool{}
	for _, ty := range types {
		want[ty] = true
	}
	out := []sim.LoggedEvent{}
	for _, e := range c.Events() {
		if want[e.Type] {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Term < out[j].Term
	})
	return out
}

func ids(ss ...string) []raft.NodeID {
	out := make([]raft.NodeID, len(ss))
	for i, s := range ss {
		out[i] = raft.NodeID(s)
	}
	return out
}

func dumpKeyEvents(t *testing.T, c *sim.Cluster) {
	t.Helper()
	for _, e := range c.Events() {
		switch e.Type {
		case raft.EvtRoleChange, raft.EvtTermChange, raft.EvtConfChange,
			raft.EvtProposalResult, raft.EvtMessageDropped, raft.EvtEntriesCommitted,
			raft.EvtReadResult:
			fmt.Printf("  tick-ev node=%s type=%v term=%d role=%s idx=%d req=%s status=%v reason=%s leader=%s\n",
				e.Node, e.Type, e.Term, e.Role, e.Index, e.ReqID, e.Status, e.Reason, e.Leader)
		}
	}
}
