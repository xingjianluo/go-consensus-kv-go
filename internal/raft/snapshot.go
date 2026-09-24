package raft

// handleSnapshot installs a leader-pushed snapshot on a follower whose
// log has been compacted past the leader's replication point.
func (n *Node) handleSnapshot(m Message) {
	sn := m.Snapshot
	if sn == nil {
		n.send(Message{To: m.From, Type: MsgSnapResp, Term: n.term, Success: false})
		return
	}
	n.leader = m.From
	n.elapsed = 0

	// Stale snapshot (older than what we already have): reject distinctly.
	cur := Index(0)
	if n.snap != nil {
		cur = n.snap.Index
	}
	if sn.Index < cur || sn.Index < n.commit {
		n.send(Message{To: m.From, Type: MsgSnapResp, Term: n.term, Success: false,
			Index: n.lastIndex()})
		n.drop(m, RejectLogOlder)
		return
	}

	// InstallSnapshot replaces the whole log: the follower may be far
	// behind, so the snapshot index can exceed its current last index.
	if err := n.opt.Storage.InstallSnapshot(*sn); err != nil {
		n.send(Message{To: m.From, Type: MsgSnapResp, Term: n.term, Success: false})
		return
	}
	n.snap = sn
	n.log0 = sn.Index
	if sn.Index > n.commit {
		n.commit = sn.Index
	}
	if n.applied < sn.Index {
		n.applied = sn.Index
	}
	n.persistHard()
	// Surface snapshot to host application via Ready.
	n.rd <- Ready{Snapshot: sn, Messages: []Message{{
		To: m.From, Type: MsgSnapResp, Term: n.term, Success: true, Index: sn.Index,
	}}}
}

func (n *Node) handleSnapshotResp(m Message) {
	pr := n.prs[m.From]
	if pr == nil {
		n.drop(m, RejectNotVoter)
		return
	}
	pr.snapshot = false
	if !m.Success {
		pr.state = probeState
		n.replicateTo(m.From)
		return
	}
	idx := m.Index
	if n.snap != nil && idx < n.snap.Index {
		idx = n.snap.Index
	}
	if idx >= pr.match {
		pr.match = idx
		pr.next = idx + 1
	}
	pr.state = probeState
	n.replicateTo(m.From)
	n.maybeCommit()
}

// maybeCompact triggers application snapshotting once the live log is
// long enough and there is something committed worth compacting.
func (n *Node) maybeCompact() {
	if n.snap != nil && n.lastIndex()-n.snap.Index < n.opt.SnapshotThreshold {
		return
	}
	if n.snap == nil && n.lastIndex()-n.log0 < n.opt.SnapshotThreshold {
		return
	}
	idx := n.applied
	if idx <= n.log0 {
		return
	}
	t, err := n.opt.Storage.Term(idx)
	if err != nil {
		return
	}
	n.emit(Event{Type: EvtSnapshotRequired, Term: n.term, Role: n.role, Index: idx})
	// Compaction is completed when the host calls SnapshotBuilt with the
	// application bytes for idx/t.
	_ = t
}
