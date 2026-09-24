package raft

// broadcastHeartbeat sends a heartbeat / replication burst to every
// voter and records read barriers (ReadIndex acks).
func (n *Node) broadcastHeartbeat() {
	conf := n.activeConfig()
	for _, v := range conf.Voters {
		if v.ID == n.opt.ID {
			continue
		}
		if _, ok := n.prs[v.ID]; !ok {
			n.prs[v.ID] = newProgress(n.lastIndex())
		}
		n.replicateTo(v.ID)
	}
}

// replicate pushes the best available content to one follower.
func (n *Node) replicate() {
	conf := n.activeConfig()
	for _, v := range conf.Voters {
		if v.ID == n.opt.ID {
			continue
		}
		if _, ok := n.prs[v.ID]; !ok {
			n.prs[v.ID] = newProgress(n.lastIndex())
		}
		n.replicateTo(v.ID)
	}
}

func (n *Node) replicateTo(id NodeID) {
	pr := n.prs[id]
	if pr == nil {
		pr = newProgress(n.lastIndex())
		n.prs[id] = pr
	}
	// A snapshot that was lost (peer crashed/partitioned) generates no
	// response; resend it on every replication round instead of parking
	// the follower forever. Duplicate snapshots are harmless: the peer
	// rejects an older one distinctly.
	if pr.snapshot {
		if n.snap != nil {
			n.send(Message{To: id, Type: MsgSnap, Term: n.term, Snapshot: n.snap})
		}
		return
	}
	prev := pr.next - 1
	if prev < n.log0+1 && n.snap != nil && prev < n.snap.Index {
		// Follower needs the snapshot first.
		n.send(Message{To: id, Type: MsgSnap, Term: n.term, Snapshot: n.snap})
		pr.snapshot = true
		return
	}
	var prevTerm Term
	if prev > 0 {
		t, err := n.opt.Storage.Term(prev)
		if err != nil {
			if n.snap != nil && prev == n.snap.Index {
				t = n.snap.Term
			} else {
				// Should not happen when snapshot logic is correct; send
				// snapshot defensively.
				if n.snap != nil {
					n.send(Message{To: id, Type: MsgSnap, Term: n.term, Snapshot: n.snap})
					pr.snapshot = true
				}
				return
			}
		}
		prevTerm = t
	}
	ents := n.entries(pr.next, n.lastIndex()+1)
	if len(ents) > n.opt.MaxSizePerMsg {
		ents = ents[:n.opt.MaxSizePerMsg]
	}
	// Heartbeats are MsgApp even with zero entries: followers refresh
	// election timers and ack commit position.
	n.send(Message{
		To: id, Type: MsgApp, Term: n.term,
		Index: prev, LogTerm: prevTerm,
		Entries: ents, Commit: n.commit,
	})
}

func (n *Node) handleAppendEntries(m Message) {
	conf := n.activeConfig()
	if len(conf.Voters) > 0 && !n.isMember(m.From, conf) {
		// Only the configured leader may replicate to us.
		n.send(Message{To: m.From, Type: MsgAppResp, Term: n.term, Reject: true,
			RejectAt: RejectNotVoter, Index: n.lastIndex()})
		return
	}
	n.leader = m.From
	n.elapsed = 0

	// Membership rule: if the leader does not include us, it is a late
	// heartbeat from before our removal. Ignore distinctly.
	// (m.From itself must of course be leader; membership of sender was
	// checked above; our own membership governs whether we may append.)
	// We allow appends from a valid leader regardless so removal entries
	// can be applied; after applying a removal entry we step down.

	if m.Index < n.log0 || (n.snap != nil && m.Index < n.snap.Index) {
		// Leader's prevLog is older than our compacted base: it is behind;
		// send back our snapshot position as a hint.
		hint := Index(0)
		if n.snap != nil {
			hint = n.snap.Index
		}
		n.send(Message{To: m.From, Type: MsgAppResp, Term: n.term, Reject: true,
			RejectAt: RejectLogOlder, Index: hint})
		return
	}

	// Verify prevLog index/term.
	if m.Index > 0 {
		have, err := n.opt.Storage.Term(m.Index)
		if err != nil {
			if n.snap != nil && m.Index == n.snap.Index {
				have = n.snap.Term
			} else {
				// Conflict: respond with the highest index we hold so the
				// leader can backtrack efficiently.
				n.rejectAppend(m)
				return
			}
		}
		if have != m.LogTerm {
			n.rejectAppend(m)
			return
		}
	}

	// Store entries (conflicting suffix is truncated inside Append).
	if len(m.Entries) > 0 {
		if err := n.opt.Storage.Append(m.Index, m.Entries); err != nil {
			n.rejectAppend(m)
			return
		}
	}
	last := m.Index
	if len(m.Entries) > 0 {
		last = m.Entries[len(m.Entries)-1].Index
	}

	// Advance commit only to entries from the leader's log and never
	// backwards. A follower never commits an entry from an older term
	// beyond what the current leader asserts.
	if m.Commit > n.commit {
		limit := m.Commit
		if last < limit {
			limit = last
		}
		if limit > n.commit {
			// Raft: follower may commit entries up to min(leaderCommit,
			// index of last new entry); term safety is provided by the
			// leader's no-op rule.
			n.commit = limit
			n.persistHard()
		}
	}

	n.send(Message{To: m.From, Type: MsgAppResp, Term: n.term, Index: last})
	n.applyConfigIfRemoved()
}

func (n *Node) rejectAppend(m Message) {
	// Compute a backtrack hint: the last index before the first term
	// mismatch in our log, capped to what the leader referenced.
	hint := n.lastIndex()
	if m.Index < hint {
		hint = m.Index
	}
	// Find the first index of our term at m.Index to skip the conflict.
	if m.Index > 0 {
		t, err := n.opt.Storage.Term(m.Index)
		if err == nil {
			for hint > 0 && n.termAt(hint) == t {
				hint--
			}
		} else if n.snap != nil && m.Index <= n.snap.Index {
			hint = n.snap.Index
		}
	}
	n.send(Message{To: m.From, Type: MsgAppResp, Term: n.term, Reject: true,
		RejectAt: RejectLogOlder, Index: hint})
}

func (n *Node) handleAppendResp(m Message) {
	pr := n.prs[m.From]
	if pr == nil {
		// Response from an already-removed peer: late message, ignored
		// safely with a distinguishable drop record.
		n.drop(m, RejectNotVoter)
		return
	}
	// Ack read-index barriers even on rejects (the peer contacted us in
	// this term; leadership confirmation needs any current-term reply).
	for bid, acks := range n.readBar {
		if !acks[m.From] {
			acks[m.From] = true
			ids := n.currentMemberIDs()
			if HasMajority(ids, acks) && !n.readConfirm[bid] {
				n.readConfirm[bid] = true
				for _, rid := range n.readBarFor[bid] {
					if _, ok := n.reads[rid]; ok {
						delete(n.reads, rid)
						n.emit(Event{Type: EvtReadResult, Term: n.term, Role: n.role,
							ReqID: rid, Reason: RejectNone, Index: n.commit})
					}
				}
			}
		}
	}

	if m.Reject {
		switch m.RejectAt {
		case RejectTermStale:
			return // already handled by term discipline
		case RejectNotVoter:
			// Peer no longer accepts us (typically we are not in its
			// config); nothing to retry until config changes.
			return
		}
		// Backtrack to the hinted index and retry.
		if m.Index > 0 && m.Index < pr.next {
			pr.next = m.Index
		} else if pr.next > 1 {
			pr.next--
		}
		pr.state = probeState
		pr.snapshot = false
		n.replicateTo(m.From)
		return
	}

	pr.snapshot = false
	if m.Index >= pr.match {
		pr.match = m.Index
		if pr.next <= m.Index {
			pr.next = m.Index + 1
		}
		pr.state = replicateState
	}
	n.replicateTo(m.From)
	n.maybeCommit()
}

// maybeCommit advances commit using match indexes of the active config.
// Quorum is computed over the *current* configuration (joint consensus
// is unnecessary because only one node changes at a time).
func (n *Node) maybeCommit() {
	conf := n.activeConfig()
	ids := make([]NodeID, 0, len(conf.Voters))
	matches := make([]Index, 0, len(conf.Voters))
	for _, v := range conf.Voters {
		ids = append(ids, v.ID)
		if v.ID == n.opt.ID {
			matches = append(matches, n.lastIndex())
		} else if pr := n.prs[v.ID]; pr != nil {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	sortIndex(matches)
	quorumIdx := matches[len(matches)-Majorize(len(matches))]
	if quorumIdx > n.commit {
		// Never commit an entry from a prior term directly (Raft §8):
		// wait until a current-term entry reaches quorum. The no-op
		// appended on election satisfies this.
		t, err := n.opt.Storage.Term(quorumIdx)
		if err != nil || t != n.term {
			return
		}
		n.commit = quorumIdx
		n.persistHard()
	}
}

func (n *Node) currentMemberIDs() []NodeID {
	conf := n.activeConfig()
	ids := make([]NodeID, 0, len(conf.Voters))
	for _, v := range conf.Voters {
		ids = append(ids, v.ID)
	}
	return ids
}

func sortIndex(s []Index) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
