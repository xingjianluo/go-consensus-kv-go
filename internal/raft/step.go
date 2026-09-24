package raft

// step processes one message. Callers hold n.mu.
//
// Safety boundaries enforced here:
//   - messages with a higher term force step-down (no stale writes);
//   - every RPC checks membership of the *sender* in the active config;
//   - only a node that believes itself leader serves proposals/reads;
//   - appends require prevLog index+term match and truncate conflicts.
func (n *Node) step(m Message) {
	// Network messages: term discipline first.
	switch m.Type {
	case MsgApp, MsgAppResp, MsgVote, MsgVoteResp, MsgSnap, MsgSnapResp, MsgJoin:
		if m.Term > n.term {
			switch m.Type {
			case MsgApp, MsgSnap:
				// Legitimate leader contact: become follower and continue
				// processing the message below.
				n.becomeFollower(m.Term, m.From)
			case MsgVote:
				// New candidate: become follower, then consider the vote.
				n.becomeFollower(m.Term, "")
			default:
				// A peer in a newer term: step down and drop this message.
				n.becomeFollower(m.Term, "")
				n.drop(m, RejectTermStale)
				return
			}
		} else if m.Term < n.term {
			// Late message from an old term: ignore with a distinct reason.
			if m.Type != MsgAppResp && m.Type != MsgVoteResp && m.Type != MsgSnapResp {
				n.respondStale(m)
			} else {
				n.drop(m, RejectTermStale)
			}
			return
		}
	}

	switch m.Type {
	case MsgHup:
		n.campaign()
	case MsgBeat:
		if n.role == RoleLeader {
			n.broadcastHeartbeat()
		}
	case MsgProp:
		n.handleProp(m)
	case MsgRead:
		n.handleRead(m)
	case MsgApp:
		if n.role == RoleLeader {
			return
		}
		// Same-term leader discovered while candidate: step down and
		// accept the append (Raft: a leader exists in this term).
		if n.role == RoleCandidate && m.Term == n.term {
			n.becomeFollower(m.Term, m.From)
		}
		n.handleAppendEntries(m)
	case MsgAppResp:
		if n.role != RoleLeader {
			n.drop(m, RejectNotLeader)
			return
		}
		n.handleAppendResp(m)
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		if n.role != RoleCandidate {
			n.drop(m, RejectTermStale)
			return
		}
		n.handleVoteResp(m)
	case MsgSnap:
		if n.role == RoleLeader {
			return
		}
		if n.role == RoleCandidate && m.Term == n.term {
			n.becomeFollower(m.Term, m.From)
		}
		n.handleSnapshot(m)
	case MsgSnapResp:
		if n.role != RoleLeader {
			n.drop(m, RejectNotLeader)
			return
		}
		n.handleSnapshotResp(m)
	case MsgJoin:
		n.handleJoin(m)
	}
}

func (n *Node) drop(m Message, reason RejectReason) {
	n.emit(Event{
		Type:   EvtMessageDropped,
		Term:   n.term,
		Role:   n.role,
		Reason: reason,
	})
}

// respondStale replies to a stale-termed leader RPC with the current
// term so the sender steps down, and records the late drop.
func (n *Node) respondStale(m Message) {
	n.drop(m, RejectTermStale)
	switch m.Type {
	case MsgApp:
		n.send(Message{To: m.From, Type: MsgAppResp, Term: n.term, Reject: true,
			RejectAt: RejectTermStale, Index: m.Index})
	case MsgVote:
		n.send(Message{To: m.From, Type: MsgVoteResp, Term: n.term, Granted: false})
	case MsgSnap:
		n.send(Message{To: m.From, Type: MsgSnapResp, Term: n.term, Success: false})
	}
}

func (n *Node) handleProp(m Message) {
	if m.ReqID != "" {
		// Application-level idempotency keys live in the payload; raft
		// only coalesces a request whose own entry is still uncommitted
		// in this term. A retry after (possible) commit is allowed to
		// append: the state machine reply cache makes it a no-op and
		// returns the original verdict.
		if st, dup := n.props[m.ReqID]; dup {
			if st.notified {
				// Original already committed: return that verdict instead
				// of appending a second entry.
				n.emit(Event{Type: EvtProposalResult, Term: n.term, Role: n.role,
					ReqID: m.ReqID, Status: PropCommitted, Reason: RejectDuplicate,
					Index: st.index})
				return
			}
			// Still uncommitted in flight: coalesce. Tell the host the
			// request is deduplicated onto the in-flight proposal; the
			// terminal verdict follows when that entry commits.
			n.emit(Event{Type: EvtProposalResult, Term: n.term, Role: n.role,
				ReqID: m.ReqID, Status: PropRejected, Reason: RejectDuplicate,
				Index: st.index})
			return
		}
	}
	if n.role != RoleLeader {
		if m.ReqID != "" {
			n.emit(Event{Type: EvtProposalResult, Term: n.term, Role: n.role,
				ReqID: m.ReqID, Status: PropRejected, Reason: RejectNotLeader,
				Leader: n.leader})
		}
		return
	}
	conf := n.activeConfig()
	if !n.isMember(n.opt.ID, conf) {
		// Leader that has been removed must not serve new writes.
		if m.ReqID != "" {
			n.emit(Event{Type: EvtProposalResult, Term: n.term, Role: n.role,
				ReqID: m.ReqID, Status: PropRejected, Reason: RejectNotVoter})
		}
		return
	}
	first := n.appendLocal(EntryData, m.Data)
	if first == 0 {
		return
	}
	if m.ReqID != "" {
		n.props[m.ReqID] = &propState{term: n.term, index: first}
	}
	n.prs[n.opt.ID].match = n.lastIndex()
	n.prs[n.opt.ID].next = n.lastIndex() + 1
	n.replicate()
	n.maybeCommit()
}

func (n *Node) handleRead(m Message) {
	if n.role != RoleLeader {
		n.emit(Event{Type: EvtReadResult, Term: n.term, Role: n.role,
			ReqID: m.ReqID, Reason: RejectNotLeader, Leader: n.leader})
		return
	}
	conf := n.activeConfig()
	if !n.isMember(n.opt.ID, conf) {
		n.emit(Event{Type: EvtReadResult, Term: n.term, Role: n.role,
			ReqID: m.ReqID, Reason: RejectNotVoter})
		return
	}
	// Single-node cluster: barrier is trivially satisfied.
	if len(conf.Voters) == 1 {
		n.emit(Event{Type: EvtReadResult, Term: n.term, Role: n.role,
			ReqID: m.ReqID, Reason: RejectNone, Index: n.commit})
		return
	}
	// ReadIndex: register pending read and open a heartbeat barrier.
	n.reads[m.ReqID] = struct{}{}
	bid := n.readBarID + 1
	n.readBarID = bid
	n.readBar[bid] = map[NodeID]bool{n.opt.ID: true}
	n.readBarFor[bid] = []ReqID{m.ReqID}
	n.broadcastHeartbeat()
}

func (n *Node) handleVote(m Message) {
	conf := n.activeConfig()
	if len(conf.Voters) == 0 {
		// Not yet a member (brand-new node waiting to join): refuse.
		n.send(Message{To: m.From, Type: MsgVoteResp, Term: n.term, Granted: false})
		return
	}
	if !n.isMember(m.From, conf) {
		n.send(Message{To: m.From, Type: MsgVoteResp, Term: n.term, Granted: false})
		n.drop(m, RejectNotVoter)
		return
	}
	canVote := n.vote == "" || n.vote == m.From
	upToDate := n.isUpToDate(m.LogTerm, m.Index)
	if canVote && upToDate {
		n.vote = m.From
		n.elapsed = 0
		n.persistHard()
		n.send(Message{To: m.From, Type: MsgVoteResp, Term: n.term, Granted: true})
		return
	}
	n.send(Message{To: m.From, Type: MsgVoteResp, Term: n.term, Granted: false})
}

func (n *Node) handleVoteResp(m Message) {
	if m.Term != n.term {
		n.drop(m, RejectTermStale)
		return
	}
	n.votes[m.From] = m.Granted
	conf := n.activeConfig()
	ids := make([]NodeID, 0, len(conf.Voters))
	for _, v := range conf.Voters {
		ids = append(ids, v.ID)
	}
	if HasMajority(ids, n.votes) {
		n.becomeLeader()
	}
}

func (n *Node) handleJoin(m Message) {
	// Only the current leader admits a joining node, and only as part
	// of an explicit add-membership change. A bare MsgJoin with no
	// pending add is ignored safely (it has no effect on state).
	if n.role != RoleLeader {
		n.send(Message{To: m.From, Type: MsgAppResp, Term: n.term, Reject: true,
			RejectAt: RejectNotLeader})
		return
	}
	// The actual membership append is driven by ProposeConfChange; the
	// join RPC is only used to register the dial address and trigger an
	// immediate catch-up. Nothing to persist here.
	conf := n.activeConfig()
	if n.isMember(m.From, conf) {
		// Existing/restarting member contacting us: start replication.
		n.replicateTo(m.From)
	}
}
