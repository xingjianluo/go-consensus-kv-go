package raft

// campaign starts an election. Only members of the active config may
// campaign; a non-member (removed or not-yet-joined) node remains idle.
func (n *Node) campaign() {
	conf := n.activeConfig()
	if len(conf.Voters) == 0 {
		// Brand-new node waiting for its first config: stay passive.
		n.elapsed = 0
		return
	}
	if !n.isMember(n.opt.ID, conf) {
		// Removed node: never campaign, and drop leadership if any.
		if n.role != RoleFollower {
			n.becomeFollower(n.term, "")
		}
		n.elapsed = 0
		return
	}
	n.elections++
	n.role = RoleCandidate
	n.term++
	n.vote = n.opt.ID
	n.leader = ""
	n.elapsed = 0
	n.leadTransferee = ""
	n.votes = map[NodeID]bool{n.opt.ID: true}
	n.abortPending(RejectTermStale)
	n.persistHard()
	n.emit(Event{Type: EvtTermChange, Term: n.term, Role: RoleCandidate})
	n.emit(Event{Type: EvtRoleChange, Term: n.term, Role: RoleCandidate})

	// Single-node cluster wins immediately.
	ids := make([]NodeID, 0, len(conf.Voters))
	for _, v := range conf.Voters {
		ids = append(ids, v.ID)
	}
	if len(ids) == 1 && ids[0] == n.opt.ID {
		n.becomeLeader()
		return
	}
	for _, id := range ids {
		if id == n.opt.ID {
			continue
		}
		n.send(Message{
			To: id, Type: MsgVote, Term: n.term,
			Index: n.lastIndex(), LogTerm: n.lastTerm(),
		})
	}
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leader = n.opt.ID
	n.elapsed = 0
	n.heartElapsed = 0
	n.leadTransferee = ""
	li := n.lastIndex()

	conf := n.activeConfig()
	n.prs = map[NodeID]*progress{}
	for _, v := range conf.Voters {
		pr := newProgress(li)
		if v.ID == n.opt.ID {
			pr.match = li
			pr.next = li + 1
			pr.state = replicateState
		}
		n.prs[v.ID] = pr
	}

	// Commit a no-op entry in the new term. This is the Raft rule that
	// makes "entry committed in term T" imply all earlier entries are
	// committed, and prevents a stale leader from claiming commit.
	n.appendLocal(EntryData, nil)
	self := n.prs[n.opt.ID]
	self.match = n.lastIndex()
	self.next = n.lastIndex() + 1

	n.pendingConf = false
	n.emit(Event{Type: EvtRoleChange, Term: n.term, Role: RoleLeader, Leader: n.opt.ID})
	n.broadcastHeartbeat()
	n.maybeCommit()
}
