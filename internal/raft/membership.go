package raft

// ProposeConfChange requests a single-node membership change. It returns
// (reason, accepted). Rejections are explicit and never imply commit.
func (n *Node) ProposeConfChange(cc ConfChange) (RejectReason, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	defer n.flush()

	if n.role != RoleLeader {
		n.emit(Event{Type: EvtConfChange, Term: n.term, Role: n.role,
			Reason: RejectNotLeader, Leader: n.leader})
		return RejectNotLeader, false
	}
	conf := n.activeConfig()
	if !n.isMember(n.opt.ID, conf) {
		return RejectNotVoter, false
	}
	if n.pendingConf {
		return RejectConfigBusy, false
	}
	switch cc.Type {
	case ConfAddNode:
		if cc.Node == "" {
			return RejectNodeMissing, false
		}
		if n.isMember(cc.Node, conf) {
			return RejectNodeExists, false
		}
	case ConfRemoveNode:
		if !n.isMember(cc.Node, conf) {
			return RejectNodeMissing, false
		}
		if cc.Node == n.opt.ID {
			// Use two-step removal: remove another member first, or shut
			// the leader down after transferring leadership.
			return RejectRemoveLeader, false
		}
		if Majorize(len(conf.Voters)-1) > len(conf.Voters)-1 {
			// Would leave a cluster unable to form a quorum (guards the
			// last-standalone corner).
			return RejectConfigBusy, false
		}
	}

	next := cloneConfig(conf)
	switch cc.Type {
	case ConfAddNode:
		next.Voters = append(next.Voters, Voter{ID: cc.Node, Addr: cc.Addr})
	case ConfRemoveNode:
		out := next.Voters[:0]
		for _, v := range next.Voters {
			if v.ID != cc.Node {
				out = append(out, v)
			}
		}
		next.Voters = out
	}
	data, err := EncodeConfig(next)
	if err != nil {
		return RejectConfigBusy, false
	}
	first := n.appendLocal(EntryConf, data)
	if first == 0 {
		return RejectConfigBusy, false
	}
	n.pendingConf = true
	// Immediately adopt the new config for quorum/heartbeat purposes.
	// One-node-at-a-time changes make old+new quorum overlap safely.
	switch cc.Type {
	case ConfAddNode:
		n.prs[cc.Node] = newProgress(n.lastIndex())
	case ConfRemoveNode:
		if pr := n.prs[cc.Node]; pr != nil {
			// Keep progress around only until commit; replicate() already
			// iterates the new config so it will not be contacted.
			_ = pr
		}
	}
	n.emit(Event{Type: EvtConfChange, Term: n.term, Role: n.role, Index: first})
	n.replicate()
	n.maybeCommit()
	return RejectNone, true
}

// cloneConfig copies a config so append slices never alias storage bytes.
func cloneConfig(c Config) Config {
	out := Config{Voters: make([]Voter, len(c.Voters))}
	copy(out.Voters, c.Voters)
	return out
}

// applyConfigIfRemoved is called by followers after applying a config
// entry's prefix: if the newest committed config excludes this node it
// must stop campaigning/serving and report why.
func (n *Node) applyConfigIfRemoved() {
	conf := n.activeConfig()
	if len(conf.Voters) == 0 {
		return
	}
	if n.isMember(n.opt.ID, conf) {
		return
	}
	// Removed: become a passive follower with no leader duties. Do not
	// delete persisted data: an admin can inspect or wipe it.
	if n.role != RoleFollower {
		n.becomeFollower(n.term, "")
	}
	n.abortPending(RejectNotVoter)
	n.emit(Event{Type: EvtConfChange, Term: n.term, Role: n.role, Reason: RejectNotVoter})
}
