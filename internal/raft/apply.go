package raft

// applyCommitted applies all committed-but-not-applied entries in order and
// resolves the corresponding client proposals.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, err := n.log.Entry(n.lastApplied)
		if err != nil {
			// Index must have been compacted unexpectedly; snap back to
			// the durable snapshot position.
			n.lastApplied = n.snapLastIndex()
			continue
		}
		switch e.Kind {
		case EntryConf:
			n.applyConf(e)
		case EntryNoop:
			// commits the leader term; nothing to apply.
			n.emit(EvApply, e.Index, "应用 no-op（提交本 term 的标志），term="+itoa(e.Term))
		default:
			n.applyClient(e)
		}
		// Compaction: once enough applied entries accumulated beyond the
		// snapshot point, ask the SM for a snapshot and truncate.
		n.maybeCompact()
	}
}

func (n *Node) applyConf(e Entry) {
	if e.Conf == nil {
		return
	}
	old := joinIDs(n.conf.Voters)
	n.conf = *e.Conf
	if n.pendingIndex == e.Index {
		n.pendingConf = nil
		n.pendingIndex = 0
	}
	n.emit(EvConfChange, e.Index,
		"成员配置生效：["+old+"] -> ["+joinIDs(e.Conf.Voters)+"]，任期="+itoa(e.Term))
	// A removed node may still be leader; if it removed itself it must
	// step down before it can commit anything else.
	if !n.conf.contains(n.id) {
		n.emit(EvConfChange, e.Index, "本节点已退出配置，停止选举与提交，作为非成员静默")
		n.role = Follower
		n.lead = ""
		n.pr = nil
		n.timers.StopTimers()
		return
	}
	if n.role == Leader {
		// Drop progress for removed peers; initialize for added ones.
		for id := range n.pr {
			if !n.conf.contains(id) {
				delete(n.pr, id)
			}
		}
		for _, id := range n.conf.Voters {
			if id == n.id {
				continue
			}
			if _, ok := n.pr[id]; !ok {
				n.pr[id] = &progress{next: e.Index + 1, match: 0}
			}
		}
	}
	if reqID, ok := n.pendingReqs[e.Index]; ok {
		n.respond(reqID, Outcome{OK: true, Committed: true, Index: e.Index,
			Term: e.Term, Leader: n.id})
		delete(n.pendingReqs, e.Index)
	}
}

func (n *Node) applyClient(e Entry) {
	res := n.sm.Apply(e, ApplyCtx{Now: n.cl.Now()})
	if !res.OK {
		kind := EvReject
		if res.Code == ErrSessionExpired || res.Code == ErrSessionUnknown {
			kind = EvSession
		}
		n.emit(kind, e.Index, "应用被拒绝：reqID="+e.ReqID+" 原因="+res.Reason)
	} else if res.Duplicate {
		n.emit(EvApply, e.Index, "重复请求 reqID="+e.ReqID+"，返回缓存结果，不重复执行")
	} else {
		n.emit(EvApply, e.Index, "应用 op="+e.Op+" key="+e.Key+
			" reqID="+e.ReqID+" term="+itoa(e.Term))
	}
	out := Outcome{
		OK: res.OK, Code: res.Code, Reason: res.Reason,
		Committed: res.OK, Index: e.Index, Term: e.Term,
		Leader: n.lead, Value: res.Value, Duplicate: res.Duplicate,
	}
	// A replay served from the SM cache is still a duplicate even though
	// the entry itself is being applied for the first time on a new leader.
	if res.Duplicate {
		out.Duplicate = true
	}
	if reqID, ok := n.pendingReqs[e.Index]; ok {
		n.respond(reqID, out)
		delete(n.pendingReqs, e.Index)
		if idx, ok2 := n.inflight[reqID]; ok2 && idx == e.Index {
			delete(n.inflight, reqID)
		}
	}
	// Cache the terminal result keyed by session+ReqID so a retry on this
	// leader returns instantly without appending another entry.
	if e.Session != "" {
		n.done[e.Session+"@"+e.ReqID] = out
	}
}

// maybeCompact truncates the log once it exceeds the snapshot threshold.
func (n *Node) maybeCompact() {
	snapIdx := n.snapLastIndex()
	if n.lastApplied-snapIdx < n.cfg.SnapshotThreshold {
		return
	}
	index := n.lastApplied
	term, err := n.log.TermAt(index)
	if err != nil {
		return
	}
	data := n.sm.Snapshot()
	if err := n.log.Compact(index, term, n.conf, data); err != nil {
		return
	}
	n.emit(EvSnapshot, index,
		"创建快照并截断日志至 lastIndex="+itoa(index)+"，firstIndex 现为 "+itoa(n.log.FirstIndex()))
}
