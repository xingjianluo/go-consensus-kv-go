package raft

import "sort"

// Propose handles a client request. Errors here are synchronous, local
// rejections only; a nil error means the request became a proposal and the
// terminal Outcome will arrive through Env.Respond.
func (n *Node) Propose(req ClientRequest) error {
	if n.stopped {
		return errStopped
	}
	if req.ReadOnly {
		return n.proposeRead(req)
	}
	if n.role != Leader {
		n.respond(req.ReqID, Outcome{
			OK: false, Code: ErrNotLeader,
			Reason: "当前节点不是 leader，不能接受写入",
			Leader: n.lead, Term: n.term,
		})
		return nil
	}
	// Membership changes go through the dedicated proposal.
	if req.Op == "add" || req.Op == "remove" {
		n.respond(req.ReqID, Outcome{OK: false, Code: ErrPendingConf,
			Reason: "成员变更必须使用 AddMember/RemoveMember", Leader: n.id})
		return nil
	}
	// One membership change at a time prevents overlapping joint changes.
	if n.pendingConf != nil {
		n.respond(req.ReqID, Outcome{OK: false, Code: ErrPendingConf,
			Reason: "已有成员变更在提交中(index=" + itoa(n.pendingIndex) + ")，拒绝新写入以防双主/回退",
			Leader: n.id})
		return nil
	}
	// Fast session validation: reject deterministically before appending
	// so callers never confuse an expired write with an unknown commit.
	// register/keepalive establish/maintain sessions, so they skip it.
	if req.Op != "register" && req.Op != "keepalive" {
		if vs, ok := n.sm.(interface {
			ValidateSession(string, int64) ErrorCode
		}); ok {
			if code := vs.ValidateSession(req.Session, n.cl.Now().UnixNano()); code != "" {
				reason := "会话已过期，拒绝写入"
				if code == ErrSessionUnknown {
					reason = "会话不存在，拒绝写入"
				}
				n.respond(req.ReqID, Outcome{OK: false, Code: code, Reason: reason,
					Leader: n.id, Term: n.term})
				n.emit(EvSession, n.commitIndex, "leader 预校验拒绝 reqID="+req.ReqID+"："+reason)
				return nil
			}
		}
	}
	// Already committed on this leader: return cached terminal result.
	if req.Session != "" {
		if cached, ok := n.done[req.Session+"@"+req.ReqID]; ok {
			cached.Duplicate = true
			n.respond(req.ReqID, cached)
			n.emit(EvApply, n.commitIndex,
				"reqID="+req.ReqID+" 已提交过，直接返回缓存结论，不再追加日志，index="+itoa(cached.Index))
			return nil
		}
	}
	// Duplicate in-flight request: do not append again.
	if idx, ok := n.inflight[req.ReqID]; ok {
		// It will be answered when index commits; attach as pending alias.
		n.pendingReqs[idx] = req.ReqID
		n.emit(EvReplicate, idx, "reqID="+req.ReqID+" 已在提案中，复用同一日志位置，不重复追加")
		return nil
	}
	e := Entry{
		Term: n.term, Kind: EntryClient,
		ReqID: req.ReqID, Session: req.Session, Op: req.Op,
		Key: req.Key, Value: req.Value, TTLNs: req.TTLNs,
	}
	idx := n.appendEntries([]Entry{e}, nil)
	n.pendingReqs[idx] = req.ReqID
	n.inflight[req.ReqID] = idx
	n.emit(EvProposal, idx, "leader 追加客户端提案 op="+req.Op+" key="+req.Key+
		" reqID="+req.ReqID+" term="+itoa(n.term))
	n.bcastAppend()
	// A single-node cluster applies immediately via commit path.
	n.tryCommit()
	return nil
}

// AddMember proposes adding voter id.
func (n *Node) AddMember(reqID, id string) error {
	return n.proposeConf(reqID, id, true)
}

// RemoveMember proposes removing voter id.
func (n *Node) RemoveMember(reqID, id string) error {
	return n.proposeConf(reqID, id, false)
}

func (n *Node) proposeConf(reqID, id string, add bool) error {
	if n.stopped {
		return errStopped
	}
	if n.role != Leader {
		n.respond(reqID, Outcome{OK: false, Code: ErrNotLeader,
			Reason: "成员变更必须经 leader 提交", Leader: n.lead, Term: n.term})
		return nil
	}
	if n.pendingConf != nil {
		n.respond(reqID, Outcome{OK: false, Code: ErrPendingConf,
			Reason: "上一个成员变更尚未提交，串行执行", Leader: n.id,
			Index: n.pendingIndex})
		return nil
	}
	set := map[string]bool{}
	for _, v := range n.conf.Voters {
		set[v] = true
	}
	if add {
		if set[id] {
			n.respond(reqID, Outcome{OK: false, Code: ErrUnknownNode,
				Reason: id + " 已在配置中", Leader: n.id})
			return nil
		}
		set[id] = true
	} else {
		if !set[id] {
			n.respond(reqID, Outcome{OK: false, Code: ErrUnknownNode,
				Reason: id + " 不在当前配置中", Leader: n.id})
			return nil
		}
		delete(set, id)
	}
	next := ConfState{}
	for v := range set {
		next.Voters = append(next.Voters, v)
	}
	sort.Strings(next.Voters)

	conf := next
	e := Entry{Term: n.term, Kind: EntryConf, ReqID: reqID, Conf: &conf}
	idx := n.appendEntries([]Entry{e}, &next)
	n.pendingConf = &next
	n.pendingIndex = idx
	n.pendingReqs[idx] = reqID

	// Proactively replicate to an added (possibly unconfigured) node so it
	// catches up and can participate once the conf entry commits.
	if add {
		n.pr[id] = &progress{next: 1, match: 0}
	}
	action := "加入"
	if !add {
		action = "退出"
	}
	n.emit(EvConfChange, idx, "提出成员"+action+" "+id+"，新配置=["+joinIDs(next.Voters)+
		"]（单节点串行变更，提交前旧配置仍生效）")
	n.bcastAppend()
	n.tryCommit()
	return nil
}

// ---- linearizable reads (ReadIndex) ----

func (n *Node) proposeRead(req ClientRequest) error {
	if n.role != Leader {
		n.respond(req.ReqID, Outcome{OK: false, Code: ErrNotLeader,
			Reason: "线性一致读必须经当前 leader，避免过期数据",
			Leader: n.lead, Term: n.term})
		return nil
	}
	// Session must be live at proposal time.
	if vs, ok := n.sm.(interface {
		ValidateSession(string, int64) ErrorCode
	}); ok {
		if code := vs.ValidateSession(req.Session, n.cl.Now().UnixNano()); code != "" {
			n.respond(req.ReqID, Outcome{OK: false, Code: code,
				Reason: "会话无效，拒绝读取以保证一致性结论明确", Leader: n.id, Term: n.term})
			return nil
		}
	}
	n.readSeq++
	r := &readState{
		reqID: req.ReqID, session: req.Session, key: req.Key,
		startAt: n.cl.Now(), index: n.commitIndex,
		acks: map[string]bool{n.id: true},
	}
	key := itoa(n.readSeq)
	n.reads[key] = r
	n.emit(EvRead, n.commitIndex,
		"ReadIndex 开始：读请求 "+req.ReqID+" 需确认本 term 已提交并获得多数心跳确认，barrier="+itoa(n.commitIndex))
	// A heartbeat round establishes leadership (and commits the term no-op,
	// which guarantees we have not been superseded).
	n.bcastAppend()
	return nil
}

// onAck processes a follower append/snapshot acknowledgement for reads.
func (n *Node) onAck(from string) {
	if n.role != Leader {
		return
	}
	readKeys := make([]string, 0, len(n.reads))
	for k := range n.reads {
		readKeys = append(readKeys, k)
	}
	sort.Strings(readKeys)
	for _, key := range readKeys {
		r := n.reads[key]
		r.acks[from] = true
		if len(r.acks) < n.conf.Quorum() {
			continue
		}
		// Wait until the barrier (and the term no-op) is applied so the
		// local state machine reflects every committed entry.
		if n.lastApplied < r.index {
			continue
		}
		// The no-op at this term must have committed, certifying this
		// leader's authority for the read.
		if !n.termCommitted() {
			continue
		}
		v, ok := n.readLocal(r)
		out := Outcome{OK: true, Committed: true, Leader: n.id, Term: n.term, Index: r.index}
		if ok {
			out.Value = v
		} else {
			out.Code = ErrorCode("key_not_found")
			out.Reason = "key 不存在（线性一致读，已确认多数派）"
		}
		n.respond(r.reqID, out)
		n.emit(EvRead, r.index,
			"ReadIndex 完成："+r.reqID+" 多数派确认数="+itoa(len(r.acks))+
				"，applied>="+itoa(r.index)+"，返回线性一致结果")
		delete(n.reads, key)
	}
}

func (n *Node) readLocal(r *readState) (string, bool) {
	type reader interface {
		Get(string) (string, bool)
	}
	if kv, ok := n.sm.(reader); ok {
		return kv.Get(r.key)
	}
	return "", false
}

// termCommitted reports whether an entry from the current term is
// committed; required before the leader serves reads.
func (n *Node) termCommitted() bool {
	t, err := n.log.TermAt(n.commitIndex)
	return err == nil && t == n.term
}

// ---- timers ----

// Tick processes one due timer (election/heartbeat).
func (n *Node) Tick(tag string) error {
	if n.stopped {
		return errStopped
	}
	switch tag {
	case "election":
		if n.role != Leader {
			n.emit(EvTimeout, n.log.LastIndex(),
				"选举超时(未收到有效 leader 心跳)，随机化的超时到期")
			return n.Step(Message{Type: MsgHup, From: n.id, To: n.id, Term: n.term})
		}
	case "heartbeat":
		if n.role == Leader {
			return n.Step(Message{Type: MsgBeat, From: n.id, To: n.id, Term: n.term})
		}
	}
	return nil
}
