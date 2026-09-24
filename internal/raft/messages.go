package raft

// Step processes one inbound raft message or local timer event.
func (n *Node) Step(m Message) error {
	if n.stopped {
		return errStopped
	}
	switch m.Type {
	case MsgHup:
		if n.role == Leader {
			return nil
		}
		n.becomeCandidate()
		return nil
	case MsgBeat:
		if n.role != Leader {
			return nil
		}
		// One-shot timer: re-arm before dispatching.
		n.timers.ResetHeartbeat(n.term)
		n.bcastAppend()
		return nil
	}

	// Higher-term handling is per-message below: notably, a RequestVote
	// from a node outside our applied configuration must be ignored
	// WITHOUT adopting its term (Raft membership-change "old server"
	// problem), or a removed node could repeatedly depose the leader.

	switch m.Type {
	case MsgRequestVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgAppend:
		n.handleAppend(m)
	case MsgAppendResp:
		n.handleAppendResp(m)
	case MsgSnapshot:
		n.handleSnapshot(m)
	default:
		// stale/unknown: safely ignored, but explainable.
		n.emit(EvStaleMessage, m.Index, "忽略未知消息类型，from="+m.From)
	}
	return nil
}

// ---- election ----

func (n *Node) handleVote(m Message) {
	// Membership gate FIRST: a removed/never-added node's RequestVote is
	// discarded and its (possibly higher) term is not adopted, so a stale
	// server cannot disrupt the configured cluster ("old server" problem).
	if n.conf.size() > 0 && !n.conf.contains(m.From) {
		n.emit(EvStaleMessage, m.Index,
			"安全忽略非成员 "+m.From+" 的 RequestVote（含其高 term="+itoa(m.Term)+
				"），不更新本地 term，防止旧节点扰乱集群")
		return
	}
	reply := Message{Type: MsgVoteResp, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		reply.Reject = true
		n.emit(EvStaleMessage, m.Index,
			"拒绝过期 RequestVote：candidate term="+itoa(m.Term)+" < 本节点 term="+itoa(n.term))
		n.send(reply)
		return
	}
	if m.Term > n.term {
		n.emit(EvTermStale, n.log.LastIndex(),
			"收到更高 term="+itoa(m.Term)+" 的 RequestVote，降级为 follower")
		n.becomeFollower(m.Term, "")
	}
	upToDate := m.LogTerm > n.lastTerm() ||
		(m.LogTerm == n.lastTerm() && m.Index >= n.log.LastIndex())
	canVote := n.vote == "" || n.vote == m.From
	if canVote && upToDate {
		n.vote = m.From
		n.persistHard()
		n.timers.ResetElection(n.term)
		reply.Term = n.term
		reply.Granted = true
		n.emit(EvElection, m.Index,
			"投票给 "+m.From+"：日志 (term="+itoa(m.LogTerm)+",idx="+itoa(m.Index)+
				") 不落后于本地 (term="+itoa(n.lastTerm())+",idx="+itoa(n.log.LastIndex())+")")
	} else {
		reply.Reject = true
		why := "本 term 已投给 " + n.vote
		if !upToDate {
			why = "候选日志落后 (term=" + itoa(m.LogTerm) + ",idx=" + itoa(m.Index) +
				") vs 本地 (term=" + itoa(n.lastTerm()) + ",idx=" + itoa(n.log.LastIndex()) + ")"
		}
		n.emit(EvReject, m.Index, "拒绝投票给 "+m.From+"："+why)
	}
	n.send(reply)
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		n.emit(EvStaleMessage, m.Index,
			"忽略过期投票响应：role="+n.role.String()+" 消息term="+itoa(m.Term))
		return
	}
	if m.Reject {
		n.emit(EvReject, m.Index, "投票被 "+m.From+" 拒绝，term="+itoa(m.Term))
		return
	}
	n.votes[m.From] = true
	granted := 0
	for _, v := range n.votes {
		if v {
			granted++
		}
	}
	if granted >= n.conf.Quorum() {
		n.becomeLeader()
	}
}

// ---- append entries (follower side) ----

func (n *Node) handleAppend(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendResp, From: n.id, To: m.From,
			Term: n.term, Reject: true, Index: n.log.LastIndex()})
		n.emit(EvStaleMessage, m.Index,
			"拒绝旧 term="+itoa(m.Term)+" 的 AppendEntries，本节点 term="+itoa(n.term))
		return
	}
	// A node with no applied configuration is unbootstrapped/pre-join and
	// must accept the first leader to receive its snapshot/conf entry.
	bootstrapping := n.conf.size() == 0
	if !bootstrapping && !n.conf.contains(m.From) {
		n.emit(EvStaleMessage, m.Index, "忽略非成员 leader "+m.From+" 的 AppendEntries")
		return
	}
	if m.Term > n.term {
		n.emit(EvTermStale, n.log.LastIndex(),
			"收到更高 term="+itoa(m.Term)+" 的 AppendEntries，降级为 follower")
		n.becomeFollower(m.Term, m.From)
	}
	// A legitimate leader for our term: candidate must step down even when
	// the term is equal (Raft §5.2).
	if n.role != Follower {
		n.becomeFollower(m.Term, m.From)
	} else {
		n.lead = m.From
		n.timers.ResetElection(n.term)
	}

	reply := Message{Type: MsgAppendResp, From: n.id, To: m.From, Term: n.term}

	// Need snapshot: leader's previous log position is compacted here.
	snapIdx := n.snapLastIndex()
	if m.Index < snapIdx {
		reply.Reject = true
		reply.NeedSnap = true
		reply.Index = snapIdx
		n.send(reply)
		n.emit(EvStaleMessage, m.Index,
			"prevLogIndex="+itoa(m.Index)+" 已被快照截断(idx="+itoa(snapIdx)+")，请求 leader 发送快照")
		return
	}

	if m.Index > 0 {
		t, err := n.log.TermAt(m.Index)
		if err != nil || t != m.LogTerm {
			reply.Reject = true
			reply.Index = n.conflictHint(m.Index)
			n.send(reply)
			n.emit(EvReject, m.Index,
				"日志一致性检查失败于 idx="+itoa(m.Index)+
					"，回退提示 idx="+itoa(reply.Index)+"，等待 leader 回退补齐")
			return
		}
	}

	// Storage.Append truncates any conflicting suffix (Raft fig. 2:
	// "if an existing entry conflicts ... delete it and all that follow").
	if len(m.Entries) > 0 {
		lastNew := m.Entries[len(m.Entries)-1].Index
		_ = n.log.Append(m.Entries)
		n.emit(EvReplicate, lastNew,
			"接收 leader 日志 ["+itoa(m.Entries[0].Index)+","+itoa(lastNew)+"] term="+itoa(m.Entries[len(m.Entries)-1].Term))
		reply.Index = lastNew
	} else {
		reply.Index = m.Index
	}

	if m.Commit > n.commitIndex {
		n.advanceCommit(minInt(m.Commit, n.log.LastIndex()))
	}
	n.send(reply)
}

// conflictHint returns a faster backtrack index for the leader.
func (n *Node) conflictHint(prevIndex int) int {
	if t, err := n.log.TermAt(prevIndex); err == nil {
		// step back over all entries with that term
		i := prevIndex
		for i > n.snapLastIndex() {
			tt, e := n.log.TermAt(i - 1)
			if e != nil || tt != t {
				break
			}
			i--
		}
		return i
	}
	return n.snapLastIndex()
}

// ---- append entries (leader side) ----

func (n *Node) bcastAppend() {
	// Replicate to applied voters AND any not-yet-effective voter that is
	// catching up for a pending membership change. Iterate in sorted id
	// order to keep the simulated schedule deterministic.
	targets := map[string]bool{}
	for _, id := range n.conf.Voters {
		targets[id] = true
	}
	for id := range n.pr {
		targets[id] = true
	}
	for _, id := range sortedVoters(ConfState{Voters: keys(targets)}) {
		if id == n.id {
			continue
		}
		n.sendAppend(id)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func (n *Node) sendAppend(to string) {
	p, ok := n.pr[to]
	if !ok {
		// newly added voter not yet in applied conf: initialize lazily
		p = &progress{next: n.log.LastIndex() + 1, match: 0}
		n.pr[to] = p
	}
	snap := n.log.Snapshot()
	if p.next <= n.snapLastIndex() {
		if snap == nil {
			return
		}
		n.send(Message{
			Type: MsgSnapshot, From: n.id, To: to, Term: n.term, Snapshot: snap,
			Index: snap.LastIndex,
		})
		return
	}
	prev := p.next - 1
	prevTerm, err := n.log.TermAt(prev)
	if err != nil {
		// index compacted on the leader between snapshots; send snapshot
		if snap != nil {
			n.send(Message{Type: MsgSnapshot, From: n.id, To: to, Term: n.term, Snapshot: snap})
		}
		return
	}
	entries, _ := n.log.Entries(p.next, n.log.LastIndex()+1)
	n.send(Message{
		Type: MsgAppend, From: n.id, To: to, Term: n.term,
		Index: prev, LogTerm: prevTerm, Entries: entries, Commit: n.commitIndex,
	})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		n.emit(EvStaleMessage, m.Index, "非 leader 忽略 AppendEntries 响应，from="+m.From)
		return
	}
	if m.Term != n.term {
		n.emit(EvStaleMessage, m.Index,
			"忽略旧 term="+itoa(m.Term)+" 的复制响应，本节点 term="+itoa(n.term))
		return
	}
	p, ok := n.pr[m.From]
	if !ok {
		// follower not in applied conf; stale reply from removed node.
		n.emit(EvStaleMessage, m.Index, "安全忽略已移除/未生效成员 "+m.From+" 的复制响应")
		return
	}
	if m.NeedSnap {
		if snap := n.log.Snapshot(); snap != nil {
			n.send(Message{Type: MsgSnapshot, From: n.id, To: m.From, Term: n.term, Snapshot: snap})
			n.emit(EvSnapshot, snap.LastIndex, "应 "+m.From+" 请求发送快照，lastIndex="+itoa(snap.LastIndex))
		}
		return
	}
	if m.Reject {
		hint := m.Index
		if hint < n.snapLastIndex() {
			hint = n.snapLastIndex()
		}
		if hint+1 < p.next {
			p.next = hint + 1
		} else if p.next > 1 {
			p.next--
		}
		n.emit(EvReplicate, m.Index, "复制到 "+m.From+" 被拒，nextIndex 回退到 "+itoa(p.next))
		n.sendAppend(m.From)
		return
	}
	p.match = m.Index
	p.next = m.Index + 1
	n.tryCommit()
	n.onAck(m.From)
}

func (n *Node) tryCommit() {
	// Highest N replicated on a majority, only for current-term entries.
	matches := []int{n.log.LastIndex()}
	for _, id := range n.conf.Voters {
		if id == n.id {
			continue
		}
		if p, ok := n.pr[id]; ok {
			matches = append(matches, p.match)
		}
	}
	sortDesc(matches)
	nth := matches[n.conf.Quorum()-1]
	if nth > n.commitIndex {
		t, err := n.log.TermAt(nth)
		if err == nil && t == n.term {
			n.advanceCommit(nth)
			n.bcastAppend() // propagate commit index immediately
		}
	}
}

func (n *Node) advanceCommit(to int) {
	if to <= n.commitIndex {
		return
	}
	n.commitIndex = to
	n.applyCommitted()
}

// ---- install snapshot (follower side) ----

func (n *Node) handleSnapshot(m Message) {
	snap := m.Snapshot
	if snap == nil {
		return
	}
	if m.Term < n.term {
		n.emit(EvStaleMessage, snap.LastIndex, "拒绝旧 term 快照")
		return
	}
	if m.Term > n.term {
		n.emit(EvTermStale, snap.LastIndex, "收到更高 term 的快照，降级为 follower")
		n.becomeFollower(m.Term, m.From)
	}
	if snap.LastIndex <= n.snapLastIndex() {
		n.emit(EvStaleMessage, snap.LastIndex,
			"安全忽略迟到快照：其 lastIndex="+itoa(snap.LastIndex)+" 不新于本地")
		return
	}
	if err := n.log.ApplySnapshot(*snap); err != nil {
		n.emit(EvReject, snap.LastIndex, "应用快照失败："+err.Error())
		return
	}
	if err := n.env.ApplySnapshotData(snap.Data); err != nil {
		n.emit(EvReject, snap.LastIndex, "恢复应用快照失败："+err.Error())
		return
	}
	n.conf = snap.Conf
	n.commitIndex = snap.LastIndex
	n.lastApplied = snap.LastIndex
	n.lead = m.From
	n.timers.ResetElection(n.term)
	if rr := n.lead; rr != "" {
		// nothing: leader-side onAck is driven by its own response handling
	}
	n.emit(EvSnapshot, snap.LastIndex,
		"安装快照恢复至 lastIndex="+itoa(snap.LastIndex)+",lastTerm="+itoa(snap.LastTerm)+
			"，配置="+joinIDs(snap.Conf.Voters))
	n.send(Message{Type: MsgAppendResp, From: n.id, To: m.From,
		Term: n.term, Index: snap.LastIndex})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortDesc(x []int) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j] > x[j-1]; j-- {
			x[j], x[j-1] = x[j-1], x[j]
		}
	}
}
