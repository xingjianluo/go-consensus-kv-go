package raft

// lastIndex returns the highest log index (including compacted base).
func (n *Node) lastIndex() Index {
	li := n.opt.Storage.LastIndex()
	if li == 0 && n.snap != nil {
		return n.snap.Index
	}
	return li
}

// lastTerm returns the term of lastIndex.
func (n *Node) lastTerm() Term {
	t, err := n.opt.Storage.Term(n.lastIndex())
	if err != nil && n.snap != nil {
		return n.snap.Term
	}
	return t
}

func (n *Node) termAt(i Index) Term {
	if i == 0 {
		return 0
	}
	if n.snap != nil && i == n.snap.Index {
		return n.snap.Term
	}
	t, err := n.opt.Storage.Term(i)
	if err != nil {
		return 0
	}
	return t
}

func (n *Node) entries(lo, hi Index) []Entry {
	ents, err := n.opt.Storage.Entries(lo, hi)
	if err != nil {
		return nil
	}
	return ents
}

// appendLocal writes entries proposed by this node to its own log and
// returns the first index they occupy.
func (n *Node) appendLocal(kind EntryKind, data ...[]byte) Index {
	li := n.lastIndex()
	es := make([]Entry, 0, len(data))
	for i, d := range data {
		es = append(es, Entry{Term: n.term, Index: li + Index(i) + 1, Kind: kind, Data: d})
	}
	if err := n.opt.Storage.Append(li, es); err != nil {
		return 0
	}
	return li + 1
}

// isUpToDate reports whether (lastTerm, lastIndex) is at least as
// fresh as the local log (RequestVote rule).
func (n *Node) isUpToDate(lastTerm Term, lastIndex Index) bool {
	myTerm := n.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= n.lastIndex()
}

// isMember reports whether id is a voter in the active config.
func (n *Node) isMember(id NodeID, conf Config) bool {
	for _, v := range conf.Voters {
		if v.ID == id {
			return true
		}
	}
	return false
}

func (n *Node) configID(conf Config, id NodeID) string {
	for _, v := range conf.Voters {
		if v.ID == id {
			return v.Addr
		}
	}
	return ""
}
