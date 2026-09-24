package raft

import "sync"

// MemStorage is an in-memory Storage used by the deterministic simulator.
// It models durable disk across a simulated crash: the simulator keeps the
// same MemStorage when a Node is rebuilt.
type MemStorage struct {
	mu   sync.Mutex
	hard HardState
	snap *SnapshotMeta
	// log always starts with an anchor entry at snapshot.LastIndex.
	log []Entry
}

// NewMemStorage returns an empty store whose anchor is at index 0/term 0.
func NewMemStorage() *MemStorage {
	s := &MemStorage{hard: HardState{Vote: ""}}
	s.log = []Entry{{Term: 0, Index: 0}}
	return s
}

func (s *MemStorage) InitialState() HardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hard
}

func (s *MemStorage) SaveHardState(st HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hard = st
	return nil
}

func (s *MemStorage) FirstIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log[0].Index + 1
}

func (s *MemStorage) LastIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log[len(s.log)-1].Index
}

func (s *MemStorage) slot(index int) int { return index - s.log[0].Index }

func (s *MemStorage) TermAt(index int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < s.log[0].Index || index > s.log[len(s.log)-1].Index {
		return 0, errCompacted
	}
	return s.log[s.slot(index)].Term, nil
}

func (s *MemStorage) Entries(lo, hi int) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.log[0].Index + 1
	if lo < first {
		return nil, errCompacted
	}
	if hi > s.log[len(s.log)-1].Index+1 {
		hi = s.log[len(s.log)-1].Index + 1
	}
	if lo > hi {
		return nil, nil
	}
	out := make([]Entry, 0, hi-lo)
	out = append(out, s.log[s.slot(lo):s.slot(hi)]...)
	return out, nil
}

func (s *MemStorage) Entry(index int) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < s.log[0].Index || index > s.log[len(s.log)-1].Index {
		return Entry{}, errCompacted
	}
	return s.log[s.slot(index)], nil
}

func (s *MemStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	first := entries[0].Index
	if first <= s.log[0].Index {
		return errCompacted
	}
	last := s.log[len(s.log)-1].Index
	if first > last+1 {
		return errUnavailable
	}
	// Overwrite conflicting suffix (Raft figure 8 truncation).
	s.log = append(s.log[:s.slot(first)], entries...)
	return nil
}

func (s *MemStorage) Snapshot() *SnapshotMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap == nil {
		return nil
	}
	cp := *s.snap
	return &cp
}

func (s *MemStorage) Compact(index, term int, conf ConfState, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := s.log[len(s.log)-1].Index
	if index <= s.log[0].Index || index > last {
		return errUnavailable
	}
	off := s.slot(index)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	s.snap = &SnapshotMeta{LastIndex: index, LastTerm: term, Conf: conf, Data: dataCopy}
	anchor := s.log[off]
	s.log = append([]Entry{anchor}, s.log[off+1:]...)
	return nil
}

func (s *MemStorage) ApplySnapshot(meta SnapshotMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dataCopy := make([]byte, len(meta.Data))
	copy(dataCopy, meta.Data)
	mp := meta
	mp.Data = dataCopy
	s.snap = &mp
	s.log = []Entry{{Term: meta.LastTerm, Index: meta.LastIndex}}
	return nil
}

func (s *MemStorage) Sync() error  { return nil }
func (s *MemStorage) Close() error { return nil }
