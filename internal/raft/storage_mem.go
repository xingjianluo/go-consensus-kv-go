package raft

import (
	"encoding/json"
	"sync"
)

// MemStorage is an in-memory Storage used by the deterministic
// simulation harness. It survives partition/network failures and
// simulated process restarts (the Cluster keeps the pointer); only an
// explicit wipe models a permanent loss of the data directory.
type MemStorage struct {
	mu   sync.RWMutex
	hard HardState
	snap Snapshot
	have bool // a snapshot has been installed
	// ents[0] is a compacted marker whose Index/Term equal the snapshot
	// (or 0/0 before any compaction); ents[1:] are live entries.
	ents []Entry
}

// NewMemStorage returns an empty in-memory storage.
func NewMemStorage() *MemStorage {
	return &MemStorage{ents: []Entry{{}}}
}

func (s *MemStorage) InitialState() (HardState, Config, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hard, s.snap.Conf, s.have
}

func (s *MemStorage) FirstIndex() Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ents[0].Index + 1
}

func (s *MemStorage) LastIndex() Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex()
}

func (s *MemStorage) lastIndex() Index {
	return s.ents[len(s.ents)-1].Index
}

func (s *MemStorage) Term(i Index) (Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < s.ents[0].Index || i > s.lastIndex() {
		return 0, ErrUnavailable
	}
	return s.ents[i-s.ents[0].Index].Term, nil
}

func (s *MemStorage) Entries(lo, hi Index) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	base := s.ents[0].Index
	if lo <= base {
		return nil, ErrUnavailable
	}
	if hi > s.lastIndex()+1 {
		hi = s.lastIndex() + 1
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]Entry, hi-lo)
	copy(out, s.ents[lo-base:hi-base])
	return out, nil
}

func (s *MemStorage) Snapshot() (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.have {
		return nil, nil
	}
	cp := s.snap
	return &cp, nil
}

func (s *MemStorage) Append(prev Index, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.ents[0].Index
	if prev < base {
		// Caller is referencing compacted history it must obtain via
		// snapshot; signal conflict.
		return ErrUnavailable
	}
	if prev > s.lastIndex() {
		// Gap: entries beyond our log cannot be appended directly.
		return ErrUnavailable
	}
	pos := prev - base
	// Truncate conflict: if an existing entry differs in term, drop the
	// suffix starting at the first incoming index.
	if len(entries) > 0 {
		cut := -1
		for i, e := range entries {
			abs := prev + Index(i) + 1
			j := abs - base
			if int(j) < len(s.ents) && s.ents[j].Term != e.Term {
				cut = int(j)
				break
			}
		}
		if cut >= 0 {
			s.ents = s.ents[:cut]
		}
	}
	// Append only entries beyond what we already hold with matching term.
	start := int(pos) + 1
	off := 0
	if start < len(s.ents) {
		off = len(s.ents) - start
		if off > len(entries) {
			off = len(entries)
		}
	}
	s.ents = append(s.ents, entries[off:]...)
	return nil
}

func (s *MemStorage) Compact(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.ents[0].Index
	if snap.Index <= base {
		return ErrUnavailable
	}
	if snap.Index > s.lastIndex() {
		return ErrUnavailable
	}
	term := s.ents[snap.Index-base].Term
	if term != snap.Term {
		// Snapshot must describe an entry actually present at that index.
		snap.Term = term
	}
	s.snap = snap
	s.have = true
	// Keep the compacted marker; live entries begin at snap.Index+1.
	s.ents = append([]Entry{{Term: snap.Term, Index: snap.Index}}, s.ents[snap.Index-base+1:]...)
	return nil
}

// InstallSnapshot replaces all log contents with the leader snapshot,
// allowing snap.Index to be ahead of the current last index.
func (s *MemStorage) InstallSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.have && snap.Index < s.snap.Index {
		return ErrUnavailable
	}
	s.snap = snap
	s.have = true
	s.ents = []Entry{{Term: snap.Term, Index: snap.Index}}
	return nil
}

func (s *MemStorage) SetHard(h HardState) error {
	s.mu.Lock()
	s.hard = h
	s.mu.Unlock()
	return nil
}

// EncodeConfig serializes a membership configuration.
func EncodeConfig(c Config) ([]byte, error) { return json.Marshal(c) }

// DecodeConfig parses a membership configuration.
func DecodeConfig(b []byte) (Config, error) {
	var c Config
	err := json.Unmarshal(b, &c)
	return c, err
}
