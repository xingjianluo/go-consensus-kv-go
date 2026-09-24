package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// DiskStorage persists hard state, the log and snapshots to a local
// directory using plain JSON files (no external services, no dependencies
// beyond the standard library). Writes are fsync-ed.
type DiskStorage struct {
	mu  sync.Mutex
	dir string

	hard HardState
	snap *SnapshotMeta
	log  []Entry // anchor-first, like MemStorage
}

const (
	hardFile = "hardstate.json"
	snapFile = "snapshot.json"
	logFile  = "log.json"
)

// OpenDiskStorage opens or creates on-disk storage at dir.
func OpenDiskStorage(dir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &DiskStorage{dir: dir}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *DiskStorage) load() error {
	s.hard = HardState{}
	if b, err := os.ReadFile(filepath.Join(s.dir, hardFile)); err == nil {
		if err := json.Unmarshal(b, &s.hard); err != nil {
			return fmt.Errorf("hardstate: %w", err)
		}
	}
	s.snap = nil
	if b, err := os.ReadFile(filepath.Join(s.dir, snapFile)); err == nil && len(b) > 0 {
		var meta SnapshotMeta
		if err := json.Unmarshal(b, &meta); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		s.snap = &meta
	}
	s.log = nil
	if b, err := os.ReadFile(filepath.Join(s.dir, logFile)); err == nil && len(b) > 0 {
		if err := json.Unmarshal(b, &s.log); err != nil {
			return fmt.Errorf("log: %w", err)
		}
	}
	if s.log == nil {
		s.log = []Entry{{Term: 0, Index: 0}}
	}
	return nil
}

func writeFileSync(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(tmp)); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *DiskStorage) InitialState() HardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hard
}

func (s *DiskStorage) SaveHardState(st HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hard = st
	return writeFileSync(filepath.Join(s.dir, hardFile), st)
}

func (s *DiskStorage) flushLogLocked() error {
	return writeFileSync(filepath.Join(s.dir, logFile), s.log)
}

func (s *DiskStorage) FirstIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log[0].Index + 1
}

func (s *DiskStorage) LastIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log[len(s.log)-1].Index
}

func (s *DiskStorage) slot(index int) int { return index - s.log[0].Index }

func (s *DiskStorage) TermAt(index int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < s.log[0].Index || index > s.log[len(s.log)-1].Index {
		return 0, errCompacted
	}
	return s.log[s.slot(index)].Term, nil
}

func (s *DiskStorage) Entries(lo, hi int) ([]Entry, error) {
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

func (s *DiskStorage) Entry(index int) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < s.log[0].Index || index > s.log[len(s.log)-1].Index {
		return Entry{}, errCompacted
	}
	return s.log[s.slot(index)], nil
}

func (s *DiskStorage) Append(entries []Entry) error {
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
	s.log = append(s.log[:s.slot(first)], entries...)
	return s.flushLogLocked()
}

func (s *DiskStorage) Snapshot() *SnapshotMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap == nil {
		return nil
	}
	cp := *s.snap
	return &cp
}

func (s *DiskStorage) Compact(index, term int, conf ConfState, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := s.log[len(s.log)-1].Index
	if index <= s.log[0].Index || index > last {
		return errUnavailable
	}
	off := s.slot(index)
	meta := SnapshotMeta{LastIndex: index, LastTerm: term, Conf: conf, Data: data}
	if err := writeFileSync(filepath.Join(s.dir, snapFile), meta); err != nil {
		return err
	}
	s.snap = &meta
	s.log = append([]Entry{s.log[off]}, s.log[off+1:]...)
	return s.flushLogLocked()
}

func (s *DiskStorage) ApplySnapshot(meta SnapshotMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeFileSync(filepath.Join(s.dir, snapFile), meta); err != nil {
		return err
	}
	mp := meta
	s.snap = &mp
	s.log = []Entry{{Term: meta.LastTerm, Index: meta.LastIndex}}
	return s.flushLogLocked()
}

func (s *DiskStorage) Sync() error  { return nil }
func (s *DiskStorage) Close() error { return nil }

// LoadSnapshotConf returns the voter set stored in the on-disk snapshot,
// or an error when absent.
func LoadSnapshotConf(dir string) (ConfState, bool) {
	if b, err := os.ReadFile(filepath.Join(dir, snapFile)); err == nil && len(b) > 0 {
		var meta SnapshotMeta
		if err := json.Unmarshal(b, &meta); err == nil {
			return meta.Conf, true
		}
	}
	return ConfState{}, false
}

var _ = sort.Strings
var _ = errors.New
