package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const walName = "wal.log"

// FileStorage is a directory-backed Storage (one append-only WAL of
// JSON frames) used by real local processes so a crash/restart
// recovers hard state, log and the latest snapshot deterministically.
type FileStorage struct {
	mu  sync.Mutex
	dir string
	f   *os.File
	mem *MemStorage
}

type appendFrame struct {
	Prev    Index
	Entries []Entry
}

// NewFileStorage opens (or creates) storage rooted at dir and replays
// the WAL. Writes are fsynced before returning.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir, mem: NewMemStorage()}
	if err := s.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	return s, nil
}

// Close releases the WAL file handle.
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

func (s *FileStorage) replay() error {
	f, err := os.Open(filepath.Join(s.dir, walName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufReader{br: bufio.NewReader(f)}
	for {
		typ, payload, err := r.readFrame()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			// A torn tail frame after a crash is truncated on next open.
			if errors.Is(err, errTornFrame) {
				return s.truncateAt(r.offset)
			}
			return err
		}
		switch typ {
		case 'a':
			var fr appendFrame
			if err := json.Unmarshal(payload, &fr); err != nil {
				return err
			}
			if err := s.mem.Append(fr.Prev, fr.Entries); err != nil {
				return fmt.Errorf("replay append: %w", err)
			}
		case 'h':
			var h HardState
			if err := json.Unmarshal(payload, &h); err != nil {
				return err
			}
			if err := s.mem.SetHard(h); err != nil {
				return err
			}
		case 'c':
			var sn Snapshot
			if err := json.Unmarshal(payload, &sn); err != nil {
				return err
			}
			if err := s.mem.Compact(sn); err != nil {
				return fmt.Errorf("replay compact: %w", err)
			}
		case 'i':
			var sn Snapshot
			if err := json.Unmarshal(payload, &sn); err != nil {
				return err
			}
			if err := s.mem.InstallSnapshot(sn); err != nil {
				return fmt.Errorf("replay install-snapshot: %w", err)
			}
		default:
			return fmt.Errorf("unknown wal frame type %q", typ)
		}
	}
}

func (s *FileStorage) truncateAt(off int64) error {
	if err := os.Truncate(filepath.Join(s.dir, walName), off); err != nil {
		return err
	}
	return nil
}

// writeFrame appends one length-prefixed frame and fsyncs.
func (s *FileStorage) writeFrame(typ byte, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	hdr := make([]byte, 5)
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := s.f.Write(hdr); err != nil {
		return err
	}
	if _, err := s.f.Write(payload); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *FileStorage) InitialState() (HardState, Config, bool) {
	return s.mem.InitialState()
}

func (s *FileStorage) FirstIndex() Index { return s.mem.FirstIndex() }
func (s *FileStorage) LastIndex() Index  { return s.mem.LastIndex() }

func (s *FileStorage) Term(i Index) (Term, error) { return s.mem.Term(i) }

func (s *FileStorage) Entries(lo, hi Index) ([]Entry, error) {
	return s.mem.Entries(lo, hi)
}

func (s *FileStorage) Snapshot() (*Snapshot, error) { return s.mem.Snapshot() }

func (s *FileStorage) Append(prev Index, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeFrame('a', appendFrame{Prev: prev, Entries: entries}); err != nil {
		return err
	}
	return s.mem.Append(prev, entries)
}

func (s *FileStorage) Compact(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeFrame('c', snap); err != nil {
		return err
	}
	return s.mem.Compact(snap)
}

// InstallSnapshot persists a leader-pushed snapshot that replaces the
// local log, then applies it to the in-memory view.
func (s *FileStorage) InstallSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeFrame('i', snap); err != nil {
		return err
	}
	return s.mem.InstallSnapshot(snap)
}

func (s *FileStorage) SetHard(h HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeFrame('h', h); err != nil {
		return err
	}
	return s.mem.SetHard(h)
}

var errTornFrame = errors.New("torn frame")

type bufReader struct {
	br     *bufio.Reader
	offset int64
}

func (b *bufReader) readFrame() (byte, []byte, error) {
	hdr := make([]byte, 5)
	n, err := io.ReadFull(b.br, hdr)
	b.offset += int64(n)
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		if n == 0 && errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, errTornFrame
	}
	if err != nil {
		return 0, nil, err
	}
	ln := binary.BigEndian.Uint32(hdr[1:])
	payload := make([]byte, ln)
	n, err = io.ReadFull(b.br, payload)
	b.offset += int64(n)
	if err != nil {
		return 0, nil, errTornFrame
	}
	return hdr[0], payload, nil
}
