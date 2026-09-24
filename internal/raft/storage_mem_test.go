package raft

import "testing"

func ent(i Index, t Term) Entry { return Entry{Term: t, Index: i, Kind: EntryData} }

func TestMemAppendOverlap(t *testing.T) {
	s := NewMemStorage()
	_ = Bootstrap(s, "x", []NodeID{"x"}) // entries: marker 0 + conf@1
	if err := s.Append(1, []Entry{ent(2, 1), ent(3, 1), ent(4, 1)}); err != nil {
		t.Fatal(err)
	}
	// duplicate full batch
	if err := s.Append(1, []Entry{ent(2, 1), ent(3, 1), ent(4, 1)}); err != nil {
		t.Fatal(err)
	}
	// later batch then a resend of the full older batch (can arrive on reconnect)
	if err := s.Append(4, []Entry{ent(5, 1), ent(6, 1), ent(7, 1), ent(8, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(1, []Entry{ent(2, 1), ent(3, 1), ent(4, 1)}); err != nil {
		t.Fatal(err)
	}
	for i := Index(1); i <= 8; i++ {
		if got := s.LastIndex(); got < i {
			t.Fatalf("missing index %d last=%d", i, got)
		}
	}
	if s.LastIndex() != 8 {
		t.Fatalf("last=%d want 8", s.LastIndex())
	}
}

func TestMemAppendTruncateConflict(t *testing.T) {
	s := NewMemStorage()
	_ = s.Append(0, []Entry{ent(1, 1), ent(2, 1), ent(3, 2)})
	// leader forces term 2 at index 2 and 3
	if err := s.Append(1, []Entry{ent(2, 2), ent(3, 2)}); err != nil {
		t.Fatal(err)
	}
	t2, _ := s.Term(2)
	t3, _ := s.Term(3)
	if t2 != 2 || t3 != 2 {
		t.Fatalf("terms after conflict: %d %d", t2, t3)
	}
}
