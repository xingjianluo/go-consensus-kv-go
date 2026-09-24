package raft

import "testing"

func TestMemStorageTruncateAndCompact(t *testing.T) {
	s := NewMemStorage()
	es := []Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 2, Index: 3}}
	if err := s.Append(es); err != nil {
		t.Fatal(err)
	}
	if s.LastIndex() != 3 {
		t.Fatalf("last=%d", s.LastIndex())
	}
	// Conflict: overwrite from index 2 with a different term suffix.
	if err := s.Append([]Entry{{Term: 2, Index: 2}, {Term: 2, Index: 3}}); err != nil {
		t.Fatal(err)
	}
	if t2, _ := s.TermAt(2); t2 != 2 {
		t.Fatalf("term at 2 = %d, want 2 (conflicting suffix truncated)", t2)
	}
	// Compact through 2.
	if err := s.Compact(2, 2, ConfState{Voters: []string{"a"}}, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if s.FirstIndex() != 3 {
		t.Fatalf("first=%d want 3", s.FirstIndex())
	}
	if _, err := s.TermAt(1); err != errCompacted {
		t.Fatalf("compacted index must error, got %v", err)
	}
	if snap := s.Snapshot(); snap.LastIndex != 2 || string(snap.Data) != "d" {
		t.Fatalf("snapshot wrong: %+v", snap)
	}
}

func TestConfQuorum(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 7: 4}
	for n, want := range cases {
		v := make([]string, n)
		for i := range v {
			v[i] = string(rune('a' + i))
		}
		if got := (ConfState{Voters: v}).Quorum(); got != want {
			t.Errorf("n=%d quorum=%d want %d", n, got, want)
		}
	}
}
