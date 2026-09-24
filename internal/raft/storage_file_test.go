package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageRestartReplays(t *testing.T) {
	dir := t.TempDir()

	func() {
		st, err := NewFileStorage(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := Bootstrap(st, "n1", []NodeID{"n1", "n2", "n3"}); err != nil &&
			err != ErrAlreadyBootstrapped {
			t.Fatal(err)
		}
		// Append entries in two terms (simulate election then writes).
		if err := st.Append(1, []Entry{ent(2, 1), ent(3, 1)}); err != nil {
			t.Fatal(err)
		}
		if err := st.SetHard(HardState{Term: 1, Commit: 3}); err != nil {
			t.Fatal(err)
		}
		if err := st.Append(3, []Entry{ent(4, 2), ent(5, 2)}); err != nil {
			t.Fatal(err)
		}
		if err := st.SetHard(HardState{Term: 2, Commit: 5}); err != nil {
			t.Fatal(err)
		}
	}()

	// Reopen: simulate process restart from disk.
	st2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	hard, _, _ := st2.InitialState()
	if hard.Term != 2 || hard.Commit != 5 {
		t.Fatalf("replayed hard state = term %d commit %d, want 2/5", hard.Term, hard.Commit)
	}
	if st2.LastIndex() != 5 {
		t.Fatalf("replayed last index=%d want 5", st2.LastIndex())
	}
	t4, _ := st2.Term(4)
	t5, _ := st2.Term(5)
	if t4 != 2 || t5 != 2 {
		t.Fatalf("terms after replay: %d %d want 2 2", t4, t5)
	}
}

func TestFileStorageSnapshotAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	func() {
		st, _ := NewFileStorage(dir)
		defer st.Close()
		_ = st.Append(0, []Entry{ent(1, 1), ent(2, 1), ent(3, 1), ent(4, 2)})
		sn := Snapshot{Index: 3, Term: 1, Conf: Config{Voters: []Voter{{ID: "a"}}}, Data: []byte("STATE")}
		if err := st.Compact(sn); err != nil {
			t.Fatal(err)
		}
	}()
	st2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	sn, err := st2.Snapshot()
	if err != nil || sn == nil || string(sn.Data) != "STATE" {
		t.Fatalf("snapshot replay failed: %v %v", err, sn)
	}
	if st2.FirstIndex() != 4 {
		t.Fatalf("first after compact replay=%d want 4", st2.FirstIndex())
	}
}

func TestFileStorageTornTailFrame(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewFileStorage(dir)
	_ = Bootstrap(st, "n", []NodeID{"n"})
	_ = st.Append(1, []Entry{ent(2, 1)})
	st.Close()

	// Append garbage bytes simulating a crash mid-write.
	p := filepath.Join(dir, walName)
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{'a', 0, 0, 0, 20, '{', 'b'}) // truncated frame
	f.Close()

	// Reopen must truncate the torn tail and recover prior data.
	st2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if st2.LastIndex() != 2 {
		t.Fatalf("last=%d want 2 after torn frame", st2.LastIndex())
	}
}
