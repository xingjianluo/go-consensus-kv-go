package raft

import "errors"

// ErrUnavailable is returned for indices that have been compacted away
// or when an append references a gap/conflict the caller must repair.
var ErrUnavailable = errors.New("raft: requested entry unavailable (compacted or gap)")

// ErrAlreadyBootstrapped reports that a storage already carries state.
var ErrAlreadyBootstrapped = errors.New("raft: storage already bootstrapped")

// HardState is the part of Raft state that must be persisted before
// responding to RPCs or changing role/term.
type HardState struct {
	Term   Term
	Vote   NodeID
	Commit Index
}

// Storage is the persistence layer used by a Node.
type Storage interface {
	// InitialState returns the persisted hard state, the configuration
	// carried by the latest snapshot (if any) and whether a snapshot exists.
	InitialState() (HardState, Config, bool)
	FirstIndex() Index
	LastIndex() Index
	Term(i Index) (Term, error)
	Entries(lo, hi Index) ([]Entry, error)
	Snapshot() (*Snapshot, error)
	// Append stores entries whose first index is prev+1. Conflicting
	// suffix entries are overwritten; a gap returns ErrUnavailable.
	Append(prev Index, entries []Entry) error
	// Compact discards log entries up to and including snap.Index and
	// installs snap as the recoverable snapshot. For a locally built
	// snapshot snap.Index must exist in the log; ErrUnavailable is
	// returned otherwise.
	Compact(snap Snapshot) error
	// InstallSnapshot replaces the entire log with the given leader
	// snapshot. Used by followers receiving InstallSnapshot for a point
	// beyond their current last index. Any conflicting local suffix is
	// discarded; the snapshot becomes the new compacted base.
	InstallSnapshot(snap Snapshot) error
	// SetHard persists hard state.
	SetHard(h HardState) error
}

// Bootstrap initialises a brand-new Storage as a member of a cluster of
// peers. Index 1 is written as an EntryConf sentinel containing the
// initial configuration, giving every member a shared committed prefix
// at term 0. Bootstrapping existing storage returns ErrAlreadyBootstrapped.
func Bootstrap(st Storage, id NodeID, peers []NodeID) error {
	_ = id
	hard, _, hadSnap := st.InitialState()
	if hard.Term != 0 || st.LastIndex() != 0 || hadSnap {
		return ErrAlreadyBootstrapped
	}
	voters := make([]Voter, 0, len(peers))
	for _, p := range peers {
		voters = append(voters, Voter{ID: p})
	}
	conf := Config{Voters: voters}
	data, err := EncodeConfig(conf)
	if err != nil {
		return err
	}
	e := Entry{Term: 0, Index: 1, Kind: EntryConf, Data: data}
	if err := st.Append(0, []Entry{e}); err != nil {
		return err
	}
	// The initial config entry is committed by definition: a fresh
	// cluster with one-member-at-a-time change rules starts here.
	return st.SetHard(HardState{Term: 0, Commit: 1})
}
