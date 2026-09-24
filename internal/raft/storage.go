package raft

// HardState is the persistent term/vote state.
type HardState struct {
	Term int
	Vote string // "" means no vote in this term
}

// Storage is the durable log + snapshot store used by a Node.
//
// Log indexing is absolute. Entries(lo, hi) is the half-open range
// [lo, hi). A compacted store keeps one synthetic anchor entry at the
// snapshot's LastIndex, so FirstIndex = LastIndex(snap)+1.
type Storage interface {
	InitialState() HardState
	SaveHardState(st HardState) error

	FirstIndex() int
	LastIndex() int
	TermAt(index int) (int, error)
	Entries(lo, hi int) ([]Entry, error)
	Entry(index int) (Entry, error)
	Append(entries []Entry) error

	// Snapshot returns the latest snapshot meta (nil if none).
	Snapshot() *SnapshotMeta
	// Compact discards log entries through index and records data.
	Compact(index, term int, conf ConfState, data []byte) error
	// ApplySnapshot resets the store to a snapshot received from a leader.
	ApplySnapshot(meta SnapshotMeta) error

	Sync() error
	Close() error
}
