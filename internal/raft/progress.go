package raft

// progress tracks a follower's replication state for a leader.
type progress struct {
	match    Index // highest known replicated entry
	next     Index // next index to send
	state    progressState
	paused   bool
	snapshot bool // sending snapshot in flight
}

type progressState uint8

const (
	probeState progressState = iota
	replicateState
	snapshotState
)

func newProgress(last Index) *progress {
	return &progress{next: last + 1, state: probeState}
}
