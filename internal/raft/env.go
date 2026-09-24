package raft

import "time"

// ClientRequest is a proposal or read handed to the leader.
type ClientRequest struct {
	ReqID   string
	Session string
	Op      string // put|delete|register|keepalive|read
	Key     string
	Value   string
	// ReadOnly marks a linearizable read (ReadIndex); nothing is logged.
	ReadOnly bool
	TTLNs    int64
}

// Outcome is the terminal client-visible result.
type Outcome struct {
	OK        bool
	Code      ErrorCode
	Reason    string
	Leader    string
	Committed bool
	Index     int
	Term      int
	// Value is present for successful reads.
	Value string
	// Duplicate marks a result served from the idempotency cache.
	Duplicate bool
}

// Env abstracts the outside world so Node stays deterministic and
// single-threaded.
type Env interface {
	Now() time.Time
	Send(msg Message)
	Emit(ev Event)
	// Respond delivers a terminal outcome for a client request.
	Respond(reqID string, out Outcome)
	// ApplySnapshotData hands a received snapshot payload to the application.
	ApplySnapshotData(data []byte) error
}

// TimerHost manages one-shot raft timers.
type TimerHost interface {
	ResetElection(term int)
	ResetHeartbeat(term int)
	StopTimers()
}
