// Package raft implements a Raft-style replicated state machine core.
//
// The Node is a single-threaded deterministic state machine: callers feed
// inbound messages, client requests and timer events through Step, and the
// node emits outbound messages, replies and structured events through Env.
package raft

import "time"

// Role of a node in the Raft state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

// EntryKind classifies a log entry.
type EntryKind int

const (
	EntryClient EntryKind = iota // a replicated client operation
	EntryConf                    // a membership (ConfState) change
	EntryNoop                    // an internal no-op committed at term start
)

// Entry is a single log record.
type Entry struct {
	Term  int       `json:"term"`
	Index int       `json:"index"`
	Kind  EntryKind `json:"kind"`
	// Req/Session carry the client context for EntryClient entries.
	ReqID   string `json:"req_id,omitempty"`
	Session string `json:"session,omitempty"`
	Op      string `json:"op,omitempty"` // put|delete|register|keepalive
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	TTLNs   int64  `json:"ttl_ns,omitempty"`
	// Conf is non-nil for EntryConf entries.
	Conf *ConfState `json:"conf,omitempty"`
}

// ConfState is a voter set configuration.
type ConfState struct {
	Voters []string `json:"voters"`
}

func (c ConfState) contains(id string) bool {
	for _, v := range c.Voters {
		if v == id {
			return true
		}
	}
	return false
}

func (c ConfState) size() int { return len(c.Voters) }

// Quorum returns the majority threshold for the configuration.
func (c ConfState) Quorum() int { return c.size()/2 + 1 }

// MsgType enumerates raft wire messages.
type MsgType int

const (
	MsgHup  MsgType = iota // local timer event (not sent over the wire)
	MsgBeat                // local periodic leader event
	MsgRequestVote
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	MsgSnapshot
)

// Message is every input and output of the raft state machine.
type Message struct {
	Type       MsgType
	From       string
	To         string
	Term       int
	Reject     bool
	RejectHint int  // suggested index for conflict resolution
	NeedSnap   bool // follower asks for a snapshot
	LogTerm    int
	Index      int // candidate last index / match index / snapshot index
	Entries    []Entry
	Commit     int
	Granted    bool
	Snapshot   *SnapshotMeta
}

// SnapshotMeta references a snapshot sent via InstallSnapshot.
type SnapshotMeta struct {
	LastIndex int
	LastTerm  int
	Conf      ConfState
	// Data is the opaque application snapshot payload.
	Data []byte
}

// EventKind enumerates observable node events.
type EventKind string

const (
	EvRoleChange   EventKind = "role_change"
	EvElection     EventKind = "election"
	EvProposal     EventKind = "proposal"
	EvReplicate    EventKind = "replicate"
	EvCommit       EventKind = "commit"
	EvApply        EventKind = "apply"
	EvReject       EventKind = "reject"
	EvStaleMessage EventKind = "stale_message"
	EvTermStale    EventKind = "stale_term"
	EvSnapshot     EventKind = "snapshot_install"
	EvConfChange   EventKind = "conf_change"
	EvSession      EventKind = "session"
	EvRead         EventKind = "read"
	EvTimeout      EventKind = "timeout"
)

// Event is a structured, explainable observation.
type Event struct {
	At     time.Time
	Node   string
	Kind   EventKind
	Term   int
	Role   Role
	Index  int
	Detail string
}

func (t MsgType) String() string {
	switch t {
	case MsgHup:
		return "election_timeout"
	case MsgBeat:
		return "heartbeat"
	case MsgRequestVote:
		return "request_vote"
	case MsgVoteResp:
		return "vote_response"
	case MsgAppend:
		return "append_entries"
	case MsgAppendResp:
		return "append_response"
	case MsgSnapshot:
		return "install_snapshot"
	}
	return "unknown"
}
