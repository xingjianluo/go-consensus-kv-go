package raft

import "errors"

// ErrorCode classifies a client-facing rejection reason.
type ErrorCode string

const (
	ErrNotLeader      ErrorCode = "not_leader"
	ErrMinority       ErrorCode = "minority_no_quorum"
	ErrTermStale      ErrorCode = "stale_term"
	ErrSessionExpired ErrorCode = "session_expired"
	ErrSessionUnknown ErrorCode = "session_unknown"
	ErrPendingConf    ErrorCode = "pending_membership_change"
	ErrUnknownNode    ErrorCode = "unknown_node"
	ErrUnsafeRead     ErrorCode = "unsafe_read_rejected"
	ErrTimeout        ErrorCode = "timeout_unknown_commit"
	ErrReplayed       ErrorCode = "replayed_result"
	ErrStopped        ErrorCode = "node_stopped"
)

// ClientError attaches an explainable code to a failure.
type ClientError struct {
	Code   ErrorCode
	Reason string
	Hint   string // e.g. current known leader
}

func (e *ClientError) Error() string { return string(e.Code) + ": " + e.Reason }

// Standard errors used internally.
var (
	errStopped     = errors.New("raft: node stopped")
	errCompacted   = errors.New("raft: requested index compacted")
	errUnavailable = errors.New("raft: unavailable")
)
