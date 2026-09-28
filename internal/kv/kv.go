// Package kv implements the replicated key/value state machine on top
// of raft: sessions, idempotent commands, linearizable reads and
// snapshot/restore of application state.
package kv

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// CmdType enumerates state-machine commands.
type CmdType uint8

const (
	CmdSessionOpen CmdType = iota
	CmdSessionClose
	CmdPut
	// CmdBatch applies several key updates atomically, optionally each
	// guarded by an expected-version precondition.
	CmdBatch
)

// Command is the application payload carried inside an EntryData.
type Command struct {
	Type       CmdType
	SessionID  string
	RequestID  string // client-unique within the session; enables dedupe
	Key        string
	Value      []byte
	TTLMillis  int64 // session idle expiration (CmdSessionOpen)
	CreateTime int64 // for deterministic TTL tests

	// Ops carries the update items of a CmdBatch request.
	Ops []BatchOp
}

// Reply is the machine-readable result recorded once per request so a
// retry returns the *original* verdict instead of reapplying.
type Reply struct {
	Verdict   Verdict
	Value     []byte
	Reason    string
	AppliedAt raft.Index
	// Version is the post-write version for single-key puts (0 when the
	// request was not a successful write).
	Version int64
	// Batch holds the full original verdict of a CmdBatch request.
	Batch *BatchResult
}

// Verdict is the machine-readable result of one request.
type Verdict uint8

const (
	VerdictOK Verdict = iota
	VerdictNotFound
	VerdictRejected
	VerdictSessionExpired
	VerdictDuplicate
	VerdictNotLeader
	VerdictTimeout
	// VerdictInvalidBatch: the batch itself is malformed (empty batch,
	// empty/duplicate key, size limit). Nothing was applied.
	VerdictInvalidBatch
	// VerdictPreconditionFailed: at least one item's expected-version
	// precondition did not match. Nothing was applied.
	VerdictPreconditionFailed
)

func (v Verdict) String() string {
	switch v {
	case VerdictOK:
		return "ok"
	case VerdictNotFound:
		return "not-found"
	case VerdictRejected:
		return "rejected"
	case VerdictSessionExpired:
		return "session-expired"
	case VerdictDuplicate:
		return "duplicate"
	case VerdictNotLeader:
		return "not-leader"
	case VerdictTimeout:
		return "timeout-unknown"
	case VerdictInvalidBatch:
		return "invalid-batch"
	case VerdictPreconditionFailed:
		return "precondition-failed"
	default:
		return "unknown"
	}
}

// Result is returned to the caller for a command.
type Result struct {
	Verdict Verdict
	Value   []byte
	Leader  raft.NodeID
	Reason  string
	Index   raft.Index
	// Version is the resulting key version of a successful single-key
	// write (read responses do not populate it; see Store.VersionOf).
	Version int64
	// Batch is populated for CmdBatch verdicts with per-key detail.
	Batch *BatchResult
}

type session struct {
	CreatedMillis  int64
	LastUsedMillis int64
	TTLMillis      int64
	Closed         bool
	Replies        map[string]Reply // request id -> first verdict
}

// Store is the KV state machine. All access is guarded by mu; Apply and
// Read are typically invoked from different goroutines (commit pump vs
// client handler), so the locking must be consistent.
type Store struct {
	mu       sync.RWMutex
	kv       map[string][]byte
	versions map[string]int64 // per-key monotonic version; absent => 0
	sessions map[string]*session
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		kv:       map[string][]byte{},
		versions: map[string]int64{},
		sessions: map[string]*session{},
	}
}

// Apply applies one committed log entry. Empty data (leader no-op) is
// ignored. The reply is derived purely from state + log, so replaying
// the same log on any node produces the same verdict.
func (s *Store) Apply(e raft.Entry) (Result, bool) {
	if e.Kind != raft.EntryData || len(e.Data) == 0 {
		return Result{}, false
	}
	cmd, err := Decode(e.Data)
	if err != nil {
		return Result{Verdict: VerdictRejected, Reason: "bad-command"}, true
	}
	s.mu.Lock()
	res := s.applyLocked(cmd, e.Index)
	s.mu.Unlock()
	// Keep an index chosen by the state machine (e.g. a duplicate's
	// ORIGINAL commit position); otherwise attribute the verdict to the
	// entry at which it was (re)evaluated.
	if res.Index == 0 {
		res.Index = e.Index
	}
	return res, true
}

func (s *Store) applyLocked(cmd Command, idx raft.Index) Result {
	sess := s.sessions[cmd.SessionID]
	switch cmd.Type {
	case CmdSessionOpen:
		if sess != nil {
			// Re-opening same id returns the original session, never twice.
			return Result{Verdict: VerdictDuplicate, Reason: "session-already-open"}
		}
		ttl := cmd.TTLMillis
		if ttl <= 0 {
			ttl = 30_000
		}
		s.sessions[cmd.SessionID] = &session{
			CreatedMillis: cmd.CreateTime, LastUsedMillis: cmd.CreateTime,
			TTLMillis: ttl, Replies: map[string]Reply{},
		}
		return Result{Verdict: VerdictOK}
	case CmdSessionClose:
		if sess == nil {
			return Result{Verdict: VerdictSessionExpired, Reason: "no-such-session"}
		}
		sess.Closed = true
		return Result{Verdict: VerdictOK}
	case CmdPut:
		if sess == nil {
			return Result{Verdict: VerdictSessionExpired, Reason: "no-such-session"}
		}
		if sess.Closed {
			return Result{Verdict: VerdictSessionExpired, Reason: "closed"}
		}
		if cmd.CreateTime > 0 && sess.TTLMillis > 0 &&
			cmd.CreateTime-sess.LastUsedMillis > sess.TTLMillis {
			sess.Closed = true
			return Result{Verdict: VerdictSessionExpired, Reason: "idle-ttl"}
		}
		// Idempotency: a repeated (session, request) returns first verdict.
		if rep, ok := sess.Replies[cmd.RequestID]; ok {
			return Result{Verdict: VerdictDuplicate, Value: rep.Value,
				Reason: "request-already-applied", Index: rep.AppliedAt,
				Version: rep.Version}
		}
		s.kv[cmd.Key] = append([]byte(nil), cmd.Value...)
		next := s.versions[cmd.Key] + 1
		s.versions[cmd.Key] = next
		if cmd.CreateTime > sess.LastUsedMillis {
			sess.LastUsedMillis = cmd.CreateTime
		}
		sess.Replies[cmd.RequestID] = Reply{Verdict: VerdictOK, AppliedAt: idx,
			Version: next}
		return Result{Verdict: VerdictOK, Index: idx, Version: next}
	case CmdBatch:
		return s.applyBatchLocked(cmd, idx)
	default:
		return Result{Verdict: VerdictRejected, Reason: "unknown-command"}
	}
}

// Read serves a linearizable get. It is called ONLY after the raft
// ReadIndex quorum barrier has succeeded, so the value is guaranteed
// to be freshest committed. Expired/unknown sessions are rejected with
// an explicit reason rather than silently serving stale data.
func (s *Store) Read(sessionID, key string, nowMillis int64) Result {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess := s.sessions[sessionID]
	if sess == nil {
		return Result{Verdict: VerdictSessionExpired, Reason: "no-such-session"}
	}
	if sess.Closed {
		return Result{Verdict: VerdictSessionExpired, Reason: "closed"}
	}
	if nowMillis > 0 && sess.TTLMillis > 0 && nowMillis-sess.LastUsedMillis > sess.TTLMillis {
		sess.Closed = true
		return Result{Verdict: VerdictSessionExpired, Reason: "idle-ttl"}
	}
	v, ok := s.kv[key]
	if !ok {
		return Result{Verdict: VerdictNotFound, Reason: "no-such-key"}
	}
	return Result{Verdict: VerdictOK, Value: append([]byte(nil), v...),
		Version: s.versions[key]}
}

// HasSession reports session existence/state (used by the service to
// fail fast before proposing).
func (s *Store) HasSession(id string) (known, closed bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess := s.sessions[id]
	if sess == nil {
		return false, false
	}
	return true, sess.Closed
}

// PendingPut returns the recorded reply for a duplicate write, if any.
func (s *Store) PendingPut(sessionID, requestID string) (Reply, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess := s.sessions[sessionID]
	if sess == nil {
		return Reply{}, false
	}
	r, ok := sess.Replies[requestID]
	return r, ok
}

// Snapshot serializes the whole store deterministically (json, keys
// sorted by the encoder's map ordering-independent apply expectations).
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	kvCopy := make(map[string][]byte, len(s.kv))
	for k, v := range s.kv {
		kvCopy[k] = append([]byte(nil), v...)
	}
	verCopy := make(map[string]int64, len(s.versions))
	for k, v := range s.versions {
		verCopy[k] = v
	}
	sd := snapData{Kv: kvCopy, Versions: verCopy, Sessions: map[string]sessionSnap{}}
	for id, sess := range s.sessions {
		cp := sessionSnap{
			CreatedMillis:  sess.CreatedMillis,
			LastUsedMillis: sess.LastUsedMillis,
			TTLMillis:      sess.TTLMillis,
			Closed:         sess.Closed,
			Replies:        sess.Replies,
		}
		sd.Sessions[id] = cp
	}
	return json.Marshal(sd)
}

// Restore replaces store contents from snapshot bytes.
func (s *Store) Restore(data []byte) error {
	var sd snapData
	if err := json.Unmarshal(data, &sd); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv = sd.Kv
	s.versions = sd.Versions
	if s.versions == nil {
		// Backward compatibility: snapshots written before versions
		// existed. Derive version 1 for every present key so later
		// conditional writes see a monotonic, collision-free history.
		s.versions = map[string]int64{}
		for k := range s.kv {
			s.versions[k] = 1
		}
	}
	s.sessions = map[string]*session{}
	for id, cp := range sd.Sessions {
		s.sessions[id] = &session{
			CreatedMillis: cp.CreatedMillis, LastUsedMillis: cp.LastUsedMillis,
			TTLMillis: cp.TTLMillis, Closed: cp.Closed, Replies: cp.Replies,
		}
	}
	return nil
}

// Equal compares stores (test helper for convergence assertions).
func (s *Store) Equal(o *Store) (bool, string) {
	s.mu.RLock()
	o.mu.RLock()
	defer s.mu.RUnlock()
	defer o.mu.RUnlock()
	if len(s.kv) != len(o.kv) {
		return false, "kv length differs"
	}
	for k, v := range s.kv {
		if !bytes.Equal(v, o.kv[k]) {
			return false, "key " + k + " differs"
		}
		if s.versions[k] != o.versions[k] {
			return false, "key " + k + " version differs"
		}
	}
	if len(s.sessions) != len(o.sessions) {
		return false, "session count differs"
	}
	return true, ""
}
