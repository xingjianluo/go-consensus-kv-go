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
	CmdBatch
)

// Batch size guards. A request beyond any of these is rejected
// deterministically (see reasons batch-*), never silently truncated.
const (
	MaxBatchItems = 256
	MaxKeyLength  = 64 * 1024
	MaxValueSize  = 1 << 20 // 1 MiB per value
	MaxBatchBytes = 4 << 20 // 4 MiB total key+value payload
)

// BatchItem is one key update inside a CmdBatch.
//
// Precondition rule (applied atomically with the write):
//   - CheckVersion == false: no expectation, the write always applies.
//   - CheckVersion == true, ExpectedVersion == 0: the key must NOT exist
//     (first-write guard / anti-overwrite create).
//   - CheckVersion == true, ExpectedVersion >= 1: the key must exist and
//     its current version must equal ExpectedVersion (CAS).
type BatchItem struct {
	Key             string `json:"key"`
	Value           []byte `json:"value"`
	CheckVersion    bool   `json:"check_version"`
	ExpectedVersion int64  `json:"expected_version"`
}

// KeyOutcome is the per-key verdict reported inside a batch reply. For an
// applied item Version is the version in effect after this batch; for a
// rejected item Version is the version observed at check time (0 when the
// key was absent) so the caller can build a fresh conditional retry.
type KeyOutcome struct {
	Key     string  `json:"key"`
	Verdict Verdict `json:"verdict"`
	Reason  string  `json:"reason,omitempty"`
	Version int64   `json:"version"`
}

// Command is the application payload carried inside an EntryData.
type Command struct {
	Type       CmdType
	SessionID  string
	RequestID  string // client-unique within the session; enables dedupe
	Key        string
	Value      []byte
	TTLMillis  int64 // session idle expiration (CmdSessionOpen)
	CreateTime int64 // for deterministic TTL tests
	Items      []BatchItem
}

// Reply is the machine-readable result recorded once per request so a
// retry returns the *original* verdict instead of reapplying.
type Reply struct {
	Verdict   Verdict
	Value     []byte
	Reason    string
	AppliedAt raft.Index
	// Version is the post-write version recorded by the original CmdPut.
	Version int64
	// Items carries the original per-key outcomes for a CmdBatch.
	Items []KeyOutcome
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
	// VerdictPreconditionFailed is a definitive, committed rejection of a
	// conditional write: the request was evaluated atomically and at least
	// one expected-version precondition did not hold. It is distinct from
	// VerdictTimeout ("outcome unknown") and from VerdictRejected (bad
	// request shape).
	VerdictPreconditionFailed
	// VerdictNotApplied is only used on KeyOutcome entries of an
	// all-or-nothing batch that did not commit because another item
	// failed; the whole batch changed nothing.
	VerdictNotApplied
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
	case VerdictPreconditionFailed:
		return "precondition-failed"
	case VerdictNotApplied:
		return "not-applied"
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
	// Version is the resulting version for a single Put (0 unless the
	// store reports versions; single-key semantics are unchanged).
	Version int64
	// Items holds per-key outcomes for a batch request.
	Items []KeyOutcome
}

type session struct {
	CreatedMillis  int64
	LastUsedMillis int64
	TTLMillis      int64
	Closed         bool
	Replies        map[string]Reply // request id -> first verdict
}

// entry is one stored value with its monotonic version. Versions start
// at 1 on the first write and increase by exactly 1 per committed write
// (Put or batch item); the absence of a key is reported as version 0.
type entry struct {
	Value   []byte `json:"v"`
	Version int64  `json:"ver"`
}

// Store is the KV state machine. All access is guarded by mu; Apply and
// Read are typically invoked from different goroutines (commit pump vs
// client handler), so the locking must be consistent.
type Store struct {
	mu       sync.RWMutex
	kv       map[string]entry
	sessions map[string]*session
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		kv:       map[string]entry{},
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
	// applyLocked returns the ORIGINAL commit index on duplicate replies;
	// only fill it in for freshly decided verdicts.
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
		ver := s.kv[cmd.Key].Version + 1
		s.kv[cmd.Key] = entry{Value: append([]byte(nil), cmd.Value...), Version: ver}
		if cmd.CreateTime > sess.LastUsedMillis {
			sess.LastUsedMillis = cmd.CreateTime
		}
		sess.Replies[cmd.RequestID] = Reply{Verdict: VerdictOK, AppliedAt: idx, Version: ver}
		return Result{Verdict: VerdictOK, Version: ver}
	case CmdBatch:
		if sess == nil {
			return Result{Verdict: VerdictSessionExpired, Reason: "no-such-session"}
		}
		return s.applyBatchLocked(sess, cmd, idx)
	default:
		return Result{Verdict: VerdictRejected, Reason: "unknown-command"}
	}
}

// applyBatchLocked is the CmdBatch state machine (caller holds s.mu and
// has resolved the session). The whole batch is decided at a single
// committed log position: validation, expected-version preconditions and
// the writes are one indivisible step, so readers can never observe a
// half-applied batch and two racing conditional updates cannot both win.
//
// The *first* terminal verdict (success or a committed rejection) is
// cached per (session, request); retries return that original verdict
// verbatim instead of re-evaluating against later state.
func (s *Store) applyBatchLocked(sess *session, cmd Command, idx raft.Index) Result {
	if sess.Closed {
		return Result{Verdict: VerdictSessionExpired, Reason: "closed"}
	}
	if cmd.CreateTime > 0 && sess.TTLMillis > 0 &&
		cmd.CreateTime-sess.LastUsedMillis > sess.TTLMillis {
		sess.Closed = true
		return Result{Verdict: VerdictSessionExpired, Reason: "idle-ttl"}
	}
	// Idempotency: return the original complete verdict, even if the
	// stored values have since moved on; never re-judge conditions.
	if rep, ok := sess.Replies[cmd.RequestID]; ok {
		return Result{Verdict: VerdictDuplicate, Reason: "request-already-applied",
			Index: rep.AppliedAt, Items: cloneOutcomes(rep.Items)}
	}

	// Phase 1: structural validation. Nothing is mutated on failure.
	if outs, reason, rejected := validateBatch(cmd.Items); rejected {
		res := Result{Verdict: VerdictRejected, Reason: reason, Items: outs}
		sess.Replies[cmd.RequestID] = Reply{Verdict: VerdictRejected,
			Reason: reason, AppliedAt: idx, Items: cloneOutcomes(outs)}
		return res
	}

	// Phase 2: evaluate every precondition against current state, in
	// request order, before writing anything.
	outs := make([]KeyOutcome, len(cmd.Items))
	for i, it := range cmd.Items {
		cur := s.kv[it.Key]
		if !it.CheckVersion {
			continue
		}
		switch {
		case it.ExpectedVersion == 0 && cur.Version != 0:
			outs[i] = KeyOutcome{Key: it.Key, Verdict: VerdictPreconditionFailed,
				Reason:  "must-not-exist-but-exists",
				Version: cur.Version}
		case it.ExpectedVersion > 0 && cur.Version != it.ExpectedVersion:
			outs[i] = KeyOutcome{Key: it.Key, Verdict: VerdictPreconditionFailed,
				Reason:  "version-mismatch",
				Version: cur.Version}
		}
		if outs[i].Verdict == VerdictPreconditionFailed {
			reason := outs[i].Reason + ":" + it.Key
			// Every other item is explicitly reported as not applied.
			for j := range outs {
				if j == i {
					continue
				}
				if outs[j].Verdict == 0 {
					outs[j] = KeyOutcome{Key: cmd.Items[j].Key,
						Verdict: VerdictNotApplied,
						Reason:  "not-applied-precondition-failed"}
				}
			}
			res := Result{Verdict: VerdictPreconditionFailed, Reason: reason, Items: outs}
			sess.Replies[cmd.RequestID] = Reply{Verdict: VerdictPreconditionFailed,
				Reason: reason, AppliedAt: idx, Items: cloneOutcomes(outs)}
			return res
		}
	}

	// Phase 3: all conditions hold — commit every item atomically.
	for i, it := range cmd.Items {
		ver := s.kv[it.Key].Version + 1
		s.kv[it.Key] = entry{Value: append([]byte(nil), it.Value...), Version: ver}
		outs[i] = KeyOutcome{Key: it.Key, Verdict: VerdictOK, Version: ver}
	}
	if cmd.CreateTime > sess.LastUsedMillis {
		sess.LastUsedMillis = cmd.CreateTime
	}
	sess.Replies[cmd.RequestID] = Reply{Verdict: VerdictOK, AppliedAt: idx,
		Items: cloneOutcomes(outs)}
	return Result{Verdict: VerdictOK, Items: outs}
}

func cloneOutcomes(in []KeyOutcome) []KeyOutcome {
	if in == nil {
		return nil
	}
	out := make([]KeyOutcome, len(in))
	copy(out, in)
	return out
}

// validateBatch enforces the request shape/size contract. On rejection it
// returns per-key outcomes (the offending item carries the reason; all
// others are not-applied) and the top-level reason code.
func validateBatch(items []BatchItem) ([]KeyOutcome, string, bool) {
	notApplied := func(reason string, bad int) ([]KeyOutcome, string, bool) {
		outs := make([]KeyOutcome, len(items))
		for i, it := range items {
			if i == bad {
				outs[i] = KeyOutcome{Key: it.Key, Verdict: VerdictRejected, Reason: reason}
			} else {
				outs[i] = KeyOutcome{Key: it.Key, Verdict: VerdictNotApplied,
					Reason: "not-applied-batch-rejected"}
			}
		}
		return outs, reason, true
	}
	switch {
	case len(items) == 0:
		// No per-item outcome exists; the reason alone explains it.
		return []KeyOutcome{}, "batch-empty", true
	case len(items) > MaxBatchItems:
		return []KeyOutcome{}, "batch-too-many-items", true
	}
	var total int
	seen := make(map[string]int, len(items))
	for i, it := range items {
		if _, dup := seen[it.Key]; dup {
			return notApplied("batch-duplicate-key", i)
		}
		seen[it.Key] = i
		switch {
		case len(it.Key) == 0:
			return notApplied("batch-empty-key", i)
		case len(it.Key) > MaxKeyLength:
			return notApplied("key-too-long", i)
		case len(it.Value) > MaxValueSize:
			return notApplied("value-too-large", i)
		case it.CheckVersion && it.ExpectedVersion < 0:
			return notApplied("bad-expected-version", i)
		}
		total += len(it.Key) + len(it.Value)
		if total > MaxBatchBytes {
			return notApplied("batch-too-large", i)
		}
	}
	return nil, "", false
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
	return Result{Verdict: VerdictOK, Value: append([]byte(nil), v.Value...),
		Version: v.Version}
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

// Snapshot serializes the whole store deterministically (values and
// versions; replay order is fixed by the raft log, never by map order).
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	kvCopy := make(map[string]entry, len(s.kv))
	for k, v := range s.kv {
		kvCopy[k] = entry{Value: append([]byte(nil), v.Value...), Version: v.Version}
	}
	sd := snapData{Kv: kvCopy, Sessions: map[string]sessionSnap{}}
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
	s.kv = map[string]entry{}
	for k, v := range sd.Kv {
		s.kv[k] = entry{Value: append([]byte(nil), v.Value...), Version: v.Version}
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
		ov, ok := o.kv[k]
		if !ok || v.Version != ov.Version || !bytes.Equal(v.Value, ov.Value) {
			return false, "key " + k + " differs"
		}
	}
	if len(s.sessions) != len(o.sessions) {
		return false, "session count differs"
	}
	return true, ""
}
