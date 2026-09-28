// Batch (atomic multi-key) writes and conditional updates live here.
// The semantics are layered on top of the same raft log + session reply
// cache as single-key puts, so they inherit exactly-once application,
// snapshot recovery and timeout-unknown retry reconciliation.
package kv

import "github.com/xingjianluo/go-consensus-kv-go/internal/raft"

// Batch size guards: a request that exceeds them is rejected decisively
// (VerdictInvalidBatch) rather than partially applied.
const (
	// MaxBatchItems bounds the number of update items in one batch.
	MaxBatchItems = 100
	// MaxKeyLen bounds a single key's byte length.
	MaxKeyLen = 4096
	// MaxValueLen bounds a single value's byte length.
	MaxValueLen = 1 << 20 // 1 MiB
	// MaxBatchPayload bounds the total key+value byte size of one batch.
	MaxBatchPayload = 4 << 20 // 4 MiB
)

// BatchItem is the caller-facing description of one batch update.
type BatchItem struct {
	Key   string
	Value []byte
	// ExpectVersion is the precondition; nil means unconditional.
	ExpectVersion *int64
}

// ExpectAbsent returns a precondition pointer meaning "the key must not
// exist yet" (expected version 0).
func ExpectAbsent() *int64 { v := int64(0); return &v }

// ExpectVersion returns a precondition pointer for "the key must exist
// at exactly version v".
func ExpectVersion(v int64) *int64 { return &v }

// BatchOp is one update item inside a CmdBatch command.
type BatchOp struct {
	Key   string
	Value []byte
	// ExpectVersion expresses the precondition on the key's current
	// version:
	//	nil            -> unconditional write;
	//	0              -> the key must NOT currently exist (create);
	//	v (>0)         -> the key must exist at exactly version v.
	ExpectVersion *int64
}

// OpResult reports the outcome of one batch item. On a successful batch
// Version is the key's version after the batch and Index is the commit
// position shared by every item.
type OpResult struct {
	Key     string
	Index   raft.Index
	Version int64
}

// BatchResult is the detailed, replay-stable verdict of a batch request.
// It is stored verbatim in the session reply cache so a retried request
// returns the ORIGINAL verdict even if later writes changed the keys.
type BatchResult struct {
	AppliedAt raft.Index
	// FailedKey names the first item whose precondition was not met.
	FailedKey string
	// ExpectVersion / ActualVersion explain a precondition failure.
	ExpectVersion int64
	ActualVersion int64
	Ops           []OpResult
}

// validateBatch checks structural constraints only (no state access):
// empty batch, empty key, duplicate keys and size limits. It returns a
// machine-readable reason ("" when valid). The checks are pure, so every
// raft node derives the same rejection from the same log entry.
func validateBatch(cmd Command) string {
	ops := cmd.Ops
	if len(ops) == 0 {
		return "empty-batch"
	}
	if len(ops) > MaxBatchItems {
		return "too-many-items"
	}
	var payload int
	seen := make(map[string]struct{}, len(ops))
	for i := range ops {
		op := &ops[i]
		if op.Key == "" {
			return "empty-key"
		}
		if len(op.Key) > MaxKeyLen {
			return "key-too-long"
		}
		if len(op.Value) > MaxValueLen {
			return "value-too-long"
		}
		if op.ExpectVersion != nil && *op.ExpectVersion < 0 {
			return "bad-expect-version"
		}
		if _, dup := seen[op.Key]; dup {
			// Duplicate keys are refused outright: never silently pick
			// one of the conflicting values.
			return "duplicate-key"
		}
		seen[op.Key] = struct{}{}
		payload += len(op.Key) + len(op.Value)
		if payload > MaxBatchPayload {
			return "payload-too-large"
		}
	}
	return ""
}

// applyBatchLocked evaluates and (if every precondition holds) applies a
// batch atomically. The caller must hold s.mu.
//
// Ordering mirrors CmdPut: session validity (and idle TTL) is checked
// first, then the per-request reply cache. A repeated (session, request)
// always returns the FIRST verdict, even when later writes have changed
// the keys or the original request was rejected.
func (s *Store) applyBatchLocked(cmd Command, idx raft.Index) Result {
	sess := s.sessions[cmd.SessionID]
	if sess == nil {
		return Result{Verdict: VerdictSessionExpired, Reason: "no-such-session", Index: idx}
	}
	if sess.Closed {
		return Result{Verdict: VerdictSessionExpired, Reason: "closed", Index: idx}
	}
	if cmd.CreateTime > 0 && sess.TTLMillis > 0 &&
		cmd.CreateTime-sess.LastUsedMillis > sess.TTLMillis {
		sess.Closed = true
		return Result{Verdict: VerdictSessionExpired, Reason: "idle-ttl", Index: idx}
	}
	if rep, ok := sess.Replies[cmd.RequestID]; ok {
		return batchResultFromReply(rep)
	}

	if reason := validateBatch(cmd); reason != "" {
		br := &BatchResult{AppliedAt: idx}
		sess.Replies[cmd.RequestID] = Reply{
			Verdict: VerdictInvalidBatch, Reason: reason, AppliedAt: idx, Batch: br,
		}
		return Result{Verdict: VerdictInvalidBatch, Reason: reason,
			Index: idx, Batch: br}
	}

	// Phase 1: evaluate ALL preconditions against current state before
	// touching anything. A mismatch rejects the whole batch with the
	// offending key and the expected/actual versions.
	for i := range cmd.Ops {
		op := &cmd.Ops[i]
		if op.ExpectVersion == nil {
			continue
		}
		cur := s.versions[op.Key] // absent key reads as version 0
		if cur != *op.ExpectVersion {
			reason := "version-mismatch"
			if *op.ExpectVersion == 0 {
				reason = "key-already-exists"
			} else if cur == 0 {
				reason = "key-not-exists"
			}
			br := &BatchResult{
				AppliedAt:     idx,
				FailedKey:     op.Key,
				ExpectVersion: *op.ExpectVersion,
				ActualVersion: cur,
			}
			sess.Replies[cmd.RequestID] = Reply{
				Verdict: VerdictPreconditionFailed, Reason: reason,
				AppliedAt: idx, Batch: br,
			}
			return Result{Verdict: VerdictPreconditionFailed, Reason: reason,
				Index: idx, Batch: br}
		}
	}

	// Phase 2: every precondition held — apply all items. The map
	// mutation is visible only after Apply returns, and all readers take
	// s.mu, so no read can observe a half-applied batch.
	ops := make([]OpResult, len(cmd.Ops))
	for i := range cmd.Ops {
		op := &cmd.Ops[i]
		next := s.versions[op.Key] + 1
		s.kv[op.Key] = append([]byte(nil), op.Value...)
		s.versions[op.Key] = next
		ops[i] = OpResult{Key: op.Key, Index: idx, Version: next}
	}
	if cmd.CreateTime > sess.LastUsedMillis {
		sess.LastUsedMillis = cmd.CreateTime
	}
	br := &BatchResult{AppliedAt: idx, Ops: ops}
	sess.Replies[cmd.RequestID] = Reply{
		Verdict: VerdictOK, AppliedAt: idx, Batch: br,
	}
	return Result{Verdict: VerdictOK, Index: idx, Batch: br}
}

// batchResultFromReply rebuilds the caller-facing result of a cached
// batch verdict. The embedded BatchResult is returned verbatim so a
// retry never re-evaluates conditions against newer state.
func batchResultFromReply(rep Reply) Result {
	return Result{
		Verdict: VerdictDuplicate,
		Reason:  "request-already-applied",
		Index:   rep.AppliedAt,
		Batch:   rep.Batch,
	}
}

// VersionOf returns the committed version of a key (0 when absent). It
// is safe to call only after a ReadIndex barrier, like Store.Read.
func (s *Store) VersionOf(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[key]
}
