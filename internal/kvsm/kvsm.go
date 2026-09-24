// Package kvsm is the replicated key/value application with sessions and
// per-session idempotency.
//
// Semantics (enforced only through committed log entries):
//
//   - A client first executes a replicated "register" op with a TTL and
//     receives its session id. Subsequent writes must carry that session.
//   - Each request carries a client-generated ReqID. The first committed
//     request with (session, reqID) is applied once; exact replays return
//     the cached result marked Duplicate.
//   - A session expires when no successful keepalive/write has refreshed
//     its deadline. Expired/unknown sessions are rejected with a
//     distinguishable code and never mutate the map.
package kvsm

import (
	"encoding/json"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// DefaultSessionTTL is used when register supplies zero.
const DefaultSessionTTL = 10_000_000_000 // 10s in ns

const maxCachedPerSession = 64

type session struct {
	ID        string
	ExpiresAt int64 // unix nano
	// reqID -> cached outcome (FIFO bounded).
	seen map[string]cachedResult
	ring []string
}

type cachedResult struct {
	OK    bool
	Value string
}

type snapshotData struct {
	Version  int
	KV       map[string]string
	Sessions map[string]int64
	Seen     map[string]map[string]cachedResult
}

// KV is the key/value state machine.
type KV struct {
	kv       map[string]string
	sessions map[string]*session
}

// New returns an empty state machine.
func New() *KV {
	return &KV{kv: map[string]string{}, sessions: map[string]*session{}}
}

func expired(s *session, nowNs int64) bool { return s.ExpiresAt != 0 && nowNs > s.ExpiresAt }

// ValidateSession classifies a session for leader-side fast rejection.
// Returns "" when alive.
func (k *KV) ValidateSession(id string, nowNs int64) raft.ErrorCode {
	s := k.sessions[id]
	if s == nil {
		return raft.ErrSessionUnknown
	}
	if expired(s, nowNs) {
		return raft.ErrSessionExpired
	}
	return ""
}

// SessionAlive reports whether a session exists and is unexpired.
func (k *KV) SessionAlive(id string, nowNs int64) bool {
	s := k.sessions[id]
	return s != nil && !expired(s, nowNs)
}

func (k *KV) remember(s *session, reqID string, r cachedResult) cachedResult {
	if s.seen == nil {
		s.seen = map[string]cachedResult{}
	}
	if prev, ok := s.seen[reqID]; ok {
		return prev
	}
	s.seen[reqID] = r
	s.ring = append(s.ring, reqID)
	if len(s.ring) > maxCachedPerSession {
		old := s.ring[0]
		s.ring = s.ring[1:]
		delete(s.seen, old)
	}
	return r
}

// Apply executes one committed entry.
func (k *KV) Apply(e raft.Entry, ctx raft.ApplyCtx) raft.ApplyResult {
	nowNs := ctx.Now.UnixNano()
	switch e.Op {
	case "register":
		// Sessions are keyed by the client-supplied ReqID, so retried
		// registers remain idempotent too.
		id := e.ReqID
		if s, ok := k.sessions[id]; ok {
			if expired(s, nowNs) {
				// Re-registering an expired id reactivates it.
				s.ExpiresAt = ttl(e, nowNs)
				s.seen = nil
				s.ring = nil
			}
			return raft.ApplyResult{OK: true, Value: id, Duplicate: true}
		}
		k.sessions[id] = &session{ID: id, ExpiresAt: ttl(e, nowNs)}
		return raft.ApplyResult{OK: true, Value: id}
	case "keepalive":
		s := k.sessions[e.Session]
		if s == nil {
			return raft.ApplyResult{Code: raft.ErrSessionUnknown, Reason: "no such session"}
		}
		if expired(s, nowNs) {
			return raft.ApplyResult{Code: raft.ErrSessionExpired, Reason: "session already expired"}
		}
		s.ExpiresAt = ttl(e, nowNs)
		return raft.ApplyResult{OK: true}
	}

	// Mutating ops require a live session.
	s := k.sessions[e.Session]
	if s == nil {
		return raft.ApplyResult{Code: raft.ErrSessionUnknown, Reason: "no such session"}
	}
	if expired(s, nowNs) {
		return raft.ApplyResult{Code: raft.ErrSessionExpired, Reason: "session expired at deadline"}
	}
	if r, ok := s.seen[e.ReqID]; ok {
		return raft.ApplyResult{OK: r.OK, Value: r.Value, Duplicate: true}
	}
	switch e.Op {
	case "put":
		k.kv[e.Key] = e.Value
		k.remember(s, e.ReqID, cachedResult{OK: true})
	case "delete":
		delete(k.kv, e.Key)
		k.remember(s, e.ReqID, cachedResult{OK: true})
	default:
		return raft.ApplyResult{Code: raft.ErrReplayed, Reason: "unknown op"}
	}
	// Ordinary writes do NOT extend the deadline: clients must keepalive
	// explicitly, so an idle short-TTL session expires as configured.
	return raft.ApplyResult{OK: true}
}

func ttl(e raft.Entry, nowNs int64) int64 {
	t := e.TTLNs
	if t <= 0 {
		t = DefaultSessionTTL
	}
	return nowNs + t
}

// Get is a local (non-replicated) read. The raft layer guarantees
// linearizability before exposing this.
func (k *KV) Get(key string) (string, bool) {
	v, ok := k.kv[key]
	return v, ok
}

// Snapshot serializes the full application state.
func (k *KV) Snapshot() []byte {
	sd := snapshotData{
		Version:  1,
		KV:       k.kv,
		Sessions: map[string]int64{},
		Seen:     map[string]map[string]cachedResult{},
	}
	for id, s := range k.sessions {
		sd.Sessions[id] = s.ExpiresAt
		if len(s.seen) > 0 {
			cp := make(map[string]cachedResult, len(s.seen))
			for r, v := range s.seen {
				cp[r] = v
			}
			sd.Seen[id] = cp
		}
	}
	b, _ := json.Marshal(sd)
	return b
}

// Restore replaces state with a snapshot payload.
func (k *KV) Restore(data []byte) error {
	var sd snapshotData
	if err := json.Unmarshal(data, &sd); err != nil {
		return err
	}
	k.kv = sd.KV
	if k.kv == nil {
		k.kv = map[string]string{}
	}
	k.sessions = map[string]*session{}
	for id, exp := range sd.Sessions {
		s := &session{ID: id, ExpiresAt: exp}
		if seen := sd.Seen[id]; seen != nil {
			s.seen = seen
			for r := range seen {
				s.ring = append(s.ring, r)
			}
		}
		k.sessions[id] = s
	}
	return nil
}
