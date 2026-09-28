package kv

import "encoding/json"

// Encode serializes a command into log-entry payload bytes.
func Encode(c Command) ([]byte, error) { return json.Marshal(c) }

// Decode parses log-entry payload bytes.
func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// snapData is the on-snapshot representation of the whole store.
type snapData struct {
	Kv       map[string]entry
	Sessions map[string]sessionSnap
}

// UnmarshalJSON accepts both the current object form {"v":..., "ver":N}
// and the legacy form where a key mapped straight to a JSON byte string
// (pre-versioning snapshots). Legacy keys are assigned version 1, which
// is the only honest version when history predates version tracking.
func (e *entry) UnmarshalJSON(b []byte) error {
	var obj struct {
		V   []byte `json:"v"`
		Ver int64  `json:"ver"`
	}
	if err := json.Unmarshal(b, &obj); err == nil && (len(b) == 0 || b[0] == '{') {
		e.Value = obj.V
		e.Version = obj.Ver
		return nil
	}
	var raw []byte
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	e.Value = raw
	e.Version = 1
	return nil
}

type sessionSnap struct {
	CreatedMillis  int64
	LastUsedMillis int64
	TTLMillis      int64
	Closed         bool
	Replies        map[string]Reply
}
