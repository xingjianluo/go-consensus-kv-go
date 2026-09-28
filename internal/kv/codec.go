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
	Kv       map[string][]byte
	Versions map[string]int64
	Sessions map[string]sessionSnap
}

type sessionSnap struct {
	CreatedMillis  int64
	LastUsedMillis int64
	TTLMillis      int64
	Closed         bool
	Replies        map[string]Reply
}
