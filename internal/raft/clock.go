package raft

import "time"

// Clock abstracts time so deadline decisions can be made deterministic.
type Clock interface {
	Now() time.Time
}

// WallClock is the real system clock.
type WallClock struct{}

// Now returns the current wall-clock time.
func (WallClock) Now() time.Time { return time.Now() }
