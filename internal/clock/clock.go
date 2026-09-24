// Package clock provides a controllable clock and a deterministic timer
// scheduler used by the simulated cluster tests.
package clock

import "time"

// Clock is the time source used by a raft node.
type Clock interface {
	Now() time.Time
}

// WallClock uses the real wall clock.
type WallClock struct{}

func (WallClock) Now() time.Time { return time.Now() }

// FakeClock is a manually advanced clock.
type FakeClock struct {
	t time.Time
}

func NewFake(start time.Time) *FakeClock { return &FakeClock{t: start} }

func (c *FakeClock) Now() time.Time { return c.t }

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
