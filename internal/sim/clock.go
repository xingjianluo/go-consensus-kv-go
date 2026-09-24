// Package sim provides a deterministic simulation harness: a fake
// clock, a controllable in-memory network with partition/latency
// fault injection, and a cluster driver that runs raft nodes with
// reproducible scheduling.
package sim

import (
	"time"
)

// FakeClock is a manually advanced clock.
type FakeClock struct {
	t time.Time
}

// NewFakeClock returns a clock at the Unix epoch.
func NewFakeClock() *FakeClock { return &FakeClock{t: time.Unix(0, 0)} }

// Now returns the current simulated time.
func (c *FakeClock) Now() time.Time { return c.t }

// Advance moves the clock forward.
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
