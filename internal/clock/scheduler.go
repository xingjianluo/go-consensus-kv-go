package clock

import (
	"sort"
	"time"
)

// TimerID identifies a pending timer.
type TimerID int64

// TimerFired is an event emitted when a due timer is popped.
type TimerFired struct {
	ID   TimerID
	At   time.Time
	Tag  string
	Node string
}

// Scheduler is a deterministic one-shot timer queue keyed by fake time.
// Equal deadlines fire in a fixed (tag, node, id) order so identical seeds
// always produce identical runs.
type Scheduler struct {
	now    time.Time
	nextID TimerID
	timers []pendingTimer
}

type pendingTimer struct {
	id   TimerID
	at   time.Time
	tag  string
	node string
}

// NewScheduler creates an empty scheduler.
func NewScheduler(start time.Time) *Scheduler {
	return &Scheduler{now: start}
}

// Now returns the scheduler's current time.
func (s *Scheduler) Now() time.Time { return s.now }

// Add registers a one-shot timer and returns its id.
func (s *Scheduler) Add(at time.Time, tag, node string) TimerID {
	s.nextID++
	id := s.nextID
	s.timers = append(s.timers, pendingTimer{id: id, at: at, tag: tag, node: node})
	sort.SliceStable(s.timers, func(i, j int) bool {
		a, b := s.timers[i], s.timers[j]
		if !a.at.Equal(b.at) {
			return a.at.Before(b.at)
		}
		if a.tag != b.tag {
			return a.tag < b.tag
		}
		if a.node != b.node {
			return a.node < b.node
		}
		return a.id < b.id
	})
	return id
}

// Cancel removes a timer if it is still pending.
func (s *Scheduler) Cancel(id TimerID) bool {
	for i, t := range s.timers {
		if t.id == id {
			s.timers = append(s.timers[:i], s.timers[i+1:]...)
			return true
		}
	}
	return false
}

// Peek returns the earliest deadline without advancing time.
func (s *Scheduler) Peek() (time.Time, bool) {
	if len(s.timers) == 0 {
		return time.Time{}, false
	}
	return s.timers[0].at, true
}

// Due returns (without removing) ... use PopDue in Advance instead.
func (s *Scheduler) Due() []TimerFired { return nil }

// Advance moves time forward by d and returns the fired timers, from the
// earliest deadline to the latest, in deterministic tie-break order.
func (s *Scheduler) Advance(d time.Duration) []TimerFired {
	if d > 0 {
		s.now = s.now.Add(d)
	}
	var fired []TimerFired
	for len(s.timers) > 0 && !s.timers[0].at.After(s.now) {
		t := s.timers[0]
		s.timers = s.timers[1:]
		fired = append(fired, TimerFired{ID: t.id, At: t.at, Tag: t.tag, Node: t.node})
	}
	return fired
}

// AdvanceTo jumps to a specific time and fires due timers.
func (s *Scheduler) AdvanceTo(t time.Time) []TimerFired {
	if t.After(s.now) {
		s.now = t
	}
	var fired []TimerFired
	for len(s.timers) > 0 && !s.timers[0].at.After(s.now) {
		t0 := s.timers[0]
		s.timers = s.timers[1:]
		fired = append(fired, TimerFired{ID: t0.id, At: t0.at, Tag: t0.tag, Node: t0.node})
	}
	return fired
}

// Pending reports how many timers are registered.
func (s *Scheduler) Pending() int { return len(s.timers) }
