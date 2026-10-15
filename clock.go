package rategen

import "time"

// Clock is the time source used by local limiters. Distributed backends use
// the server clock instead. Tests substitute ratetest.FakeClock.
type Clock interface {
	Now() time.Time
	// NewTimer returns a timer that fires once after d.
	NewTimer(d time.Duration) Timer
}

// Timer is the subset of *time.Timer used by waiting operations.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// SystemClock returns a Clock backed by the time package.
func SystemClock() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(d time.Duration) Timer { return systemTimer{time.NewTimer(d)} }

type systemTimer struct{ t *time.Timer }

func (t systemTimer) C() <-chan time.Time { return t.t.C }
func (t systemTimer) Stop() bool          { return t.t.Stop() }

// Monotonic converts a Clock into a nanosecond timeline that is aligned to
// the Unix epoch when created but advances using the clock's monotonic
// reading, so wall-clock steps (NTP corrections, manual changes) do not move
// rate-limit state backwards or forwards. Window algorithms align windows to
// this timeline.
type Monotonic struct {
	clock     Clock
	base      time.Time
	baseNanos int64
}

// NewMonotonic returns a Monotonic timeline for c; a nil c means SystemClock.
func NewMonotonic(c Clock) *Monotonic {
	if c == nil {
		c = SystemClock()
	}
	now := c.Now()
	return &Monotonic{clock: c, base: now, baseNanos: now.UnixNano()}
}

// Nanos returns the current position on the timeline.
func (m *Monotonic) Nanos() int64 {
	return m.baseNanos + int64(m.clock.Now().Sub(m.base))
}

// Clock returns the underlying clock.
func (m *Monotonic) Clock() Clock { return m.clock }
