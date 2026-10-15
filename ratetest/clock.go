// Package ratetest provides helpers for testing code that uses rategen,
// chiefly a deterministic clock.
package ratetest

import (
	"context"
	"sync"
	"time"

	"github.com/mjahidhasand10/rategen"
)

// FakeClock is a manually advanced rategen.Clock. Its zero value is not
// usable; create one with NewFakeClock. It is safe for concurrent use.
//
// Times returned by FakeClock carry no monotonic reading, so they behave
// like wall-clock times in comparisons.
type FakeClock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	timers  []*fakeTimer
	created int
}

// NewFakeClock returns a FakeClock set to start.
func NewFakeClock(start time.Time) *FakeClock {
	c := &FakeClock{now: start.Round(0)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

var _ rategen.Clock = (*FakeClock)(nil)

// Now returns the clock's current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d and fires any timers that are due.
// A negative d moves the clock backwards (simulating a wall-clock step) and
// fires nothing.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.timers[:0]
	for _, t := range c.timers {
		if !t.at.After(c.now) {
			t.ch <- c.now
			continue
		}
		kept = append(kept, t)
	}
	clear(c.timers[len(kept):])
	c.timers = kept
}

// Set moves the clock to t, firing timers due at or before t.
func (c *FakeClock) Set(t time.Time) {
	c.Advance(t.Round(0).Sub(c.Now()))
}

// NewTimer returns a timer that fires when the clock is advanced by at
// least d. A non-positive d fires immediately.
func (c *FakeClock) NewTimer(d time.Duration) rategen.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.created++
	if d <= 0 {
		t.ch <- c.now
	} else {
		c.timers = append(c.timers, t)
	}
	c.cond.Broadcast()
	return t
}

// PendingTimers returns the number of timers that have not yet fired or
// been stopped.
func (c *FakeClock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// WaitForTimers blocks until at least n timers have been created over the
// clock's lifetime, or ctx is done. It lets tests advance the clock only
// after a goroutine under test has started waiting, without sleeping.
func (c *FakeClock) WaitForTimers(ctx context.Context, n int) error {
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.cond.Broadcast()
	})
	defer stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.created < n {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.cond.Wait()
	}
	return nil
}

type fakeTimer struct {
	c  *FakeClock
	at time.Time
	ch chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	for i, x := range t.c.timers {
		if x == t {
			t.c.timers = append(t.c.timers[:i], t.c.timers[i+1:]...)
			return true
		}
	}
	return false
}
