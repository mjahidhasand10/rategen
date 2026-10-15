package ratetest_test

import (
	"context"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen/ratetest"
)

func TestFakeClockTimers(t *testing.T) {
	start := time.Unix(1000, 0)
	c := ratetest.NewFakeClock(start)
	t1 := c.NewTimer(time.Second)
	t2 := c.NewTimer(2 * time.Second)
	t0 := c.NewTimer(0)
	select {
	case <-t0.C():
	default:
		t.Fatal("zero-duration timer did not fire immediately")
	}
	c.Advance(time.Second)
	select {
	case at := <-t1.C():
		if !at.Equal(start.Add(time.Second)) {
			t.Fatalf("fired at %v", at)
		}
	default:
		t.Fatal("due timer did not fire")
	}
	if !t2.Stop() || t2.Stop() {
		t.Fatal("Stop should report true once")
	}
	c.Advance(time.Hour)
	select {
	case <-t2.C():
		t.Fatal("stopped timer fired")
	default:
	}
	if c.PendingTimers() != 0 {
		t.Fatal("timers pending")
	}
	c.Set(start)
	if !c.Now().Equal(start) {
		t.Fatal("Set did not move the clock")
	}
}

func TestWaitForTimersHonoursContext(t *testing.T) {
	c := ratetest.NewFakeClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.WaitForTimers(ctx, 1); err == nil {
		t.Fatal("WaitForTimers ignored a canceled context")
	}
}
