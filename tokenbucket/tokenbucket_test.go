package tokenbucket_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/ratetest"
	"github.com/mjahidhasand10/rategen/tokenbucket"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newLimiter(t testing.TB, rate rategen.Rate, burst int64) (*tokenbucket.Limiter, *ratetest.FakeClock) {
	t.Helper()
	clk := ratetest.NewFakeClock(epoch)
	l, err := tokenbucket.New(rate, burst, tokenbucket.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	return l, clk
}

func TestNewValidates(t *testing.T) {
	for _, tc := range []struct {
		rate  rategen.Rate
		burst int64
	}{
		{rategen.Rate{Limit: 0, Period: time.Second}, 1},
		{rategen.Rate{Limit: 1, Period: 0}, 1},
		{rategen.Rate{Limit: -1, Period: time.Second}, 1},
		{rategen.PerSecond(1), 0},
	} {
		if _, err := tokenbucket.New(tc.rate, tc.burst); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("New(%v, %d) error = %v, want ErrInvalidConfig", tc.rate, tc.burst, err)
		}
	}
}

func TestBurstAndRefill(t *testing.T) {
	l, clk := newLimiter(t, rategen.PerSecond(10), 5)
	for i := range 5 {
		if !l.Allow() {
			t.Fatalf("request %d of initial burst rejected", i)
		}
	}
	if l.Allow() {
		t.Fatal("request beyond burst admitted")
	}
	clk.Advance(100 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("refilled token not available")
	}
	if l.Allow() {
		t.Fatal("more than one token refilled in 100ms")
	}
	clk.Advance(time.Hour)
	if got := l.Tokens(); got != 5 {
		t.Fatalf("tokens after long idle = %v, want capped at 5", got)
	}
}

func TestDecideN(t *testing.T) {
	l, _ := newLimiter(t, rategen.PerSecond(2), 4)
	d, err := l.DecideN(3)
	if err != nil || !d.Allowed || d.Remaining != 1 || d.Limit != 4 {
		t.Fatalf("DecideN(3) = %+v, %v", d, err)
	}
	if d.ResetAfter != 1500*time.Millisecond {
		t.Fatalf("ResetAfter = %v, want 1.5s", d.ResetAfter)
	}
	d, _ = l.DecideN(2)
	if d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("DecideN(2) = %+v, want rejected with 500ms retry", d)
	}
	if _, err := l.DecideN(5); !errors.Is(err, rategen.ErrExceedsCapacity) {
		t.Fatalf("DecideN(5) error = %v", err)
	}
	if _, err := l.DecideN(0); !errors.Is(err, rategen.ErrInvalidCost) {
		t.Fatalf("DecideN(0) error = %v", err)
	}
	if l.Tokens() != 1 {
		t.Fatalf("invalid requests consumed tokens: %v", l.Tokens())
	}
}

func TestClockStepBackwards(t *testing.T) {
	l, clk := newLimiter(t, rategen.PerSecond(1), 1)
	l.Allow()
	clk.Advance(-time.Hour)
	if l.Allow() {
		t.Fatal("token appeared after clock stepped back")
	}
	clk.Advance(time.Hour + time.Second)
	if !l.Allow() {
		t.Fatal("token not available after the clock recovered")
	}
}

func TestReserveAndCancel(t *testing.T) {
	l, clk := newLimiter(t, rategen.PerSecond(1), 1)
	r1 := l.Reserve()
	if !r1.OK() || r1.Delay() != 0 {
		t.Fatalf("first reservation: ok=%v delay=%v", r1.OK(), r1.Delay())
	}
	r2 := l.Reserve()
	if !r2.OK() || r2.Delay() != time.Second {
		t.Fatalf("second reservation: ok=%v delay=%v", r2.OK(), r2.Delay())
	}
	r3 := l.ReserveN(1, 1500*time.Millisecond)
	if r3.OK() {
		t.Fatal("reservation beyond maxWait granted")
	}
	if l.ReserveN(2, -1).OK() {
		t.Fatal("reservation above burst granted")
	}
	r2.Cancel()
	r2.Cancel() // idempotent
	if got := l.Tokens(); got != 0 {
		t.Fatalf("tokens after cancel = %v, want 0", got)
	}
	// Cancel after the time to act does nothing.
	r4 := l.Reserve()
	clk.Advance(2 * time.Second)
	r4.Cancel()
	if got := l.Tokens(); got != 1 {
		t.Fatalf("late cancel refunded tokens: %v", got)
	}
}

func TestWait(t *testing.T) {
	l, clk := newLimiter(t, rategen.PerSecond(1), 1)
	ctx := t.Context()
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx) }()
	if err := clk.WaitForTimers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("Wait returned before the token was available")
	default:
	}
	clk.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatalf("Wait = %v", err)
	}
}

func TestWaitCancelReturnsTokens(t *testing.T) {
	l, clk := newLimiter(t, rategen.PerSecond(1), 1)
	l.Allow()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx) }()
	if err := clk.WaitForTimers(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	if got := l.Tokens(); got != 0 {
		t.Fatalf("tokens after canceled wait = %v, want 0 (debt refunded)", got)
	}
	if clk.PendingTimers() != 0 {
		t.Fatal("timer leaked after cancel")
	}
}

func TestWaitDeadlineTooShort(t *testing.T) {
	l, clk := newLimiter(t, rategen.PerSecond(1), 1)
	l.Allow()
	ctx, cancel := context.WithDeadline(t.Context(), clk.Now().Add(500*time.Millisecond))
	defer cancel()
	if err := l.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want DeadlineExceeded", err)
	}
	if got := l.Tokens(); got != 0 {
		t.Fatalf("failed wait consumed tokens: %v", got)
	}
	if err := l.WaitN(t.Context(), 2); !errors.Is(err, rategen.ErrExceedsCapacity) {
		t.Fatalf("WaitN(2) = %v", err)
	}
}

func TestConcurrentAllowExact(t *testing.T) {
	l, _ := newLimiter(t, rategen.PerSecond(1), 1000)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 100 {
				if l.Allow() {
					admitted.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if got := admitted.Load(); got != 1000 {
		t.Fatalf("admitted %d with a frozen clock, want exactly the burst (1000)", got)
	}
}

func TestConcurrentRealClock(t *testing.T) {
	l, err := tokenbucket.New(rategen.PerSecond(1000), 10)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				l.Allow()
				r := l.ReserveN(1, time.Millisecond)
				r.Cancel()
				_ = l.Tokens()
			}
		})
	}
	wg.Wait()
	if tok := l.Tokens(); tok > 10 {
		t.Fatalf("tokens %v exceed burst", tok)
	}
}

func BenchmarkAllow(b *testing.B) {
	l, _ := tokenbucket.New(rategen.PerSecond(1e9), 1e9)
	for b.Loop() {
		l.Allow()
	}
}

func BenchmarkAllowReject(b *testing.B) {
	l, _ := newLimiter(b, rategen.PerSecond(1), 1)
	l.Allow()
	for b.Loop() {
		l.Allow()
	}
}

func BenchmarkAllowParallel(b *testing.B) {
	l, _ := tokenbucket.New(rategen.PerSecond(1e9), 1e9)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow()
		}
	})
}
