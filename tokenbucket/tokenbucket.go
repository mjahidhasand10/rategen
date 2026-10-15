// Package tokenbucket provides a single-key, in-process token-bucket limiter
// for pacing work inside one program: outgoing API calls, background jobs,
// log sampling. For per-client limits use the memory or redis stores, which
// manage one bucket per key.
//
// A bucket holds up to burst tokens and refills continuously at the
// configured rate. It starts full. Each event consumes tokens; Allow drops
// events that do not fit, Reserve schedules them, and Wait blocks for them.
package tokenbucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/internal/algo"
	"github.com/mjahidhasand10/rategen/internal/wait"
)

// Limiter is a token bucket. It is safe for concurrent use.
type Limiter struct {
	mu    sync.Mutex
	p     algo.Params
	s     algo.State
	rate  rategen.Rate
	burst int64
	tl    *rategen.Monotonic
	clock rategen.Clock
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithClock sets the time source, for tests. The default is the system clock.
func WithClock(c rategen.Clock) Option {
	return func(l *Limiter) { l.clock = c }
}

// New returns a full bucket that refills at rate and holds at most burst
// tokens. For example, New(rategen.PerSecond(100), 20) sustains 100 events
// per second with bursts of up to 20.
func New(rate rategen.Rate, burst int64, opts ...Option) (*Limiter, error) {
	if err := rate.Validate(); err != nil {
		return nil, err
	}
	if burst <= 0 {
		return nil, fmt.Errorf("%w: burst must be positive, got %d", rategen.ErrInvalidConfig, burst)
	}
	l := &Limiter{rate: rate, burst: burst, p: algo.NewTokenBucket(rate.Interval(), burst)}
	for _, o := range opts {
		o(l)
	}
	if l.clock == nil {
		l.clock = rategen.SystemClock()
	}
	l.tl = rategen.NewMonotonic(l.clock)
	l.s = algo.Fresh(l.p, l.tl.Nanos())
	return l, nil
}

// Rate returns the refill rate.
func (l *Limiter) Rate() rategen.Rate { return l.rate }

// Burst returns the bucket capacity.
func (l *Limiter) Burst() int64 { return l.burst }

// Tokens returns the number of tokens currently available. It is negative
// while outstanding reservations hold the bucket in debt.
func (l *Limiter) Tokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return algo.Tokens(l.p, l.s, l.tl.Nanos())
}

// Allow is shorthand for AllowN(1).
func (l *Limiter) Allow() bool { return l.AllowN(1) }

// AllowN reports whether n tokens are available now and consumes them if
// so. It never blocks. Use it to drop events that exceed the rate.
func (l *Limiter) AllowN(n int64) bool {
	d, _ := l.DecideN(n)
	return d.Allowed
}

// DecideN is AllowN with full decision details: remaining tokens and retry
// and reset times. It returns ErrInvalidCost for n <= 0 and
// ErrExceedsCapacity for n > burst; neither consumes tokens.
func (l *Limiter) DecideN(n int64) (rategen.Decision, error) {
	d := rategen.Decision{Limit: l.burst, Window: time.Duration(l.p.NominalWindow())}
	if err := l.checkCost(n); err != nil {
		return d, err
	}
	l.mu.Lock()
	next, r := algo.Decide(l.p, l.s, l.tl.Nanos(), n)
	if r.Allowed {
		l.s = next
	}
	l.mu.Unlock()
	d.Allowed = r.Allowed
	d.Remaining = r.Remaining
	d.RetryAfter = time.Duration(r.RetryAfter)
	d.ResetAfter = time.Duration(r.ResetAfter)
	return d, nil
}

// Reservation is a claim on tokens made by Reserve.
type Reservation struct {
	l      *Limiter
	ok     bool
	n      int64
	delay  time.Duration
	actAt  int64 // timeline nanos
	cancel bool
}

var _ rategen.Reservation = (*Reservation)(nil)

// OK reports whether the reservation was made. It is false when n exceeds
// the burst or the wait would exceed the requested maximum.
func (r *Reservation) OK() bool { return r.ok }

// Delay returns how long the holder must wait, from the time Reserve was
// called, before acting. It is meaningless when OK is false.
func (r *Reservation) Delay() time.Duration { return r.delay }

// Cancel returns the reserved tokens to the bucket if the reservation's time
// to act has not yet arrived. Calling it again, after the time to act, or on
// a reservation that is not OK does nothing.
func (r *Reservation) Cancel() {
	if !r.ok {
		return
	}
	l := r.l
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.tl.Nanos()
	if r.cancel || now >= r.actAt {
		return
	}
	r.cancel = true
	l.s = algo.Refund(l.p, l.s, now, r.n, 0)
}

// Reserve is shorthand for ReserveN(1, -1).
func (l *Limiter) Reserve() *Reservation { return l.ReserveN(1, -1) }

// ReserveN consumes n tokens, borrowing from future refills if necessary,
// and returns a Reservation telling the caller how long to wait before
// acting. If the wait would exceed maxWait (negative means no limit), or n
// exceeds the burst, no tokens are consumed and the reservation is not OK.
func (l *Limiter) ReserveN(n int64, maxWait time.Duration) *Reservation {
	r := &Reservation{l: l, n: n}
	if l.checkCost(n) != nil {
		return r
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.tl.Nanos()
	next, delay, ok := algo.Reserve(l.p, l.s, now, n, int64(maxWait))
	if !ok {
		return r
	}
	l.s = next
	r.ok, r.delay, r.actAt = true, time.Duration(delay), now+delay
	return r
}

// Wait is shorthand for WaitN(ctx, 1).
func (l *Limiter) Wait(ctx context.Context) error { return l.WaitN(ctx, 1) }

// WaitN blocks until n tokens are available and consumes them. It returns
// an error without consuming tokens if n is invalid or exceeds the burst,
// if ctx is already done, or if ctx's deadline would expire before the
// tokens are available. If ctx is canceled while waiting, the reserved
// tokens are returned.
func (l *Limiter) WaitN(ctx context.Context, n int64) error {
	if err := l.checkCost(n); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r := l.ReserveN(n, wait.MaxWait(ctx, l.clock))
	if !r.ok {
		return wait.ErrDeadline
	}
	return wait.Sleep(ctx, l.clock, r.delay, r.Cancel)
}

func (l *Limiter) checkCost(n int64) error {
	if n <= 0 {
		return fmt.Errorf("%w: got %d", rategen.ErrInvalidCost, n)
	}
	if n > l.burst {
		return fmt.Errorf("%w: cost %d, burst %d", rategen.ErrExceedsCapacity, n, l.burst)
	}
	return nil
}
