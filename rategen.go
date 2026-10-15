package rategen

import (
	"context"
	"fmt"
	"time"
)

// Rate is an average admission rate: Limit events per Period.
//
// For window algorithms Limit is the number of events admitted per window of
// length Period. For token bucket and GCRA, Limit/Period is the refill rate;
// the burst capacity is configured separately.
type Rate struct {
	Limit  int64
	Period time.Duration
}

// PerSecond returns a Rate of n events per second.
func PerSecond(n int64) Rate { return Rate{Limit: n, Period: time.Second} }

// PerMinute returns a Rate of n events per minute.
func PerMinute(n int64) Rate { return Rate{Limit: n, Period: time.Minute} }

// PerHour returns a Rate of n events per hour.
func PerHour(n int64) Rate { return Rate{Limit: n, Period: time.Hour} }

// PerDay returns a Rate of n events per 24 hours.
func PerDay(n int64) Rate { return Rate{Limit: n, Period: 24 * time.Hour} }

// Every returns a Rate of one event per interval.
func Every(interval time.Duration) Rate { return Rate{Limit: 1, Period: interval} }

// Validate reports whether r is usable: both Limit and Period must be positive.
func (r Rate) Validate() error {
	if r.Limit <= 0 {
		return fmt.Errorf("%w: rate limit must be positive, got %d", ErrInvalidConfig, r.Limit)
	}
	if r.Period <= 0 {
		return fmt.Errorf("%w: rate period must be positive, got %v", ErrInvalidConfig, r.Period)
	}
	return nil
}

// Interval returns the average time between events, in nanoseconds, as a
// float to avoid truncation for rates that do not divide Period evenly.
func (r Rate) Interval() float64 {
	return float64(r.Period) / float64(r.Limit)
}

// String formats r as "<limit>/<period>", for example "100/1s".
func (r Rate) String() string {
	return fmt.Sprintf("%d/%v", r.Limit, r.Period)
}

// Decision is the outcome of a rate-limit check.
type Decision struct {
	// Allowed reports whether the request may proceed. Callers must act on
	// Allowed even when an error accompanies the decision: under a fail-open
	// policy a decision can be allowed despite a storage error.
	Allowed bool

	// Policy is the name of the limiter that produced the decision.
	Policy string

	// Limit is the maximum number of units admissible at once: the burst
	// capacity for token bucket and GCRA, or the per-window limit for
	// window algorithms.
	Limit int64

	// Remaining is the number of units that could still be admitted
	// immediately after this decision. It is a best-effort snapshot.
	Remaining int64

	// RetryAfter is how long to wait before the same request could be
	// admitted. It is zero when Allowed is true, and also zero when the
	// request can never be admitted (cost greater than Limit).
	RetryAfter time.Duration

	// ResetAfter is how long until the limiter for this key returns to its
	// initial, fully available state if no further requests arrive.
	ResetAfter time.Duration

	// Window is the policy's nominal window, used for rate-limit headers:
	// the window length for window algorithms, and the time to refill the
	// full capacity for token bucket and GCRA.
	Window time.Duration

	// Degraded reports that the decision was not made by the configured
	// backend because it was unavailable, and a failure policy decided
	// instead. Under FailOpen the other fields carry no meaningful values;
	// under FailToLocal they describe the per-process fallback limiter,
	// not the shared limit.
	Degraded bool

	// Epoch identifies, for window algorithms, the window an admitted
	// request was charged to. It is opaque and backend-specific (zero for
	// other algorithms); its only use is passing the Decision back to
	// Refunder.RefundN, so refunds go to the window that was charged.
	Epoch int64
}

// Limiter is a keyed rate limiter. Implementations must be safe for
// concurrent use.
type Limiter interface {
	// AllowN reports whether n units may be consumed for key now, and
	// consumes them if so. AllowN always returns a usable Decision; a
	// non-nil error explains why the decision was not made normally.
	AllowN(ctx context.Context, key string, n int64) (Decision, error)
}

// LimiterFunc adapts a function to Limiter, for custom limiters and tests.
type LimiterFunc func(ctx context.Context, key string, n int64) (Decision, error)

// AllowN calls f(ctx, key, n).
func (f LimiterFunc) AllowN(ctx context.Context, key string, n int64) (Decision, error) {
	return f(ctx, key, n)
}

// Allow is shorthand for l.AllowN(ctx, key, 1).
func Allow(ctx context.Context, l Limiter, key string) (Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// Reservation is a claim on future capacity, returned by Reserver.
type Reservation interface {
	// OK reports whether the reservation can ever be honoured. It is false
	// when n exceeds the limiter's capacity or the wait exceeds the
	// requested maximum.
	OK() bool
	// Delay returns how long the holder must wait before acting, measured
	// from the time of the call.
	Delay() time.Duration
	// Cancel returns the reserved capacity if the reservation's time to act
	// has not yet arrived. It is a no-op otherwise, and safe to call more
	// than once.
	Cancel()
}

// Reserver is implemented by limiters that can admit work at a future time.
type Reserver interface {
	Limiter
	// ReserveN reserves n units for key. maxWait bounds the acceptable
	// delay; a reservation whose delay would exceed maxWait is not made
	// and reports OK() == false. A negative maxWait means no bound.
	ReserveN(ctx context.Context, key string, n int64, maxWait time.Duration) (Reservation, error)
}

// Waiter is implemented by limiters that can block until capacity is
// available.
type Waiter interface {
	Limiter
	// WaitN blocks until n units are available for key and consumes them,
	// or returns an error if ctx is done first or its deadline is too soon
	// for the wait to complete. No capacity is consumed on error.
	WaitN(ctx context.Context, key string, n int64) error
}

// Refunder is implemented by limiters that can return consumed capacity.
// Refunds are clamped so that a key never holds more capacity than its
// initial state.
type Refunder interface {
	Limiter
	// RefundN returns n units consumed for key by the decision consumed,
	// which must be the Decision returned when they were consumed.
	//
	// For window algorithms the units go back to the window they were
	// charged to. If that window no longer counts towards the limit, the
	// refund is dropped: crediting a later window would admit traffic the
	// limit never allowed. A refund with a zero Decision is likewise
	// dropped for window algorithms. Token bucket and GCRA refunds need no
	// window and always apply.
	RefundN(ctx context.Context, key string, n int64, consumed Decision) error
}

// Info describes a limiter for diagnostics and metrics.
type Info struct {
	Policy    string // limiter name chosen by the user
	Algorithm string // "token_bucket", "fixed_window", "sliding_window", "gcra"
	Backend   string // "memory", "redis", ...
	Rate      Rate
	Burst     int64 // capacity; equals Rate.Limit for window algorithms
}

// Describer is implemented by limiters that can describe themselves.
type Describer interface {
	Info() Info
}

// ValidatePolicyName reports whether name is usable as a policy name.
// Names appear in storage keys, metrics labels and response headers, so
// they are restricted to 1-64 characters from [A-Za-z0-9_.:-].
func ValidatePolicyName(name string) error {
	if len(name) == 0 || len(name) > 64 {
		return fmt.Errorf("%w: policy name must be 1-64 characters, got %q", ErrInvalidConfig, name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '_', c == '.', c == ':', c == '-':
		default:
			return fmt.Errorf("%w: policy name %q contains %q; allowed characters are [A-Za-z0-9_.:-]", ErrInvalidConfig, name, c)
		}
	}
	return nil
}
