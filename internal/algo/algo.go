// Package algo implements the rate-limiting algorithms as pure state
// transitions. Functions take a state value and the current time, and return
// the new state and the outcome; they never lock, allocate or read clocks.
// Backends own storage and concurrency. The Redis backend implements the same
// arithmetic in Lua; tests cross-check the two.
//
// All times are nanoseconds on a caller-defined timeline (see
// rategen.Monotonic). Time moving backwards is treated as no time passing.
package algo

import (
	"math"
)

// Kind identifies an algorithm.
type Kind uint8

// Supported algorithms.
const (
	TokenBucket Kind = iota + 1
	FixedWindow
	SlidingWindow
	GCRA
)

// String returns the algorithm's stable identifier, used in metrics labels.
func (k Kind) String() string {
	switch k {
	case TokenBucket:
		return "token_bucket"
	case FixedWindow:
		return "fixed_window"
	case SlidingWindow:
		return "sliding_window"
	case GCRA:
		return "gcra"
	default:
		return "unknown"
	}
}

// Params are the validated, precomputed parameters of one policy.
type Params struct {
	Kind Kind
	// Limit is the burst capacity (token bucket, GCRA) or the per-window
	// limit (fixed and sliding window).
	Limit int64
	// Window is the window length in ns (fixed and sliding window).
	Window int64
	// Interval is the average time between events in ns (token bucket).
	Interval float64
	// Emission is the GCRA emission interval in ns, rounded up so the
	// integer arithmetic never admits faster than the configured rate.
	Emission int64
}

// NewTokenBucket returns parameters for a token bucket refilling one token
// every interval ns, holding at most burst tokens.
func NewTokenBucket(interval float64, burst int64) Params {
	return Params{Kind: TokenBucket, Limit: burst, Interval: interval}
}

// NewGCRA returns parameters for GCRA with the given emission interval and
// burst tolerance expressed in requests.
func NewGCRA(interval float64, burst int64) Params {
	e := int64(math.Ceil(interval))
	if e < 1 {
		e = 1
	}
	return Params{Kind: GCRA, Limit: burst, Interval: interval, Emission: e}
}

// NewFixedWindow returns parameters for a fixed window of window ns.
func NewFixedWindow(limit, window int64) Params {
	return Params{Kind: FixedWindow, Limit: limit, Window: window}
}

// NewSlidingWindow returns parameters for a sliding-window counter.
func NewSlidingWindow(limit, window int64) Params {
	return Params{Kind: SlidingWindow, Limit: limit, Window: window}
}

// NominalWindow is the policy window advertised in headers: the window length
// for window algorithms and the time to refill full capacity otherwise.
func (p Params) NominalWindow() int64 {
	switch p.Kind {
	case FixedWindow, SlidingWindow:
		return p.Window
	case GCRA:
		return satMul(p.Emission, p.Limit)
	default:
		return ceilNanos(p.Interval * float64(p.Limit))
	}
}

// State is the per-key state of any algorithm. The meaning of the fields
// depends on the algorithm:
//
//	token bucket:   A = last update time, F = tokens
//	fixed window:   A = window start,     B = count
//	sliding window: A = current window start, B = current count, C = previous count
//	GCRA:           A = theoretical arrival time (TAT)
type State struct {
	A, B, C int64
	F       float64
}

// Result is the outcome of a decision. Durations are in ns.
type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter int64
	ResetAfter int64
	// Epoch identifies the window that an admitted request was charged
	// to (its start time) for window algorithms, and is zero otherwise.
	// Refund needs it to return units to the right window.
	Epoch int64
}

// Fresh returns the initial state for a key first seen at now.
func Fresh(p Params, now int64) State {
	switch p.Kind {
	case TokenBucket:
		return State{A: now, F: float64(p.Limit)}
	default:
		// The zero state already means "nothing consumed" for the other
		// algorithms: window starts never match, and a zero TAT is in the
		// past.
		return State{}
	}
}

// tokenEpsilon absorbs float rounding when comparing token counts, so that a
// bucket that has mathematically refilled to n tokens is not rejected because
// it holds n-1e-12.
const tokenEpsilon = 1e-9

// Decide evaluates a request of cost n (1 <= n <= p.Limit) at time now.
// The returned state must be stored only if the result is allowed; on
// rejection it equals the input state advanced to now, which is
// semantically identical to the input.
func Decide(p Params, s State, now, n int64) (State, Result) {
	switch p.Kind {
	case TokenBucket:
		return decideTokenBucket(p, s, now, n)
	case FixedWindow:
		return decideFixedWindow(p, s, now, n)
	case SlidingWindow:
		return decideSlidingWindow(p, s, now, n)
	case GCRA:
		return decideGCRA(p, s, now, n)
	default:
		panic("algo: unknown kind")
	}
}

// Reserve consumes n units unconditionally, possibly into debt, and returns
// the delay after which the reservation may act. Only token bucket and GCRA
// support reservations; ok is false for other kinds or when the delay would
// exceed maxWait (maxWait < 0 means unbounded). The state must be stored only
// when ok is true.
func Reserve(p Params, s State, now, n, maxWait int64) (next State, delay int64, ok bool) {
	switch p.Kind {
	case TokenBucket:
		s = refill(p, s, now)
		if s.F >= float64(n)-tokenEpsilon {
			s.F -= float64(n)
			return s, 0, true
		}
		delay = tokenWait(p, s.F, float64(n))
		if maxWait >= 0 && delay > maxWait {
			return s, delay, false
		}
		s.F -= float64(n)
		return s, delay, true
	case GCRA:
		tat, debt, slack, cost := gcraTerms(p, s, now, n)
		delay = max(0, debt-slack)
		if maxWait >= 0 && delay > maxWait {
			return s, delay, false
		}
		s.A = satAdd(tat, cost)
		return s, delay, true
	default:
		return s, 0, false
	}
}

// Refund returns n previously consumed units at time now. Refunds never
// raise availability above the initial state.
//
// For window algorithms, epoch must be the Result.Epoch of the decision
// that consumed the units: they are returned to the window they were
// charged to, or dropped if that window no longer counts (returning them to
// a later window would admit traffic the limit never allowed). Token bucket
// and GCRA ignore epoch: their state has no windows, and the clamp below
// keeps a refund from exceeding what never consuming would have left.
func Refund(p Params, s State, now, n, epoch int64) State {
	switch p.Kind {
	case TokenBucket:
		s = refill(p, s, now)
		s.F = math.Min(float64(p.Limit), s.F+float64(n))
	case FixedWindow:
		if epoch != 0 && s.A == epoch && s.A == currentWindow(now, p.Window, s.A) {
			s.B = max(0, s.B-n)
		}
	case SlidingWindow:
		s = rollSliding(p, s, now)
		switch {
		case epoch == 0:
		case epoch == s.A:
			s.B = max(0, s.B-n)
		case epoch == s.A-p.Window:
			s.C = max(0, s.C-n)
		}
	case GCRA:
		s.A = max(now, s.A-satMul(n, p.Emission))
	}
	return s
}

// ExpiresAt returns the time at which s becomes equivalent to Fresh state, so
// that discarding it does not change any future decision.
func ExpiresAt(p Params, s State) int64 {
	switch p.Kind {
	case TokenBucket:
		missing := float64(p.Limit) - s.F
		if missing <= 0 {
			return s.A
		}
		// Rounded up generously: deleting the entry early would grant
		// the missing fraction of a token for free.
		f := math.Ceil(missing * p.Interval)
		if f >= math.MaxInt64 {
			return math.MaxInt64
		}
		return satAdd(s.A, int64(f)+1)
	case FixedWindow:
		return satAdd(s.A, p.Window)
	case SlidingWindow:
		if s.B > 0 {
			return satAdd(s.A, 2*p.Window)
		}
		return satAdd(s.A, p.Window)
	case GCRA:
		return s.A
	default:
		return 0
	}
}

// Tokens returns the token-bucket level at now (negative while in debt).
func Tokens(p Params, s State, now int64) float64 {
	return refill(p, s, now).F
}

func refill(p Params, s State, now int64) State {
	if now > s.A {
		s.F = math.Min(float64(p.Limit), s.F+float64(now-s.A)/p.Interval)
		s.A = now
	}
	return s
}

func decideTokenBucket(p Params, s State, now, n int64) (State, Result) {
	s = refill(p, s, now)
	need := float64(n)
	var r Result
	if s.F >= need-tokenEpsilon {
		s.F = math.Max(0, s.F-need)
		r.Allowed = true
	} else {
		r.RetryAfter = tokenWait(p, s.F, need)
	}
	r.Remaining = int64(math.Floor(s.F + tokenEpsilon))
	r.ResetAfter = ceilNanos((float64(p.Limit) - s.F) * p.Interval)
	return s, r
}

// tokenWait returns the time until a bucket holding tokens has need
// tokens. The float estimate is verified against the same refill arithmetic
// Decide uses and nudged up, so a caller retrying after exactly that long is
// admitted.
func tokenWait(p Params, tokens, need float64) int64 {
	d := ceilNanos((need - tokens) * p.Interval)
	for range 8 {
		if tokens+float64(d)/p.Interval >= need-tokenEpsilon {
			return d
		}
		d++
	}
	return satAdd(d, ceilNanos(tokenEpsilon*p.Interval)+1)
}

func windowStart(now, window int64) int64 {
	m := now % window
	if m < 0 {
		m += window
	}
	return now - m
}

// currentWindow returns the window containing now, but never one earlier
// than the stored window start: a stale timestamp (a caller that read the
// clock before another crossed a boundary, or a clock step backwards) is
// treated as belonging to the stored window rather than rolling it back.
func currentWindow(now, window, stored int64) int64 {
	return max(windowStart(now, window), stored)
}

func decideFixedWindow(p Params, s State, now, n int64) (State, Result) {
	start := currentWindow(now, p.Window, s.A)
	now = max(now, start)
	if s.A != start {
		s.A, s.B = start, 0
	}
	var r Result
	reset := start + p.Window - now
	if n <= p.Limit-s.B { // not s.B+n <= Limit, which can overflow
		s.B += n
		r.Allowed = true
		r.Epoch = s.A
	} else {
		r.RetryAfter = reset
	}
	// The stored count can exceed the limit after the limit is lowered
	// under the same policy name.
	r.Remaining = max(0, p.Limit-s.B)
	r.ResetAfter = reset
	return s, r
}

func rollSliding(p Params, s State, now int64) State {
	start := currentWindow(now, p.Window, s.A)
	if s.A != start {
		if s.A == start-p.Window {
			s.C = s.B
		} else {
			s.C = 0
		}
		s.A, s.B = start, 0
	}
	return s
}

// slidingEstimate rolls s to now and returns the rolled state, the time
// elapsed in the current window and the weighted count estimate.
func slidingEstimate(p Params, s State, now int64) (State, int64, float64) {
	s = rollSliding(p, s, now)
	elapsed := max(0, now-s.A)
	w := float64(p.Window)
	weight := (w - float64(elapsed)) / w
	return s, elapsed, float64(s.C)*weight + float64(s.B)
}

func slidingFits(p Params, est float64, n int64) bool {
	return est+float64(n) <= float64(p.Limit)+tokenEpsilon
}

func decideSlidingWindow(p Params, s State, now, n int64) (State, Result) {
	s, elapsed, est := slidingEstimate(p, s, now)
	var r Result
	if slidingFits(p, est, n) {
		s.B += n
		est += float64(n)
		r.Allowed = true
		r.Epoch = s.A
	} else {
		r.RetryAfter = slidingRetry(p, s, elapsed, n)
		// Verify against the estimate Decide itself will compute, so
		// float error in the weights never makes a retry too early.
		for range 8 {
			if _, _, e := slidingEstimate(p, s, now+r.RetryAfter); slidingFits(p, e, n) {
				break
			}
			r.RetryAfter++
		}
	}
	r.Remaining = max(0, int64(math.Floor(float64(p.Limit)-est+tokenEpsilon)))
	switch {
	case s.B > 0:
		r.ResetAfter = 2*p.Window - elapsed
	case s.C > 0:
		r.ResetAfter = p.Window - elapsed
	}
	return s, r
}

// slidingRetry returns the earliest time at which a request of cost n fits
// under the weighted estimate, assuming no other traffic.
func slidingRetry(p Params, s State, elapsed, n int64) int64 {
	w := float64(p.Window)
	if avail := p.Limit - s.B - n; avail >= 0 && s.C > 0 {
		// Fits later in this window once the previous window's weight
		// decays: C*(W-e)/W <= avail.
		e := w * (1 - float64(avail)/float64(s.C))
		return max(1, ceilNanos(e)-elapsed)
	}
	// Must wait for the next window, where the current count becomes the
	// previous one: B*(W-e')/W + n <= Limit.
	var e2 float64
	if s.B > 0 {
		e2 = math.Max(0, w*(1-float64(p.Limit-n)/float64(s.B)))
	}
	return p.Window - elapsed + ceilNanos(e2)
}

// gcraTerms returns the GCRA quantities in a form that cannot overflow:
// debt = max(TAT, now) - now (how far the key is ahead of schedule) and
// slack = τ - n·T (how far ahead it may be after admitting n). A request is
// admitted iff debt <= slack. Comparing these, rather than computing
// TAT + n·T and subtracting τ, stays correct when values saturate.
func gcraTerms(p Params, s State, now, n int64) (tat, debt, slack, cost int64) {
	tau := satMul(p.Emission, p.Limit)
	cost = satMul(n, p.Emission)
	tat = max(s.A, now)
	return tat, tat - now, tau - cost, cost
}

func decideGCRA(p Params, s State, now, n int64) (State, Result) {
	tat, debt, slack, cost := gcraTerms(p, s, now, n)
	var r Result
	if debt <= slack {
		s.A = satAdd(tat, cost)
		r.Allowed = true
		r.Remaining = (slack - debt) / p.Emission
		r.ResetAfter = satAdd(debt, cost)
	} else {
		r.RetryAfter = debt - slack
		r.Remaining = max(0, (slack+cost-debt)/p.Emission)
		r.ResetAfter = debt
	}
	return s, r
}

// ceilNanos rounds a float duration up to whole nanoseconds, ignoring
// sub-nanosecond noise from float arithmetic (1e8 * 0.01 is not exactly 1e6).
func ceilNanos(f float64) int64 {
	if f <= 0 {
		return 0
	}
	if f >= math.MaxInt64 {
		return math.MaxInt64
	}
	return max(1, int64(math.Ceil(f-1e-3)))
}

func satAdd(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func satMul(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}
