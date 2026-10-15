package algo

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

const (
	ms  = int64(time.Millisecond)
	sec = int64(time.Second)
)

// t0 is an arbitrary, window-aligned start time, far from zero so that bugs
// relying on zero-valued state show up.
const t0 = 1_700_000_000 * sec

type step struct {
	at      int64 // offset from t0
	n       int64
	allowed bool
	remain  int64
	retry   int64 // checked only when >= 0
}

func run(t *testing.T, p Params, steps []step) {
	t.Helper()
	s := Fresh(p, t0)
	for i, st := range steps {
		next, r := Decide(p, s, t0+st.at, st.n)
		if r.Allowed != st.allowed {
			t.Fatalf("step %d at +%v: allowed=%v, want %v (result %+v)", i, time.Duration(st.at), r.Allowed, st.allowed, r)
		}
		if r.Remaining != st.remain {
			t.Fatalf("step %d: remaining=%d, want %d", i, r.Remaining, st.remain)
		}
		if st.retry >= 0 && r.RetryAfter != st.retry {
			t.Fatalf("step %d: retry=%v, want %v", i, time.Duration(r.RetryAfter), time.Duration(st.retry))
		}
		if r.Allowed {
			s = next
		}
	}
}

func TestTokenBucket(t *testing.T) {
	p := NewTokenBucket(float64(100*ms), 3) // 10/s, burst 3
	run(t, p, []step{
		{0, 1, true, 2, 0},
		{0, 1, true, 1, 0},
		{0, 1, true, 0, 0},
		{0, 1, false, 0, 100 * ms},
		{99 * ms, 1, false, 0, 1 * ms},
		{100 * ms, 1, true, 0, 0},
		{100 * ms, 2, false, 0, 200 * ms},
		{1 * sec, 3, true, 0, 0},         // fully refilled, cap at 3
		{1 * sec, 4, false, 0, 400 * ms}, // caller rejects n > Limit; algo still answers
	})
}

func TestTokenBucketTimeBackwards(t *testing.T) {
	p := NewTokenBucket(float64(sec), 1)
	s := Fresh(p, t0)
	s, r := Decide(p, s, t0, 1)
	if !r.Allowed {
		t.Fatal("first request rejected")
	}
	// Clock goes back one hour: no refill, and no negative elapsed time.
	s2, r := Decide(p, s, t0-int64(time.Hour), 1)
	if r.Allowed {
		t.Fatal("allowed after clock moved backwards")
	}
	if s2.A != s.A || s2.F != s.F {
		t.Fatalf("state changed when time moved backwards: %+v -> %+v", s, s2)
	}
}

func TestTokenBucketReserveRefund(t *testing.T) {
	p := NewTokenBucket(float64(sec), 2)
	s := Fresh(p, t0)
	s, d, ok := Reserve(p, s, t0, 2, -1)
	if !ok || d != 0 {
		t.Fatalf("reserve from full bucket: delay=%v ok=%v", d, ok)
	}
	s, d, ok = Reserve(p, s, t0, 1, -1)
	if !ok || d != sec {
		t.Fatalf("reserve into debt: delay=%v ok=%v", time.Duration(d), ok)
	}
	if _, _, ok := Reserve(p, s, t0, 1, sec); ok {
		t.Fatal("reservation beyond maxWait was granted")
	}
	// Refund the debt reservation: back to zero tokens, not above.
	s = Refund(p, s, t0, 1, 0)
	if s.F != 0 {
		t.Fatalf("tokens after refund = %v, want 0", s.F)
	}
	s = Refund(p, s, t0, 10, 0)
	if s.F != 2 {
		t.Fatalf("refund not clamped to capacity: %v", s.F)
	}
}

func TestFixedWindow(t *testing.T) {
	p := NewFixedWindow(2, sec)
	run(t, p, []step{
		{0, 1, true, 1, 0},
		{500 * ms, 1, true, 0, 0},
		{999 * ms, 1, false, 0, 1 * ms},
		// The boundary instant belongs to the new window.
		{sec, 2, true, 0, 0},
		{sec + 1, 1, false, 0, sec - 1},
		{3 * sec, 3, false, 2, sec}, // cost above limit never fits
	})
}

func TestFixedWindowNegativeTime(t *testing.T) {
	if got := windowStart(-1, 10); got != -10 {
		t.Fatalf("windowStart(-1,10) = %d, want -10", got)
	}
}

func TestFixedWindowRefund(t *testing.T) {
	p := NewFixedWindow(2, sec)
	s, r := Decide(p, Fresh(p, t0), t0, 2)
	s = Refund(p, s, t0+ms, 1, r.Epoch)
	if s.B != 1 {
		t.Fatalf("count after refund = %d, want 1", s.B)
	}
	// Refund in a later window does not affect the new window.
	s2 := Refund(p, s, t0+sec, 1, r.Epoch)
	if s2 != s {
		t.Fatalf("cross-window refund changed state: %+v", s2)
	}
}

func TestSlidingWindow(t *testing.T) {
	p := NewSlidingWindow(10, sec)
	s := Fresh(p, t0)
	// Fill the first window.
	s, r := Decide(p, s, t0+900*ms, 10)
	if !r.Allowed || r.Remaining != 0 {
		t.Fatalf("fill: %+v", r)
	}
	// Halfway through the next window, half of the previous window's
	// count still weighs: estimate 5, so 5 more fit.
	s, r = Decide(p, s, t0+1500*ms, 5)
	if !r.Allowed || r.Remaining != 0 {
		t.Fatalf("half-window: %+v", r)
	}
	_, r = Decide(p, s, t0+1500*ms, 1)
	if r.Allowed {
		t.Fatal("over-limit request admitted")
	}
	// After two full idle windows the state is fresh again.
	_, r = Decide(p, s, t0+4*sec, 10)
	if !r.Allowed {
		t.Fatal("state not reset after idle windows")
	}
}

func TestGCRA(t *testing.T) {
	p := NewGCRA(float64(100*ms), 3) // 10/s, burst 3
	run(t, p, []step{
		{0, 1, true, 2, 0},
		{0, 2, true, 0, 0},
		{0, 1, false, 0, 100 * ms},
		{100 * ms, 1, true, 0, 0},
		{150 * ms, 1, false, 0, 50 * ms},
		{10 * sec, 3, true, 0, 0},
	})
}

func TestGCRAEmissionRoundsUp(t *testing.T) {
	p := NewGCRA(float64(sec)/3, 1)
	if p.Emission != 333_333_334 {
		t.Fatalf("emission = %d, want rounded up", p.Emission)
	}
}

func TestExpiresAtIsFresh(t *testing.T) {
	for _, p := range allParams() {
		t.Run(p.Kind.String(), func(t *testing.T) {
			s := Fresh(p, t0)
			s, _ = Decide(p, s, t0+3*ms, 2)
			exp := ExpiresAt(p, s)
			// At expiry the key must admit a full burst, exactly as a
			// fresh key would.
			_, r := Decide(p, s, exp, p.Limit)
			_, fr := Decide(p, Fresh(p, exp), exp, p.Limit)
			if r.Allowed != fr.Allowed || !r.Allowed {
				t.Fatalf("state at ExpiresAt not equivalent to fresh: %+v vs %+v", r, fr)
			}
		})
	}
}

func allParams() []Params {
	return []Params{
		NewTokenBucket(float64(50*ms), 5),
		NewFixedWindow(5, 250*ms),
		NewSlidingWindow(5, 250*ms),
		NewGCRA(float64(50*ms), 5),
	}
}

// TestRetryAfterIsAccurate checks, for random traffic, that a rejected
// request is admitted exactly RetryAfter later (if no other traffic arrives)
// and not meaningfully earlier.
func TestRetryAfterIsAccurate(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, p := range allParams() {
		t.Run(p.Kind.String(), func(t *testing.T) {
			s := Fresh(p, t0)
			now := t0
			checked := 0
			for range 20000 {
				now += rng.Int64N(20 * ms)
				n := 1 + rng.Int64N(p.Limit)
				next, r := Decide(p, s, now, n)
				if r.Allowed {
					s = next
					continue
				}
				checked++
				if r.RetryAfter <= 0 {
					t.Fatalf("rejected with RetryAfter=%d", r.RetryAfter)
				}
				if _, r2 := Decide(p, s, now+r.RetryAfter, n); !r2.Allowed {
					t.Fatalf("not admitted at RetryAfter (%v): state %+v n=%d", time.Duration(r.RetryAfter), s, n)
				}
				// Allow tolerance for float rounding (token bucket
				// epsilon, sliding-window weights).
				tol := int64(tokenEpsilon*p.Interval) + 2 + p.Window/1e6
				if early := now + r.RetryAfter - tol; early > now {
					if _, r3 := Decide(p, s, early, n); r3.Allowed {
						t.Fatalf("admitted %v before RetryAfter", time.Duration(tol))
					}
				}
			}
			if checked == 0 {
				t.Fatal("no rejections exercised")
			}
		})
	}
}

// TestAdmissionBounds drives a greedy client that retries as early as
// possible and verifies documented upper bounds on admitted units in rolling
// intervals.
func TestAdmissionBounds(t *testing.T) {
	const limit = 10
	window := sec
	cases := []struct {
		p Params
		// bound returns the maximum admissible units in any interval of
		// length d.
		bound func(d int64) float64
	}{
		{NewTokenBucket(float64(window)/limit, limit), func(d int64) float64 { return limit + float64(d)*limit/float64(window) + 1e-6 }},
		{NewGCRA(float64(window)/limit, limit), func(d int64) float64 { return limit + float64(d)*limit/float64(window) + 1e-6 }},
		// Fixed window: up to 2x the limit across a boundary.
		{NewFixedWindow(limit, window), func(d int64) float64 {
			if d <= window {
				return 2 * limit
			}
			return float64(limit) * float64(d/window+2)
		}},
		// Sliding window counter: documented worst case < 2x limit in a
		// single window length.
		{NewSlidingWindow(limit, window), func(d int64) float64 {
			if d <= window {
				return 2 * limit
			}
			return float64(limit) * float64(d/window+2)
		}},
	}
	rng := rand.New(rand.NewPCG(3, 4))
	for _, c := range cases {
		t.Run(c.p.Kind.String(), func(t *testing.T) {
			s := Fresh(c.p, t0)
			now := t0
			var admits []int64
			for range 5000 {
				next, r := Decide(c.p, s, now, 1)
				if r.Allowed {
					s = next
					admits = append(admits, now)
					now += rng.Int64N(5 * ms)
				} else {
					now += r.RetryAfter
				}
			}
			for _, d := range []int64{window / 10, window / 2, window, 3 * window} {
				j := 0
				for i := range admits {
					for admits[i]-admits[j] >= d {
						j++
					}
					if cnt := float64(i - j + 1); cnt > c.bound(d) {
						t.Fatalf("%d admitted within %v, bound %.1f", int(cnt), time.Duration(d), c.bound(d))
					}
				}
			}
		})
	}
}

// TestSlidingWindowSmoothsBoundary demonstrates the difference that
// motivates the sliding-window counter: a client that saturates the end of
// one window gets a full fresh limit from a fixed window but not from the
// sliding window.
func TestSlidingWindowSmoothsBoundary(t *testing.T) {
	fw := NewFixedWindow(10, sec)
	sw := NewSlidingWindow(10, sec)
	at := t0 + 999*ms
	fs, _ := Decide(fw, Fresh(fw, at), at, 10)
	ss, _ := Decide(sw, Fresh(sw, at), at, 10)
	_, fr := Decide(fw, fs, t0+sec, 10)
	_, sr := Decide(sw, ss, t0+sec, 10)
	if !fr.Allowed {
		t.Fatal("fixed window should admit a full limit at the boundary")
	}
	if sr.Allowed {
		t.Fatal("sliding window admitted a full limit right after the boundary")
	}
}

func FuzzDecide(f *testing.F) {
	f.Add(uint8(1), int64(5), int64(1000), int64(3), int64(10))
	f.Fuzz(func(t *testing.T, kind uint8, limit, period, n, dt int64) {
		if limit <= 0 || limit > 1e6 || period <= 0 || period > int64(24*time.Hour) || n <= 0 || n > limit {
			t.Skip()
		}
		var p Params
		switch Kind(kind%4 + 1) {
		case TokenBucket:
			p = NewTokenBucket(float64(period)/float64(limit), limit)
		case FixedWindow:
			p = NewFixedWindow(limit, period)
		case SlidingWindow:
			p = NewSlidingWindow(limit, period)
		case GCRA:
			p = NewGCRA(float64(period)/float64(limit), limit)
		}
		s := Fresh(p, t0)
		now := t0
		for range 50 {
			now += dt % period
			next, r := Decide(p, s, now, n)
			if r.Remaining < 0 || r.Remaining > p.Limit || r.RetryAfter < 0 || r.ResetAfter < 0 {
				t.Fatalf("out-of-range result %+v for %+v", r, p)
			}
			if r.Allowed {
				s = next
			}
		}
	})
}

// TestWindowsIgnoreStaleTime checks that a decision timestamped before the
// stored window (a caller that read the clock before a concurrent caller
// crossed a boundary, or a clock step) does not roll the window back and
// reset its count.
func TestWindowsIgnoreStaleTime(t *testing.T) {
	for _, p := range []Params{NewFixedWindow(2, sec), NewSlidingWindow(2, sec)} {
		t.Run(p.Kind.String(), func(t *testing.T) {
			s := Fresh(p, t0)
			s, r := Decide(p, s, t0+sec, 2) // fill the window starting at t0+1s
			if _, r := Decide(p, s, t0+sec-1, 1); r.Allowed {
				t.Fatal("stale timestamp reset the window")
			}
			if s2 := Refund(p, s, t0+sec-1, 1, r.Epoch); s2.B != 1 {
				t.Fatalf("stale refund ignored the current window: %+v", s2)
			}
		})
	}
}

func TestNoOverflowAtExtremeLimits(t *testing.T) {
	const big = int64(math.MaxInt64)
	for _, p := range []Params{
		NewFixedWindow(big, sec),
		NewSlidingWindow(big, sec),
		NewGCRA(1, big),
		NewTokenBucket(1, big),
	} {
		t.Run(p.Kind.String(), func(t *testing.T) {
			s := Fresh(p, t0)
			s, r := Decide(p, s, t0, big)
			if !r.Allowed {
				t.Fatal("full-capacity request rejected")
			}
			if _, r := Decide(p, s, t0, big); r.Allowed {
				t.Fatal("second full-capacity request admitted (overflow)")
			}
			if r.Remaining < 0 || r.RetryAfter < 0 || r.ResetAfter < 0 {
				t.Fatalf("negative values: %+v", r)
			}
		})
	}
}

// TestWindowRefundGoesToChargedWindow covers refunds that arrive after a
// window boundary. Units consumed in one window must never be credited to
// a later window's count: that admits traffic the limit never allowed.
func TestWindowRefundGoesToChargedWindow(t *testing.T) {
	t.Run("sliding window across a boundary", func(t *testing.T) {
		p := NewSlidingWindow(10, sec)
		s := Fresh(p, t0)
		s, long := Decide(p, s, t0+500*ms, 10) // a long-running request
		s, r := Decide(p, s, t0+1900*ms, 9)    // allowed: old window weighs 0.1
		if !r.Allowed {
			t.Fatal("setup: second request rejected")
		}
		s = Refund(p, s, t0+1900*ms, 10, long.Epoch) // the long request fails
		admitted := int64(0)
		for {
			next, r := Decide(p, s, t0+1900*ms, 1)
			if !r.Allowed {
				break
			}
			s, admitted = next, admitted+1
		}
		// Correct: estimate = 0*0.1 + 9 = 9, so exactly 1 more fits.
		if admitted != 1 {
			t.Fatalf("admitted %d after the refund, want 1", admitted)
		}
	})
	t.Run("fixed window rolled by another request", func(t *testing.T) {
		p := NewFixedWindow(2, sec)
		s := Fresh(p, t0)
		s, early := Decide(p, s, t0+999*ms, 1) // consumed in window 1
		s, _ = Decide(p, s, t0+sec, 2)         // another client rolls into window 2 and fills it
		s = Refund(p, s, t0+sec+ms, 1, early.Epoch)
		if _, r := Decide(p, s, t0+sec+ms, 1); r.Allowed {
			t.Fatal("refund of a window-1 unit freed capacity in window 2")
		}
	})
	t.Run("zero epoch is dropped for windows", func(t *testing.T) {
		for _, p := range []Params{NewFixedWindow(2, sec), NewSlidingWindow(2, sec)} {
			s, _ := Decide(p, Fresh(p, t0), t0, 2)
			if s2 := Refund(p, s, t0, 2, 0); s2 != s {
				t.Fatalf("%v: refund without epoch applied", p.Kind)
			}
		}
	})
}

// TestTokenBucketExpiresAtNeverEarly checks, over many realistic
// parameters, that a token-bucket entry is never deemed fresh while it
// still holds less than a full burst.
func TestTokenBucketExpiresAtNeverEarly(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	for range 20000 {
		limit := 1 + rng.Int64N(100_000)
		period := ms + rng.Int64N(int64(time.Hour))
		p := NewTokenBucket(float64(period)/float64(limit), limit)
		s := Fresh(p, t0)
		s, _ = Decide(p, s, t0, 1+rng.Int64N(limit))
		exp := ExpiresAt(p, s)
		if _, r := Decide(p, s, exp, limit); !r.Allowed {
			t.Fatalf("limit %d period %v: state at ExpiresAt rejects a full burst (%+v)", limit, time.Duration(period), r)
		}
	}
}

// TestRetryAfterNeverShort checks that retrying after exactly RetryAfter is
// admitted, across many realistic configurations.
func TestRetryAfterNeverShort(t *testing.T) {
	rng := rand.New(rand.NewPCG(13, 14))
	for range 20000 {
		limit := 1 + rng.Int64N(100_000)
		period := ms + rng.Int64N(int64(time.Hour))
		var p Params
		switch rng.IntN(4) {
		case 0:
			p = NewTokenBucket(float64(period)/float64(limit), limit)
		case 1:
			p = NewGCRA(float64(period)/float64(limit), limit)
		case 2:
			p = NewFixedWindow(limit, period)
		default:
			p = NewSlidingWindow(limit, period)
		}
		s := Fresh(p, t0)
		now := t0 + rng.Int64N(period)
		s, _ = Decide(p, s, now, 1+rng.Int64N(limit))
		now += rng.Int64N(period / 2)
		n := 1 + rng.Int64N(limit)
		_, r := Decide(p, s, now, n)
		if r.Allowed {
			continue
		}
		if _, r2 := Decide(p, s, now+r.RetryAfter, n); !r2.Allowed {
			t.Fatalf("%v limit %d period %v n %d: rejected again at RetryAfter %v (%+v)",
				p.Kind, limit, time.Duration(period), n, time.Duration(r.RetryAfter), r2)
		}
	}
}
