package rategen_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/ratetest"
)

func TestRate(t *testing.T) {
	if r := rategen.PerMinute(120); r.Interval() != float64(500*time.Millisecond) || r.String() != "120/1m0s" {
		t.Fatalf("PerMinute(120): interval %v, string %q", r.Interval(), r)
	}
	if r := rategen.Every(250 * time.Millisecond); r.Limit != 1 || r.Period != 250*time.Millisecond {
		t.Fatalf("Every: %+v", r)
	}
	for _, r := range []rategen.Rate{{}, {Limit: 1}, {Period: time.Second}, {Limit: -1, Period: time.Second}} {
		if err := r.Validate(); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("Validate(%+v) = %v", r, err)
		}
	}
	if err := rategen.PerDay(1).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestFailurePolicyMustBeExplicit(t *testing.T) {
	var zero rategen.FailurePolicy
	if err := zero.Validate(); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Fatalf("zero FailurePolicy validated: %v", err)
	}
	for _, p := range []rategen.FailurePolicy{rategen.FailClosed, rategen.FailOpen, rategen.FailToLocal} {
		if err := p.Validate(); err != nil || p.String() == "unset" {
			t.Fatalf("%v: %v", p, err)
		}
	}
	if err := rategen.FailurePolicy(42).Validate(); err == nil {
		t.Fatal("unknown policy validated")
	}
}

func TestValidatePolicyName(t *testing.T) {
	for _, ok := range []string{"a", "per-user", "api.v1:login_2"} {
		if err := rategen.ValidatePolicyName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "a b", "a\"b", "x{y}", "é", strings.Repeat("a", 65)} {
		if err := rategen.ValidatePolicyName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestResultDecision(t *testing.T) {
	allowed := rategen.Result{Allowed: true, Decisions: []rategen.Decision{
		{Allowed: true, Policy: "a", Remaining: 5},
		{Allowed: true, Policy: "b", Remaining: 1},
	}}
	if d := allowed.Decision(); d.Policy != "b" || !d.Allowed {
		t.Fatalf("allowed summary = %+v", d)
	}
	rejected := rategen.Result{Decisions: []rategen.Decision{
		{Allowed: true, Policy: "a"},
		{Policy: "b", RetryAfter: time.Second},
		{Policy: "c", RetryAfter: time.Minute},
	}}
	if d := rejected.Decision(); d.Policy != "c" || d.Allowed {
		t.Fatalf("rejected summary = %+v", d)
	}
	never := rategen.Result{Decisions: []rategen.Decision{
		{Policy: "a", RetryAfter: time.Hour},
		{Policy: "b", RetryAfter: 0},
	}}
	if d := never.Decision(); d.Policy != "b" {
		t.Fatalf("never-admissible check not preferred: %+v", d)
	}
}

type fakeLimiter struct {
	allowed bool
	err     error
	calls   int
	refunds int
}

func (f *fakeLimiter) AllowN(context.Context, string, int64) (rategen.Decision, error) {
	f.calls++
	return rategen.Decision{Allowed: f.allowed, Policy: "fake"}, f.err
}

type refundingLimiter struct{ fakeLimiter }

func (f *refundingLimiter) RefundN(context.Context, string, int64, rategen.Decision) error {
	f.refunds++
	return nil
}

func TestEvaluateSequential(t *testing.T) {
	ctx := context.Background()
	if _, err := rategen.Evaluate(ctx); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Fatalf("no checks: %v", err)
	}
	if _, err := rategen.Evaluate(ctx, rategen.Check{}); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Fatalf("nil limiter: %v", err)
	}
	a := &refundingLimiter{fakeLimiter{allowed: true}}
	b := &fakeLimiter{allowed: true}
	c := &fakeLimiter{allowed: false}
	d := &fakeLimiter{allowed: true}
	res, err := rategen.Evaluate(ctx,
		rategen.Check{Limiter: a, N: 1}, rategen.Check{Limiter: b, N: 1},
		rategen.Check{Limiter: c, N: 1}, rategen.Check{Limiter: d, N: 1})
	if res.Allowed || res.Atomic || len(res.Decisions) != 3 {
		t.Fatalf("result %+v", res)
	}
	if a.refunds != 1 {
		t.Fatal("refundable limiter not refunded")
	}
	if !errors.Is(err, rategen.ErrUnsupported) {
		t.Fatalf("non-refundable limiter not reported: %v", err)
	}
	if d.calls != 0 {
		t.Fatal("evaluation continued after rejection")
	}
}

func TestEvaluateFailOpenErrorIsReported(t *testing.T) {
	boom := errors.New("boom")
	l := &fakeLimiter{allowed: true, err: boom}
	res, err := rategen.Evaluate(context.Background(), rategen.Check{Limiter: l, N: 1}, rategen.Check{Limiter: &fakeLimiter{allowed: true}, N: 1})
	if !res.Allowed || !errors.Is(err, boom) {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestObserveAndSlog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	l := rategen.Observe(&fakeLimiter{allowed: false}, rategen.Observers(rategen.SlogObserver(logger)))
	if _, err := l.AllowN(context.Background(), "user@example.com", 1); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "rate limit rejected request") {
		t.Fatalf("rejection not logged: %q", out)
	}
	if strings.Contains(out, "user@example.com") {
		t.Fatal("log exposes the key")
	}
	buf.Reset()
	ok := rategen.Observe(&fakeLimiter{allowed: true}, rategen.SlogObserver(logger))
	ok.AllowN(context.Background(), "k", 1)
	if buf.Len() != 0 {
		t.Fatalf("allowed decision logged: %q", buf.String())
	}
}

func TestMonotonicIgnoresWallSteps(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := ratetest.NewFakeClock(start)
	m := rategen.NewMonotonic(clk)
	if m.Nanos() != start.UnixNano() {
		t.Fatal("timeline not aligned to the epoch at creation")
	}
	clk.Advance(time.Second)
	if got := m.Nanos() - start.UnixNano(); got != int64(time.Second) {
		t.Fatalf("advanced %v", time.Duration(got))
	}
	// With the real clock, time.Time carries a monotonic reading, so the
	// timeline only moves forward.
	rm := rategen.NewMonotonic(nil)
	a := rm.Nanos()
	if b := rm.Nanos(); b < a {
		t.Fatal("system timeline went backwards")
	}
}
