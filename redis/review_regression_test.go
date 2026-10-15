package redis_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/httplimit"
	"github.com/mjahidhasand10/rategen/memory"
	"github.com/mjahidhasand10/rategen/redis"
)

// Lowering a policy's limit keeps its state; Remaining must not go
// negative (it reaches response headers).
func TestLoweredLimitRemainingNotNegative(t *testing.T) {
	s, c := newStore(t, redis.Config{})
	high := must(s.FixedWindow("api", rategen.PerHour(10)))
	allow(t, high, "k", 10)
	s2 := must(redis.New(c, redis.Config{Prefix: s.Prefix(), Failure: rategen.FailClosed, Timeout: 2 * time.Second}))
	low := must(s2.FixedWindow("api", rategen.PerHour(3)))
	if d := allow(t, low, "k", 1); d.Allowed || d.Remaining != 0 {
		t.Fatalf("after lowering the limit: %+v", d)
	}
}

// Units consumed in one window and refunded after the window rolled must
// not free capacity in the new window.
func TestRefundAfterWindowRolled(t *testing.T) {
	s, _ := newStore(t, redis.Config{})
	for _, l := range []*redis.Limiter{
		must(s.FixedWindow("fw", rategen.Rate{Limit: 2, Period: 300 * time.Millisecond})),
		must(s.SlidingWindow("sw", rategen.Rate{Limit: 2, Period: 300 * time.Millisecond})),
	} {
		old := allow(t, l, "k", 1)
		time.Sleep(old.ResetAfter + 20*time.Millisecond) // both windows have passed
		allow(t, l, "k", 2)                              // the new window is full
		if err := l.RefundN(context.Background(), "k", 1, old); err != nil {
			t.Fatal(err)
		}
		if d := allow(t, l, "k", 1); d.Allowed {
			t.Errorf("%s: stale refund freed capacity in a later window", l.Info().Algorithm)
		}
	}
}

// Under FailToLocal, a rejection by the local fallback is a real limit
// verdict: the middleware answers 429 with Retry-After, not 503.
func TestFailToLocalRejectionIs429(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", ContextTimeoutEnabled: true, MaxRetries: -1})
	defer c.Close()
	s := must(redis.New(c, redis.Config{Failure: rategen.FailToLocal, Local: must(memory.New(memory.Config{})), Timeout: 50 * time.Millisecond}))
	l := must(s.FixedWindow("x", rategen.PerHour(1)))
	h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}})).Handler(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	codes := make([]int, 2)
	var retry string
	for i := range codes {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "192.0.2.1:1"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		codes[i], retry = rec.Code, rec.Header().Get("Retry-After")
	}
	if codes[0] != 200 || codes[1] != http.StatusTooManyRequests || retry == "" {
		t.Fatalf("codes %v, Retry-After %q; want [200 429] with Retry-After", codes, retry)
	}
}
