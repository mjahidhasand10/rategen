package memory_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
	"github.com/mjahidhasand10/rategen/ratetest"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newStore(t testing.TB, cfg memory.Config) (*memory.Store, *ratetest.FakeClock) {
	t.Helper()
	clk := ratetest.NewFakeClock(epoch)
	if cfg.Clock == nil {
		cfg.Clock = clk
	}
	s, err := memory.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s, clk
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func allow(t testing.TB, l rategen.Limiter, key string, n int64) rategen.Decision {
	t.Helper()
	d, err := l.AllowN(context.Background(), key, n)
	if err != nil {
		t.Fatalf("AllowN(%q, %d): %v", key, n, err)
	}
	return d
}

// limiters returns one limiter per algorithm, each admitting 5 per second.
func limiters(t testing.TB, s *memory.Store) map[string]rategen.Limiter {
	return map[string]rategen.Limiter{
		"token_bucket":   must(s.TokenBucket("tb", rategen.PerSecond(5), 5)),
		"gcra":           must(s.GCRA("gcra", rategen.PerSecond(5), 5)),
		"fixed_window":   must(s.FixedWindow("fw", rategen.PerSecond(5))),
		"sliding_window": must(s.SlidingWindow("sw", rategen.PerSecond(5))),
	}
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []memory.Config{
		{MaxKeys: -1},
		{Shards: 3},
		{Shards: -4},
		{OnFull: memory.FullPolicy(9)},
	} {
		if _, err := memory.New(cfg); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("New(%+v) = %v, want ErrInvalidConfig", cfg, err)
		}
	}
	s, _ := newStore(t, memory.Config{})
	if _, err := s.FixedWindow("a", rategen.PerSecond(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GCRA("a", rategen.PerSecond(1), 1); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Errorf("duplicate policy name accepted: %v", err)
	}
	for _, name := range []string{"", "has space", "bad{tag}", string(make([]byte, 65))} {
		if _, err := s.FixedWindow(name, rategen.PerSecond(1)); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("policy name %q accepted", name)
		}
	}
	if _, err := s.TokenBucket("tb", rategen.PerSecond(1), 0); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Errorf("zero burst accepted: %v", err)
	}
	if _, err := s.SlidingWindow("sw", rategen.Rate{}); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Errorf("zero rate accepted: %v", err)
	}
}

func TestAlgorithmsThroughStore(t *testing.T) {
	s, clk := newStore(t, memory.Config{})
	for name, l := range limiters(t, s) {
		t.Run(name, func(t *testing.T) {
			for i := range 5 {
				if d := allow(t, l, "k", 1); !d.Allowed || d.Remaining != int64(4-i) {
					t.Fatalf("request %d: %+v", i, d)
				}
			}
			d := allow(t, l, "k", 1)
			if d.Allowed || d.RetryAfter <= 0 || d.Limit != 5 || d.Policy == "" {
				t.Fatalf("over-limit decision: %+v", d)
			}
			// A different key is independent.
			if d := allow(t, l, "other", 5); !d.Allowed {
				t.Fatalf("independent key rejected: %+v", d)
			}
		})
	}
	clk.Advance(3 * time.Second)
	for name, l := range limiters2(t, s) {
		if d := allow(t, l, "k", 5); !d.Allowed {
			t.Errorf("%s: not reset after idle period: %+v", name, d)
		}
	}
}

// limiters2 fetches the same policies again under new names, to show
// policies are isolated from each other for the same key.
func limiters2(t testing.TB, s *memory.Store) map[string]rategen.Limiter {
	return map[string]rategen.Limiter{
		"token_bucket": must(s.TokenBucket("tb2", rategen.PerSecond(5), 5)),
		"gcra":         must(s.GCRA("gcra2", rategen.PerSecond(5), 5)),
	}
}

func TestCostValidation(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.FixedWindow("fw", rategen.PerMinute(3)))
	ctx := context.Background()
	if d, err := l.AllowN(ctx, "k", 0); !errors.Is(err, rategen.ErrInvalidCost) || d.Allowed {
		t.Fatalf("n=0: %+v, %v", d, err)
	}
	if d, err := l.AllowN(ctx, "k", 4); !errors.Is(err, rategen.ErrExceedsCapacity) || d.Allowed || d.RetryAfter != 0 {
		t.Fatalf("n>limit: %+v, %v", d, err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if d, err := l.AllowN(cctx, "k", 1); !errors.Is(err, context.Canceled) || d.Allowed {
		t.Fatalf("canceled ctx: %+v, %v", d, err)
	}
	if d := allow(t, l, "k", 3); !d.Allowed {
		t.Fatal("invalid requests consumed capacity")
	}
}

func TestExpiryAndSweep(t *testing.T) {
	s, clk := newStore(t, memory.Config{})
	l := must(s.GCRA("g", rategen.PerSecond(10), 10))
	for i := range 100 {
		allow(t, l, strconv.Itoa(i), 1)
	}
	if got := s.Stats().Keys; got != 100 {
		t.Fatalf("keys = %d, want 100", got)
	}
	clk.Advance(50 * time.Millisecond)
	if n := s.Sweep(context.Background()); n != 0 {
		t.Fatalf("swept %d live entries", n)
	}
	clk.Advance(100 * time.Millisecond)
	if n := s.Sweep(context.Background()); n != 100 {
		t.Fatalf("swept %d, want 100", n)
	}
	if st := s.Stats(); st.Keys != 0 || st.Expired != 100 {
		t.Fatalf("stats after sweep: %+v", st)
	}
}

func TestRunJanitor(t *testing.T) {
	s, clk := newStore(t, memory.Config{})
	l := must(s.FixedWindow("fw", rategen.PerSecond(1)))
	allow(t, l, "k", 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.RunJanitor(ctx, time.Minute) }()
	if err := clk.WaitForTimers(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	if err := clk.WaitForTimers(t.Context(), 2); err != nil { // swept and re-armed
		t.Fatal(err)
	}
	if got := s.Stats().Keys; got != 0 {
		t.Fatalf("janitor left %d keys", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunJanitor = %v", err)
	}
	if err := s.RunJanitor(t.Context(), 0); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Fatalf("zero interval: %v", err)
	}
}

func TestBoundedEvictLRU(t *testing.T) {
	s, _ := newStore(t, memory.Config{MaxKeys: 64, Shards: 4})
	l := must(s.FixedWindow("fw", rategen.PerHour(1)))
	for i := range 10_000 {
		allow(t, l, "attacker-"+strconv.Itoa(i), 1)
	}
	st := s.Stats()
	if st.Keys > 64 {
		t.Fatalf("store holds %d keys, bound is 64", st.Keys)
	}
	if st.Evictions < 10_000-64 {
		t.Fatalf("evictions = %d", st.Evictions)
	}
}

func TestBoundedRejectNew(t *testing.T) {
	s, clk := newStore(t, memory.Config{MaxKeys: 8, Shards: 1, OnFull: memory.RejectNew})
	l := must(s.FixedWindow("fw", rategen.PerSecond(1)))
	for i := range 8 {
		allow(t, l, strconv.Itoa(i), 1)
	}
	d, err := l.AllowN(context.Background(), "new", 1)
	if !errors.Is(err, rategen.ErrStoreFull) || d.Allowed {
		t.Fatalf("new key in full store: %+v, %v", d, err)
	}
	// Existing keys keep their state.
	if d := allow(t, l, "0", 1); d.Allowed {
		t.Fatal("existing key's state was lost")
	}
	// Once entries expire, they make room.
	clk.Advance(time.Second)
	if d := allow(t, l, "new", 1); !d.Allowed {
		t.Fatal("expired entries did not make room")
	}
}

func TestReserveWaitCancel(t *testing.T) {
	s, clk := newStore(t, memory.Config{})
	for _, l := range []*memory.ReservableLimiter{
		must(s.TokenBucket("tb", rategen.PerSecond(1), 1)),
		must(s.GCRA("g", rategen.PerSecond(1), 1)),
	} {
		t.Run(l.Info().Algorithm, func(t *testing.T) {
			ctx := t.Context()
			r := must(l.ReserveN(ctx, "k", 1, -1))
			if !r.OK() || r.Delay() != 0 {
				t.Fatalf("first reservation: %v %v", r.OK(), r.Delay())
			}
			r2 := must(l.ReserveN(ctx, "k", 1, -1))
			if !r2.OK() || r2.Delay() != time.Second {
				t.Fatalf("second reservation delay = %v", r2.Delay())
			}
			if r3 := must(l.ReserveN(ctx, "k", 1, time.Second)); r3.OK() {
				t.Fatal("reservation beyond maxWait granted")
			}
			r2.Cancel()
			// After cancel, the next reservation waits only for the
			// first one's debt.
			if r4 := must(l.ReserveN(ctx, "k", 1, -1)); r4.Delay() != time.Second {
				t.Fatalf("delay after cancel = %v, want 1s", r4.Delay())
			}

			done := make(chan error, 1)
			before := clk.PendingTimers()
			go func() { done <- l.WaitN(ctx, "w", 1) }()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			go func() { done <- l.WaitN(ctx, "w", 1) }()
			for clk.PendingTimers() == before {
				if err := clk.WaitForTimers(ctx, 1); err != nil {
					t.Fatal(err)
				}
			}
			clk.Advance(time.Second)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			dctx, cancel := context.WithDeadline(ctx, clk.Now().Add(time.Millisecond))
			defer cancel()
			if err := l.WaitN(dctx, "w", 1); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("WaitN past deadline = %v", err)
			}
		})
	}
}

func TestBatchAtomic(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	perSec := must(s.TokenBucket("per-sec", rategen.PerSecond(10), 10))
	daily := must(s.FixedWindow("daily", rategen.PerDay(3)))
	ctx := context.Background()
	checks := []rategen.Check{{Limiter: perSec, Key: "u", N: 1}, {Limiter: daily, Key: "u", N: 1}}
	for i := range 3 {
		res, err := rategen.Evaluate(ctx, checks...)
		if err != nil || !res.Allowed || !res.Atomic {
			t.Fatalf("request %d: %+v %v", i, res, err)
		}
	}
	res, err := rategen.Evaluate(ctx, checks...)
	if err != nil || res.Allowed || !res.Atomic {
		t.Fatalf("4th request: %+v, %v", res, err)
	}
	if !res.Decisions[0].Allowed || res.Decisions[0].Remaining != 7 {
		t.Fatalf("passing check reported %+v, want unconsumed remaining 7", res.Decisions[0])
	}
	if res.Decision().Policy != "daily" {
		t.Fatalf("summary decision = %+v", res.Decision())
	}
	// The per-second bucket was not consumed by the rejected request.
	if d := allow(t, perSec, "u", 7); !d.Allowed {
		t.Fatalf("rejected batch consumed per-second capacity: %+v", d)
	}
}

func TestBatchSameEntryTwice(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.FixedWindow("fw", rategen.PerMinute(5)))
	res, err := rategen.Evaluate(context.Background(),
		rategen.Check{Limiter: l, Key: "k", N: 3},
		rategen.Check{Limiter: l, Key: "k", N: 3})
	if err != nil || res.Allowed {
		t.Fatalf("3+3 > 5 admitted: %+v %v", res, err)
	}
	res, _ = rategen.Evaluate(context.Background(),
		rategen.Check{Limiter: l, Key: "k", N: 2},
		rategen.Check{Limiter: l, Key: "k", N: 3})
	if !res.Allowed || res.Decisions[1].Remaining != 0 {
		t.Fatalf("2+3 = 5: %+v", res)
	}
	if d := allow(t, l, "k", 1); d.Allowed {
		t.Fatal("duplicate checks were not both consumed")
	}
}

func TestBatchRejectNewIsAllOrNothing(t *testing.T) {
	s, _ := newStore(t, memory.Config{MaxKeys: 2, Shards: 1, OnFull: memory.RejectNew})
	a := must(s.FixedWindow("a", rategen.PerMinute(5)))
	b := must(s.FixedWindow("b", rategen.PerMinute(5)))
	allow(t, a, "x", 1)
	_, err := rategen.Evaluate(context.Background(),
		rategen.Check{Limiter: a, Key: "y", N: 1},
		rategen.Check{Limiter: b, Key: "y", N: 1})
	if !errors.Is(err, rategen.ErrStoreFull) {
		t.Fatalf("err = %v, want ErrStoreFull", err)
	}
	if got := s.Stats().Keys; got != 1 {
		t.Fatalf("partial batch written: %d keys", got)
	}
}

func TestBatchCostErrors(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	a := must(s.FixedWindow("a", rategen.PerMinute(5)))
	b := must(s.FixedWindow("b", rategen.PerMinute(2)))
	res, err := rategen.Evaluate(context.Background(),
		rategen.Check{Limiter: a, Key: "k", N: 3},
		rategen.Check{Limiter: b, Key: "k", N: 3})
	if !errors.Is(err, rategen.ErrExceedsCapacity) || res.Allowed {
		t.Fatalf("%+v, %v", res, err)
	}
	if d := allow(t, a, "k", 5); !d.Allowed {
		t.Fatal("invalid batch consumed capacity")
	}
}

func TestEvaluateAcrossStoresRefunds(t *testing.T) {
	s1, _ := newStore(t, memory.Config{})
	s2, _ := newStore(t, memory.Config{})
	a := must(s1.FixedWindow("a", rategen.PerMinute(5)))
	b := must(s2.FixedWindow("b", rategen.PerMinute(1)))
	ctx := context.Background()
	allow(t, b, "k", 1)
	res, err := rategen.Evaluate(ctx, rategen.Check{Limiter: a, Key: "k", N: 2}, rategen.Check{Limiter: b, Key: "k", N: 1})
	if err != nil || res.Allowed || res.Atomic {
		t.Fatalf("%+v %v", res, err)
	}
	if d := allow(t, a, "k", 5); !d.Allowed {
		t.Fatal("non-atomic path did not refund the first limiter")
	}
	if _, err := rategen.EvaluateAtomic(ctx, rategen.Check{Limiter: a, Key: "k", N: 1}, rategen.Check{Limiter: b, Key: "k", N: 1}); !errors.Is(err, rategen.ErrNotAtomic) {
		t.Fatalf("EvaluateAtomic across stores = %v", err)
	}
}

func TestRefund(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	for name, l := range limiters(t, s) {
		d := allow(t, l, "k", 5)
		if err := l.(rategen.Refunder).RefundN(context.Background(), "k", 100, d); err != nil {
			t.Fatal(err)
		}
		if d := allow(t, l, "k", 5); !d.Allowed {
			t.Errorf("%s: refund did not restore capacity", name)
		}
		if d := allow(t, l, "k", 1); d.Allowed {
			t.Errorf("%s: refund exceeded capacity", name)
		}
	}
}

func TestObserver(t *testing.T) {
	var mu sync.Mutex
	var events []rategen.Event
	obs := rategen.ObserverFunc(func(_ context.Context, e rategen.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	s, _ := newStore(t, memory.Config{Observer: obs})
	l := must(s.FixedWindow("fw", rategen.PerMinute(1)))
	allow(t, l, "secret-user-id", 1)
	allow(t, l, "secret-user-id", 1)
	if len(events) != 2 || !events[0].Decision.Allowed || events[1].Decision.Allowed {
		t.Fatalf("events: %+v", events)
	}
	if e := events[0]; e.Info.Algorithm != "fixed_window" || e.Info.Backend != "memory" || e.Info.Policy != "fw" {
		t.Fatalf("info: %+v", e.Info)
	}
	if s := fmt.Sprintf("%+v", events); strings.Contains(s, "secret-user-id") {
		t.Fatal("event exposes the key")
	}
}

func TestConcurrentSameKeyExact(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	for name, l := range limiters(t, s) {
		t.Run(name, func(t *testing.T) {
			var admitted atomic.Int64
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() {
					for range 50 {
						if d, _ := l.AllowN(context.Background(), "hot", 1); d.Allowed {
							admitted.Add(1)
						}
					}
				})
			}
			wg.Wait()
			if got := admitted.Load(); got != 5 {
				t.Fatalf("admitted %d with a frozen clock, want 5", got)
			}
		})
	}
}

func TestConcurrentMixedOperations(t *testing.T) {
	// Real clock: exercises lazy expiry, eviction, sweeping and batches
	// concurrently under the race detector.
	s, err := memory.New(memory.Config{MaxKeys: 256, Shards: 8})
	if err != nil {
		t.Fatal(err)
	}
	tb := must(s.TokenBucket("tb", rategen.PerSecond(1000), 5))
	fw := must(s.FixedWindow("fw", rategen.Rate{Limit: 5, Period: time.Millisecond}))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go s.RunJanitor(ctx, time.Millisecond)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 500 {
				key := strconv.Itoa((g * 31 * i) % 1000)
				tb.AllowN(ctx, key, 1)
				fw.AllowN(ctx, key, 2)
				rategen.Evaluate(ctx, rategen.Check{Limiter: tb, Key: key, N: 1}, rategen.Check{Limiter: fw, Key: key, N: 1})
				if r, _ := tb.ReserveN(ctx, key, 1, time.Millisecond); r != nil {
					r.Cancel()
				}
				tb.RefundN(ctx, key, 1, rategen.Decision{})
				if i%100 == 0 {
					s.Sweep(ctx)
					_ = s.Stats()
				}
			}
		})
	}
	wg.Wait()
	if k := s.Stats().Keys; k > 256 {
		t.Fatalf("store exceeded bound: %d", k)
	}
}

func TestMaxKeysIsExact(t *testing.T) {
	// These bounds do not divide evenly over 64 shards; the bound must
	// still be exact, not rounded up per shard.
	for _, maxKeys := range []int{1000, 5000, 63, 65} {
		s, _ := newStore(t, memory.Config{MaxKeys: maxKeys})
		l := must(s.FixedWindow("fw", rategen.PerHour(1)))
		for i := range maxKeys * 20 {
			allow(t, l, strconv.Itoa(i), 1)
		}
		if k := s.Stats().Keys; k > maxKeys {
			t.Fatalf("MaxKeys %d: store holds %d keys", maxKeys, k)
		}
	}
}

// TestFullStoreReclaimsExpiredAwayFromTail: policies with different
// lifetimes share a store, so expired entries need not be near the LRU
// tail. A full store must still find them before rejecting new keys.
func TestFullStoreReclaimsExpiredAwayFromTail(t *testing.T) {
	for _, policy := range []memory.FullPolicy{memory.RejectNew, memory.EvictLRU} {
		s, clk := newStore(t, memory.Config{MaxKeys: 10, Shards: 1, OnFull: policy})
		daily := must(s.FixedWindow("daily", rategen.PerDay(5)))
		short := must(s.TokenBucket("short", rategen.PerSecond(10), 10))
		for i := range 9 {
			allow(t, daily, "d"+strconv.Itoa(i), 1) // long-lived, at the LRU tail
		}
		allow(t, short, "s", 1) // short-lived, at the head
		clk.Advance(2 * time.Second)
		d, err := daily.AllowN(context.Background(), "new", 1)
		if err != nil || !d.Allowed {
			t.Fatalf("policy %d: new key refused although an entry had expired: %+v %v", policy, d, err)
		}
		if st := s.Stats(); st.Evictions != 0 {
			t.Fatalf("policy %d: live keys evicted while expired ones existed: %+v", policy, st)
		}
	}
}

// TestImpossibleBatchEvictsNothing: a batch that cannot fit even after
// evicting everything else must fail before evicting (and so resetting)
// other keys.
func TestImpossibleBatchEvictsNothing(t *testing.T) {
	s, _ := newStore(t, memory.Config{MaxKeys: 2, Shards: 1})
	l := must(s.FixedWindow("fw", rategen.PerMinute(1)))
	allow(t, l, "victim", 1)
	_, err := rategen.EvaluateAtomic(context.Background(),
		rategen.Check{Limiter: l, Key: "a", N: 1},
		rategen.Check{Limiter: l, Key: "b", N: 1},
		rategen.Check{Limiter: l, Key: "c", N: 1})
	if !errors.Is(err, rategen.ErrStoreFull) {
		t.Fatalf("err = %v, want ErrStoreFull", err)
	}
	if st := s.Stats(); st.Evictions != 0 {
		t.Fatalf("failed batch evicted keys: %+v", st)
	}
	if d := allow(t, l, "victim", 1); d.Allowed {
		t.Fatal("victim's limit was reset by a failed batch")
	}
}
