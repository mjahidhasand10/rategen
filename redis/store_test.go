package redis_test

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
	"github.com/mjahidhasand10/rategen/redis"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// redisAddr returns the integration Redis address or skips the test.
func redisAddr(t testing.TB) string {
	addr := os.Getenv("RATEGEN_REDIS_ADDR")
	if addr == "" {
		t.Skip("RATEGEN_REDIS_ADDR not set; run scripts/redis-up.sh to start Redis")
	}
	return addr
}

func newClient(t testing.TB, addr string) *goredis.Client {
	c := goredis.NewClient(&goredis.Options{Addr: addr, ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { c.Close() })
	return c
}

// prefix returns a unique key prefix and deletes its keys after the test.
func prefix(t testing.TB, c *goredis.Client) string {
	var b [6]byte
	crand.Read(b[:])
	p := "rgtest-" + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx := context.Background()
		iter := c.Scan(ctx, 0, p+":*", 1000).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
	})
	return p
}

func newStore(t testing.TB, cfg redis.Config) (*redis.Store, *goredis.Client) {
	c := newClient(t, redisAddr(t))
	if cfg.Prefix == "" {
		cfg.Prefix = prefix(t, c)
	}
	if cfg.Failure == 0 {
		cfg.Failure = rategen.FailClosed
	}
	if cfg.Timeout == 0 {
		// Generous, so that a loaded CI machine does not turn into store
		// failures; timeout behaviour itself is tested in
		// TestOutageAndRecovery.
		cfg.Timeout = 2 * time.Second
	}
	return must(redis.New(c, cfg)), c
}

func TestConfigValidation(t *testing.T) {
	ok := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", ContextTimeoutEnabled: true})
	defer ok.Close()
	noCtx := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer noCtx.Close()
	local := must(memory.New(memory.Config{}))
	cases := map[string]struct {
		c   goredis.UniversalClient
		cfg redis.Config
	}{
		"nil client":            {nil, redis.Config{Failure: rategen.FailClosed}},
		"no failure policy":     {ok, redis.Config{}},
		"local without store":   {ok, redis.Config{Failure: rategen.FailToLocal}},
		"store without local":   {ok, redis.Config{Failure: rategen.FailOpen, Local: local}},
		"no context timeouts":   {noCtx, redis.Config{Failure: rategen.FailClosed}},
		"bad prefix":            {ok, redis.Config{Failure: rategen.FailClosed, Prefix: "a{b}"}},
		"negative timeout":      {ok, redis.Config{Failure: rategen.FailClosed, Timeout: -1}},
		"unknown failure value": {ok, redis.Config{Failure: rategen.FailurePolicy(99)}},
	}
	for name, tc := range cases {
		if _, err := redis.New(tc.c, tc.cfg); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidConfig", name, err)
		}
	}
	s := must(redis.New(ok, redis.Config{Failure: rategen.FailClosed}))
	must(s.GCRA("a", rategen.PerSecond(1), 1))
	if _, err := s.FixedWindow("a", rategen.PerSecond(1)); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Error("duplicate policy accepted")
	}
	if _, err := s.FixedWindow("tiny", rategen.Rate{Limit: 1, Period: time.Microsecond}); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Error("sub-millisecond window accepted")
	}
	if _, err := s.TokenBucket("tb", rategen.PerSecond(1), 0); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Error("zero burst accepted")
	}
	if _, err := s.GCRA("huge", rategen.PerSecond(1), redis.MaxLimit+1); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Error("burst above 2^50 accepted")
	}
	if _, err := s.FixedWindow("hugew", rategen.PerSecond(redis.MaxLimit+1)); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Error("limit above 2^50 accepted")
	}
	if _, err := s.TokenBucket("slow", rategen.Every(200*365*24*time.Hour), 2); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Error("refill over 100 years accepted")
	}
}

func TestKeyFormat(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", ContextTimeoutEnabled: true})
	defer c.Close()
	s := must(redis.New(c, redis.Config{Failure: rategen.FailClosed, Prefix: "app"}))
	l := must(s.FixedWindow("login", rategen.PerMinute(5)))
	k := l.Key("alice@example.com")
	if strings.Contains(k, "alice") || !strings.HasPrefix(k, "app:{") || !strings.HasSuffix(k, "}:login:fw") {
		t.Fatalf("key %q", k)
	}
	if l.Key("alice@example.com") != k || l.Key("bob") == k {
		t.Fatal("key derivation is not a function of the identity")
	}
}

func allow(t testing.TB, l rategen.Limiter, key string, n int64) rategen.Decision {
	t.Helper()
	d, err := l.AllowN(context.Background(), key, n)
	if err != nil {
		t.Fatalf("AllowN: %v", err)
	}
	return d
}

func TestAlgorithms(t *testing.T) {
	s, _ := newStore(t, redis.Config{})
	ls := map[string]rategen.Limiter{
		"token_bucket":   must(s.TokenBucket("tb", rategen.PerHour(5), 5)),
		"gcra":           must(s.GCRA("gc", rategen.PerHour(5), 5)),
		"fixed_window":   must(s.FixedWindow("fw", rategen.PerHour(5))),
		"sliding_window": must(s.SlidingWindow("sw", rategen.PerHour(5))),
	}
	for name, l := range ls {
		t.Run(name, func(t *testing.T) {
			for i := range 5 {
				if d := allow(t, l, "k", 1); !d.Allowed || d.Remaining != int64(4-i) {
					t.Fatalf("request %d: %+v", i, d)
				}
			}
			d := allow(t, l, "k", 1)
			// A sliding window can ask for up to two windows: the count
			// moves into the "previous" slot and decays over the next one.
			if d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 2*time.Hour || d.Policy == "" || d.Limit != 5 {
				t.Fatalf("over limit: %+v", d)
			}
			if d := allow(t, l, "other", 5); !d.Allowed {
				t.Fatal("keys not isolated")
			}
			if _, err := l.AllowN(context.Background(), "k", 6); !errors.Is(err, rategen.ErrExceedsCapacity) {
				t.Fatalf("n > limit: %v", err)
			}
		})
	}
}

// TestParityWithMemory replays the same random burst against Redis and
// the memory store. Rates are slow enough (per day) that the few
// milliseconds between calls do not refill anything, so decisions must
// match exactly.
func TestParityWithMemory(t *testing.T) {
	rs, _ := newStore(t, redis.Config{})
	ms := must(memory.New(memory.Config{}))
	type pair struct{ r, m rategen.Limiter }
	pairs := map[string]pair{
		"token_bucket":   {must(rs.TokenBucket("tb", rategen.PerDay(50), 20)), must(ms.TokenBucket("tb", rategen.PerDay(50), 20))},
		"gcra":           {must(rs.GCRA("gc", rategen.PerDay(50), 20)), must(ms.GCRA("gc", rategen.PerDay(50), 20))},
		"fixed_window":   {must(rs.FixedWindow("fw", rategen.PerDay(20))), must(ms.FixedWindow("fw", rategen.PerDay(20)))},
		"sliding_window": {must(rs.SlidingWindow("sw", rategen.PerDay(20))), must(ms.SlidingWindow("sw", rategen.PerDay(20)))},
	}
	rng := rand.New(rand.NewPCG(7, 8))
	for name, p := range pairs {
		t.Run(name, func(t *testing.T) {
			if name == "sliding_window" || name == "fixed_window" {
				if now := time.Now().UTC(); now.Hour() == 23 && now.Minute() == 59 {
					t.Skip("too close to the daily window boundary")
				}
			}
			for i := range 40 {
				n := 1 + rng.Int64N(4)
				rd := allow(t, p.r, "k", n)
				md := allow(t, p.m, "k", n)
				if rd.Allowed != md.Allowed || rd.Remaining != md.Remaining {
					t.Fatalf("step %d n=%d: redis %+v, memory %+v", i, n, rd, md)
				}
			}
		})
	}
}

// TestDistributedInstances runs several independent clients and stores (as
// separate application instances would) against one key and checks that
// the global limit holds exactly.
func TestDistributedInstances(t *testing.T) {
	addr := redisAddr(t)
	pfx := prefix(t, newClient(t, addr))
	const instances, goroutines, attempts, limit = 6, 8, 25, 137
	for _, algo := range []string{"fixed_window", "gcra", "token_bucket", "sliding_window"} {
		t.Run(algo, func(t *testing.T) {
			var admitted atomic.Int64
			var wg sync.WaitGroup
			for range instances {
				c := goredis.NewClient(&goredis.Options{Addr: addr, ContextTimeoutEnabled: true, PoolSize: goroutines})
				defer c.Close()
				s := must(redis.New(c, redis.Config{Prefix: pfx, Failure: rategen.FailClosed, Timeout: 5 * time.Second}))
				var l rategen.Limiter
				switch algo {
				case "fixed_window":
					l = must(s.FixedWindow("p", rategen.PerDay(limit)))
				case "sliding_window":
					l = must(s.SlidingWindow("p", rategen.PerDay(limit)))
				case "gcra":
					l = must(s.GCRA("p", rategen.PerDay(limit), limit))
				case "token_bucket":
					l = must(s.TokenBucket("p", rategen.PerDay(limit), limit))
				}
				for range goroutines {
					wg.Go(func() {
						for range attempts {
							d, err := l.AllowN(context.Background(), "shared", 1)
							if err != nil {
								t.Error(err)
								return
							}
							if d.Allowed {
								admitted.Add(1)
							}
						}
					})
				}
			}
			wg.Wait()
			// Token bucket and GCRA may refill a fraction of a token
			// during the test at 137/day; that is far below one.
			if got := admitted.Load(); got != limit {
				t.Fatalf("admitted %d across %d instances, want exactly %d", got, instances, limit)
			}
		})
	}
}

func TestBatchAtomic(t *testing.T) {
	s, _ := newStore(t, redis.Config{})
	burst := must(s.TokenBucket("burst", rategen.PerHour(100), 100))
	daily := must(s.FixedWindow("daily", rategen.PerDay(3)))
	ctx := context.Background()
	checks := []rategen.Check{{Limiter: burst, Key: "u", N: 2}, {Limiter: daily, Key: "u", N: 1}}
	for range 3 {
		res, err := rategen.Evaluate(ctx, checks...)
		if err != nil || !res.Allowed || !res.Atomic {
			t.Fatalf("%+v %v", res, err)
		}
	}
	res, err := rategen.Evaluate(ctx, checks...)
	if err != nil || res.Allowed || !res.Atomic {
		t.Fatalf("over daily quota: %+v %v", res, err)
	}
	if d := res.Decisions[0]; !d.Allowed || d.Remaining != 94 {
		t.Fatalf("unconsumed check reported %+v", d)
	}
	if d := allow(t, burst, "u", 94); !d.Allowed {
		t.Fatal("rejected batch consumed burst capacity")
	}
	// Same key twice in one batch: both apply, in order.
	fw := must(s.FixedWindow("dup", rategen.PerHour(5)))
	res, _ = rategen.Evaluate(ctx, rategen.Check{Limiter: fw, Key: "k", N: 3}, rategen.Check{Limiter: fw, Key: "k", N: 3})
	if res.Allowed {
		t.Fatal("3+3 > 5 admitted")
	}
	res, _ = rategen.Evaluate(ctx, rategen.Check{Limiter: fw, Key: "k", N: 2}, rategen.Check{Limiter: fw, Key: "k", N: 3})
	if !res.Allowed || allow(t, fw, "k", 1).Allowed {
		t.Fatal("duplicate checks not both consumed")
	}
}

func TestClusterModeFallsBackForMixedIdentities(t *testing.T) {
	s, _ := newStore(t, redis.Config{Cluster: true})
	perIP := must(s.FixedWindow("ip", rategen.PerHour(10)))
	global := must(s.FixedWindow("global", rategen.PerHour(1)))
	ctx := context.Background()
	res, err := rategen.Evaluate(ctx, rategen.Check{Limiter: perIP, Key: "1.2.3.4", N: 1}, rategen.Check{Limiter: global, Key: "all", N: 1})
	if err != nil || !res.Allowed || res.Atomic {
		t.Fatalf("%+v %v", res, err)
	}
	res, _ = rategen.Evaluate(ctx, rategen.Check{Limiter: perIP, Key: "1.2.3.4", N: 1}, rategen.Check{Limiter: global, Key: "all", N: 1})
	if res.Allowed || res.Atomic {
		t.Fatalf("%+v", res)
	}
	// The refund restored the per-IP unit consumed by the rejected request.
	if d := allow(t, perIP, "1.2.3.4", 9); !d.Allowed {
		t.Fatal("non-atomic path did not refund")
	}
	if _, err := rategen.EvaluateAtomic(ctx, rategen.Check{Limiter: perIP, Key: "a", N: 1}, rategen.Check{Limiter: global, Key: "b", N: 1}); !errors.Is(err, rategen.ErrNotAtomic) {
		t.Fatalf("EvaluateAtomic = %v", err)
	}
	// Same identity in cluster mode stays atomic.
	other := must(s.FixedWindow("other", rategen.PerHour(5)))
	if res, _ := rategen.Evaluate(ctx, rategen.Check{Limiter: perIP, Key: "u", N: 1}, rategen.Check{Limiter: other, Key: "u", N: 1}); !res.Atomic {
		t.Fatal("same-identity batch not atomic in cluster mode")
	}
}

func TestExpiryAndIsolation(t *testing.T) {
	s, c := newStore(t, redis.Config{})
	fw := must(s.FixedWindow("fw", rategen.Rate{Limit: 1, Period: time.Second}))
	quota := must(s.FixedWindow("quota", rategen.PerMinute(5)))
	gc := must(s.GCRA("gc", rategen.PerMinute(60), 3))
	ctx := context.Background()
	allow(t, quota, "secret-identity", 1)
	allow(t, gc, "secret-identity", 1)
	keys := must(c.Keys(ctx, "*secret-identity*").Result())
	if len(keys) != 0 {
		t.Fatalf("raw identity stored in Redis: %v", keys)
	}
	if ttl := must(c.PTTL(ctx, quota.Key("secret-identity")).Result()); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("fixed-window TTL = %v, want within the window", ttl)
	}
	if ttl := must(c.PTTL(ctx, gc.Key("secret-identity")).Result()); ttl <= 0 || ttl > time.Second {
		t.Fatalf("GCRA TTL = %v, want <= one emission interval", ttl)
	}
	allow(t, fw, "secret-identity", 1)
	d := allow(t, fw, "secret-identity", 1)
	if d.Allowed {
		t.Fatal("second request in window admitted")
	}
	time.Sleep(d.RetryAfter + 10*time.Millisecond) // wait exactly as instructed
	if d := allow(t, fw, "secret-identity", 1); !d.Allowed {
		t.Fatalf("not admitted after RetryAfter: %+v", d)
	}
	// Same policy name under another prefix is independent.
	s2 := must(redis.New(c, redis.Config{Prefix: prefix(t, c), Failure: rategen.FailClosed}))
	fw2 := must(s2.FixedWindow("fw", rategen.Rate{Limit: 1, Period: time.Second}))
	if d := allow(t, fw2, "secret-identity", 1); !d.Allowed {
		t.Fatal("prefixes not isolated")
	}
}

func TestRefund(t *testing.T) {
	s, _ := newStore(t, redis.Config{})
	for _, l := range []*redis.Limiter{
		must(s.TokenBucket("tb", rategen.PerHour(3), 3)),
		must(s.GCRA("gc", rategen.PerHour(3), 3)),
		must(s.FixedWindow("fw", rategen.PerHour(3))),
		must(s.SlidingWindow("sw", rategen.PerHour(3))),
	} {
		d := allow(t, l, "k", 3)
		if err := l.RefundN(context.Background(), "k", 10, d); err != nil {
			t.Fatal(err)
		}
		if !allow(t, l, "k", 3).Allowed || allow(t, l, "k", 1).Allowed {
			t.Errorf("%s: refund not exact", l.Info().Algorithm)
		}
	}
}

// proxy is a TCP proxy to Redis that can be switched off (refusing and
// dropping connections) or stalled (accepting but never forwarding).
type proxy struct {
	ln      net.Listener
	target  string
	mu      sync.Mutex
	down    bool
	stalled bool
	conns   []net.Conn
}

func newProxy(t *testing.T, target string) *proxy {
	ln := must(net.Listen("tcp", "127.0.0.1:0"))
	p := &proxy{ln: ln, target: target}
	t.Cleanup(func() { ln.Close(); p.setDown(true) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			down, stalled := p.down, p.stalled
			if !down {
				p.conns = append(p.conns, c)
			}
			p.mu.Unlock()
			if down {
				c.Close()
				continue
			}
			if stalled {
				continue // hold the connection open, forward nothing
			}
			go p.pipe(c)
		}
	}()
	return p
}

func (p *proxy) pipe(c net.Conn) {
	u, err := net.Dial("tcp", p.target)
	if err != nil {
		c.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, u)
	p.mu.Unlock()
	go func() { io.Copy(u, c); u.Close() }()
	io.Copy(c, u)
	c.Close()
}

func (p *proxy) setDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, c := range p.conns {
			c.Close()
		}
		p.conns = nil
	}
}

func (p *proxy) setStalled(v bool) {
	p.setDown(true) // drop existing pooled connections
	p.mu.Lock()
	p.down, p.stalled = false, v
	p.mu.Unlock()
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func TestOutageAndRecovery(t *testing.T) {
	addr := redisAddr(t)
	px := newProxy(t, addr)
	pfx := prefix(t, newClient(t, addr))
	mk := func(policy rategen.FailurePolicy, local *memory.Store) *redis.Limiter {
		c := goredis.NewClient(&goredis.Options{Addr: px.addr(), ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
		t.Cleanup(func() { c.Close() })
		s := must(redis.New(c, redis.Config{Prefix: pfx + "-" + policy.String(), Failure: policy, Local: local, Timeout: 150 * time.Millisecond}))
		return must(s.FixedWindow("p", rategen.PerHour(2)))
	}
	closed := mk(rategen.FailClosed, nil)
	open := mk(rategen.FailOpen, nil)
	local := mk(rategen.FailToLocal, must(memory.New(memory.Config{})))
	ctx := context.Background()

	for _, l := range []*redis.Limiter{closed, open, local} {
		if d := allow(t, l, "k", 1); !d.Allowed || d.Degraded {
			t.Fatalf("healthy: %+v", d)
		}
	}

	for _, mode := range []string{"down", "stalled"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "down" {
				px.setDown(true)
			} else {
				px.setStalled(true)
			}
			start := time.Now()
			d, err := closed.AllowN(ctx, "k", 1)
			if d.Allowed || !errors.Is(err, rategen.ErrStoreUnavailable) {
				t.Fatalf("fail-closed: %+v %v", d, err)
			}
			if el := time.Since(start); el > time.Second {
				t.Fatalf("decision took %v despite a 150ms timeout", el)
			}
			d, err = open.AllowN(ctx, "k", 1)
			if !d.Allowed || !d.Degraded || !errors.Is(err, rategen.ErrStoreUnavailable) {
				t.Fatalf("fail-open: %+v %v", d, err)
			}
			// Fail-to-local enforces the same limit per process: the
			// local limiter starts fresh, so two are admitted, then
			// rejected.
			var admitted int
			for range 4 {
				d, err := local.AllowN(ctx, "k-"+mode, 1)
				if !d.Degraded || !errors.Is(err, rategen.ErrStoreUnavailable) {
					t.Fatalf("fail-to-local: %+v %v", d, err)
				}
				if d.Allowed {
					admitted++
				}
			}
			if admitted != 2 {
				t.Fatalf("local fallback admitted %d, want 2", admitted)
			}
			// A canceled request is rejected with its own error,
			// whatever the policy.
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			if d, err := open.AllowN(cctx, "k", 1); d.Allowed || !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled under fail-open: %+v %v", d, err)
			}

			px.setStalled(false)
			px.setDown(false)
			// Recovery: the next decision reaches Redis again, and the
			// state written before the outage is intact.
			d, err = closed.AllowN(ctx, "k", 1)
			if err != nil || d.Degraded {
				t.Fatalf("after recovery: %+v %v", d, err)
			}
			if mode == "down" && !d.Allowed {
				t.Fatal("second unit of the limit should still be available")
			}
			if mode == "stalled" && d.Allowed {
				t.Fatal("state before the outage was lost")
			}
		})
	}
}

func TestObserver(t *testing.T) {
	var events []rategen.Event
	var mu sync.Mutex
	s, _ := newStore(t, redis.Config{Observer: rategen.ObserverFunc(func(_ context.Context, e rategen.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})})
	l := must(s.GCRA("gc", rategen.PerHour(1), 1))
	allow(t, l, "k", 1)
	allow(t, l, "k", 1)
	if len(events) != 2 || events[0].Info.Backend != "redis" || events[0].Latency <= 0 || events[1].Decision.Allowed {
		t.Fatalf("%+v", events)
	}
}

func BenchmarkRedis(b *testing.B) {
	s, c := newStore(b, redis.Config{Timeout: time.Second})
	ctx := context.Background()
	// Baseline: one PING round trip on the same client, to separate
	// network latency from script cost.
	b.Run("ping-baseline", func(b *testing.B) {
		for b.Loop() {
			c.Ping(ctx)
		}
	})
	ls := map[string]rategen.Limiter{
		"token_bucket":   must(s.TokenBucket("tb", rategen.PerSecond(1<<30), 1<<30)),
		"gcra":           must(s.GCRA("gc", rategen.PerSecond(1<<30), 1<<30)),
		"fixed_window":   must(s.FixedWindow("fw", rategen.PerSecond(1<<30))),
		"sliding_window": must(s.SlidingWindow("sw", rategen.PerSecond(1<<30))),
	}
	for name, l := range ls {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				l.AllowN(ctx, "k", 1)
			}
		})
		b.Run(name+"/parallel", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.AllowN(ctx, "k", 1)
				}
			})
		})
	}
	b.Run("batch2", func(b *testing.B) {
		checks := []rategen.Check{{Limiter: ls["gcra"], Key: "u", N: 1}, {Limiter: ls["fixed_window"], Key: "u", N: 1}}
		for b.Loop() {
			rategen.Evaluate(ctx, checks...)
		}
	})
}
