package benchmarks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/didip/tollbooth/v8"
	tblimiter "github.com/didip/tollbooth/v8/limiter"
	"github.com/go-redis/redis_rate/v10"
	goredis "github.com/redis/go-redis/v9"
	ulule "github.com/ulule/limiter/v3"
	ulstdlib "github.com/ulule/limiter/v3/drivers/middleware/stdlib"
	ulmemory "github.com/ulule/limiter/v3/drivers/store/memory"
	xrate "golang.org/x/time/rate"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/httplimit"
	"github.com/mjahidhasand10/rategen/memory"
	rgredis "github.com/mjahidhasand10/rategen/redis"
	"github.com/mjahidhasand10/rategen/tokenbucket"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

const huge = 1 << 40

// Single in-process token bucket: rategen/tokenbucket vs x/time/rate, same
// rate and burst.
func BenchmarkTokenBucket(b *testing.B) {
	rg := must(tokenbucket.New(rategen.PerSecond(huge), huge))
	x := xrate.NewLimiter(xrate.Limit(huge), huge)
	b.Run("rategen/serial", func(b *testing.B) {
		for b.Loop() {
			rg.Allow()
		}
	})
	b.Run("x-time-rate/serial", func(b *testing.B) {
		for b.Loop() {
			x.Allow()
		}
	})
	b.Run("rategen/parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rg.Allow()
			}
		})
	})
	b.Run("x-time-rate/parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				x.Allow()
			}
		})
	})
	// Reject path: an exhausted bucket with a slow refill.
	rgSlow := must(tokenbucket.New(rategen.Every(time.Hour), 1))
	xSlow := xrate.NewLimiter(xrate.Every(time.Hour), 1)
	rgSlow.Allow()
	xSlow.Allow()
	b.Run("rategen/reject", func(b *testing.B) {
		for b.Loop() {
			rgSlow.Allow()
		}
	})
	b.Run("x-time-rate/reject", func(b *testing.B) {
		for b.Loop() {
			xSlow.Allow()
		}
	})
}

// Keyed in-memory fixed-window counters: rategen/memory FixedWindow vs
// ulule/limiter's memory store, which also counts per key per period.
func BenchmarkKeyedMemory(b *testing.B) {
	ctx := context.Background()
	rgStore := must(memory.New(memory.Config{}))
	rg := must(rgStore.FixedWindow("fw", rategen.Rate{Limit: huge, Period: time.Hour}))
	ul := ulule.New(ulmemory.NewStore(), ulule.Rate{Limit: huge, Period: time.Hour})
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = "client-" + strconv.Itoa(i)
	}
	b.Run("rategen/one-key", func(b *testing.B) {
		for b.Loop() {
			rg.AllowN(ctx, "k", 1)
		}
	})
	b.Run("ulule/one-key", func(b *testing.B) {
		for b.Loop() {
			ul.Get(ctx, "k")
		}
	})
	b.Run("rategen/10k-keys-parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				rg.AllowN(ctx, keys[i%len(keys)], 1)
				i++
			}
		})
	})
	b.Run("ulule/10k-keys-parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				ul.Get(ctx, keys[i%len(keys)])
				i++
			}
		})
	})
}

// net/http middleware limiting by RemoteAddr: rategen/httplimit (token
// bucket) vs tollbooth (token bucket via x/time/rate) and ulule's stdlib
// middleware (fixed window). Each wraps a no-op handler.
func BenchmarkHTTPMiddleware(b *testing.B) {
	noop := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	rgStore := must(memory.New(memory.Config{}))
	rgTB := must(rgStore.TokenBucket("ip", rategen.PerSecond(huge), huge))
	rg := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: rgTB, Key: httplimit.ByIP()}}})).Handler(noop)

	tbl := tollbooth.NewLimiter(float64(huge), &tblimiter.ExpirableOptions{DefaultExpirationTTL: time.Hour}).
		SetBurst(huge).
		SetIPLookup(tblimiter.IPLookup{Name: "RemoteAddr"})
	tb := tollbooth.LimitHandler(tbl, noop)

	ul := ulstdlib.NewMiddleware(ulule.New(ulmemory.NewStore(), ulule.Rate{Limit: huge, Period: time.Hour})).Handler(noop)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	for _, c := range []struct {
		name string
		h    http.Handler
	}{{"baseline", noop}, {"rategen", rg}, {"tollbooth", tb}, {"ulule", ul}} {
		b.Run(c.name, func(b *testing.B) {
			w := httptest.NewRecorder()
			for b.Loop() {
				clear(w.Header())
				c.h.ServeHTTP(w, req)
			}
		})
	}
}

// Redis-backed GCRA: rategen/redis vs go-redis/redis_rate, same rate and
// burst, same Redis server and client options. Requires RATEGEN_REDIS_ADDR.
func BenchmarkRedisGCRA(b *testing.B) {
	addr := os.Getenv("RATEGEN_REDIS_ADDR")
	if addr == "" {
		b.Skip("RATEGEN_REDIS_ADDR not set")
	}
	ctx := context.Background()
	c := goredis.NewClient(&goredis.Options{Addr: addr, ContextTimeoutEnabled: true})
	defer c.Close()
	defer func() {
		for _, pattern := range []string{"rategen-bench:*", "rate:rategen-bench-rr"} {
			iter := c.Scan(ctx, 0, pattern, 100).Iterator()
			for iter.Next(ctx) {
				c.Del(ctx, iter.Val())
			}
		}
	}()
	rgStore := must(rgredis.New(c, rgredis.Config{Prefix: "rategen-bench", Failure: rategen.FailClosed, Timeout: time.Second}))
	rg := must(rgStore.GCRA("gcra", rategen.PerSecond(1<<30), 1<<30))
	rr := redis_rate.NewLimiter(c)
	limit := redis_rate.Limit{Rate: 1 << 30, Burst: 1 << 30, Period: time.Second}
	b.Run("ping-baseline", func(b *testing.B) {
		for b.Loop() {
			c.Ping(ctx)
		}
	})
	b.Run("rategen", func(b *testing.B) {
		for b.Loop() {
			rg.AllowN(ctx, "bench", 1)
		}
	})
	b.Run("redis_rate", func(b *testing.B) {
		for b.Loop() {
			rr.Allow(ctx, "rategen-bench-rr", limit)
		}
	})
	b.Run("rategen/parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rg.AllowN(ctx, "bench", 1)
			}
		})
	})
	b.Run("redis_rate/parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rr.Allow(ctx, "rategen-bench-rr", limit)
			}
		})
	})
}
