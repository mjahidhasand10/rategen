package redis_test

import (
	"context"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/redis"
)

// TestSoakRedis runs a sustained mixed workload (single checks and batches,
// hot and unique keys) against Redis for RATEGEN_SOAK, verifying that the
// configured limit is never exceeded for a tracked key, that keys expire
// (the database does not grow without bound), that no goroutines leak, and
// reporting latency percentiles.
func TestSoakRedis(t *testing.T) {
	v := os.Getenv("RATEGEN_SOAK")
	if v == "" {
		t.Skip("RATEGEN_SOAK not set")
	}
	dur, err := time.ParseDuration(v)
	if err != nil {
		t.Fatal(err)
	}
	s, c := newStore(t, redis.Config{})
	baseGoroutines := runtime.NumGoroutine()
	// A short-window policy for the high-cardinality stream so its keys
	// expire during the soak.
	burst := must(s.GCRA("burst", rategen.PerSecond(100), 10))
	window := must(s.FixedWindow("window", rategen.Rate{Limit: 50, Period: time.Second}))
	ctx := context.Background()

	var (
		calls, errs   atomic.Int64
		trackedAdmits sync.Map // window start (unix seconds) -> *atomic.Int64
		latMu         sync.Mutex
		lat           []time.Duration
		wg            sync.WaitGroup
		seq           atomic.Int64
	)
	deadline := time.Now().Add(dur)
	for g := range 16 {
		wg.Go(func() {
			var local []time.Duration
			for i := 0; time.Now().Before(deadline); i++ {
				start := time.Now()
				switch {
				case g == 0:
					// One tracked key on the fixed window: count admits
					// per window and check the limit.
					d, err := window.AllowN(ctx, "tracked", 1)
					if err != nil {
						errs.Add(1)
						continue
					}
					if d.Allowed {
						// Attribute the admit to its window via the reset
						// time Redis reported; rounding tolerates small
						// skew between this host's clock and Redis's.
						end := time.Now().Add(d.ResetAfter).Round(time.Second).Unix()
						ctr, _ := trackedAdmits.LoadOrStore(end, new(atomic.Int64))
						ctr.(*atomic.Int64).Add(1)
					}
				case i%3 == 0:
					key := "u-" + strconv.FormatInt(seq.Add(1), 36)
					if _, err := rategen.Evaluate(ctx,
						rategen.Check{Limiter: burst, Key: key, N: 1},
						rategen.Check{Limiter: window, Key: key, N: 1}); err != nil {
						errs.Add(1)
					}
				default:
					if _, err := burst.AllowN(ctx, "hot-"+strconv.Itoa(i%32), 1); err != nil {
						errs.Add(1)
					}
				}
				calls.Add(1)
				if i%16 == 0 {
					local = append(local, time.Since(start))
				}
			}
			latMu.Lock()
			lat = append(lat, local...)
			latMu.Unlock()
		})
	}
	wg.Wait()

	if e := errs.Load(); e > 0 {
		t.Fatalf("%d errors during soak", e)
	}
	trackedAdmits.Range(func(k, v any) bool {
		// Windows at the soak's edges are partial; none may exceed 50.
		if n := v.(*atomic.Int64).Load(); n > 50 {
			t.Errorf("window ending %v admitted %d, limit 50", time.Unix(k.(int64), 0), n)
		}
		return true
	})
	// Keys for unique identities expire within ~1s (window) or ~100ms
	// (GCRA), so after a pause the database holds only recent keys.
	time.Sleep(1500 * time.Millisecond) // let the last keys' TTLs pass
	var left int
	iter := c.Scan(ctx, 0, s.Prefix()+":*", 1000).Iterator()
	for iter.Next(ctx) {
		left++
	}
	if left > 100 {
		t.Fatalf("%d keys still present 1.5s after the soak; TTLs not applied", left)
	}
	for i := 0; runtime.NumGoroutine() > baseGoroutines+4 && i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseGoroutines+4 {
		t.Fatalf("goroutines: %d at start, %d at end", baseGoroutines, n)
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))] }
	t.Logf("redis soak %v: %d calls (%.0f/s), unique identities %d, keys left %d",
		dur, calls.Load(), float64(calls.Load())/dur.Seconds(), seq.Load(), left)
	t.Logf("latency p50 %v, p99 %v, p99.9 %v, max %v", pct(0.5), pct(0.99), pct(0.999), lat[len(lat)-1])
}
