package httplimit_test

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/httplimit"
	"github.com/mjahidhasand10/rategen/memory"
)

// soakDuration returns the soak duration from RATEGEN_SOAK (a Go duration,
// for example "10m"), or skips the test.
func soakDuration(t *testing.T) time.Duration {
	v := os.Getenv("RATEGEN_SOAK")
	if v == "" {
		t.Skip("RATEGEN_SOAK not set; e.g. RATEGEN_SOAK=10m")
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("RATEGEN_SOAK: %v", err)
	}
	return d
}

// TestSoak drives the middleware and memory store with a mix of a few hot
// clients and a stream of never-repeating attacker addresses for the soak
// duration, with a janitor running. It checks that the key bound holds,
// that the heap stops growing once the store is full, that no goroutines
// leak, and reports decision latency percentiles.
func TestSoak(t *testing.T) {
	dur := soakDuration(t)
	baseGoroutines := runtime.NumGoroutine()

	const maxKeys = 50_000
	store := must(memory.New(memory.Config{MaxKeys: maxKeys}))
	// 60/min: each attacker address's entry lives about a second, so the
	// stream of new addresses overflows MaxKeys and exercises eviction.
	perIP := must(store.GCRA("per-ip", rategen.PerMinute(60), 20))
	global := must(store.TokenBucket("global", rategen.PerSecond(1_000_000), 100_000))
	mw := must(httplimit.New(httplimit.Config{
		Rules: []httplimit.Rule{
			{Limiter: perIP, Key: httplimit.ByIP()},
			{Limiter: global, Key: httplimit.Static("all")},
		},
		Headers: httplimit.HeadersDraft11,
	}))
	h := mw.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	ctx, cancel := context.WithCancel(context.Background())
	janitorDone := make(chan struct{})
	go func() { store.RunJanitor(ctx, time.Second); close(janitorDone) }()

	var (
		requests, rejected atomic.Int64
		attackerSeq        atomic.Uint32
		latMu              sync.Mutex
		lat                []time.Duration
		wg                 sync.WaitGroup
	)
	deadline := time.Now().Add(dur)
	for g := range runtime.GOMAXPROCS(0) * 2 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(g), 99))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			w := &statusWriter{h: http.Header{}}
			// Fixed-size reservoir sample, so the test's own memory
			// does not grow with the soak duration.
			const reservoir = 4096
			local := make([]time.Duration, 0, reservoir)
			seen := 0
			for i := 0; time.Now().Before(deadline); i++ {
				if rng.IntN(4) == 0 {
					req.RemoteAddr = "192.0.2." + strconv.Itoa(rng.IntN(8)) + ":1" // hot clients
				} else {
					n := attackerSeq.Add(1)
					req.RemoteAddr = "10." + strconv.Itoa(int(n>>16&255)) + "." + strconv.Itoa(int(n>>8&255)) + "." + strconv.Itoa(int(n&255)) + ":1"
				}
				clear(w.h)
				w.code = http.StatusOK
				start := time.Now()
				h.ServeHTTP(w, req)
				if i%64 == 0 {
					d := time.Since(start)
					seen++
					if len(local) < reservoir {
						local = append(local, d)
					} else if j := rng.IntN(seen); j < reservoir {
						local[j] = d
					}
				}
				requests.Add(1)
				if w.code == http.StatusTooManyRequests {
					rejected.Add(1)
				}
			}
			latMu.Lock()
			lat = append(lat, local...)
			latMu.Unlock()
		})
	}

	// Sample the heap while the load runs.
	var heaps []uint64
	sampleEvery := max(dur/20, time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(sampleEvery) // pacing the sampler, not synchronising the test
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heaps = append(heaps, ms.HeapInuse)
		if k := store.Stats().Keys; k > maxKeys {
			t.Fatalf("store holds %d keys, bound %d", k, maxKeys)
		}
	}
	wg.Wait()
	cancel()
	<-janitorDone

	st := store.Stats()
	if st.Keys > maxKeys {
		t.Fatalf("store holds %d keys, bound %d", st.Keys, maxKeys)
	}
	// After warm-up (the first quarter), the heap must plateau: allow 50%
	// headroom over the warm-up level for GC timing noise.
	if len(heaps) >= 4 {
		warm := heaps[len(heaps)/4]
		peak := slices.Max(heaps[len(heaps)/4:])
		if float64(peak) > 1.5*float64(warm) {
			t.Fatalf("heap kept growing: %d MiB after warm-up, peak %d MiB (samples %v)", warm>>20, peak>>20, heaps)
		}
	}
	// No goroutines left behind (allow the runtime a moment to reap).
	for i := 0; runtime.NumGoroutine() > baseGoroutines+2 && i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseGoroutines+2 {
		t.Fatalf("goroutines: %d at start, %d at end", baseGoroutines, n)
	}

	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))] }
	t.Logf("soak %v: %d requests (%.0f/s), %d rejected, keys %d, evictions %d, expired %d",
		dur, requests.Load(), float64(requests.Load())/dur.Seconds(), rejected.Load(), st.Keys, st.Evictions, st.Expired)
	t.Logf("latency p50 %v, p99 %v, p99.9 %v, max %v; heap samples (MiB) %v",
		pct(0.5), pct(0.99), pct(0.999), lat[len(lat)-1], mib(heaps))
}

// statusWriter records every response's status and discards bodies.
// (httptest.ResponseRecorder records only the first status it sees and
// keeps every body, which would distort a long soak.)
type statusWriter struct {
	h    http.Header
	code int
}

func (w *statusWriter) Header() http.Header         { return w.h }
func (w *statusWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *statusWriter) WriteHeader(code int)        { w.code = code }

func mib(v []uint64) []uint64 {
	out := make([]uint64, len(v))
	for i, x := range v {
		out[i] = x >> 20
	}
	return out
}
