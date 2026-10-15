package memory_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
)

func benchLimiters(b *testing.B, s *memory.Store, limit int64) map[string]rategen.Limiter {
	return map[string]rategen.Limiter{
		"token_bucket":   must(s.TokenBucket("tb", rategen.PerSecond(limit), limit)),
		"gcra":           must(s.GCRA("gcra", rategen.PerSecond(limit), limit)),
		"fixed_window":   must(s.FixedWindow("fw", rategen.PerSecond(limit))),
		"sliding_window": must(s.SlidingWindow("sw", rategen.PerSecond(limit))),
	}
}

// BenchmarkAllow measures the admit path for a single key.
func BenchmarkAllow(b *testing.B) {
	s := must(memory.New(memory.Config{}))
	ctx := context.Background()
	for name, l := range benchLimiters(b, s, 1<<40) {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				l.AllowN(ctx, "key", 1)
			}
		})
	}
}

// BenchmarkReject measures the reject path: the key is exhausted and the
// clock is frozen so every call is rejected.
func BenchmarkReject(b *testing.B) {
	s, _ := newStore(b, memory.Config{})
	ctx := context.Background()
	for name, l := range benchLimiters(b, s, 1) {
		l.AllowN(ctx, "key", 1)
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				if d, _ := l.AllowN(ctx, "key", 1); d.Allowed {
					b.Fatal("admitted")
				}
			}
		})
	}
}

// BenchmarkContendedKey measures many goroutines hitting one key.
func BenchmarkContendedKey(b *testing.B) {
	s := must(memory.New(memory.Config{}))
	ctx := context.Background()
	for name, l := range benchLimiters(b, s, 1<<40) {
		b.Run(name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.AllowN(ctx, "hot", 1)
				}
			})
		})
	}
}

// BenchmarkIndependentKeys measures goroutines each using their own small
// set of keys, so contention is limited to shard collisions.
func BenchmarkIndependentKeys(b *testing.B) {
	s := must(memory.New(memory.Config{}))
	l := must(s.GCRA("gcra", rategen.PerSecond(1<<40), 1<<40))
	ctx := context.Background()
	var g atomic.Int64
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	b.RunParallel(func(pb *testing.PB) {
		base := int(g.Add(1)) * 64
		i := 0
		for pb.Next() {
			l.AllowN(ctx, keys[(base+i%64)%len(keys)], 1)
			i++
		}
	})
}

// BenchmarkHighCardinality inserts a new key on almost every call into a
// store bounded at 100k keys, exercising insertion and LRU eviction.
func BenchmarkHighCardinality(b *testing.B) {
	s := must(memory.New(memory.Config{MaxKeys: 100_000}))
	l := must(s.GCRA("gcra", rategen.PerSecond(10), 10))
	ctx := context.Background()
	keys := make([]string, 1_000_000)
	for i := range keys {
		keys[i] = "client-" + strconv.Itoa(i)
	}
	i := 0
	for b.Loop() {
		l.AllowN(ctx, keys[i%len(keys)], 1)
		i++
	}
	b.ReportMetric(float64(s.Stats().Keys), "keys")
}

// BenchmarkBatch measures atomic evaluation of a per-second and a daily
// limit for one key.
func BenchmarkBatch(b *testing.B) {
	s := must(memory.New(memory.Config{}))
	perSec := must(s.TokenBucket("per-sec", rategen.PerSecond(1<<40), 1<<40))
	daily := must(s.FixedWindow("daily", rategen.PerDay(1<<40)))
	ctx := context.Background()
	checks := []rategen.Check{{Limiter: perSec, Key: "u", N: 1}, {Limiter: daily, Key: "u", N: 1}}
	for b.Loop() {
		rategen.Evaluate(ctx, checks...)
	}
}
