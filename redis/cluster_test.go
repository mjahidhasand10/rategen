package redis_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/redis"
)

// clusterClient returns a client for the Redis Cluster listed in
// RATEGEN_REDIS_CLUSTER_ADDRS (comma separated), or skips the test, and a
// unique prefix whose keys are removed from every master afterwards.
func clusterClient(t testing.TB) (*goredis.ClusterClient, string) {
	addrs := os.Getenv("RATEGEN_REDIS_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("RATEGEN_REDIS_CLUSTER_ADDRS not set")
	}
	c := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:                 strings.Split(addrs, ","),
		ContextTimeoutEnabled: true,
	})
	pfx := "rgcluster-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	t.Cleanup(func() {
		ctx := context.Background()
		c.ForEachMaster(ctx, func(ctx context.Context, n *goredis.Client) error {
			iter := n.Scan(ctx, 0, pfx+":*", 1000).Iterator()
			for iter.Next(ctx) {
				n.Del(ctx, iter.Val())
			}
			return nil
		})
		c.Close()
	})
	return c, pfx
}

func TestClusterKeysSpreadAcrossNodes(t *testing.T) {
	c, pfx := clusterClient(t)
	s := must(redis.New(c, redis.Config{Prefix: pfx, Failure: rategen.FailClosed, Timeout: 2 * time.Second}))
	l := must(s.GCRA("spread", rategen.PerMinute(10), 10))
	ctx := context.Background()
	for i := range 200 {
		if _, err := l.AllowN(ctx, "client-"+strconv.Itoa(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	nodes := map[string]int{}
	err := c.ForEachMaster(ctx, func(ctx context.Context, n *goredis.Client) error {
		var count int
		iter := n.Scan(ctx, 0, pfx+":*", 1000).Iterator()
		for iter.Next(ctx) {
			count++
		}
		mu.Lock()
		nodes[n.Options().Addr] = count
		mu.Unlock()
		return iter.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	total, used := 0, 0
	for _, n := range nodes {
		total += n
		if n > 0 {
			used++
		}
	}
	if total != 200 || used < 2 {
		t.Fatalf("keys per node %v: want 200 keys spread over several nodes", nodes)
	}
}

func TestClusterBatches(t *testing.T) {
	c, pfx := clusterClient(t)
	s := must(redis.New(c, redis.Config{Prefix: pfx, Failure: rategen.FailClosed, Timeout: 2 * time.Second}))
	burst := must(s.TokenBucket("burst", rategen.PerHour(100), 10))
	daily := must(s.FixedWindow("daily", rategen.PerDay(2)))
	global := must(s.FixedWindow("global", rategen.PerHour(1000)))
	ctx := context.Background()

	// Same identity: one hash slot, so the batch is atomic.
	same := []rategen.Check{{Limiter: burst, Key: "user-1", N: 1}, {Limiter: daily, Key: "user-1", N: 1}}
	for range 2 {
		if res, err := rategen.Evaluate(ctx, same...); err != nil || !res.Allowed || !res.Atomic {
			t.Fatalf("%+v %v", res, err)
		}
	}
	res, err := rategen.Evaluate(ctx, same...)
	if err != nil || res.Allowed || !res.Atomic {
		t.Fatalf("over quota: %+v %v", res, err)
	}
	if d, _ := burst.AllowN(ctx, "user-1", 8); !d.Allowed {
		t.Fatal("rejected atomic batch consumed burst capacity")
	}

	// Different identities span slots: Evaluate must fall back to the
	// non-atomic path instead of failing with CROSSSLOT.
	mixed := []rategen.Check{{Limiter: global, Key: "all", N: 1}, {Limiter: daily, Key: "user-2", N: 1}}
	for range 2 {
		if res, err := rategen.Evaluate(ctx, mixed...); err != nil || !res.Allowed || res.Atomic {
			t.Fatalf("mixed: %+v %v", res, err)
		}
	}
	res, err = rategen.Evaluate(ctx, mixed...)
	if err != nil || res.Allowed || res.Atomic {
		t.Fatalf("mixed over quota: %+v %v", res, err)
	}
	// The global unit consumed by the rejected request was refunded.
	if d, _ := global.AllowN(ctx, "all", 998); !d.Allowed {
		t.Fatal("non-atomic path did not refund the global limit")
	}
	if _, err := rategen.EvaluateAtomic(ctx, mixed...); !errors.Is(err, rategen.ErrNotAtomic) {
		t.Fatalf("EvaluateAtomic across slots = %v", err)
	}
}

func TestClusterDistributedExactness(t *testing.T) {
	_, pfx := clusterClient(t)
	addrs := strings.Split(os.Getenv("RATEGEN_REDIS_CLUSTER_ADDRS"), ",")
	const instances, goroutines, attempts, limit = 4, 8, 20, 101
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range instances {
		ic := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs, ContextTimeoutEnabled: true})
		defer ic.Close()
		s := must(redis.New(ic, redis.Config{Prefix: pfx, Failure: rategen.FailClosed, Timeout: 5 * time.Second}))
		l := must(s.SlidingWindow("shared", rategen.PerDay(limit)))
		for range goroutines {
			wg.Go(func() {
				for range attempts {
					d, err := l.AllowN(context.Background(), "hot", 1)
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
	if got := admitted.Load(); got != limit {
		t.Fatalf("admitted %d across %d cluster clients, want exactly %d", got, instances, limit)
	}
}
