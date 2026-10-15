// Command distributed enforces one limit across all instances of a service
// using Redis. Start several copies against the same Redis and the limit is
// shared between them.
//
//	REDIS_ADDR=127.0.0.1:6379 go run ./examples/distributed
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
	"github.com/mjahidhasand10/rategen/redis"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{
		Addr: addr,
		// Required: lets per-decision timeouts apply to network I/O.
		ContextTimeoutEnabled: true,
	})
	defer client.Close()

	// If Redis is unreachable, fall back to a per-process limit. This
	// keeps some protection, but with N instances up to N times the
	// configured rate is admitted during the outage.
	local, err := memory.New(memory.Config{})
	if err != nil {
		log.Fatal(err)
	}
	store, err := redis.New(client, redis.Config{
		Prefix:  "example",
		Failure: rategen.FailToLocal,
		Local:   local,
		Timeout: 50 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	limiter, err := store.GCRA("api", rategen.PerSecond(5), 5)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for i := range 8 {
		d, err := limiter.AllowN(ctx, "tenant-42", 1)
		if err != nil {
			log.Printf("decision degraded: %v", err)
		}
		fmt.Printf("request %d: allowed=%v remaining=%d retry-after=%v degraded=%v\n",
			i+1, d.Allowed, d.Remaining, d.RetryAfter.Round(time.Millisecond), d.Degraded)
	}
}
