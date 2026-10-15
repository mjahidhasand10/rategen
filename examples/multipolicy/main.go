// Command multipolicy applies a per-second burst limit and a daily quota to
// the same API key. Both limits are evaluated atomically: a request that the
// daily quota rejects does not consume burst capacity.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
)

func main() {
	store, err := memory.New(memory.Config{})
	if err != nil {
		log.Fatal(err)
	}
	burst, err := store.GCRA("burst", rategen.PerSecond(10), 5)
	if err != nil {
		log.Fatal(err)
	}
	daily, err := store.FixedWindow("daily", rategen.PerDay(3))
	if err != nil {
		log.Fatal(err)
	}
	global, err := store.TokenBucket("global", rategen.PerSecond(1000), 1000)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for i := range 5 {
		res, err := rategen.Evaluate(ctx,
			rategen.Check{Limiter: burst, Key: "key-123", N: 1},
			rategen.Check{Limiter: daily, Key: "key-123", N: 1},
			// A different key for a limit shared by all callers.
			rategen.Check{Limiter: global, Key: "all", N: 1},
		)
		if err != nil {
			log.Fatal(err)
		}
		d := res.Decision()
		fmt.Printf("request %d: allowed=%v atomic=%v deciding-policy=%s remaining=%d\n",
			i+1, res.Allowed, res.Atomic, d.Policy, d.Remaining)
	}
}
