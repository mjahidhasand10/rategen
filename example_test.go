package rategen_test

import (
	"context"
	"fmt"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
	"github.com/mjahidhasand10/rategen/ratetest"
	"github.com/mjahidhasand10/rategen/tokenbucket"
)

func Example() {
	// A per-client limit: 2 requests per second with bursts of 2.
	store, _ := memory.New(memory.Config{})
	limiter, _ := store.GCRA("per-client", rategen.PerSecond(2), 2)

	ctx := context.Background()
	for range 3 {
		d, err := limiter.AllowN(ctx, "client-1", 1)
		if err != nil {
			panic(err)
		}
		fmt.Println(d.Allowed, d.Remaining)
	}
	// Output:
	// true 1
	// true 0
	// false 0
}

func ExampleEvaluate() {
	store, _ := memory.New(memory.Config{})
	perSecond, _ := store.TokenBucket("per-second", rategen.PerSecond(10), 10)
	daily, _ := store.FixedWindow("daily", rategen.PerDay(1))

	ctx := context.Background()
	for range 2 {
		res, _ := rategen.Evaluate(ctx,
			rategen.Check{Limiter: perSecond, Key: "user-1", N: 1},
			rategen.Check{Limiter: daily, Key: "user-1", N: 1},
		)
		fmt.Println(res.Allowed, res.Atomic, res.Decision().Policy)
	}
	// The rejected request consumed nothing from the per-second bucket.
	d, _ := perSecond.AllowN(ctx, "user-1", 9)
	fmt.Println(d.Allowed)
	// Output:
	// true true daily
	// false true daily
	// true
}

func Example_testingWithFakeClock() {
	clock := ratetest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	limiter, _ := tokenbucket.New(rategen.PerSecond(1), 1, tokenbucket.WithClock(clock))

	fmt.Println(limiter.Allow(), limiter.Allow())
	clock.Advance(time.Second)
	fmt.Println(limiter.Allow())
	// Output:
	// true false
	// true
}
