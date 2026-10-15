// Command basic paces outgoing work with a single token bucket: at most 5
// calls per second, with bursts of 2.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/tokenbucket"
)

func main() {
	limiter, err := tokenbucket.New(rategen.PerSecond(5), 2)
	if err != nil {
		log.Fatal(err)
	}

	// Drop work that exceeds the rate.
	for i := range 4 {
		fmt.Printf("event %d allowed: %v\n", i, limiter.Allow())
	}

	// Or wait for capacity, bounded by a deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	for i := range 5 {
		if err := limiter.Wait(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("call %d at +%v\n", i, time.Since(start).Round(10*time.Millisecond))
	}
}
