// Package wait implements the blocking part of Wait operations shared by
// local limiters.
package wait

import (
	"context"
	"fmt"
	"time"

	"github.com/mjahidhasand10/rategen"
)

// ErrDeadline reports that the context deadline is too close for the
// required wait. It wraps context.DeadlineExceeded.
var ErrDeadline = fmt.Errorf("rategen: wait would exceed context deadline: %w", context.DeadlineExceeded)

// MaxWait returns the longest acceptable wait given ctx's deadline measured
// against clock, or -1 if ctx has no deadline.
func MaxWait(ctx context.Context, clock rategen.Clock) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	return max(0, dl.Sub(clock.Now()))
}

// Sleep blocks for d on clock, or until ctx is done, in which case it calls
// cancel and returns ctx's error.
func Sleep(ctx context.Context, clock rategen.Clock, d time.Duration, cancel func()) error {
	if d <= 0 {
		return nil
	}
	t := clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		cancel()
		return ctx.Err()
	}
}
