package rategen

import (
	"context"
	"log/slog"
	"time"
)

// Event describes one rate-limit decision for observability. It
// deliberately carries no key: keys are often user identifiers, IP
// addresses or credentials, and must not reach logs or metrics.
type Event struct {
	Info     Info
	Decision Decision
	Cost     int64
	// Err is the error that accompanied the decision, if any.
	Err error
	// Latency is the time the backend took to decide.
	Latency time.Duration
	// Batch reports that the decision was part of an atomic multi-limit
	// evaluation.
	Batch bool
}

// Observer receives decision events. Implementations are called
// synchronously on the request path, so they must be fast and must not
// block; they must also be safe for concurrent use.
type Observer interface {
	Observe(ctx context.Context, e Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, e Event)

// Observe calls f(ctx, e).
func (f ObserverFunc) Observe(ctx context.Context, e Event) { f(ctx, e) }

// Observers returns an Observer that forwards each event to all of obs.
func Observers(obs ...Observer) Observer {
	obs = append([]Observer(nil), obs...)
	return ObserverFunc(func(ctx context.Context, e Event) {
		for _, o := range obs {
			o.Observe(ctx, e)
		}
	})
}

// SlogObserver returns an Observer that logs rejections at Info level and
// errors at Warn level to logger. Allowed decisions are not logged. Log
// records contain the policy, algorithm, backend and timing, never the key.
func SlogObserver(logger *slog.Logger) Observer {
	return ObserverFunc(func(ctx context.Context, e Event) {
		if e.Decision.Allowed && e.Err == nil {
			return
		}
		attrs := []slog.Attr{
			slog.String("policy", e.Info.Policy),
			slog.String("algorithm", e.Info.Algorithm),
			slog.String("backend", e.Info.Backend),
			slog.Bool("allowed", e.Decision.Allowed),
			slog.Bool("degraded", e.Decision.Degraded),
			slog.Int64("cost", e.Cost),
			slog.Duration("retry_after", e.Decision.RetryAfter),
			slog.Duration("latency", e.Latency),
		}
		level, msg := slog.LevelInfo, "rate limit rejected request"
		if e.Err != nil {
			level, msg = slog.LevelWarn, "rate limit decision error"
			attrs = append(attrs, slog.String("error", e.Err.Error()))
		}
		logger.LogAttrs(ctx, level, msg, attrs...)
	})
}

// Observe returns a Limiter that reports every decision of l to o. Info is
// taken from l if it implements Describer.
//
// The wrapper implements only Limiter: it hides Reserver, Waiter, Refunder
// and Batcher. Prefer the Observer option of the memory and redis stores,
// which observes without hiding capabilities; use Observe for custom
// limiters or around a Multi.
func Observe(l Limiter, o Observer) Limiter {
	var info Info
	if d, ok := l.(Describer); ok {
		info = d.Info()
	}
	return &observed{l: l, o: o, info: info}
}

type observed struct {
	l    Limiter
	o    Observer
	info Info
}

func (w *observed) AllowN(ctx context.Context, key string, n int64) (Decision, error) {
	start := time.Now()
	d, err := w.l.AllowN(ctx, key, n)
	w.o.Observe(ctx, Event{Info: w.info, Decision: d, Cost: n, Err: err, Latency: time.Since(start)})
	return d, err
}

func (w *observed) Info() Info { return w.info }
