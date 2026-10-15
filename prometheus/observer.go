// Package prometheus exports rategen decision metrics to Prometheus.
//
// Labels are limited to values with bounded cardinality that the operator
// controls: policy name, algorithm, backend, result and error kind. Keys,
// IP addresses, user IDs and URLs are never used as labels.
package prometheus

import (
	"context"
	"errors"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/mjahidhasand10/rategen"
)

// Options configures an Observer.
type Options struct {
	// Namespace prefixes metric names; default "rategen".
	Namespace string
	// Buckets are the latency histogram buckets in seconds. The default
	// spans 10µs to 1s, covering in-memory and network decisions.
	Buckets []float64
	// ConstLabels are added to every metric, for example a service name.
	ConstLabels prom.Labels
}

// Observer is a rategen.Observer that records Prometheus metrics:
//
//	<ns>_decisions_total{policy,algorithm,backend,result,degraded}
//	<ns>_errors_total{policy,algorithm,backend,kind}
//	<ns>_decision_duration_seconds{policy,algorithm,backend}
//
// result is "allowed" or "rejected"; degraded is "true" when a failure
// policy decided instead of the backend; kind is one of
// "store_unavailable", "store_full", "canceled", "invalid_request" or
// "other".
type Observer struct {
	decisions *prom.CounterVec
	errors    *prom.CounterVec
	latency   *prom.HistogramVec
}

var _ rategen.Observer = (*Observer)(nil)

// NewObserver creates an Observer and registers its metrics with reg.
func NewObserver(reg prom.Registerer, opts Options) (*Observer, error) {
	if opts.Namespace == "" {
		opts.Namespace = "rategen"
	}
	if opts.Buckets == nil {
		opts.Buckets = []float64{1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2, 2.5e-2, 5e-2, 0.1, 0.25, 1}
	}
	base := []string{"policy", "algorithm", "backend"}
	o := &Observer{
		decisions: prom.NewCounterVec(prom.CounterOpts{
			Namespace: opts.Namespace, Name: "decisions_total", ConstLabels: opts.ConstLabels,
			Help: "Rate-limit decisions by policy and result.",
		}, append(base, "result", "degraded")),
		errors: prom.NewCounterVec(prom.CounterOpts{
			Namespace: opts.Namespace, Name: "errors_total", ConstLabels: opts.ConstLabels,
			Help: "Errors that accompanied rate-limit decisions.",
		}, append(base, "kind")),
		latency: prom.NewHistogramVec(prom.HistogramOpts{
			Namespace: opts.Namespace, Name: "decision_duration_seconds", ConstLabels: opts.ConstLabels,
			Help: "Time taken by the backend to reach a rate-limit decision.", Buckets: opts.Buckets,
		}, base),
	}
	for _, c := range []prom.Collector{o.decisions, o.errors, o.latency} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// Observe implements rategen.Observer.
func (o *Observer) Observe(_ context.Context, e rategen.Event) {
	p, a, b := e.Info.Policy, e.Info.Algorithm, e.Info.Backend
	result := "rejected"
	if e.Decision.Allowed {
		result = "allowed"
	}
	degraded := "false"
	if e.Decision.Degraded {
		degraded = "true"
	}
	o.decisions.WithLabelValues(p, a, b, result, degraded).Inc()
	if e.Err != nil {
		o.errors.WithLabelValues(p, a, b, errorKind(e.Err)).Inc()
	}
	o.latency.WithLabelValues(p, a, b).Observe(e.Latency.Seconds())
}

func errorKind(err error) string {
	switch {
	case errors.Is(err, rategen.ErrStoreUnavailable):
		return "store_unavailable"
	case errors.Is(err, rategen.ErrStoreFull):
		return "store_full"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	case errors.Is(err, rategen.ErrInvalidCost), errors.Is(err, rategen.ErrExceedsCapacity):
		return "invalid_request"
	default:
		return "other"
	}
}
