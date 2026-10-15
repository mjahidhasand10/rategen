# Observability

Every decision can be reported to a `rategen.Observer`:

```go
type Observer interface {
    Observe(ctx context.Context, e rategen.Event)
}
```

`Event` carries `Info` (policy, algorithm, backend, rate, burst), the
`Decision`, the cost, the error, the backend latency, and whether the
decision was part of an atomic batch. **It never carries the key.**

Attach an observer:

- to a store, with `memory.Config.Observer` or `redis.Config.Observer`.
  Every limiter in the store reports, and no capabilities are hidden. This
  is the preferred way.
- to any single limiter, with `rategen.Observe(l, obs)`. The wrapper
  exposes only `Limiter`, so use it for custom limiters or around a `Multi`.

Observers run synchronously on the request path. They must be fast,
non-blocking and safe for concurrent use. `rategen.Observers(a, b)` fans
out to several.

## Structured logs

`rategen.SlogObserver(logger)` logs rejections at Info level and errors at
Warn level, with policy, algorithm, backend, cost, retry-after and latency
attributes. It does not log allowed decisions. Under heavy rejection
traffic, consider sampling in your `slog.Handler`.

## Prometheus

```sh
go get github.com/mjahidhasand10/rategen/prometheus
```

```go
obs, err := rgprom.NewObserver(prometheus.DefaultRegisterer, rgprom.Options{})
store, err := memory.New(memory.Config{Observer: obs})
```

| Metric | Labels |
|---|---|
| `rategen_decisions_total` | `policy`, `algorithm`, `backend`, `result` (`allowed`/`rejected`), `degraded` (`true`/`false`) |
| `rategen_errors_total` | `policy`, `algorithm`, `backend`, `kind` (`store_unavailable`, `store_full`, `canceled`, `invalid_request`, `other`) |
| `rategen_decision_duration_seconds` | `policy`, `algorithm`, `backend` |

Label values come from your policy names and a fixed set of strings. Keys,
IPs and URLs are never labels, so the number of series grows only with the
number of policies.

Useful queries:

```promql
# Rejection ratio per policy
sum by (policy) (rate(rategen_decisions_total{result="rejected"}[5m]))
  / sum by (policy) (rate(rategen_decisions_total[5m]))

# Decisions made without the backend (fail-open / fail-to-local)
sum by (policy) (rate(rategen_decisions_total{degraded="true"}[5m])) > 0

# p99 Redis decision latency
histogram_quantile(0.99, sum by (le) (rate(rategen_decision_duration_seconds_bucket{backend="redis"}[5m])))
```

## Store statistics

`memory.Store.Stats()` returns the current key count, the evictions (live
keys removed because the store was full) and the expired entries removed. A
steady eviction rate means `MaxKeys` is too small or you are under a key
flood.

## Other systems

Implement `Observer` to feed OpenTelemetry, StatsD or your own pipeline.
Map `Event.Info` fields to attributes, and keep the key out.
