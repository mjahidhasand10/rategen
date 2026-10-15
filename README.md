# rategen

Rate limiting for Go services, with one API for in-process and Redis-backed
limits.

- **Four algorithms:** token bucket, GCRA, fixed window and sliding-window
  counter, each with documented semantics and limitations.
- **Local and distributed** stores behind the same `Limiter` interface. The
  memory store is bounded and starts no goroutines. The Redis store makes
  every decision in one atomic Lua script, using the Redis server clock.
- **Several limits per request** (per-second *and* daily, per-user *and*
  global), evaluated all-or-nothing when they share a store.
- **Explicit failure policies** for Redis outages: fail closed, fail open, or
  fall back to local limits. There is no silent default.
- **`net/http` middleware** that ignores forwarding headers unless the
  request comes from a trusted proxy. It hashes identities, bounds key
  growth, and offers `Retry-After`, legacy headers and draft IETF
  `RateLimit` headers.
- **Observability** hooks that never expose keys. A `log/slog` observer is
  built in, and Prometheus is available as a separate module.
- The core module depends only on the standard library.

```sh
go get github.com/mjahidhasand10/rategen             # core, memory, httplimit
go get github.com/mjahidhasand10/rategen/redis       # Redis store
go get github.com/mjahidhasand10/rategen/prometheus  # Prometheus metrics
```

Requires Go 1.26 or newer.

## Quick start

### Pace work in one process

```go
limiter, err := tokenbucket.New(rategen.PerSecond(100), 20) // 100/s, bursts of 20
if err != nil {
    log.Fatal(err)
}
if limiter.Allow() {
    // do the work
}
err = limiter.Wait(ctx) // or block until allowed (honours ctx)
```

### Limit per client

```go
store, _ := memory.New(memory.Config{MaxKeys: 100_000})
perClient, _ := store.GCRA("per-client", rategen.PerSecond(10), 20)

d, err := perClient.AllowN(ctx, clientID, 1)
if !d.Allowed {
    // reject; d.RetryAfter says when to try again
}
```

`AllowN` always returns a usable `Decision`. Act on `d.Allowed`; `err`
explains decisions that were not made normally (see
[docs/failure.md](docs/failure.md)).

### Protect an HTTP API

```go
ips, _ := httplimit.NewIPExtractor(httplimit.IPConfig{
    TrustedProxies: must(httplimit.ParsePrefixes("10.0.0.0/8")), // your load balancers
})
mw, err := httplimit.New(httplimit.Config{
    Rules: []httplimit.Rule{
        {Limiter: perClient, Key: ips.Key()},
        {Limiter: logins, Key: ips.Key(), Match: isLogin},
    },
    Skip:    func(r *http.Request) bool { return r.URL.Path == "/healthz" },
    Headers: httplimit.HeadersDraft11,
})
http.ListenAndServe(":8080", mw.Handler(mux))
```

Rejected requests get `429 Too Many Requests` with `Retry-After`.

### Several limits at once

```go
res, err := rategen.Evaluate(ctx,
    rategen.Check{Limiter: perSecond, Key: apiKey, N: 1},
    rategen.Check{Limiter: daily, Key: apiKey, N: 1},
    rategen.Check{Limiter: global, Key: "all", N: 1},
)
// res.Allowed only if all three admit; if one rejects, none is consumed.
```

### Share limits across instances

```go
client := goredis.NewClient(&goredis.Options{Addr: "redis:6379", ContextTimeoutEnabled: true})
store, err := redis.New(client, redis.Config{
    Prefix:  "myservice",
    Failure: rategen.FailClosed, // required: what to do if Redis is down
    Timeout: 50 * time.Millisecond,
})
api, _ := store.GCRA("api", rategen.PerSecond(100), 50)
```

The Redis store offers the same algorithms and works with `Evaluate` and
`httplimit` unchanged.

## Choosing an algorithm

| Use | Algorithm |
|---|---|
| Request rates for APIs, smooth with bursts | `GCRA` (or `TokenBucket`) |
| Pacing work, waiting for capacity | `tokenbucket` package, or memory `TokenBucket` with `WaitN` |
| Quotas in calendar units ("1000 per day") | `FixedWindow` |
| "N per minute" without the fixed-window boundary burst | `SlidingWindow` |

Details, formulas and worst cases are in [docs/algorithms.md](docs/algorithms.md).

| Capability | Token bucket | GCRA | Fixed window | Sliding window |
|---|---|---|---|---|
| Allow / AllowN | ✓ | ✓ | ✓ | ✓ |
| Atomic multi-limit (`Evaluate`) | ✓ | ✓ | ✓ | ✓ |
| Refund | ✓ | ✓ | ✓ | ✓ |
| Reserve / Wait (memory only) | ✓ | ✓ | – | – |
| Redis | ✓ | ✓ | ✓ | ✓ |

## Documentation

- [Algorithms](docs/algorithms.md): semantics, accuracy, boundary behaviour.
- [HTTP middleware, identities and policies](docs/http.md): rules, proxies
  and response headers.
- [Distributed limits with Redis](docs/distributed.md): atomicity, clocks,
  keys, Cluster and operations.
- [Errors and failure policies](docs/failure.md)
- [Concurrency, memory and cleanup](docs/memory.md)
- [Observability](docs/observability.md)
- [Security and threat model](docs/security.md)
- [Benchmarks and methodology](docs/benchmarks.md)
- [Compatibility and migration](docs/compatibility.md)
- [Comparison with other libraries](docs/comparison.md)
- [Design](docs/design.md) and [release checklist](docs/release-checklist.md)

Runnable examples: [basic](examples/basic), [HTTP API](examples/httpapi),
[per-user](examples/peruser), [multiple policies](examples/multipolicy),
[distributed with Redis](redis/examples/distributed).

## Development

```sh
scripts/redis-up.sh                    # Redis in Docker for integration tests
export RATEGEN_REDIS_ADDR=127.0.0.1:6379
for m in . redis prometheus benchmarks; do (cd $m && go test -race ./...); done
```

The repository is a Go workspace (`go.work`) of four modules: the core,
`redis`, `prometheus` and `benchmarks` (comparisons only, never imported).
Without `RATEGEN_REDIS_ADDR`, the Redis integration tests are skipped.

## License

MIT. See [LICENSE](LICENSE).
