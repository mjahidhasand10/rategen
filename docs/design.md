# rategen design

## Scope

rategen is a rate-limiting toolkit for Go services:

- four algorithms (token bucket, fixed window, sliding-window counter, GCRA);
- a single-key local limiter (`tokenbucket`) for in-process pacing;
- a keyed in-memory store (`memory`) and a Redis store (`redis` sub-module);
- all-or-nothing evaluation of several limits for one request;
- `net/http` middleware with safe client identification (`httplimit`);
- vendor-neutral observation hooks, with Prometheus in a sub-module.

Out of scope for v0: framework adapters (Gin, Echo, Fiber), concurrency
limiters (in-flight request caps), and adaptive or feedback-based limiting.

## Supported versions

- Go: the two most recent stable releases. The `go` directive is
  `1.26.0`; CI tests 1.26 and 1.27.
- Redis: 6.2, 7.2, 7.4 and 8.2, Valkey 8.1, and Redis Cluster are tested. See
  [distributed.md](distributed.md).

## Modules

| Module | Purpose | Dependencies |
|---|---|---|
| `github.com/mjahidhasand10/rategen` | Core API, algorithms, memory store, tokenbucket, httplimit, ratetest | Standard library only |
| `github.com/mjahidhasand10/rategen/redis` | Redis store | core, `github.com/redis/go-redis/v9` |
| `github.com/mjahidhasand10/rategen/prometheus` | Prometheus observer | core, `github.com/prometheus/client_golang` |
| `github.com/mjahidhasand10/rategen/benchmarks` | Comparison benchmarks (not for import) | core, redis, competitors |

A `go.work` file at the repository root ties the modules together for local
development. Each sub-module is tagged separately (`redis/v0.1.0`, …).

## Layers

```
            httplimit (net/http)            your code
                   │                            │
                   ▼                            ▼
   rategen core: Limiter, Decision, Check, Evaluate, FailurePolicy, Observer
                   │
       ┌───────────┴────────────┐
       ▼                        ▼
  memory.Store               redis.Store            (backends)
       │                        │
       └──── internal/algo ─────┘ (Go)  /  Lua script (same math)
```

- **Algorithms** (`internal/algo`) are pure functions:
  `(state, now, n, params) → (state', Decision)`. They know nothing about
  locking, storage or HTTP.
- **Backends** own state and concurrency. The memory store holds per-key state
  in sharded maps under mutexes. The Redis store runs the same math inside a
  Lua script, using the Redis server clock.
- **Core** defines the contracts every backend implements, and composes them
  (multi-limit evaluation, failure policy, observation).
- **Middleware** maps an `*http.Request` to keys and costs, and turns
  decisions into responses. It never sees algorithm state.

## Core contracts

```go
type Limiter interface {
    AllowN(ctx context.Context, key string, n int64) (Decision, error)
}
```

`AllowN` always returns a usable `Decision`. A non-nil error explains why the
decision could not be made normally. `Decision.Allowed` then reflects the
limiter's configured failure behaviour, and callers must act on `Allowed`, not
on the error alone.

Optional capabilities are discovered by interface assertion:

| Interface | Meaning | Implemented by |
|---|---|---|
| `Reserver` | Consume now, act later; cancellable | memory token bucket and GCRA |
| `Waiter` | Block until allowed or `ctx` is done | memory token bucket and GCRA |
| `Refunder` | Return previously consumed capacity | memory (all), redis (all) |
| `Batcher` | Atomic evaluation with other limiters of the same backend | memory, redis |

Fixed and sliding windows cannot schedule future admissions meaningfully, so
they do not implement `Reserver` or `Waiter`. Redis limiters do not implement
them either, because a blocking distributed wait would hold no lock and could
not be cancelled atomically.

## Multiple limits

`rategen.Evaluate(ctx, checks...)` evaluates several `(limiter, key, n)`
checks for one request:

1. If every limiter is a `Batcher` from the same backend group, the backend
   evaluates all checks atomically. Either all are consumed or none are.
2. Otherwise, the checks are evaluated in order. On the first rejection, the
   checks already consumed are refunded through `Refunder`. This is **not
   atomic**: between consumption and refund, other callers can observe the
   temporarily reduced capacity. `Result.Atomic` reports which path ran.

## Failure behaviour

Distributed stores require an explicit `rategen.FailurePolicy`:
`FailClosed`, `FailOpen` or `FailToLocal`. The zero value is invalid, and the
constructor rejects it. See [failure.md](failure.md).

## Threat model (summary)

The full treatment is in [security.md](security.md). Its main points:

- Client IPs come from `RemoteAddr` unless the peer is a configured trusted
  proxy.
- Identities that may be secret (API keys, session tokens) are hashed before
  use as keys, and are never logged.
- Memory usage is bounded by `memory.Config.MaxKeys`. Keys longer than the
  middleware limit are hashed, so their size is bounded too.
- Redis keys contain a hash of the identity, never the identity itself, and
  are namespaced by prefix and policy name.
- Metrics labels are limited to policy, algorithm, backend and outcome.
