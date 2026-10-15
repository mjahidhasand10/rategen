# Errors and failure policies

## The contract

`Limiter.AllowN` always returns a usable `Decision`. **Act on
`Decision.Allowed`**. The error explains why the decision was not made
normally. It is not a second verdict.

```go
d, err := limiter.AllowN(ctx, key, 1)
if err != nil {
    log.Printf("rate limit: %v", err) // never contains the key
}
if !d.Allowed {
    // reject
}
```

## Errors by cause

| Situation | Error (`errors.Is`) | Decision |
|---|---|---|
| Invalid configuration | `ErrInvalidConfig` at construction | n/a |
| `n <= 0` | `ErrInvalidCost` | rejected, nothing consumed |
| `n` greater than the capacity | `ErrExceedsCapacity` | rejected, `RetryAfter == 0` (never fits) |
| `ctx` canceled or past its deadline | `context.Canceled` / `DeadlineExceeded` | rejected, regardless of failure policy |
| Memory store full under `RejectNew` | `ErrStoreFull` | rejected |
| Redis unreachable, timed out or erroring | `ErrStoreUnavailable` (wrapping the cause) | per `FailurePolicy` |
| Operation not offered by the limiter | `ErrUnsupported` | n/a |
| Batch cannot be atomic (`EvaluateAtomic`) | `ErrNotAtomic` | nothing consumed |
| HTTP: identity missing | `ErrMissingKey` | middleware responds 400, or skips an `Optional` rule |
| HTTP: unparsable client IP | `httplimit.ErrInvalidClientIP` | middleware responds 400 |
| HTTP: identity header sent more than once | `httplimit.ErrInvalidKey` | middleware responds 400 (never skipped as `Optional`) |
| HTTP: more than `MaxHops` trusted proxies | `httplimit.ErrInvalidClientIP` | middleware responds 400 |

Wait operations return `context.DeadlineExceeded` without consuming
anything when the context deadline is earlier than the time capacity will be
available.

## Failure policies

Distributed stores need an explicit `rategen.FailurePolicy`. The zero value
is invalid, and `redis.New` rejects it. No option is safe for every
deployment, so you have to choose one.

### FailClosed

Requests whose decision cannot be verified are rejected (the middleware
answers 503).

- Preserves the limit exactly.
- A Redis outage becomes an outage of everything behind the limiter.
- Choose it for limits that protect security properties: login and password
  reset attempts, OTP verification, expensive or abusable operations, and
  paid quotas.

### FailOpen

Requests are admitted, and decisions are marked `Degraded`.

- Preserves availability. **No limit is enforced during the outage**, which
  an attacker who can degrade Redis (for example by flooding it) can
  exploit.
- Choose it for limits that exist for fairness or cost smoothing, where a
  brief lapse is acceptable.
- Alert on `rategen_errors_total{kind="store_unavailable"}` or the
  equivalent. Otherwise nothing tells you that limits have stopped.

### FailToLocal

Each Redis limiter has a twin with the same parameters in a `memory.Store`
(`Config.Local`). During an outage, decisions come from the twin and are
marked `Degraded`.

- Keeps per-process protection against floods from a single client.
- **It is not a global limit.** With `k` instances behind a load balancer, a
  client can get up to `k ×` the configured limit. The local state starts
  empty, so the outage effectively resets every limit once.
- Local state is not reconciled with Redis on recovery. Consumption during
  the outage is not charged to the shared limit.
- Batches fall back atomically, because all twins share one memory store.
- Choose it when some protection matters more than exactness, and you can
  bound `k`.

## What counts as a failure

Any error from the Redis round trip, including the store's own `Timeout`, is
a store failure. The caller's context ending is not: in that case the
request is rejected with the context's error, and the failure policy is not
consulted, because there is no caller left to serve.

Refunds (`RefundN`, and the non-atomic path of `Evaluate`) take the
`Decision` that consumed the units. For window algorithms, the refund goes
back to the window that was charged (`Decision.Epoch`), and is dropped if
that window no longer counts. Crediting a later window would admit traffic
the limit never allowed. Refunds are not subject to the failure policy. A failed refund returns an error and leaves the
capacity consumed, which errs towards the limit.

## Testing your policy

`TestOutageAndRecovery` in `redis/store_test.go` shows the pattern. Route
the client through a TCP proxy you can stop or stall, and assert each
policy's decisions and latency. Use `rategen.LimiterFunc` to simulate
failures in middleware tests without Redis:

```go
down := rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
    return rategen.Decision{}, rategen.ErrStoreUnavailable
})
```
