# Distributed limits with Redis

```sh
go get github.com/mjahidhasand10/rategen/redis
```

```go
client := goredis.NewClient(&goredis.Options{
    Addr:                  "redis:6379",
    ContextTimeoutEnabled: true, // required, see "Timeouts"
})
store, err := redis.New(client, redis.Config{
    Prefix:  "myservice",
    Failure: rategen.FailClosed, // required, see failure.md
    Timeout: 50 * time.Millisecond,
})
api, err := store.GCRA("api", rategen.PerSecond(100), 50)
```

The Redis store offers the same constructors as the memory store
(`TokenBucket`, `GCRA`, `FixedWindow`, `SlidingWindow`) and returns
`*redis.Limiter`, which implements `rategen.Limiter`, `Refunder`, `Batcher`
and `Describer`.

## Local versus distributed

| | memory | redis |
|---|---|---|
| Scope of a limit | one process | every process using the same Redis and prefix |
| Latency | ~0.1–0.4µs on the reference machine | one network round trip |
| Clock | process monotonic clock | Redis server clock |
| Reserve / Wait | token bucket, GCRA | not supported |
| Atomic batches | always | single node: always. Cluster: only for checks on the same identity |
| Failure modes | none (bounded memory) | network, timeouts, server errors, so a `FailurePolicy` is required |
| Time resolution | 1ns | 1µs (GCRA emission intervals are rounded up to whole µs) |
| Largest limit / burst | 2^63−1 | 2^50 (`redis.MaxLimit`, keeps Lua's double arithmetic exact) |

Algorithms use the same arithmetic in both stores, and a parity test checks
that identical request sequences give identical decisions. The only
differences are the clock and the time resolution above.

## Atomicity

Every decision runs as one Lua script (`EVALSHA`, falling back to `EVAL`
when the script is not yet cached). Redis runs scripts one at a time, so a
read-modify-write is never interleaved with another client's, even across
many application instances. There is no client-side locking.

A batch from `rategen.Evaluate` runs as one script, which applies every
check or none. If the same key appears twice in a batch, both checks apply
in order.

`go-redis` may retry a command after a network error. If the first attempt
reached Redis but its reply was lost, the retry consumes capacity again.
This errs towards limiting. Set `MaxRetries: -1` on the client to disable
retries.

## Clock

Scripts read the time with `TIME` on the Redis server, so application hosts'
clocks do not need to agree. Writing after `TIME` requires effects
replication: the default since Redis 5, and the only mode in Redis 7. The
script enables it on older servers where needed. On failover, the new
primary's clock takes over. A clock step on the primary affects all limits:
a backward step pauses refills, and a forward step grants extra capacity
proportional to the step.

## Keys

```
<prefix>:{<identity hash>}:<policy>:<algorithm>
```

- The identity (the key passed to `AllowN`) is hashed with SHA-256 and
  truncated to 128 bits, so IP addresses, user IDs and tokens are never
  stored in Redis. Keys are at most about 160 bytes, regardless of the
  identity's length.
- `Prefix` separates applications and environments that share a database.
  Policy names separate limits. The algorithm suffix keeps state from being
  misread if a policy changes algorithm.
- Each key is a hash with fields `a`, `b`, `c`, `f`. Every write sets a
  `PEXPIRE` equal to the time until the state is equivalent to fresh, so idle
  keys disappear on their own and no cleanup job is needed. An expired key is
  indistinguishable from one never used, so eviction under `maxmemory` with
  `volatile-*` or `allkeys-*` policies only ever resets limits; it never
  corrupts them.

Do not share a prefix between unrelated services that may reuse policy
names. Changing a policy's rate under the same name keeps existing state;
the new parameters apply from the next request.

## Redis Cluster

All keys of one identity share a hash slot, because the hash tag is the
identity hash. So:

- single-check decisions always work;
- batches whose checks all use the **same key string** are atomic;
- batches mixing identities (a per-IP and a global limit, say) span slots,
  so `AllowBatch` returns `ErrNotAtomic` and `Evaluate` falls back to
  sequential evaluation with refunds. `Result.Atomic` is then false.
  `EvaluateAtomic` refuses such batches.

Cluster mode is detected for `*goredis.ClusterClient` and `*goredis.Ring`.
Set `Config.Cluster` for other sharded clients.

A global limit (one key for all callers) lives on a single node and is
limited by that node's throughput. Shard it into N keys
(`"global-" + strconv.Itoa(hash%N)`), each with 1/N of the limit, if
necessary. This trades exactness for throughput.

## Timeouts

Each decision runs under `context.WithTimeout(ctx, Config.Timeout)`,
default 100ms. go-redis applies context deadlines to network I/O only when
`ContextTimeoutEnabled` is set on the client. Otherwise a stalled server
blocks for the client's `ReadTimeout` (3s by default). `redis.New`
therefore rejects `*Client`, `*ClusterClient` and `*Ring` without it.

If the caller's own context ends first, the decision is a rejection carrying
the context's error, whatever the failure policy, because there is no longer
a caller to serve.

## Operations

- Size: one hash per active (identity, policy) pair. `MEMORY USAGE`
  reports 144 bytes for a typical key on Redis 7.4 (excluding the expiry
  index entry). Measure with your own prefix and policy names.
- Persistence is not required. Losing state resets limits; it never makes
  them stricter.
- Latency: each decision is one round trip plus a few microseconds of script
  time. Batching N limits into one `Evaluate` costs one round trip, not N.
- Monitor `rategen_errors_total{kind="store_unavailable"}` (see
  [observability.md](observability.md)) and alert on it. Under `FailOpen`,
  this metric is the only signal that limits are not being enforced.
- Tested in CI against Redis 6.2, 7.2, 7.4 and 8.2, Valkey 8.1, and a
  three-node Redis 7.4 Cluster. The script uses only `TIME`, `HMGET`,
  multi-field `HSET` (Redis 4.0+), `PEXPIRE` and `DEL`.
