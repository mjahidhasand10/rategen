# Concurrency, memory and cleanup

## Concurrency guarantees

- Every exported type with methods (`tokenbucket.Limiter`, `memory.Store`,
  `memory.Limiter`, `redis.Store`, `redis.Limiter`, `httplimit.Middleware`,
  `httplimit.IPExtractor`, `ratetest.FakeClock`) is safe for concurrent use.
  `Reservation` values may be canceled from any goroutine.
- A decision is linearizable per key. In memory, it runs entirely under the
  key's shard mutex. In Redis, it is one script.
- `Evaluate` on limiters of one store is linearizable across all the keys
  involved. Memory locks the shards in ascending order (no deadlock); Redis
  runs one script.
- Limiters hold no per-call state, so any number of goroutines can share
  them. Under heavy contention on one hot key, throughput is bounded by that
  key's mutex (memory) or by Redis's single-threaded execution.
- The race detector runs over the whole test suite, including tests that
  mix decisions, reservations, refunds, batches, sweeps and evictions on a
  real clock.

## Memory usage (memory store)

Each `(policy, key)` pair holds one entry: the key string, the algorithm
state and LRU links, plus Go map overhead. In the high-cardinality
benchmark, which inserts new keys into a full 100k-key store, each insertion
allocates about 96 bytes besides the key string itself
([benchmarks.md](benchmarks.md)). Measure under your
own workload.

The store never holds more than `Config.MaxKeys` entries (default
1,000,000). The bound is split evenly across shards, so a store can start
evicting slightly before `MaxKeys` total when keys hash unevenly.

## When the store is full

When a new key arrives and its shard is full, the store:

1. removes an expired entry, if one is among the 8 least recently used;
2. otherwise applies `OnFull`:
   - `EvictLRU` (default) evicts the least recently used live entry, which
     resets that key's limits;
   - `RejectNew` rejects the request with `ErrStoreFull`, keeping existing
     state.

## Expiry and cleanup

An entry *expires* when its state becomes identical to a fresh key's state:
for example, the bucket is full again, the window has ended, or the TAT is
in the past. Deleting an expired entry therefore never changes a decision.

Expired entries are removed:

- lazily, when the key is used again (its state is reset in place);
- when space is needed (see above);
- by `Store.Sweep(ctx)`, which scans every shard once, locking one shard at
  a time;
- by `Store.RunJanitor(ctx, interval)`, which calls `Sweep` periodically. It
  runs in **your** goroutine (`go store.RunJanitor(ctx, time.Minute)`) and
  returns when `ctx` ends.

No package in rategen starts a goroutine on its own, so there is nothing to
close and nothing to leak. Without a janitor, memory is still bounded by
`MaxKeys`. The janitor only returns memory sooner after traffic spikes.

## Time

See [algorithms.md](algorithms.md#time). Local limiters use the monotonic
clock and accept a `rategen.Clock` for tests (`ratetest.FakeClock`).
