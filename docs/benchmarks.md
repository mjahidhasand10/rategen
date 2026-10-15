# Benchmarks, soak tests and methodology

These numbers come from one low-power laptop. Use them to compare
implementations on equal terms, not to predict production throughput. The
raw output of every run is in
[`benchmarks/results/2026-10-15/`](../benchmarks/results/2026-10-15/).

## Environment

| | |
|---|---|
| CPU | AMD Ryzen 3 3250U (2 cores, 4 threads), laptop power profile |
| OS | Windows 11 |
| Go | go1.26.9 windows/amd64, `GOMAXPROCS=4` |
| Redis | 7.4.11 in Docker Desktop (WSL2), reached over the Docker port forward |
| Command | `go test -run '^$' -bench . -benchmem -count 6 -benchtime 1s` |
| Statistics | median of 6 runs (`benchstat`); `±` is the 95% confidence interval |

Network latency to Redis here is dominated by Docker Desktop's WSL2 port
forwarding, at about 1.4 ms per round trip. On a LAN, expect a few hundred
µs or less. The Redis figures mostly measure that forwarding, so read them
relative to the `PING` baseline. Results with a large `±` (over 50%) were
noisy on this machine and should not be relied on.

## rategen alone

### Memory store (`memory/bench_test.go`)

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| Allow, token bucket | 289 | 0 | 0 |
| Allow, GCRA | 249 | 0 | 0 |
| Allow, fixed window | 214 | 0 | 0 |
| Allow, sliding window | 235 | 0 | 0 |
| Reject, token bucket | 231 | 0 | 0 |
| Reject, GCRA | 241 | 0 | 0 |
| Reject, fixed window | 213 | 0 | 0 |
| Reject, sliding window | 286 | 0 | 0 |
| One hot key, 4 goroutines, GCRA | 409 | 0 | 0 |
| One hot key, 4 goroutines, fixed window | 380 | 0 | 0 |
| Independent keys, 4 goroutines, GCRA | 123 (±31%) | 0 | 0 |
| New key per call into a full 100k-key store (insert + LRU eviction) | 1,095 | 100 | 1 |
| Atomic batch of 2 limits (`Evaluate`) | 1,026 | 448 | 3 |

The admit and reject paths make no allocations. In the hot-key case,
throughput is bounded by the key's shard mutex.

### Single token bucket (`tokenbucket`)

| Benchmark | ns/op | allocs/op |
|---|---|---|
| Allow | 137 (±20%) | 0 |
| Allow, rejected | 143 | 0 |
| Allow, 4 goroutines | 221 | 0 |

### HTTP middleware (`httplimit`, per-IP GCRA, no-op handler)

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| No middleware | 12 | 0 | 0 |
| With middleware | 800 | 160 | 4 |
| With middleware and draft-11 headers | 1,393 (±185%) | 248 | 8 |

### Redis store (`redis/store_test.go`)

| Benchmark | serial | 4 goroutines | B/op | allocs/op |
|---|---|---|---|---|
| `PING` baseline | 1.45 ms | n/a | 148 | 3 |
| Token bucket | 1.51 ms | 540 µs | 1.2 KiB | 22 |
| GCRA | 1.51 ms | 538 µs | 1.2 KiB | 21 |
| Fixed window | 1.49 ms | 533 µs | 1.2 KiB | 24 |
| Sliding window | 1.53 ms | 549 µs | 1.2 KiB | 24 |
| Batch of 2 limits | 1.55 ms | n/a | 1.9 KiB | 31 |

Every Redis decision, including a batch of several limits, is within about
7% of a bare `PING`. The script itself costs little next to the network
round trip, and a batch costs one round trip, not one per limit.

## Comparisons (`benchmarks/compare_test.go`)

Each pair uses the same algorithm family and the same parameters, and
measures the admit path unless noted. The comparisons are only as equal as
the libraries allow; see the caveats under each table.

### Single in-process token bucket vs `golang.org/x/time/rate` v0.15.0

| Benchmark | rategen | x/time/rate |
|---|---|---|
| Serial | 113 ns | **71 ns** |
| 4 goroutines | 243 ns (±127%) | **155 ns** |
| Rejected | 130 ns | **74 ns** |

`x/time/rate` is faster in all three cases. rategen's bucket reads its
monotonic timeline and computes the full decision (remaining, retry and
reset times, the retry verified against the refill arithmetic) on every
call. Neither allocates.

### Keyed in-memory counters vs `ulule/limiter` v3.11.2 memory store

| Benchmark | rategen fixed window | ulule |
|---|---|---|
| One key | 220 ns (±32%), 0 allocs | 220 ns, 2 allocs |
| 10k keys, 4 goroutines | **109 ns**, 0 allocs | 156 ns, 2 allocs |

Caveat: ulule's store is a per-key counter with expiry. The two have similar
fixed-window semantics but are not identical implementations. ulule's
memory store also runs a background cleanup goroutine, which rategen does
not.

### `net/http` middleware by RemoteAddr vs tollbooth v8.0.1 and ulule

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| No middleware | 8.3 | 0 | 0 |
| rategen (`httplimit`, token bucket) | **757** | 160 | 4 |
| tollbooth (token bucket via x/time/rate) | 5,136 | 584 | 25 |
| ulule stdlib middleware (fixed window) | 1,928 | 240 | 14 |

Caveats:

- With default settings, tollbooth and ulule write several response headers
  on every request, while rategen writes none (`HeadersNone`). With
  draft-11 headers, rategen measured 1,393 ns and 8 allocs (see above).
- ulule uses a fixed window, the others a token bucket.

### Redis GCRA vs `go-redis/redis_rate` v10.0.1

Both are GCRA, use the same Redis server and client, and run the same
parameters.

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `PING` baseline | 1.41 ms | 148 | 3 |
| rategen | 1.53 ms | 1.2 KiB | 21 |
| redis_rate | 1.46 ms (±32%) | **632** | **19** |
| rategen, 4 goroutines | 553 µs | 1.2 KiB | 21 |
| redis_rate, 4 goroutines | **480 µs** | **632** | **19** |

redis_rate is faster under concurrency here, and allocates about half as
much per call. rategen's script is more general: it covers four algorithms,
multi-limit batches and refunds in one script, and passes more arguments.
Serial latency is within noise of each other and of the network.

## Soak tests

Soak tests run with `RATEGEN_SOAK=<duration>` and are skipped otherwise.

| Test | Duration | Load | Result |
|---|---|---|---|
| `httplimit.TestSoak`: middleware + memory store (`MaxKeys` 50,000), janitor running, a quarter of traffic from 8 hot clients and the rest from never-repeating addresses | 5 min | 55.7M requests (186k/s) | keys ≤ 50,000 throughout (49,999 at the end), 41.6M evictions, heap flat at 19–20 MiB, no goroutine leak; p99 553 µs, max 16 ms |
| `redis.TestSoakRedis`: 16 goroutines mixing hot keys, unique identities and batches; a tracked key checked against its per-window limit | 3 min | 620k calls, 194k unique identities | no errors, tracked limit never exceeded, every key expired afterwards, no goroutine leak; p50 4.4 ms, p99 10.8 ms |

An earlier 10-minute run of each, on code from before the final review
fixes, also passed: 99M requests with a flat 19–24 MiB heap, and 2M Redis
calls.

## Reproducing

```sh
scripts/redis-up.sh && export RATEGEN_REDIS_ADDR=127.0.0.1:6379
go test -run '^$' -bench . -benchmem -count 6 ./memory/ ./tokenbucket/ ./httplimit/ > core.txt
(cd benchmarks && go test -run '^$' -bench . -benchmem -count 6 ./...) > compare.txt
(cd redis && go test -run '^$' -bench . -benchmem -count 6 ./...) > redis.txt
go run golang.org/x/perf/cmd/benchstat@latest core.txt compare.txt redis.txt
RATEGEN_SOAK=10m go test -run TestSoak -v ./httplimit/
(cd redis && RATEGEN_SOAK=10m go test -run TestSoakRedis -v ./)
```

Record your CPU, OS, Go version and Redis deployment with the results.
Compare only results from the same machine. Never edit published numbers
by hand: regenerate them.
