# Compatibility and migration

## Versioning

rategen follows semantic versioning. Before v1.0.0, minor releases may make
breaking changes, and each one will be listed in the release notes with
migration steps. After v1.0.0, the exported API of each module stays
backward compatible within a major version.

Each module is versioned and tagged separately:

| Module | Tag format |
|---|---|
| `github.com/mjahidhasand10/rategen` | `vX.Y.Z` |
| `github.com/mjahidhasand10/rategen/redis` | `redis/vX.Y.Z` |
| `github.com/mjahidhasand10/rategen/prometheus` | `prometheus/vX.Y.Z` |

Sub-modules state the minimum core version they need in their `go.mod`.

Not covered by compatibility guarantees:

- packages under `internal/`;
- the exact text of error messages (use `errors.Is` with the exported
  sentinels);
- the Redis key layout and Lua script, across minor versions. A change is
  announced in the release notes, and it resets limits once, because new
  keys start fresh.

`HeadersDraft11` will keep emitting draft-11 syntax. A later draft will get
a new `HeaderStyle` constant. Policy names, `Decision` fields and the
semantics documented in [algorithms.md](algorithms.md) are part of the API.

## Supported Go versions

The two most recent major Go releases (currently 1.26 and 1.27), matching
the Go project's own support policy. Build with the latest patch release:
older patches of the standard library have known vulnerabilities that
`govulncheck` reports.

## Migrating from other libraries

| From | To |
|---|---|
| `x/time/rate.NewLimiter(r, b)` | `tokenbucket.New(rategen.Rate{...}, b)`. `Allow`, `AllowN`, `Reserve`, `ReserveN` (with an explicit max wait), `Wait` and `WaitN` correspond directly. There is no `SetLimit`; create a new limiter instead. |
| A map of `*rate.Limiter` per client | `memory.Store.TokenBucket(name, rate, burst)` and `AllowN(ctx, key, 1)`. The store bounds and expires the map for you. |
| `tollbooth` | `httplimit` with `ByIP()`, or with an `IPExtractor` and `TrustedProxies` instead of `SetIPLookup`. |
| `ulule/limiter` (`"100-M"`) | `store.FixedWindow(name, rategen.PerMinute(100))`. For Redis, use `redis.Store` with an explicit `Failure` policy. |
| `go-redis/redis_rate` (GCRA) | `redis.Store.GCRA(name, rate, burst)`. Same algorithm, also timed by the Redis clock. Keys differ, so limits restart at deployment. |
