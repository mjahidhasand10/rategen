# Comparison with established Go rate limiters

This page records what four widely used Go rate-limiting packages **document**
about themselves, as reviewed on 2026-10-15 from their pkg.go.dev pages and
READMEs. It is not a benchmark and makes no claim that rategen is better than
any of them. Where a project does not document something, the table says
"not documented" rather than guessing.

| | `golang.org/x/time/rate` | `didip/tollbooth` | `ulule/limiter` (v3) | `go-redis/redis_rate` |
|---|---|---|---|---|
| Algorithm | Token bucket | Token bucket (via `x/time/rate`) | Not named; README describes a per-key counter with expiry (fixed-window style) | GCRA ("aka leaky bucket") |
| Keyed (per-client) limits | No (single limiter) | Yes (IP, path, method, headers, basic-auth user) | Yes (store key, default client IP) | Yes (caller-supplied key) |
| Distributed state | No | No ("does not require external storage") | Yes (Redis store) | Yes (Redis only) |
| Allow / Reserve / Wait | All three, plus `Reservation.Cancel` | Allow (HTTP) | Allow-style `Get` / `Peek` | `Allow`, documented in README |
| HTTP middleware | No | `net/http` | `net/http`, FastHTTP, Gin (examples for others) | No |
| Client IP trust | n/a | Explicit `SetIPLookup` choice required (v8) | Forwarding headers off by default; opt-in `TrustForwardHeader`, `ClientIPHeader` | n/a |
| Response headers | n/a | `X-Rate-Limit-*` and draft `RateLimit-*` | `X-Ratelimit-*` (per README) | n/a |
| Memory cleanup | n/a | Expirable buckets with TTL | go-cache fork with a background cleanup goroutine | Redis TTL |
| License | BSD-3-Clause | MIT | MIT | BSD-2-Clause |

## Observations

These are the design pressures rategen takes from the survey, phrased as
goals rather than as criticisms:

- `x/time/rate` has a carefully specified single-limiter API (Allow, Reserve,
  Wait, Cancel). rategen's `tokenbucket` package offers the same set of
  operations with its own implementation and an injectable clock.
- The keyed HTTP libraries make IP-header trust an explicit decision.
  rategen keeps that, and goes further by requiring trusted-proxy CIDRs
  rather than a header name alone, so a header is only honoured when the
  immediate peer is a configured proxy.
- None of the surveyed packages documents a single API covering several
  algorithms across both local and Redis backends, nor all-or-nothing
  evaluation of several limits for one request. rategen focuses on those two
  areas.
- Redis-backed decisions are only as good as their atomicity and failure
  handling. rategen evaluates every decision inside one Lua script, uses the
  Redis server clock, and refuses to start without an explicit failure policy.

Nothing from these projects is copied into rategen. They are referenced only
for documented behaviour, and their licenses (BSD/MIT) impose no obligations
on rategen unless code is reused, which it is not. The `benchmarks` module
imports them solely for comparison benchmarks.

## Sources

- https://pkg.go.dev/golang.org/x/time/rate
- https://github.com/didip/tollbooth
- https://github.com/ulule/limiter
- https://github.com/go-redis/redis_rate
