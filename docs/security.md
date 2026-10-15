# Security considerations and threat model

A rate limiter reduces the impact of abusive traffic. It does not make a
service immune to denial of service. Volumetric attacks must be absorbed
before they reach the application: at the network edge, the load balancer
or a CDN. This page describes what rategen defends against, how, and what
remains your responsibility.

## Assets and attackers

- **Assets:** the limits themselves (for example, a login-attempt limit
  protecting accounts); the availability of the protected service; and the
  identities used as keys (IP addresses, API keys, user IDs).
- **Attackers:** unauthenticated clients sending arbitrary requests and
  headers; authenticated clients trying to exceed their quotas; and anyone
  who can read logs, metrics or Redis contents.

## Threats and mitigations

| Threat | Mitigation | Residual risk |
|---|---|---|
| **Spoofed client IP** via `X-Forwarded-For`, `X-Real-IP` etc. | `ByIP()` and the default `IPExtractor` use only `RemoteAddr`. Forwarding headers are read only when the direct peer is in `TrustedProxies`, and then right to left, skipping trusted hops, so entries a client prepends are never reached. Single-value headers must appear exactly once. | Listing a network that clients can send from (for example `0.0.0.0/0`, or a shared cloud range) re-enables spoofing. A trusted proxy that does not append correctly breaks the guarantee. |
| **IPv6 address rotation** (one host using many addresses) | IPv6 keys are aggregated to /64 by default (`IPConfig.IPv6Prefix`). | Clients with larger allocations (/48) can still rotate. Lower the prefix if needed, at the cost of grouping unrelated users. |
| **Attacker-controlled high-cardinality keys** (random IPs, random API keys) | `memory.Store` is bounded by `MaxKeys`. Each key is at most `httplimit.MaxKeyLen` bytes (default 128; longer keys are hashed). Identity values over `MaxIdentityLen` (4 KiB) are rejected. Redis keys expire automatically. | Under `EvictLRU`, a flood evicts idle keys and resets their limits. Under `RejectNew`, it denies new clients. Pair an unverified-identity rule (API key) with a per-IP rule, or rate-limit after authentication. |
| **Bypass through missing identity** | A missing key fails the request (400) unless the rule is explicitly `Optional`. An empty key never means "unlimited". | `Optional` rules are skipped by design, so cover anonymous traffic with another rule. |
| **Unverified identities** (a client inventing API keys for fresh limits) | Documented on `ByHeader`. | Validate credentials before limiting by them, or combine with per-IP limits. |
| **API-key and identity leakage** | `ByHeader` hashes values (SHA-256 by default, `HMACSHA256` with a secret recommended). The Redis store hashes every identity again before building keys. `Event` has no key field, and error messages contain no keys or request data. The default error responses are generic. | `ByFunc` with a nil hasher, and memory-store keys, keep raw identities in process memory. Custom `OnReject`/`ErrorLog` handlers must not echo secrets. |
| **Offline guessing of hashed identities** (leaked Redis dump) | `HMACSHA256` with a secret that is not stored in Redis. | Unkeyed SHA-256 of low-entropy values (emails, numeric IDs) can be brute-forced. |
| **Tenant collisions in Redis** | Keys are namespaced by `Prefix`, policy name and algorithm, and the identity is a fixed-length hash, so different tenants or policies cannot collide or inject key syntax. Prefixes and policy names are restricted to `[A-Za-z0-9_.:-]`. | Two applications that deliberately share a prefix and policy name share a limit. |
| **Race conditions / lost updates** | Memory: per-shard mutexes cover every read-modify-write, and batches lock their shards in a fixed order (no deadlock). Redis: each decision and batch is one Lua script, which Redis runs serially. The race detector runs over all tests, including multi-instance Redis tests. | None known. Report a reproduction if you find one. |
| **Partial consumption across limits** | `Evaluate` is atomic within one store. Mixed stores fall back to refunds, and `Result.Atomic` reports which path ran. | On the non-atomic path, capacity is briefly consumed before the refund. A limiter without `Refunder` keeps it. |
| **Clock manipulation / drift** | Local stores use the monotonic clock, so wall-clock steps have no effect. Redis uses the server clock, so client clocks do not matter. Time going backwards is treated as no time passing. | A forward step of the Redis primary's clock refills capacity early. |
| **Failure-policy bypass** (degrading Redis to lift limits) | No default policy. `FailClosed` keeps limits during outages. Timeouts bound how long an attacker can stall a decision. | `FailOpen` lifts limits by design. `FailToLocal` multiplies them by the instance count. |
| **Expensive identity extraction** | Built-in `KeyFunc`s do bounded work: at most 16 forwarding-header entries are examined (empty entries count), identities are capped at 4 KiB, and each rule computes at most one hash. | Custom `KeyFunc`s run before any limit. Keep them cheap and avoid I/O. |
| **High-cardinality metrics** | The Prometheus observer labels only by policy, algorithm, backend, result and error kind. | Do not generate policy names from request data. |
| **Numeric overflow / precision loss** | Go arithmetic saturates instead of wrapping. The Redis store rejects limits above 2^50 and refill periods over 100 years, keeping every value exact in Lua's doubles. | None known. |
| **Goroutine leaks** | No package starts goroutines on its own. `RunJanitor` runs in the caller's goroutine and stops with its context. Waits stop their timers on return. | None. |
| **Information in rejections** | The default 429 body is a fixed string. `Retry-After` and optional headers reveal only the limit, remaining count and timing. | `HeadersDraft11` reveals policy names. Choose names that are safe to publish. |

## Recommendations

1. Behind a proxy, set `TrustedProxies` to exactly the proxies' addresses,
   and make the proxy overwrite or append the forwarding header.
2. Rate-limit by verified identity (after authentication) for per-user
   quotas. Keep an IP-based rule in front for unauthenticated traffic.
3. Use `HMACSHA256` for API keys and user IDs, with a secret from your
   secret manager.
4. Size `memory.Config.MaxKeys` for legitimate peak cardinality plus margin.
   Watch `Stats().Evictions`.
5. Choose `FailClosed` for security-relevant limits, and alert on store
   errors whatever the policy.
6. Put volumetric protection in front of the application. rategen spends a
   few hundred nanoseconds per request in memory, and one Redis round trip
   when distributed. Under a large enough flood, that cost alone matters.

## Reporting vulnerabilities

Please report suspected vulnerabilities privately to the maintainers through
GitHub's "Report a vulnerability" feature on the repository, not in public
issues.
