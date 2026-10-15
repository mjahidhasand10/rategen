# HTTP middleware, identities and policies

```go
mw, err := httplimit.New(httplimit.Config{
    Rules: []httplimit.Rule{
        {Limiter: perIP, Key: httplimit.ByIP()},
    },
})
handler := mw.Handler(mux)
```

`httplimit` wraps any `http.Handler`. It passes the original
`ResponseWriter` through, so `http.Flusher`, `http.Hijacker` and
`http.ResponseController` keep working. It also never reads the request
body.

## Rules

Each request is checked against every rule whose `Match` accepts it. All
those checks go to `rategen.Evaluate` together, so they are all-or-nothing
when the limiters share a store. The request proceeds only if every rule
admits it.

| Field | Purpose |
|---|---|
| `Limiter` | Any `rategen.Limiter`: memory, redis, or your own. |
| `Key` | `KeyFunc` deriving the key from the request. |
| `Cost` | Units consumed. The default is 1. Use it to weight expensive endpoints. |
| `Match` | Restricts the rule, for example to `POST /login`. |
| `Optional` | Skip the rule when its key is missing, instead of rejecting with 400. |

Policies to combine:

```go
rules := []httplimit.Rule{
    // Global service limit (one key for everyone).
    {Limiter: global, Key: httplimit.Static("all")},
    // Per client IP.
    {Limiter: perIP, Key: ips.Key()},
    // Per API key, when one is presented.
    {Limiter: perKey, Key: httplimit.ByHeader("X-API-Key", hmac), Optional: true},
    // Per user and route, after authentication middleware.
    {Limiter: perUserRoute, Key: httplimit.Join(httplimit.ByFunc(userID, hmac), httplimit.ByPattern())},
    // Per tenant daily quota.
    {Limiter: tenantDaily, Key: httplimit.ByFunc(tenantID, nil)},
}
```

`ByPattern` uses `http.Request.Pattern`, which `http.ServeMux` sets only
inside the matched handler. Wrap individual routes with the middleware, or
key per route with `Match`. Keying by URL path is not offered, because paths
are attacker-controlled and unbounded.

`Join` length-prefixes each part, so `("ab","c")` and `("a","bc")` give
different keys.

## Client IPs and proxies

```go
proxies, _ := httplimit.ParsePrefixes("10.0.0.0/8", "fd00::/8")
ips, _ := httplimit.NewIPExtractor(httplimit.IPConfig{
    TrustedProxies: proxies,
    Header:         "X-Forwarded-For", // or "Forwarded", "CF-Connecting-IP", "X-Real-IP"
})
```

- With no `TrustedProxies` (the default, and `ByIP()`), only `RemoteAddr`
  is used.
- When the peer is trusted, the header is read **from the right**, skipping
  trusted addresses. The first untrusted address is the client. Anything a
  client wrote to the left is ignored.
- Single-address headers (`CF-Connecting-IP`, `X-Real-IP`) are honoured
  only from trusted peers, and must occur exactly once.
- Unparsable or obfuscated addresses (`unknown`, `_hidden`) from a trusted
  proxy produce `ErrInvalidClientIP` (400).
- IPv6 clients are keyed by /64 by default. IPv4-mapped IPv6 addresses
  (in requests and in `TrustedProxies`) are treated as IPv4.
- At most `MaxHops` entries (default 16, empty entries included) are
  examined. A chain of more trusted proxies than that is refused with
  `ErrInvalidClientIP` (400), rather than keying every client by the last
  proxy's address.
- If the proxy's own `Forwarded` element has no `for=` parameter, the
  request is refused. Skipping to the next element would trust an entry the
  client may have written.

## Responses

| Outcome | Status | Headers |
|---|---|---|
| Allowed | handler's | rate-limit headers, if configured |
| Rejected by a limit | `StatusCode` (429) | `Retry-After` (whole seconds, rounded up; omitted when the request can never fit) plus configured headers |
| Required identity missing or invalid, or an identity header repeated (`ErrInvalidKey`, even for `Optional` rules) | 400 via `OnError` | none |
| Store failed under `FailClosed`, with no other rule rejecting by its limit | 503 via `OnError` | none |
| Store failed under `FailOpen`/`FailToLocal` | handler's; 429 if the local fallback or another rule rejects by its limit | headers omitted for degraded decisions |
| Deadline set upstream passed before the decision | 503 via `OnError` | none |
| Client disconnected before the decision | nothing written | none |

If any rule rejects by its limit, the answer is 429, even when other rules'
backends failed in the same request. The failures still go to
`ErrorLog`. `rategen.Result.RejectedByLimit` makes the same distinction for
your own code.

Error bodies are fixed strings. Errors go to `ErrorLog`, never to the
client. Override `OnReject` for JSON problem documents, and `OnError` to
change error statuses.

### Rate-limit headers

`Retry-After` is always sent on retryable rejections. Choose more with
`Config.Headers`:

**`HeadersLegacy`** reports the most restrictive rule:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 42
X-RateLimit-Reset: 17          (seconds until fully reset)
```

These headers are a convention with no specification. Clients interpret
`Reset` inconsistently (some expect a Unix timestamp), so rategen documents
it as delta-seconds.

**`HeadersDraft11`** follows
[draft-ietf-httpapi-ratelimit-headers-11](https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/)
(23 May 2026), with one item per rule:

```
RateLimit-Policy: "burst";q=10;w=10, "daily";q=1000;w=86400
RateLimit: "burst";r=9;t=1, "daily";r=999;t=86400
```

- `q` is `Decision.Limit` (burst capacity, or window limit). `w` is
  `Decision.Window` in whole seconds, at least 1.
- `r` is `Decision.Remaining`. `t` is `Decision.ResetAfter` in seconds,
  rounded up.
- The `pk` (partition key) parameter is never sent, because it would
  disclose the key.
- The draft is not an RFC. If later drafts change the syntax, rategen will
  add a new `HeaderStyle` rather than change this one.

## Piloting with DryRun

Set `DryRun: true` to run limits against live traffic without enforcing
them:

```go
mw, _ := httplimit.New(httplimit.Config{
    Rules:  rules,
    DryRun: true,
    OnWouldReject: func(r *http.Request, res rategen.Result) {
        d := res.Decision()
        slog.Info("would rate limit", "policy", d.Policy, "path", r.URL.Path)
    },
    ErrorLog: func(r *http.Request, err error) { slog.Warn("rate limit error", "err", err) },
})
```

Every rule is evaluated and capacity is consumed as in enforcing mode, so
the would-be rejections are exactly the ones enforcement would produce.
Nothing is ever rejected. Missing keys and store failures are logged and
the request proceeds. No rate-limit headers are sent, so clients see no
difference. Combined with the Prometheus observer, this shows each policy's
rejection rate before you turn enforcement on.

## Exclusions

`Skip` exempts requests from all rules, for health checks, metrics and
readiness probes. Make sure skipped routes cannot be reached by rewriting
paths in ways your router accepts. `Skip` sees the same `*http.Request` the
router will.

## Composition

Middlewares compose like any `http.Handler` wrapper, and several
`Middleware`s can share limiters. When nested middlewares both send
headers, `HeadersDraft11` lists every policy from both, and `HeadersLegacy`
keeps the values of whichever limit has fewer units remaining.
