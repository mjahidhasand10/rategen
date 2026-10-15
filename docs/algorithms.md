# Choosing an algorithm

| | Token bucket | GCRA | Fixed window | Sliding-window counter |
|---|---|---|---|---|
| Parameters | rate, burst | rate, burst | limit per window | limit per window |
| State per key | 2 numbers | 1 number | 2 numbers | 3 numbers |
| Smooth admission | yes | yes | no | approximately |
| Worst case in any window `W` | `burst + rate·W` | `burst + rate·W` | `2 × limit` | `< 2 × limit` (rare; see below) |
| Reserve / Wait (memory) | yes | yes | no | no |
| Good for | API request rates, pacing | the same, with integer state | quotas ("1000 per day") | user-facing "N per minute" limits |

If you are unsure, use **GCRA** for request-rate limits and **fixed window**
for quotas that users think of in calendar units.

All algorithms reject a request whose cost `n` exceeds the capacity (burst
for token bucket and GCRA, the limit for windows) with
`ErrExceedsCapacity`. Such a request can never succeed, so its
`RetryAfter` is zero.

## Token bucket

A bucket holds up to `burst` tokens and starts full. Tokens refill
continuously at `rate.Limit / rate.Period`. A request of cost `n` is
admitted if at least `n` tokens are present, and then consumes them.

- `Remaining` is `floor(tokens)` after the decision.
- `RetryAfter` is `(n − tokens) / rate`, rounded up to whole nanoseconds.
- `ResetAfter` is the time until the bucket is full again.
- Tokens are stored as a float64. Comparisons tolerate 1e-9 tokens of
  rounding error. For very slow rates (one token per day), this lets a
  request in up to about 90µs early.
- Reservations may drive the bucket into debt (negative tokens). Later
  callers then wait for the debt to be repaid. `Cancel` before the time to
  act returns the reserved tokens. After that time it does nothing.

## GCRA

The Generic Cell Rate Algorithm keeps a single *theoretical arrival time*
(TAT) per key. The emission interval is `T = Period / Limit`, rounded up to
whole nanoseconds, so the integer arithmetic never admits faster than
configured. The burst tolerance is `τ = T × burst`.

A request of cost `n` arriving at `now` computes
`newTAT = max(TAT, now) + n·T` and is admitted if `newTAT − τ ≤ now`.

- `Remaining` is `floor((τ − (newTAT − now)) / T)`.
- `RetryAfter` is `newTAT − τ − now`.
- `ResetAfter` is `TAT − now`.

GCRA admits exactly the same traffic as a token bucket with the same rate
and burst, apart from rounding. It stores one integer instead of two numbers,
which makes its Redis script slightly cheaper.

## Fixed window

Time is divided into windows of length `Period`, aligned to the Unix epoch
(for example, a 24h window resets at 00:00 UTC). Each key counts units per
window. A request is admitted if `count + n ≤ limit`.

- The window boundary instant belongs to the new window.
- `RetryAfter` and `ResetAfter` are both the time until the window ends.
- **Boundary burst:** a client can use the full limit at the end of one
  window and again at the start of the next, so up to `2 × limit` requests
  can arrive within a span much shorter than one window. Use a sliding
  window, GCRA or token bucket if that matters.

## Sliding-window counter

Each key keeps counts for the current and previous aligned windows. When a
fraction `e` of the current window has elapsed, the estimated count is

```
estimate = previous × (1 − e) + current
```

A request is admitted if `estimate + n ≤ limit`. State is O(1) per key,
regardless of traffic.

- The estimate assumes that the previous window's requests were spread
  evenly. It is exact for steady traffic. It undercounts when the previous
  window's requests were concentrated near its end.
- **Worst case:** a client that saves its whole limit for the last instant
  of a window can be admitted again gradually as the old window's weight
  decays. The count in some rolling window can then approach, but not
  reach, `2 × limit`. The tests check this bound. Unlike a fixed window, the
  second batch is spread over the following window rather than admitted all
  at once at the boundary.
- `RetryAfter` is computed exactly from the estimate: the earliest time at
  which the request fits, assuming no other traffic. It can exceed one
  window, by up to a second window, because a full current window keeps
  weighing on the estimate as it decays through the next one.
- `ResetAfter` is the time until both windows' counts stop contributing.

For a strict bound in every rolling window, use GCRA or token bucket.

## Time

Local limiters read a `rategen.Clock`. They convert it to a timeline that is
aligned to the Unix epoch at start-up and then advances by the monotonic
clock, so wall-clock steps (NTP, manual changes, VM migration) neither grant
nor withhold capacity. If time appears to move backwards, the algorithms
treat that as no time passing.

The Redis store uses the Redis server's clock instead. See
[distributed.md](distributed.md).
