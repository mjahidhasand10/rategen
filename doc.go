// Package rategen is a rate-limiting toolkit with one API for local and
// distributed limits.
//
// The central interface is Limiter: AllowN(ctx, key, n) consumes n units for
// key and returns a Decision describing whether the request may proceed,
// how much capacity remains and when to retry. Limiters come from stores:
//
//   - memory.Store keeps state in process: sharded, bounded and
//     goroutine-free.
//   - redis.Store (module github.com/mjahidhasand10/rategen/redis) shares
//     state between instances through atomic Lua scripts.
//
// Both stores offer the same four algorithms: token bucket, GCRA, fixed
// window and sliding-window counter. See docs/algorithms.md for how to
// choose. For pacing within one program without keys, use the tokenbucket
// package.
//
// Several limits can apply to one request. Evaluate checks them together,
// atomically when the limiters share a store, so a request that one limit
// rejects does not consume the others.
//
// Some capabilities depend on the algorithm and store, and are exposed as
// optional interfaces: Reserver, Waiter, Refunder, Batcher and Describer.
//
// Distributed stores require an explicit FailurePolicy for outages:
// FailClosed, FailOpen or FailToLocal. There is no default.
//
// The httplimit package provides net/http middleware with safe client-IP
// extraction. Observer hooks report decisions without exposing keys. A
// Prometheus implementation is in the module
// github.com/mjahidhasand10/rategen/prometheus.
package rategen
