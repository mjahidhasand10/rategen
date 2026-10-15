package rategen

import (
	"context"
	"errors"
	"fmt"
)

// Check is one limit to apply to a request: consume N units of Limiter for
// Key.
type Check struct {
	Limiter Limiter
	Key     string
	N       int64
}

// BatchGroup evaluates several checks against one backend atomically: either
// every check is consumed or none is.
type BatchGroup interface {
	// AllowBatch evaluates checks, all of whose limiters belong to this
	// group. It returns one Decision per check, in order. If any check is
	// rejected, no check consumes capacity; the decisions of checks that
	// would have passed report Allowed=true with their current Remaining.
	// It returns ErrNotAtomic, consuming nothing, if this particular set of
	// checks cannot be evaluated atomically.
	AllowBatch(ctx context.Context, checks []Check) ([]Decision, error)
}

// Batcher is implemented by limiters that can join atomic multi-limit
// evaluation with other limiters of the same backend.
type Batcher interface {
	Limiter
	// BatchGroup returns the group this limiter belongs to. Limiters with
	// equal (==) groups can be evaluated together atomically.
	BatchGroup() BatchGroup
}

// Result is the outcome of Evaluate.
type Result struct {
	// Allowed reports whether every check was admitted.
	Allowed bool
	// Decisions holds one decision per evaluated check, in order. On the
	// non-atomic path evaluation stops at the first rejection, so later
	// checks have no decision.
	Decisions []Decision
	// Atomic reports whether the checks were evaluated as one atomic
	// operation. When false, a rejected request may have temporarily
	// consumed capacity from earlier checks before it was refunded.
	Atomic bool
	// Errs holds the error that accompanied each decision (nil if none),
	// aligned with Decisions. Errors from refunds are not included; they
	// appear only in the error returned by Evaluate.
	Errs []error
}

// RejectedByLimit reports whether at least one check was rejected by its
// limit, as opposed to being rejected because its backend failed (a
// fail-closed decision) or its context ended. A request can be rejected and
// RejectedByLimit still be false: then no limit verdict exists, and the
// rejection is an error condition (for example HTTP 503 rather than 429).
// A cost exceeding a limiter's capacity counts as rejected by the limit, and
// so does a degraded rejection: under FailToLocal the fallback limiter's
// verdict is a real, if local, limit decision.
func (r Result) RejectedByLimit() bool {
	for i, d := range r.Decisions {
		if d.Allowed {
			continue
		}
		if d.Degraded {
			return true
		}
		var err error
		if i < len(r.Errs) {
			err = r.Errs[i]
		}
		if err == nil || errors.Is(err, ErrExceedsCapacity) {
			return true
		}
	}
	return false
}

// Decision returns the decision that best summarises the result for
// response headers: when rejected, the rejecting decision with the longest
// RetryAfter (a request that can never fit wins); when allowed, the decision
// with the fewest remaining units.
func (r Result) Decision() Decision {
	var best Decision
	found := false
	for _, d := range r.Decisions {
		if !found {
			best, found = d, true
			continue
		}
		if r.Allowed {
			if d.Remaining < best.Remaining {
				best = d
			}
			continue
		}
		switch {
		case best.Allowed && !d.Allowed:
			best = d
		case !d.Allowed && !best.Allowed && best.RetryAfter != 0 &&
			(d.RetryAfter == 0 || d.RetryAfter > best.RetryAfter):
			best = d
		}
	}
	best.Allowed = r.Allowed
	return best
}

// Evaluate applies several limits to one request.
//
// If all limiters are Batchers of the same BatchGroup, the checks are
// evaluated atomically by the backend: all are consumed or none are.
// Otherwise they are evaluated in order, stopping at the first rejection,
// and capacity already consumed by earlier checks is returned through
// Refunder where supported. That path is not atomic; Result.Atomic reports
// which path was taken. Use EvaluateAtomic to require atomicity.
//
// Errors from individual limiters (for example a store outage handled by a
// fail-open policy) are joined into the returned error; Result.Allowed is
// authoritative.
func Evaluate(ctx context.Context, checks ...Check) (Result, error) {
	if err := validateChecks(checks); err != nil {
		return Result{}, err
	}
	if len(checks) == 1 {
		d, err := checks[0].Limiter.AllowN(ctx, checks[0].Key, checks[0].N)
		return Result{Allowed: d.Allowed, Decisions: []Decision{d}, Atomic: true, Errs: []error{err}}, err
	}
	if g := commonGroup(checks); g != nil {
		ds, err := g.AllowBatch(ctx, checks)
		if !errors.Is(err, ErrNotAtomic) {
			return batchResult(ds, err), err
		}
	}
	return evaluateSequential(ctx, checks)
}

// EvaluateAtomic is like Evaluate but returns ErrNotAtomic, consuming
// nothing, when the checks cannot be evaluated atomically.
func EvaluateAtomic(ctx context.Context, checks ...Check) (Result, error) {
	if err := validateChecks(checks); err != nil {
		return Result{}, err
	}
	if len(checks) == 1 {
		return Evaluate(ctx, checks...)
	}
	g := commonGroup(checks)
	if g == nil {
		return Result{}, fmt.Errorf("%w: limiters do not share a backend", ErrNotAtomic)
	}
	ds, err := g.AllowBatch(ctx, checks)
	if errors.Is(err, ErrNotAtomic) {
		return Result{}, err
	}
	return batchResult(ds, err), err
}

func validateChecks(checks []Check) error {
	if len(checks) == 0 {
		return fmt.Errorf("%w: no checks", ErrInvalidConfig)
	}
	for i, c := range checks {
		if c.Limiter == nil {
			return fmt.Errorf("%w: check %d has a nil limiter", ErrInvalidConfig, i)
		}
	}
	return nil
}

// batchResult builds the Result of an atomic batch. A batch error concerns
// the whole batch, so it accompanies every decision.
func batchResult(ds []Decision, err error) Result {
	r := Result{Allowed: len(ds) > 0, Decisions: ds, Atomic: true, Errs: make([]error, len(ds))}
	for i := range r.Errs {
		r.Errs[i] = err
	}
	for _, d := range ds {
		if !d.Allowed {
			r.Allowed = false
		}
	}
	return r
}

func commonGroup(checks []Check) BatchGroup {
	var g BatchGroup
	for i, c := range checks {
		b, ok := c.Limiter.(Batcher)
		if !ok {
			return nil
		}
		bg := b.BatchGroup()
		if bg == nil {
			return nil
		}
		if i == 0 {
			g = bg
		} else if bg != g {
			return nil
		}
	}
	return g
}

func evaluateSequential(ctx context.Context, checks []Check) (Result, error) {
	res := Result{Allowed: true, Decisions: make([]Decision, 0, len(checks)), Errs: make([]error, 0, len(checks))}
	var errs []error
	for i, c := range checks {
		d, err := c.Limiter.AllowN(ctx, c.Key, c.N)
		res.Decisions = append(res.Decisions, d)
		res.Errs = append(res.Errs, err)
		if err != nil {
			errs = append(errs, err)
		}
		if d.Allowed {
			continue
		}
		res.Allowed = false
		// Return what the earlier checks consumed. Degraded decisions
		// consumed nothing in the authoritative store, so they are skipped.
		for j := range i {
			if res.Decisions[j].Degraded {
				continue
			}
			prev := checks[j]
			rf, ok := prev.Limiter.(Refunder)
			if !ok {
				errs = append(errs, fmt.Errorf("%w: limiter %q cannot refund; its capacity stays consumed", ErrUnsupported, res.Decisions[j].Policy))
				continue
			}
			if err := rf.RefundN(context.WithoutCancel(ctx), prev.Key, prev.N, res.Decisions[j]); err != nil {
				errs = append(errs, err)
			}
		}
		break
	}
	return res, errors.Join(errs...)
}

// Multi applies several limiters to the same key. It implements Limiter, so
// it can be used wherever a single limiter is expected.
type Multi struct {
	limiters []Limiter
}

// All returns a Multi that admits a request only if every limiter admits
// it, using the same key and cost for each. See Evaluate for atomicity.
// For limits that need different keys per request (per-user and global, for
// example), call Evaluate with explicit Checks instead.
func All(limiters ...Limiter) *Multi {
	return &Multi{limiters: append([]Limiter(nil), limiters...)}
}

// Evaluate applies every limiter to key and returns all decisions.
func (m *Multi) Evaluate(ctx context.Context, key string, n int64) (Result, error) {
	checks := make([]Check, len(m.limiters))
	for i, l := range m.limiters {
		checks[i] = Check{Limiter: l, Key: key, N: n}
	}
	return Evaluate(ctx, checks...)
}

// AllowN implements Limiter. The returned decision is Result.Decision.
func (m *Multi) AllowN(ctx context.Context, key string, n int64) (Decision, error) {
	r, err := m.Evaluate(ctx, key, n)
	return r.Decision(), err
}
