package rategen

import "fmt"

// FailurePolicy decides what a distributed limiter does when it cannot reach
// a decision because its store failed. There is deliberately no default: the
// zero value is invalid and constructors reject it, so every deployment makes
// the trade-off explicitly. See docs/failure.md.
type FailurePolicy int

const (
	failureUnset FailurePolicy = iota

	// FailClosed rejects requests whose decision cannot be verified. It
	// preserves the limit at the cost of availability: a store outage
	// rejects all traffic governed by the limiter.
	FailClosed

	// FailOpen admits requests whose decision cannot be verified. It
	// preserves availability at the cost of the limit: during an outage
	// the limit is not enforced at all. Decisions are marked Degraded.
	FailOpen

	// FailToLocal evaluates the request against a per-process in-memory
	// limiter with the same parameters. This keeps some protection during
	// an outage, but it is NOT a global limit: with k instances the
	// effective limit becomes up to k times the configured one. Decisions
	// are marked Degraded.
	FailToLocal
)

// Validate reports whether p is one of the defined policies.
func (p FailurePolicy) Validate() error {
	switch p {
	case FailClosed, FailOpen, FailToLocal:
		return nil
	case failureUnset:
		return fmt.Errorf("%w: a FailurePolicy (FailClosed, FailOpen or FailToLocal) must be chosen explicitly", ErrInvalidConfig)
	default:
		return fmt.Errorf("%w: unknown FailurePolicy %d", ErrInvalidConfig, int(p))
	}
}

// String returns the policy name.
func (p FailurePolicy) String() string {
	switch p {
	case FailClosed:
		return "fail_closed"
	case FailOpen:
		return "fail_open"
	case FailToLocal:
		return "fail_to_local"
	default:
		return "unset"
	}
}
