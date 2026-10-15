package rategen

import "errors"

// Sentinel errors. Backends wrap these with context; test with errors.Is.
var (
	// ErrInvalidConfig reports a configuration that cannot work, detected
	// at construction time.
	ErrInvalidConfig = errors.New("rategen: invalid configuration")

	// ErrInvalidCost reports a non-positive request cost.
	ErrInvalidCost = errors.New("rategen: cost must be positive")

	// ErrExceedsCapacity reports a request whose cost is larger than the
	// limiter's capacity. Such a request can never be admitted.
	ErrExceedsCapacity = errors.New("rategen: cost exceeds limiter capacity")

	// ErrStoreUnavailable reports that a backend could not be reached or
	// did not answer in time. The decision returned alongside it follows
	// the backend's FailurePolicy.
	ErrStoreUnavailable = errors.New("rategen: store unavailable")

	// ErrStoreFull reports that an in-memory store reached its key limit
	// and is configured to reject new keys.
	ErrStoreFull = errors.New("rategen: store full")

	// ErrUnsupported reports an operation the limiter does not support.
	ErrUnsupported = errors.New("rategen: operation not supported")

	// ErrNotAtomic is returned by a BatchGroup that cannot evaluate a
	// particular set of checks atomically (for example, keys in different
	// Redis Cluster slots). Evaluate then falls back to the non-atomic path.
	ErrNotAtomic = errors.New("rategen: checks cannot be evaluated atomically")

	// ErrMissingKey reports that no identity could be derived for a request.
	ErrMissingKey = errors.New("rategen: missing rate-limit key")
)
