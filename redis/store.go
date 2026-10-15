// Package redis provides a Redis-backed rategen store for limits shared by
// several application instances.
//
// Every decision runs as one Lua script on the Redis server, so concurrent
// requests from any number of instances are serialised by Redis and never
// race. Time comes from the Redis server clock (the TIME command inside the
// script), so application hosts' clocks do not need to agree. A batch of
// checks (rategen.Evaluate) runs in a single script and is all-or-nothing.
//
// A Store must be configured with an explicit rategen.FailurePolicy that
// says what happens when Redis cannot be reached. See docs/distributed.md.
package redis

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
)

//go:embed script.lua
var scriptSource string

var script = goredis.NewScript(scriptSource)

// DefaultTimeout bounds each Redis round trip when Config.Timeout is zero.
const DefaultTimeout = 100 * time.Millisecond

// Config configures a Store.
type Config struct {
	// Failure decides what happens when Redis fails or times out.
	// Required: the zero value is rejected.
	Failure rategen.FailurePolicy

	// Local is the in-memory store used for fallback limiters under
	// rategen.FailToLocal. It is required with FailToLocal and must be nil
	// otherwise. Use a dedicated store: the Redis store creates one
	// policy in it for every Redis policy, with the same name.
	Local *memory.Store

	// Prefix namespaces all keys, so that several applications or
	// environments can share a Redis database. Default "rategen".
	// Allowed characters are [A-Za-z0-9_.:-].
	Prefix string

	// Timeout bounds each decision's round trip to Redis, including
	// retries performed by the client. Default DefaultTimeout. The client
	// must have ContextTimeoutEnabled set for the timeout to apply to
	// network I/O; New checks this for the go-redis client types.
	Timeout time.Duration

	// Cluster declares that keys may live on different nodes (Redis
	// Cluster or a client-side Ring). Batches whose checks do not share
	// an identity then cannot be atomic and Evaluate falls back to its
	// non-atomic path. It is set automatically for *goredis.ClusterClient
	// and *goredis.Ring.
	Cluster bool

	// Observer, if set, receives an event for every decision.
	Observer rategen.Observer
}

// Store creates Redis-backed limiters. It is safe for concurrent use.
type Store struct {
	client  goredis.UniversalClient
	prefix  string
	timeout time.Duration
	failure rategen.FailurePolicy
	local   *memory.Store
	cluster bool
	obs     rategen.Observer
	group   *group

	mu    sync.Mutex
	names map[string]bool
}

// New returns a Store using client.
func New(client goredis.UniversalClient, cfg Config) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil redis client", rategen.ErrInvalidConfig)
	}
	if err := cfg.Failure.Validate(); err != nil {
		return nil, err
	}
	if cfg.Failure == rategen.FailToLocal && cfg.Local == nil {
		return nil, fmt.Errorf("%w: FailToLocal requires Config.Local", rategen.ErrInvalidConfig)
	}
	if cfg.Failure != rategen.FailToLocal && cfg.Local != nil {
		return nil, fmt.Errorf("%w: Config.Local is only used with FailToLocal", rategen.ErrInvalidConfig)
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "rategen"
	}
	if err := rategen.ValidatePolicyName(cfg.Prefix); err != nil {
		return nil, fmt.Errorf("prefix: %w", err)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf("%w: negative Timeout", rategen.ErrInvalidConfig)
	}
	cluster := cfg.Cluster
	ctxTimeouts := true
	switch c := client.(type) {
	case *goredis.Client:
		ctxTimeouts = c.Options().ContextTimeoutEnabled
	case *goredis.ClusterClient:
		ctxTimeouts = c.Options().ContextTimeoutEnabled
		cluster = true
	case *goredis.Ring:
		ctxTimeouts = c.Options().ContextTimeoutEnabled
		cluster = true
	}
	if !ctxTimeouts {
		return nil, fmt.Errorf("%w: the redis client must set ContextTimeoutEnabled: true, otherwise network I/O ignores Config.Timeout and a stalled server blocks requests for the client's ReadTimeout", rategen.ErrInvalidConfig)
	}
	s := &Store{
		client:  client,
		prefix:  cfg.Prefix,
		timeout: cfg.Timeout,
		failure: cfg.Failure,
		local:   cfg.Local,
		cluster: cluster,
		obs:     cfg.Observer,
		names:   make(map[string]bool),
	}
	s.group = &group{s: s}
	return s, nil
}

// Prefix returns the key prefix of this store.
func (s *Store) Prefix() string { return s.prefix }

// Limiter is a Redis-backed limiter. It implements rategen.Limiter,
// rategen.Refunder, rategen.Batcher and rategen.Describer. It does not
// implement Reserver or Waiter: see docs/distributed.md.
type Limiter struct {
	s          *Store
	kind       string
	info       rategen.Info
	limit      int64
	windowUS   int64
	intervalUS float64
	emissionUS int64
	nominal    time.Duration
	local      rategen.Limiter
}

var (
	_ rategen.Refunder  = (*Limiter)(nil)
	_ rategen.Batcher   = (*Limiter)(nil)
	_ rategen.Describer = (*Limiter)(nil)
)

// TokenBucket returns a limiter giving each key a bucket of burst tokens
// refilled at rate.
func (s *Store) TokenBucket(name string, rate rategen.Rate, burst int64) (*Limiter, error) {
	if err := validate(rate, burst); err != nil {
		return nil, err
	}
	interval := rate.Interval() / 1e3
	return s.newLimiter(name, "tb", "token_bucket", rate, burst, func(l *Limiter) {
		l.intervalUS = interval
		l.nominal = time.Duration(math.Ceil(rate.Interval() * float64(burst)))
	}, func(m *memory.Store) (rategen.Limiter, error) { return m.TokenBucket(name, rate, burst) })
}

// GCRA returns a limiter using the Generic Cell Rate Algorithm.
func (s *Store) GCRA(name string, rate rategen.Rate, burst int64) (*Limiter, error) {
	if err := validate(rate, burst); err != nil {
		return nil, err
	}
	emission := max(1, int64(math.Ceil(rate.Interval()/1e3-1e-6)))
	return s.newLimiter(name, "gc", "gcra", rate, burst, func(l *Limiter) {
		l.emissionUS = emission
		l.nominal = time.Duration(emission*burst) * time.Microsecond
	}, func(m *memory.Store) (rategen.Limiter, error) { return m.GCRA(name, rate, burst) })
}

// FixedWindow returns a limiter admitting rate.Limit units per key in each
// window of rate.Period, aligned to the Unix epoch on the Redis clock.
func (s *Store) FixedWindow(name string, rate rategen.Rate) (*Limiter, error) {
	if err := validateWindow(rate); err != nil {
		return nil, err
	}
	return s.newLimiter(name, "fw", "fixed_window", rate, rate.Limit, func(l *Limiter) {
		l.windowUS = rate.Period.Microseconds()
		l.nominal = rate.Period
	}, func(m *memory.Store) (rategen.Limiter, error) { return m.FixedWindow(name, rate) })
}

// SlidingWindow returns a limiter using a sliding-window counter.
func (s *Store) SlidingWindow(name string, rate rategen.Rate) (*Limiter, error) {
	if err := validateWindow(rate); err != nil {
		return nil, err
	}
	return s.newLimiter(name, "sw", "sliding_window", rate, rate.Limit, func(l *Limiter) {
		l.windowUS = rate.Period.Microseconds()
		l.nominal = rate.Period
	}, func(m *memory.Store) (rategen.Limiter, error) { return m.SlidingWindow(name, rate) })
}

// MaxLimit is the largest limit or burst the Redis store accepts. Lua
// numbers are IEEE-754 doubles, exact only up to 2^53; capping limits at
// 2^50 keeps counts, token levels and µs timestamps (about 2^51 today)
// exactly representable in the script's arithmetic.
const MaxLimit = 1 << 50

func validate(rate rategen.Rate, burst int64) error {
	if err := rate.Validate(); err != nil {
		return err
	}
	if burst <= 0 {
		return fmt.Errorf("%w: burst must be positive, got %d", rategen.ErrInvalidConfig, burst)
	}
	if burst > MaxLimit || rate.Limit > MaxLimit {
		return fmt.Errorf("%w: redis limits and bursts must not exceed 2^50", rategen.ErrInvalidConfig)
	}
	if rate.Interval()*float64(burst) > float64(100*365*24*time.Hour) {
		return fmt.Errorf("%w: refilling the burst would take over 100 years", rategen.ErrInvalidConfig)
	}
	return nil
}

func validateWindow(rate rategen.Rate) error {
	if err := rate.Validate(); err != nil {
		return err
	}
	if rate.Limit > MaxLimit {
		return fmt.Errorf("%w: redis limits must not exceed 2^50", rategen.ErrInvalidConfig)
	}
	if rate.Period > 100*365*24*time.Hour {
		return fmt.Errorf("%w: windows longer than 100 years are not supported", rategen.ErrInvalidConfig)
	}
	if rate.Period < time.Millisecond || rate.Period%time.Microsecond != 0 {
		return fmt.Errorf("%w: redis windows must be at least 1ms and a whole number of microseconds", rategen.ErrInvalidConfig)
	}
	return nil
}

func (s *Store) newLimiter(name, kind, algorithm string, rate rategen.Rate, burst int64, set func(*Limiter), fallback func(*memory.Store) (rategen.Limiter, error)) (*Limiter, error) {
	if err := rategen.ValidatePolicyName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.names[name] {
		return nil, fmt.Errorf("%w: policy %q already exists in this store", rategen.ErrInvalidConfig, name)
	}
	l := &Limiter{
		s:     s,
		kind:  kind,
		limit: burst,
		info:  rategen.Info{Policy: name, Algorithm: algorithm, Backend: "redis", Rate: rate, Burst: burst},
	}
	set(l)
	if s.local != nil {
		local, err := fallback(s.local)
		if err != nil {
			return nil, fmt.Errorf("creating local fallback: %w", err)
		}
		l.local = local
	}
	s.names[name] = true
	return l, nil
}

// Info implements rategen.Describer.
func (l *Limiter) Info() rategen.Info { return l.info }

// BatchGroup implements rategen.Batcher. All limiters of one Store share a
// group.
func (l *Limiter) BatchGroup() rategen.BatchGroup { return l.s.group }

// Key returns the Redis key holding the state of key under this policy:
// "<prefix>:{<hash>}:<policy>:<algorithm>". The identity is hashed, so raw
// keys (IPs, user IDs) are never stored in Redis; the hash tag keeps all
// policies of one identity in the same Redis Cluster slot.
func (l *Limiter) Key(key string) string {
	return l.s.prefix + ":{" + identity(key) + "}:" + l.info.Policy + ":" + l.kind
}

func identity(key string) string {
	sum := sha256.Sum256([]byte(key))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}

// args returns the script arguments for one check (see script.lua). epoch
// is the window a refund applies to, and zero for decisions.
func (l *Limiter) args(n, epoch int64) []any {
	return []any{l.kind, n, l.limit, l.windowUS, l.intervalUS, l.emissionUS, epoch}
}

// replyWidth is the number of script reply values per check.
const replyWidth = 5

func (l *Limiter) reject() rategen.Decision {
	return rategen.Decision{Policy: l.info.Policy, Limit: l.limit, Window: l.nominal}
}

func (l *Limiter) decision(v []int64) rategen.Decision {
	return rategen.Decision{
		Allowed:    v[0] == 1,
		Policy:     l.info.Policy,
		Limit:      l.limit,
		Remaining:  v[1],
		RetryAfter: time.Duration(v[2]) * time.Microsecond,
		ResetAfter: time.Duration(v[3]) * time.Microsecond,
		Window:     l.nominal,
		Epoch:      v[4],
	}
}

func (l *Limiter) checkCost(n int64) error {
	if n <= 0 {
		return fmt.Errorf("%w: got %d", rategen.ErrInvalidCost, n)
	}
	if n > l.limit {
		return fmt.Errorf("%w: policy %q, cost %d, capacity %d", rategen.ErrExceedsCapacity, l.info.Policy, n, l.limit)
	}
	return nil
}

// AllowN implements rategen.Limiter. If Redis fails or times out, the
// returned decision follows the store's FailurePolicy and the error wraps
// rategen.ErrStoreUnavailable. If ctx itself is done, the request is
// rejected with ctx's error regardless of policy.
func (l *Limiter) AllowN(ctx context.Context, key string, n int64) (rategen.Decision, error) {
	start := time.Now()
	d, err := l.allowN(ctx, key, n)
	if l.s.obs != nil {
		l.s.obs.Observe(ctx, rategen.Event{Info: l.info, Decision: d, Cost: n, Err: err, Latency: time.Since(start)})
	}
	return d, err
}

func (l *Limiter) allowN(ctx context.Context, key string, n int64) (rategen.Decision, error) {
	if err := l.checkCost(n); err != nil {
		return l.reject(), err
	}
	if err := ctx.Err(); err != nil {
		return l.reject(), err
	}
	args := append([]any{"allow", 1}, l.args(n, 0)...)
	v, err := l.s.run(ctx, []string{l.Key(key)}, args)
	if err == nil && len(v) != 1+replyWidth {
		err = fmt.Errorf("%w: unexpected script reply of length %d", rategen.ErrStoreUnavailable, len(v))
	}
	if err != nil {
		if !errors.Is(err, rategen.ErrStoreUnavailable) {
			return l.reject(), err // the caller's context ended
		}
		return l.s.fail(ctx, []rategen.Check{{Limiter: l, Key: key, N: n}}, err)[0], err
	}
	return l.decision(v[1 : 1+replyWidth]), nil
}

// RefundN implements rategen.Refunder. Refunds are not subject to the
// failure policy: a failed refund returns an error and leaves the capacity
// consumed, which errs on the side of the limit.
//
// Window refunds go to the window recorded in consumed (Decision.Epoch) and
// are dropped if that window no longer counts; see rategen.Refunder.
func (l *Limiter) RefundN(ctx context.Context, key string, n int64, consumed rategen.Decision) error {
	if n <= 0 {
		return fmt.Errorf("%w: got %d", rategen.ErrInvalidCost, n)
	}
	args := append([]any{"refund", 1}, l.args(n, consumed.Epoch)...)
	_, err := l.s.run(ctx, []string{l.Key(key)}, args)
	return err
}

// run executes the script. A failure is wrapped in ErrStoreUnavailable,
// except when the caller's context ended first, in which case its error is
// returned unwrapped. Whose deadline fired is decided by comparing the
// deadlines, not by inspecting the parent afterwards, which would misread a
// store timeout followed an instant later by the caller's deadline.
func (s *Store) run(ctx context.Context, keys []string, args []any) ([]int64, error) {
	deadline := time.Now().Add(s.timeout)
	parentDeadline, hasParentDeadline := ctx.Deadline()
	callerFirst := hasParentDeadline && !parentDeadline.After(deadline)
	cctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	v, err := script.Run(cctx, s.client, keys, args...).Int64Slice()
	if err != nil {
		if perr := ctx.Err(); perr != nil && (errors.Is(perr, context.Canceled) || callerFirst) {
			return nil, perr
		}
		return nil, fmt.Errorf("%w: %w", rategen.ErrStoreUnavailable, err)
	}
	return v, nil
}

// fail produces decisions for checks whose evaluation failed with err,
// according to the store's FailurePolicy.
func (s *Store) fail(ctx context.Context, checks []rategen.Check, err error) []rategen.Decision {
	ds := make([]rategen.Decision, len(checks))
	switch s.failure {
	case rategen.FailOpen:
		for i, c := range checks {
			d := c.Limiter.(*Limiter).reject()
			d.Allowed, d.Degraded = true, true
			ds[i] = d
		}
	case rategen.FailToLocal:
		local := make([]rategen.Check, len(checks))
		for i, c := range checks {
			local[i] = rategen.Check{Limiter: c.Limiter.(*Limiter).local, Key: c.Key, N: c.N}
		}
		// The fallback limiters share one memory store, so this is
		// atomic.
		res, _ := rategen.Evaluate(ctx, local...)
		for i := range ds {
			if i < len(res.Decisions) {
				ds[i] = res.Decisions[i]
			} else {
				ds[i] = checks[i].Limiter.(*Limiter).reject()
			}
			ds[i].Degraded = true
		}
	default: // FailClosed
		for i, c := range checks {
			ds[i] = c.Limiter.(*Limiter).reject()
		}
	}
	return ds
}

// group implements rategen.BatchGroup for one Store.
type group struct{ s *Store }

// AllowBatch implements rategen.BatchGroup: all checks run in one Lua
// script, all-or-nothing. With Config.Cluster, checks must share one
// identity (the same key string) so that their Redis keys share a hash
// slot; otherwise AllowBatch returns rategen.ErrNotAtomic.
func (g *group) AllowBatch(ctx context.Context, checks []rategen.Check) ([]rategen.Decision, error) {
	start := time.Now()
	ds, err := g.allowBatch(ctx, checks)
	if g.s.obs != nil && !errors.Is(err, rategen.ErrNotAtomic) {
		lat := time.Since(start)
		for i, c := range checks {
			g.s.obs.Observe(ctx, rategen.Event{Info: c.Limiter.(*Limiter).info, Decision: ds[i], Cost: c.N, Err: err, Latency: lat, Batch: true})
		}
	}
	return ds, err
}

func (g *group) allowBatch(ctx context.Context, checks []rategen.Check) ([]rategen.Decision, error) {
	ls := make([]*Limiter, len(checks))
	for i, c := range checks {
		l, ok := c.Limiter.(*Limiter)
		if !ok || l.s != g.s {
			return nil, fmt.Errorf("%w: check %d does not belong to this redis store", rategen.ErrNotAtomic, i)
		}
		ls[i] = l
		if g.s.cluster && c.Key != checks[0].Key {
			return nil, fmt.Errorf("%w: redis cluster batch spans several identities", rategen.ErrNotAtomic)
		}
	}
	ds := make([]rategen.Decision, len(checks))
	for i, l := range ls {
		ds[i] = l.reject()
	}
	for i, l := range ls {
		if err := l.checkCost(checks[i].N); err != nil {
			return ds, err
		}
	}
	if err := ctx.Err(); err != nil {
		return ds, err
	}
	keys := make([]string, len(checks))
	args := make([]any, 0, 2+7*len(checks))
	args = append(args, "allow", len(checks))
	for i, l := range ls {
		keys[i] = l.Key(checks[i].Key)
		args = append(args, l.args(checks[i].N, 0)...)
	}
	v, err := g.s.run(ctx, keys, args)
	if err == nil && len(v) != 1+replyWidth*len(checks) {
		err = fmt.Errorf("%w: unexpected script reply of length %d", rategen.ErrStoreUnavailable, len(v))
	}
	if err != nil {
		if !errors.Is(err, rategen.ErrStoreUnavailable) {
			return ds, err // the caller's context ended
		}
		return g.s.fail(ctx, checks, err), err
	}
	all := v[0] == 1
	for i, l := range ls {
		ds[i] = l.decision(v[1+replyWidth*i : 1+replyWidth*(i+1)])
		if !all && ds[i].Allowed {
			ds[i].Remaining = min(l.limit, ds[i].Remaining+checks[i].N)
		}
	}
	return ds, nil
}
