package memory

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/internal/algo"
	"github.com/mjahidhasand10/rategen/internal/wait"
)

// Limiter is a keyed limiter backed by a Store. It implements
// rategen.Limiter, rategen.Refunder, rategen.Batcher and rategen.Describer.
type Limiter struct {
	s    *Store
	id   uint32
	p    algo.Params
	info rategen.Info
}

// ReservableLimiter is a Limiter whose algorithm can schedule future
// admissions (token bucket and GCRA). It additionally implements
// rategen.Reserver and rategen.Waiter.
type ReservableLimiter struct {
	*Limiter
}

var (
	_ rategen.Refunder  = (*Limiter)(nil)
	_ rategen.Batcher   = (*Limiter)(nil)
	_ rategen.Describer = (*Limiter)(nil)
	_ rategen.Reserver  = (*ReservableLimiter)(nil)
	_ rategen.Waiter    = (*ReservableLimiter)(nil)
)

// TokenBucket returns a limiter named name giving each key a bucket of
// burst tokens refilled at rate.
func (s *Store) TokenBucket(name string, rate rategen.Rate, burst int64) (*ReservableLimiter, error) {
	if err := validate(rate, burst); err != nil {
		return nil, err
	}
	l, err := s.newLimiter(name, algo.NewTokenBucket(rate.Interval(), burst), rate, burst)
	if err != nil {
		return nil, err
	}
	return &ReservableLimiter{l}, nil
}

// GCRA returns a limiter named name using the Generic Cell Rate Algorithm:
// requests are admitted at rate on average, with up to burst admitted
// back-to-back.
func (s *Store) GCRA(name string, rate rategen.Rate, burst int64) (*ReservableLimiter, error) {
	if err := validate(rate, burst); err != nil {
		return nil, err
	}
	l, err := s.newLimiter(name, algo.NewGCRA(rate.Interval(), burst), rate, burst)
	if err != nil {
		return nil, err
	}
	return &ReservableLimiter{l}, nil
}

// FixedWindow returns a limiter named name admitting rate.Limit units per
// key in each window of length rate.Period. Windows are aligned to the Unix
// epoch. Up to twice the limit can be admitted across a window boundary.
func (s *Store) FixedWindow(name string, rate rategen.Rate) (*Limiter, error) {
	if err := rate.Validate(); err != nil {
		return nil, err
	}
	return s.newLimiter(name, algo.NewFixedWindow(rate.Limit, int64(rate.Period)), rate, rate.Limit)
}

// SlidingWindow returns a limiter named name using a sliding-window counter:
// rate.Limit units per rate.Period, estimated from the current and previous
// windows' counts. See docs/algorithms.md for its accuracy.
func (s *Store) SlidingWindow(name string, rate rategen.Rate) (*Limiter, error) {
	if err := rate.Validate(); err != nil {
		return nil, err
	}
	return s.newLimiter(name, algo.NewSlidingWindow(rate.Limit, int64(rate.Period)), rate, rate.Limit)
}

func validate(rate rategen.Rate, burst int64) error {
	if err := rate.Validate(); err != nil {
		return err
	}
	if burst <= 0 {
		return fmt.Errorf("%w: burst must be positive, got %d", rategen.ErrInvalidConfig, burst)
	}
	return nil
}

func (s *Store) newLimiter(name string, p algo.Params, rate rategen.Rate, burst int64) (*Limiter, error) {
	id, err := s.register(name)
	if err != nil {
		return nil, err
	}
	return &Limiter{
		s:  s,
		id: id,
		p:  p,
		info: rategen.Info{
			Policy:    name,
			Algorithm: p.Kind.String(),
			Backend:   "memory",
			Rate:      rate,
			Burst:     burst,
		},
	}, nil
}

// Info implements rategen.Describer.
func (l *Limiter) Info() rategen.Info { return l.info }

// BatchGroup implements rategen.Batcher. All limiters of one Store share a
// group.
func (l *Limiter) BatchGroup() rategen.BatchGroup { return l.s.group }

func (l *Limiter) decision(r algo.Result) rategen.Decision {
	return rategen.Decision{
		Allowed:    r.Allowed,
		Policy:     l.info.Policy,
		Limit:      l.p.Limit,
		Remaining:  r.Remaining,
		RetryAfter: time.Duration(r.RetryAfter),
		ResetAfter: time.Duration(r.ResetAfter),
		Window:     time.Duration(l.p.NominalWindow()),
		Epoch:      r.Epoch,
	}
}

func (l *Limiter) reject() rategen.Decision {
	return rategen.Decision{Policy: l.info.Policy, Limit: l.p.Limit, Window: time.Duration(l.p.NominalWindow())}
}

func (l *Limiter) checkCost(n int64) error {
	if n <= 0 {
		return fmt.Errorf("%w: got %d", rategen.ErrInvalidCost, n)
	}
	if n > l.p.Limit {
		return fmt.Errorf("%w: policy %q, cost %d, capacity %d", rategen.ErrExceedsCapacity, l.info.Policy, n, l.p.Limit)
	}
	return nil
}

// AllowN implements rategen.Limiter. It returns a rejected decision with an
// error if ctx is already done, n is invalid or exceeds the capacity, or the
// store is full under RejectNew.
func (l *Limiter) AllowN(ctx context.Context, key string, n int64) (rategen.Decision, error) {
	var start time.Time
	if l.s.obs != nil {
		start = time.Now()
	}
	d, err := l.allowN(ctx, key, n)
	if l.s.obs != nil {
		l.s.obs.Observe(ctx, rategen.Event{Info: l.info, Decision: d, Cost: n, Err: err, Latency: time.Since(start)})
	}
	return d, err
}

func (l *Limiter) allowN(ctx context.Context, key string, n int64) (rategen.Decision, error) {
	if err := ctx.Err(); err != nil {
		return l.reject(), err
	}
	if err := l.checkCost(n); err != nil {
		return l.reject(), err
	}
	k := entryKey{id: l.id, key: key}
	sh := &l.s.shards[l.s.shardIndex(k)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	// Read the clock under the lock so decisions on a key see
	// non-decreasing time.
	now := l.s.tl.Nanos()
	e, st := sh.load(k, l.p, now)
	next, r := algo.Decide(l.p, st, now, n)
	if r.Allowed {
		if err := l.s.store(sh, e, k, l.p, next); err != nil {
			return l.reject(), err
		}
	} else if e != nil {
		sh.moveToFront(e)
	}
	return l.decision(r), nil
}

// RefundN implements rategen.Refunder: it returns n units to key, never
// exceeding the key's initial capacity.
func (l *Limiter) RefundN(ctx context.Context, key string, n int64, consumed rategen.Decision) error {
	if n <= 0 {
		return fmt.Errorf("%w: got %d", rategen.ErrInvalidCost, n)
	}
	l.s.refund(entryKey{id: l.id, key: key}, l.p, n, consumed.Epoch)
	return nil
}

func (s *Store) refund(k entryKey, p algo.Params, n, epoch int64) {
	sh := &s.shards[s.shardIndex(k)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.tl.Nanos()
	e := sh.m[k]
	if e == nil || e.exp <= now {
		return // already at full capacity
	}
	e.st = algo.Refund(p, e.st, now, n, epoch)
	e.exp = algo.ExpiresAt(p, e.st)
}

// Reservation is a claim on future capacity made by ReserveN.
type Reservation struct {
	l      *Limiter
	k      entryKey
	n      int64
	ok     bool
	delay  time.Duration
	actAt  int64
	cancel bool // guarded by the key's shard lock
}

var _ rategen.Reservation = (*Reservation)(nil)

// OK reports whether the reservation was made.
func (r *Reservation) OK() bool { return r.ok }

// Delay returns how long to wait, from the ReserveN call, before acting.
func (r *Reservation) Delay() time.Duration { return r.delay }

// Cancel returns the reserved units if the time to act has not arrived.
func (r *Reservation) Cancel() {
	if !r.ok {
		return
	}
	s := r.l.s
	sh := &s.shards[s.shardIndex(r.k)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.tl.Nanos()
	if r.cancel || now >= r.actAt {
		return
	}
	r.cancel = true
	if e := sh.m[r.k]; e != nil && e.exp > now {
		e.st = algo.Refund(r.l.p, e.st, now, r.n, 0)
		e.exp = algo.ExpiresAt(r.l.p, e.st)
	}
}

// ReserveN implements rategen.Reserver. It consumes n units for key now,
// borrowing from future capacity if needed, and reports how long the caller
// must wait before acting. If the wait would exceed maxWait (negative means
// unbounded), nothing is consumed and the reservation is not OK.
func (l *ReservableLimiter) ReserveN(ctx context.Context, key string, n int64, maxWait time.Duration) (rategen.Reservation, error) {
	return l.reserve(ctx, key, n, maxWait)
}

func (l *ReservableLimiter) reserve(ctx context.Context, key string, n int64, maxWait time.Duration) (*Reservation, error) {
	k := entryKey{id: l.id, key: key}
	res := &Reservation{l: l.Limiter, k: k, n: n}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	if err := l.checkCost(n); err != nil {
		return res, err
	}
	s := l.s
	sh := &s.shards[s.shardIndex(k)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.tl.Nanos()
	e, st := sh.load(k, l.p, now)
	next, delay, ok := algo.Reserve(l.p, st, now, n, int64(maxWait))
	if !ok {
		return res, nil
	}
	if err := s.store(sh, e, k, l.p, next); err != nil {
		return res, err
	}
	res.ok, res.delay, res.actAt = true, time.Duration(delay), now+delay
	return res, nil
}

// WaitN implements rategen.Waiter: it blocks until n units are available
// for key and consumes them. It fails without consuming if ctx's deadline
// is too soon; if ctx is canceled while waiting, the units are returned.
func (l *ReservableLimiter) WaitN(ctx context.Context, key string, n int64) error {
	r, err := l.reserve(ctx, key, n, wait.MaxWait(ctx, l.s.clock))
	if err != nil {
		return err
	}
	if !r.ok {
		return wait.ErrDeadline
	}
	return wait.Sleep(ctx, l.s.clock, r.delay, r.Cancel)
}

// group implements rategen.BatchGroup for one Store.
type group struct{ s *Store }

type pending struct {
	l     *Limiter
	k     entryKey
	shard int
	e     *entry
	st    algo.State
	dup   bool // a later check in the batch uses the same entry
	r     algo.Result
}

// AllowBatch implements rategen.BatchGroup. Checks are evaluated under the
// locks of every shard involved, taken in index order, so the batch is
// atomic with respect to all other operations on the store. Several checks
// may target the same limiter and key; they are applied in order.
func (g *group) AllowBatch(ctx context.Context, checks []rategen.Check) ([]rategen.Decision, error) {
	var start time.Time
	if g.s.obs != nil {
		start = time.Now()
	}
	ds, err := g.allowBatch(ctx, checks)
	if g.s.obs != nil {
		lat := time.Since(start)
		for i, c := range checks {
			if i < len(ds) {
				l := asLimiter(c.Limiter)
				g.s.obs.Observe(ctx, rategen.Event{Info: l.info, Decision: ds[i], Cost: c.N, Err: err, Latency: lat, Batch: true})
			}
		}
	}
	return ds, err
}

func asLimiter(l rategen.Limiter) *Limiter {
	switch v := l.(type) {
	case *Limiter:
		return v
	case *ReservableLimiter:
		return v.Limiter
	}
	return nil
}

func (g *group) allowBatch(ctx context.Context, checks []rategen.Check) ([]rategen.Decision, error) {
	s := g.s
	ps := make([]pending, len(checks))
	ds := make([]rategen.Decision, len(checks))
	var costErr error
	for i, c := range checks {
		l := asLimiter(c.Limiter)
		if l == nil || l.s != s {
			return nil, fmt.Errorf("%w: check %d does not belong to this memory store", rategen.ErrNotAtomic, i)
		}
		k := entryKey{id: l.id, key: c.Key}
		ps[i] = pending{l: l, k: k, shard: s.shardIndex(k)}
		ds[i] = l.reject()
		if err := l.checkCost(c.N); err != nil && costErr == nil {
			costErr = err
		}
	}
	if err := ctx.Err(); err != nil {
		return ds, err
	}
	if costErr != nil {
		return ds, costErr
	}

	shards := make([]int, 0, len(ps))
	for _, p := range ps {
		shards = append(shards, p.shard)
	}
	slices.Sort(shards)
	shards = slices.Compact(shards)
	for _, i := range shards {
		s.shards[i].mu.Lock()
	}
	defer func() {
		for _, i := range shards {
			s.shards[i].mu.Unlock()
		}
	}()

	now := s.tl.Nanos()
	allowed := true
	for i := range ps {
		p := &ps[i]
		var st algo.State
		if j := lastSame(ps[:i], p.k); j >= 0 {
			// Same entry earlier in the batch: continue from its
			// tentative state.
			ps[j].dup = true
			p.e, st = ps[j].e, ps[j].st
		} else {
			p.e, st = s.shards[p.shard].load(p.k, p.l.p, now)
		}
		next, r := algo.Decide(p.l.p, st, now, checks[i].N)
		p.r = r
		if r.Allowed {
			p.st = next
		} else {
			p.st = st
			allowed = false
		}
	}

	if allowed {
		if err := g.reserveRoom(ps); err != nil {
			return ds, err
		}
		for i := range ps {
			p := &ps[i]
			if p.dup {
				continue // a later check carries the final state
			}
			sh := &s.shards[p.shard]
			if p.e == nil {
				p.e = sh.m[p.k] // inserted by an earlier duplicate
			}
			if err := s.store(sh, p.e, p.k, p.l.p, p.st); err != nil {
				return ds, err // unreachable: room was reserved
			}
		}
	}
	for i := range ps {
		r := ps[i].r
		if !allowed && r.Allowed {
			// Not consumed: report capacity as it stands.
			r.Remaining = min(ps[i].l.p.Limit, r.Remaining+checks[i].N)
		}
		ds[i] = ps[i].l.decision(r)
	}
	return ds, nil
}

func lastSame(ps []pending, k entryKey) int {
	for j := len(ps) - 1; j >= 0; j-- {
		if ps[j].k == k {
			return j
		}
	}
	return -1
}

// reserveRoom ensures every shard can take the batch's new entries before
// anything is written, so a full RejectNew store fails the whole batch
// rather than part of it.
func (g *group) reserveRoom(ps []pending) error {
	need := map[int]int{}
	for i := range ps {
		p := &ps[i]
		if p.e == nil && lastSame(ps[:i], p.k) < 0 {
			need[p.shard]++
		}
	}
	// Check feasibility before freeing anything: a batch that cannot fit
	// even after evicting every entry it does not use must fail without
	// evicting (and so resetting) other keys first.
	for idx, n := range need {
		inUse := 0
		for i := range ps {
			if ps[i].shard == idx && ps[i].e != nil && lastSame(ps[:i], ps[i].k) < 0 {
				inUse++
			}
		}
		if inUse+n > g.s.shards[idx].limit {
			return rategen.ErrStoreFull
		}
	}
	for idx, n := range need {
		sh := &g.s.shards[idx]
		for len(sh.m)+n > sh.limit {
			if !g.s.makeRoomExcept(sh, ps) {
				return rategen.ErrStoreFull
			}
		}
	}
	return nil
}
