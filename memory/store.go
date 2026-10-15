// Package memory provides an in-process, keyed rate-limit store.
//
// A Store holds the per-key state of any number of limiters (policies). It
// is safe for concurrent use, bounded in size, and starts no goroutines:
// expired state is removed lazily on access and when space is needed, or
// explicitly by Sweep or RunJanitor.
//
// State lives in one process. With several application instances, each
// enforces its own copy of every limit; use the redis module for limits
// shared between instances.
package memory

import (
	"context"
	"fmt"
	"hash/maphash"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/internal/algo"
)

// FullPolicy selects what a Store does when a new key arrives and the store
// already holds MaxKeys keys none of which has expired.
type FullPolicy int

const (
	// EvictLRU evicts the least recently used key. Evicting a key resets
	// its limits, so an attacker able to create many keys can reset the
	// limits of idle keys; size MaxKeys for your key cardinality. This is
	// the default because it keeps serving new clients under key floods.
	EvictLRU FullPolicy = iota

	// RejectNew rejects requests for new keys with ErrStoreFull, keeping
	// the state of existing keys intact. Under a key flood, new legitimate
	// clients are rejected until keys expire.
	RejectNew
)

// Defaults for Config fields left zero.
const (
	DefaultMaxKeys = 1_000_000
	DefaultShards  = 64
)

// Config configures a Store. The zero value is valid.
type Config struct {
	// MaxKeys bounds the number of (policy, key) entries held; the store
	// never holds more. Zero means DefaultMaxKeys. The bound is split
	// evenly across shards, so the store may evict slightly before
	// MaxKeys is reached when keys are unevenly distributed.
	MaxKeys int
	// Shards is the number of independently locked partitions; it must be
	// a power of two. Zero means DefaultShards.
	Shards int
	// OnFull selects behaviour when the store is full.
	OnFull FullPolicy
	// Clock is the time source; nil means the system clock.
	Clock rategen.Clock
	// Observer, if set, receives an event for every decision.
	Observer rategen.Observer
}

// Store is an in-memory, sharded, bounded rate-limit store.
type Store struct {
	shards []shard
	mask   uint64
	seed   maphash.Seed
	onFull FullPolicy
	tl     *rategen.Monotonic
	clock  rategen.Clock
	obs    rategen.Observer
	group  *group

	mu     sync.Mutex
	names  map[string]bool
	nextID uint32

	evictions atomic.Int64
	expired   atomic.Int64
}

type shard struct {
	mu         sync.Mutex
	m          map[entryKey]*entry
	head, tail *entry  // LRU list; head is most recently used
	limit      int     // maximum entries in this shard
	minExp     int64   // lower bound on the earliest entry expiry
	lastSweep  int64   // when the shard was last swept in full
	_          [8]byte // pad to 64 bytes to reduce false sharing between shards
}

type entryKey struct {
	id  uint32
	key string
}

type entry struct {
	k          entryKey
	st         algo.State
	exp        int64
	prev, next *entry
}

// New returns a Store configured by cfg.
func New(cfg Config) (*Store, error) {
	if cfg.MaxKeys < 0 {
		return nil, fmt.Errorf("%w: MaxKeys must not be negative", rategen.ErrInvalidConfig)
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = DefaultMaxKeys
	}
	if cfg.Shards == 0 {
		cfg.Shards = DefaultShards
	}
	if cfg.Shards < 0 || cfg.Shards&(cfg.Shards-1) != 0 {
		return nil, fmt.Errorf("%w: Shards must be a power of two, got %d", rategen.ErrInvalidConfig, cfg.Shards)
	}
	if cfg.Shards > cfg.MaxKeys {
		cfg.Shards = 1
		for cfg.Shards*2 <= cfg.MaxKeys {
			cfg.Shards *= 2
		}
	}
	if cfg.OnFull != EvictLRU && cfg.OnFull != RejectNew {
		return nil, fmt.Errorf("%w: unknown FullPolicy %d", rategen.ErrInvalidConfig, cfg.OnFull)
	}
	if cfg.Clock == nil {
		cfg.Clock = rategen.SystemClock()
	}
	s := &Store{
		shards: make([]shard, cfg.Shards),
		mask:   uint64(cfg.Shards - 1),
		seed:   maphash.MakeSeed(),
		onFull: cfg.OnFull,
		tl:     rategen.NewMonotonic(cfg.Clock),
		clock:  cfg.Clock,
		obs:    cfg.Observer,
		names:  make(map[string]bool),
	}
	s.group = &group{s: s}
	// Split MaxKeys exactly: the first MaxKeys%Shards shards hold one
	// extra entry, so the capacities sum to MaxKeys.
	base, extra := cfg.MaxKeys/cfg.Shards, cfg.MaxKeys%cfg.Shards
	for i := range s.shards {
		s.shards[i].m = make(map[entryKey]*entry)
		s.shards[i].limit = base
		if i < extra {
			s.shards[i].limit++
		}
	}
	return s, nil
}

// Stats is a snapshot of store counters.
type Stats struct {
	Keys      int   // entries currently held
	Evictions int64 // live entries evicted because the store was full
	Expired   int64 // expired entries removed
}

// Stats returns current counters. Keys is computed by locking each shard in
// turn, so it is not an atomic snapshot under concurrent use.
func (s *Store) Stats() Stats {
	st := Stats{Evictions: s.evictions.Load(), Expired: s.expired.Load()}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		st.Keys += len(sh.m)
		sh.mu.Unlock()
	}
	return st
}

// Sweep removes all expired entries and returns how many were removed.
// Expired entries are equivalent to absent ones, so sweeping never changes a
// decision; it only releases memory. Sweep locks one shard at a time and
// stops early if ctx is done.
func (s *Store) Sweep(ctx context.Context) int {
	removed := 0
	for i := range s.shards {
		if ctx.Err() != nil {
			break
		}
		sh := &s.shards[i]
		sh.mu.Lock()
		removed += s.sweepShard(sh, s.tl.Nanos(), nil)
		sh.mu.Unlock()
	}
	return removed
}

// RunJanitor calls Sweep every interval until ctx is done, then returns
// ctx's error. It is the caller's goroutine: start it with
// `go store.RunJanitor(ctx, time.Minute)` and cancel ctx to stop it.
// Without a janitor, expired entries are still removed lazily.
func (s *Store) RunJanitor(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("%w: janitor interval must be positive", rategen.ErrInvalidConfig)
	}
	for {
		t := s.clock.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C():
			s.Sweep(ctx)
		}
	}
}

func (s *Store) register(name string) (uint32, error) {
	if err := rategen.ValidatePolicyName(name); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.names[name] {
		return 0, fmt.Errorf("%w: policy %q already exists in this store", rategen.ErrInvalidConfig, name)
	}
	s.names[name] = true
	s.nextID++
	return s.nextID, nil
}

func (s *Store) shardIndex(k entryKey) int {
	h := maphash.String(s.seed, k.key) ^ (uint64(k.id) * 0x9e3779b97f4a7c15)
	h ^= h >> 29
	return int(h & s.mask)
}

// load returns the current state for k at now and the entry holding it, if
// any. Expired entries yield fresh state.
func (sh *shard) load(k entryKey, p algo.Params, now int64) (*entry, algo.State) {
	e := sh.m[k]
	if e == nil || e.exp <= now {
		return e, algo.Fresh(p, now)
	}
	return e, e.st
}

// store writes st for k, inserting an entry if needed. e is the entry
// returned by load (possibly nil).
func (s *Store) store(sh *shard, e *entry, k entryKey, p algo.Params, st algo.State) error {
	if e == nil {
		if len(sh.m) >= sh.limit && !s.makeRoom(sh) {
			return rategen.ErrStoreFull
		}
		e = &entry{k: k}
		sh.m[k] = e
		sh.pushFront(e)
	} else {
		sh.moveToFront(e)
	}
	e.st = st
	e.exp = algo.ExpiresAt(p, st)
	sh.minExp = min(sh.minExp, e.exp)
	return nil
}

// fullSweepInterval bounds how often makeRoom scans a whole shard, so that a
// flood of new keys cannot turn every insertion into a full scan.
const fullSweepInterval = int64(5 * time.Millisecond)

// sweepShard removes expired entries (except those in keep) from a locked
// shard, recomputes its earliest expiry, and returns the number removed.
func (s *Store) sweepShard(sh *shard, now int64, keep []pending) int {
	removed := 0
	minExp := int64(math.MaxInt64)
	for _, e := range sh.m {
		if e.exp <= now && !kept(keep, e) {
			sh.remove(e)
			removed++
			continue
		}
		minExp = min(minExp, e.exp)
	}
	sh.minExp, sh.lastSweep = minExp, now
	s.expired.Add(int64(removed))
	return removed
}

func kept(keep []pending, e *entry) bool {
	for i := range keep {
		if keep[i].e == e {
			return true
		}
	}
	return false
}

// makeRoom frees one slot in a full shard. It prefers expired entries near
// the LRU tail and otherwise applies the FullPolicy.
func (s *Store) makeRoom(sh *shard) bool { return s.makeRoomExcept(sh, nil) }

// makeRoomExcept is makeRoom that never removes an entry referenced by keep
// (the entries of an in-progress batch).
func (s *Store) makeRoomExcept(sh *shard, keep []pending) bool {
	now := s.tl.Nanos()
	e := sh.tail
	for i := 0; e != nil && i < 8; i++ {
		if e.exp <= now && !kept(keep, e) {
			sh.remove(e)
			s.expired.Add(1)
			return true
		}
		e = e.prev
	}
	// Expired entries may sit away from the LRU tail when policies with
	// different lifetimes share a store. Before rejecting a key or
	// evicting a live one (which resets its limits), look for them, if
	// any may exist and the shard was not just scanned.
	if now >= sh.minExp && now-sh.lastSweep >= fullSweepInterval {
		if s.sweepShard(sh, now, keep) > 0 {
			return true
		}
	}
	if s.onFull == RejectNew {
		return false
	}
	for e := sh.tail; e != nil; e = e.prev {
		if !kept(keep, e) {
			sh.remove(e)
			s.evictions.Add(1)
			return true
		}
	}
	return false
}

func (sh *shard) pushFront(e *entry) {
	e.prev, e.next = nil, sh.head
	if sh.head != nil {
		sh.head.prev = e
	}
	sh.head = e
	if sh.tail == nil {
		sh.tail = e
	}
}

func (sh *shard) unlink(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		sh.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		sh.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (sh *shard) moveToFront(e *entry) {
	if sh.head == e {
		return
	}
	sh.unlink(e)
	sh.pushFront(e)
}

func (sh *shard) remove(e *entry) {
	sh.unlink(e)
	delete(sh.m, e.k)
}
