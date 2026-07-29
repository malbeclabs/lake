package handlers

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/malbeclabs/lake/api/metrics"
)

const (
	// totalStakeCacheTTL bounds staleness of the cached network total stake.
	// activated_stake changes only across Solana epochs (~2 days), so a 5-minute
	// TTL is invisible while removing the per-request re-scan of the vote-accounts
	// window-function view that computed sum(activated_stake_lamports).
	totalStakeCacheTTL = 5 * time.Minute

	// currentSlotCacheTTL bounds staleness of the cached cluster slot. ~15s ≈ ~37
	// slots; the pages that display it poll at 30s, so a value at most this stale is
	// well within one poll interval.
	currentSlotCacheTTL = 15 * time.Second

	// scalarCacheQueryTimeout bounds the detached miss-path query so a collapsed
	// run can't hang after the winning caller disconnects (see cachedScalar).
	scalarCacheQueryTimeout = 30 * time.Second
)

// scalarCacheEntry is one cached scalar value with its expiry.
type scalarCacheEntry struct {
	value   int64
	expires time.Time
}

// scalarCache is a small per-key TTL cache for cheap-to-cache scalar values
// (network total stake, current cluster slot) that were previously recomputed on
// every dashboard request against expensive window-function views. Keyed by
// env+name so a value computed against one environment's ClickHouse connection is
// never served to another. Concurrent misses collapse into one query via
// singleflight. The zero value is ready to use (map is created lazily), so a
// bare handlers.API — as constructed directly in tests and startup — needs no
// initialization.
type scalarCache struct {
	mu      sync.Mutex
	entries map[string]scalarCacheEntry
	sf      singleflight.Group
}

// get returns the cached value for key if present and not expired as of now.
func (c *scalarCache) get(key string, now time.Time) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || !now.Before(e.expires) {
		return 0, false
	}
	return e.value, true
}

// set caches val for key with the given TTL. A zero value is never cached:
// sum()/max() over an empty or cold table returns 0, and pinning that would keep
// serving an empty answer past a data load; callers see the live 0 until real
// data appears.
func (c *scalarCache) set(key string, val int64, ttl time.Duration, now time.Time) {
	if val == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]scalarCacheEntry)
	}
	c.entries[key] = scalarCacheEntry{value: val, expires: now.Add(ttl)}
}

// cachedScalar returns a per-env TTL-cached scalar, computing it via query on a
// miss. Errors are returned to the caller (these scalars were previously computed
// inside the same query, so they share its failure domain).
func (a *API) cachedScalar(ctx context.Context, name string, ttl time.Duration, query string) (int64, error) {
	key := string(EnvFromContext(ctx)) + ":" + name
	if v, ok := a.scalarCache.get(key, time.Now()); ok {
		return v, nil
	}
	// The collapsed miss-path query must not be tied to the winning caller's
	// context: with a plain Do the shared query inherits the winner's ctx, so one
	// caller's disconnect would 500 every collapsed waiter. Detach with
	// WithoutCancel (keeps the env value envDB routes on) under its own deadline,
	// and select on the caller's ctx via DoChan so a disconnecting caller returns
	// promptly without failing the rest. (Same pattern as fetchPublisherCheckLive.)
	ch := a.scalarCache.sf.DoChan(key, func() (any, error) {
		qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scalarCacheQueryTimeout)
		defer cancel()
		var val int64
		start := time.Now()
		err := a.envDB(qctx).QueryRow(qctx, query).Scan(&val)
		metrics.RecordClickHouseQuery("scalar_cache:"+name, time.Since(start), err)
		if err != nil {
			return int64(0), err
		}
		a.scalarCache.set(key, val, ttl, time.Now())
		return val, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return 0, res.Err
		}
		return res.Val.(int64), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// cachedTotalStake returns the network total activated stake (lamports), cached
// per env for totalStakeCacheTTL. Replaces the per-request total_stake CTE that
// re-scanned the vote-accounts history window view.
func (a *API) cachedTotalStake(ctx context.Context) (int64, error) {
	return a.cachedScalar(ctx, "total_stake", totalStakeCacheTTL,
		`SELECT sum(activated_stake_lamports) FROM solana_vote_accounts_current`)
}

// cachedCurrentSlot returns the latest cluster slot, cached per env for
// currentSlotCacheTTL. Replaces the per-request max(cluster_slot) subquery over
// the vote-account activity window.
func (a *API) cachedCurrentSlot(ctx context.Context) (int64, error) {
	return a.cachedScalar(ctx, "current_slot", currentSlotCacheTTL,
		`SELECT max(cluster_slot) FROM fact_solana_vote_account_activity WHERE event_ts >= now() - INTERVAL 2 MINUTE`)
}
