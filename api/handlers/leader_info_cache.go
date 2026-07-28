package handlers

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/malbeclabs/lake/api/metrics"
)

// leaderInfoCacheTTL bounds staleness of the cached leader-info map. Validator
// names, IPs, ASN org, and geo change on ~daily timescales, so a 5-minute TTL is
// invisible in the edge scoreboard's live tail while it removes the per-run
// validatorsapp+geoip join (which re-scanned the validatorsapp window view every
// scoreboard refresh — the scoreboard's cache worker alone drove ~9,000 runs/day).
const leaderInfoCacheTTL = 5 * time.Minute

// leaderInfoQueryTimeout bounds the detached miss-path query so a collapsed run
// can't hang after the winning caller disconnects (see cachedLeaderInfo).
const leaderInfoQueryTimeout = 30 * time.Second

// leaderInfoEntry is validator display/geo enrichment for one account.
type leaderInfoEntry struct {
	name, ip, asnOrg, city, country string
}

type leaderInfoCacheEntry struct {
	m       map[string]leaderInfoEntry
	expires time.Time
}

// leaderInfoCache caches the full account→enrichment map per env with a TTL. The
// underlying table is a few thousand small rows, so holding the whole map is
// trivial and lets every leader lookup become a Go map read instead of a query.
// The zero value is ready to use (map created lazily).
type leaderInfoCache struct {
	mu    sync.Mutex
	byEnv map[string]leaderInfoCacheEntry
	sf    singleflight.Group
}

func (c *leaderInfoCache) get(env string, now time.Time) (map[string]leaderInfoEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.byEnv[env]
	if !ok || !now.Before(e.expires) {
		return nil, false
	}
	return e.m, true
}

// set caches a non-empty map. An empty map is never cached: it means the source
// tables were empty/cold, and pinning it would keep enrichment blank past a data
// load; callers fall back to empty enrichment (identical to the old LEFT-JOIN
// no-match) until real data appears.
func (c *leaderInfoCache) set(env string, m map[string]leaderInfoEntry, ttl time.Duration, now time.Time) {
	if len(m) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byEnv == nil {
		c.byEnv = make(map[string]leaderInfoCacheEntry)
	}
	c.byEnv[env] = leaderInfoCacheEntry{m: m, expires: now.Add(ttl)}
}

// cachedLeaderInfo returns the per-env account→enrichment map, rebuilding it via
// one unfiltered validatorsapp+geoip query on a miss. Concurrent misses collapse
// into one query via singleflight. The returned map is read-only (shared across
// callers); callers must not mutate it.
func (a *API) cachedLeaderInfo(ctx context.Context) (map[string]leaderInfoEntry, error) {
	env := string(EnvFromContext(ctx))
	if m, ok := a.leaderInfoCache.get(env, time.Now()); ok {
		return m, nil
	}
	// Detach the collapsed miss-path query from the winning caller's context so one
	// caller's disconnect doesn't fail every collapsed waiter; select on the
	// caller's ctx via DoChan. (Same pattern as fetchPublisherCheckLive.)
	ch := a.leaderInfoCache.sf.DoChan(env, func() (any, error) {
		qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaderInfoQueryTimeout)
		defer cancel()
		m, err := a.fetchLeaderInfo(qctx)
		if err != nil {
			return map[string]leaderInfoEntry(nil), err
		}
		a.leaderInfoCache.set(env, m, leaderInfoCacheTTL, time.Now())
		return m, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(map[string]leaderInfoEntry), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *API) fetchLeaderInfo(ctx context.Context) (map[string]leaderInfoEntry, error) {
	const query = `
		SELECT
			v.account,
			COALESCE(v.name, ''),
			COALESCE(v.ip, ''),
			COALESCE(g.asn_org, ''),
			COALESCE(g.city, ''),
			COALESCE(g.country, '')
		FROM validatorsapp_validators_current v
		LEFT JOIN geoip_records_current g ON g.ip = v.ip
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("edge_scoreboard:leader_info", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string]leaderInfoEntry)
	for rows.Next() {
		var account string
		var e leaderInfoEntry
		if err := rows.Scan(&account, &e.name, &e.ip, &e.asnOrg, &e.city, &e.country); err != nil {
			return nil, err
		}
		m[account] = e
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return m, nil
}
