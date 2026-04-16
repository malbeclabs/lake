package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/api/metrics"
)

// EdgeScoreboardLeadTime holds pairwise lead time stats (winner vs specific loser).
type EdgeScoreboardLeadTime struct {
	LoserFeed string  `json:"loser_feed"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	SlotCount uint64  `json:"slot_count"`
}

// EdgeScoreboardFeedStats holds per-feed win rate and pairwise lead time stats for an edge node.
type EdgeScoreboardFeedStats struct {
	ShredsWon   uint64                   `json:"shreds_won"`
	TotalShreds uint64                   `json:"total_shreds"`
	WinRatePct  float64                  `json:"win_rate_pct"`
	LeadTimes   []EdgeScoreboardLeadTime `json:"lead_times"`
}

// EdgeScoreboardNode holds aggregated stats for a single edge node.
type EdgeScoreboardNode struct {
	Host          string                              `json:"host"`
	Location      string                              `json:"location"`
	MetroName     string                              `json:"metro_name"`
	Latitude      float64                             `json:"latitude"`
	Longitude     float64                             `json:"longitude"`
	Feeds         map[string]*EdgeScoreboardFeedStats `json:"feeds"`
	StakeSol      float64                             `json:"stake_sol"`
	Validators    uint64                              `json:"validators"`
	TotalSlots    uint64                              `json:"total_slots"`
	SlotsObserved uint64                              `json:"slots_observed"`  // view-dependent: DZ-leader slots in leaders_only mode, DZ+dz_rebop in all-slots mode
	DZLeaderSlots uint64                              `json:"dz_leader_slots"` // slots where the dz feed won shreds and slot was a DZ-leader slot (per-node, informational)
	LastUpdated   time.Time                           `json:"last_updated"`
	Name          string                              `json:"name,omitempty"`
	GossipPubkey  string                              `json:"gossip_pubkey,omitempty"`
	GossipIP      string                              `json:"gossip_ip,omitempty"`
	ASN           int64                               `json:"asn,omitempty"`
	ASNOrg        string                              `json:"asn_org,omitempty"`
	City          string                              `json:"city,omitempty"`
	Country       string                              `json:"country,omitempty"`
}

// edgeNodeIPs maps known edge node host names to their public IP addresses.
// Used to enrich node entries with geoip data (ASN, city, country).
var edgeNodeIPs = map[string]string{
	"slc-qa-bm1": "64.130.33.90",
	"nyc-mn-bm1": "64.130.37.175",
	"ams-mn-bm1": "23.109.62.84",
	"ams-mn-bm2": "64.34.87.163",
	"fra-mn-bm1": "198.13.138.107",
	"fra-mn-bm2": "85.195.100.119",
	"sin-mn-bm1": "177.54.154.15",
	"tyo-mn-bm1": "208.91.107.71",
	"lon-mn-bm2": "64.34.92.15",
	"tyo-mn-bm2": "206.223.226.183",
}

// EdgeScoreboardSlotRace holds per-slot per-feed win data for recent slots.
type EdgeScoreboardSlotRace struct {
	Host      string  `json:"host"`
	Slot      uint64  `json:"slot"`
	Feed      string  `json:"feed"`
	ShredsWon uint64  `json:"shreds_won"`
	WinPct    float64 `json:"win_pct"`
}

// EdgeScoreboardSlotBucket holds aggregated win-rate data bucketed by slot range,
// covering the full selected time window. Raw counts are returned so the frontend
// can re-aggregate into display buckets of any size.
type EdgeScoreboardSlotBucket struct {
	Host        string `json:"host"`
	SlotBucket  uint64 `json:"slot_bucket"` // first slot of the bucket
	Feed        string `json:"feed"`
	FeedWon     uint64 `json:"feed_won"`     // sum(shreds_won) for this feed in bucket
	BucketTotal uint64 `json:"bucket_total"` // sum(shreds_won) across all feeds in bucket
}

// EdgeScoreboardLeader holds leader validator info for a slot.
type EdgeScoreboardLeader struct {
	Name    string `json:"name,omitempty"`
	Pubkey  string `json:"pubkey"`
	IP      string `json:"ip,omitempty"`
	ASNOrg  string `json:"asn_org,omitempty"`
	City    string `json:"city,omitempty"`
	Country string `json:"country,omitempty"`
}

// EdgeScoreboardResponse is the response for the edge scoreboard endpoint.
type EdgeScoreboardResponse struct {
	Window             string                           `json:"window"`
	LeadersOnly        bool                             `json:"leaders_only"`
	GeneratedAt        time.Time                        `json:"generated_at"`
	CurrentEpoch       uint64                           `json:"current_epoch"`
	CurrentSlot        uint64                           `json:"current_slot"`
	TotalSlots         uint64                           `json:"total_slots"`
	GlobalTotalSlots   uint64                           `json:"global_total_slots"`
	DZSlots            uint64                           `json:"dz_slots"`
	TotalDZLeaderSlots uint64                           `json:"total_dz_leader_slots"`
	CompletenessPct    float64                          `json:"completeness_pct"`
	PublisherCount     uint64                           `json:"publisher_count"`
	PublishingCount    uint64                           `json:"publishing_count"`
	PublishingStakePct float64                          `json:"publishing_stake_pct"`
	Nodes              []EdgeScoreboardNode             `json:"nodes"`
	RecentSlots        []EdgeScoreboardSlotRace         `json:"recent_slots"`
	SlotBuckets        []EdgeScoreboardSlotBucket       `json:"slot_buckets,omitempty"`
	SlotBucketSize     uint64                           `json:"slot_bucket_size,omitempty"`
	SlotLeaders        map[string]*EdgeScoreboardLeader `json:"slot_leaders,omitempty"`
	// DataLagMs is how far behind wall clock the latest row in slot_feed_race_summary is
	// (server now − max(event_ts)). The client adds this to its own queue depth to show a
	// pill reflecting actual on-chain time.
	DataLagMs uint64 `json:"data_lag_ms,omitempty"`
}

// maxValidSlot caps max(slot) queries against shredder tables to exclude corrupted rows.
// Occasional bad inserts produce slot values near 2^64; any of those poison the preamble
// `SELECT max(slot)` so the derived slot-range filter excludes all real data. Valid Solana
// slots are currently ~4e8 and advance ~80M/year, so 1e12 is a generous cap with centuries
// of headroom.
const maxValidSlot = 1_000_000_000_000

// scoreboardFeeds is the whitelist of feed names included in edge scoreboard results.
const scoreboardFeeds = `'dz', 'dz_rebop', 'jito', 'turbine'`

// scoreboardLoserFeeds is the whitelist of competitor feeds shown in lead-time comparisons.
const scoreboardLoserFeeds = `'jito', 'turbine'`

// validWindows maps window parameter values to ClickHouse interval expressions.
var validWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"3d":  "3 DAY",
	"7d":  "7 DAY",
	"30d": "30 DAY",
	"all": "",
}

// slotsPerWindow bounds how many Solana slots fit in each window. Used to derive
// a slot-range filter on slot_feed_race_summary, which is ORDER BY (host, slot, …)
// — so a slot range lets the primary index prune, whereas event_ts alone forces
// a full monthly-partition scan. A small over-estimate is fine (the event_ts
// filter still enforces exact window semantics).
var slotsPerWindow = map[string]uint64{
	"1h":  10_000,
	"24h": 230_000,
	"3d":  700_000,
	"7d":  1_600_000,
	"30d": 6_800_000,
}

// edgeScoreboardCacheKey returns the page cache key for a request, or "" if the request
// is not eligible for caching (non-default window or cursor mode).
func edgeScoreboardCacheKey(r *http.Request) string {
	if r.URL.Query().Get("since_slot") != "" || r.URL.Query().Get("before_slot") != "" {
		return ""
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window != "" && window != "24h" {
		return ""
	}
	// Match the handler default in GetEdgeScoreboard: omitted leaders_only means true.
	leadersOnly := strings.TrimSpace(r.URL.Query().Get("leaders_only")) != "false"
	if leadersOnly {
		return "edge_scoreboard:leaders"
	}
	return "edge_scoreboard"
}

// edgeScoreboardLatestCacheKey returns the cache key for the fast-refreshed latest-slots
// slice. Used by live-tail polls (since_slot set, no before_slot) to avoid hammering
// ClickHouse every few seconds. Returns "" if the request isn't eligible.
func edgeScoreboardLatestCacheKey(r *http.Request) string {
	if r.URL.Query().Get("since_slot") == "" || r.URL.Query().Get("before_slot") != "" {
		return ""
	}
	leadersOnly := strings.TrimSpace(r.URL.Query().Get("leaders_only")) != "false"
	if leadersOnly {
		return "edge_scoreboard:latest:leaders"
	}
	return "edge_scoreboard:latest"
}

// filterSlotsSince returns slots with slot > sinceSlot, in ASC order, capped at limit.
// The cached payload is DESC (latest first) with multiple rows per slot (one per feed/host),
// so we collect all matching rows, then sort ASC by slot, then cap by distinct slot count.
func filterSlotsSince(slots []EdgeScoreboardSlotRace, sinceSlot uint64, limit int) []EdgeScoreboardSlotRace {
	out := make([]EdgeScoreboardSlotRace, 0, len(slots))
	for _, s := range slots {
		if s.Slot > sinceSlot {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	if limit <= 0 {
		return out
	}
	// Cap by distinct slot count so N rows-per-slot don't truncate the feed groupings.
	seen := make(map[uint64]struct{})
	cut := len(out)
	for i, s := range out {
		if _, ok := seen[s.Slot]; !ok {
			if len(seen) >= limit {
				cut = i
				break
			}
			seen[s.Slot] = struct{}{}
		}
	}
	return out[:cut]
}

// GetEdgeScoreboard returns aggregated win rate / completeness data for DZ Edge nodes.
func (a *API) GetEdgeScoreboard(w http.ResponseWriter, r *http.Request) {
	// Try to serve from cache for default (window=1h) requests.
	if isMainnet(r.Context()) {
		if cacheKey := edgeScoreboardCacheKey(r); cacheKey != "" {
			if data, err := a.readPageCache(r.Context(), cacheKey); err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				_, _ = w.Write(data)
				return
			}
		}
	}

	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if _, ok := validWindows[window]; !ok {
		window = "1h"
	}

	leadersOnly := strings.TrimSpace(r.URL.Query().Get("leaders_only")) != "false"

	var sinceSlot uint64
	if s := r.URL.Query().Get("since_slot"); s != "" {
		_, _ = fmt.Sscanf(s, "%d", &sinceSlot)
	}
	var beforeSlot uint64
	if s := r.URL.Query().Get("before_slot"); s != "" {
		_, _ = fmt.Sscanf(s, "%d", &beforeSlot)
	}
	slotLimit := 200
	if l := r.URL.Query().Get("limit"); l != "" {
		_, _ = fmt.Sscanf(l, "%d", &slotLimit)
		if slotLimit < 1 || slotLimit > 3000 {
			slotLimit = 200
		}
	}

	// Live-tail fast path: serve since_slot from the fast-refreshed latest cache when possible.
	if isMainnet(r.Context()) && sinceSlot > 0 && beforeSlot == 0 {
		if cacheKey := edgeScoreboardLatestCacheKey(r); cacheKey != "" {
			if data, err := a.readPageCache(r.Context(), cacheKey); err == nil {
				var cached EdgeScoreboardResponse
				if err := json.Unmarshal(data, &cached); err == nil && cached.CurrentSlot >= sinceSlot {
					trimmed := filterSlotsSince(cached.RecentSlots, sinceSlot, slotLimit)
					resp := cached
					resp.RecentSlots = trimmed
					// Trim slot_leaders to only the slots we're actually returning. The cached
					// payload holds ~1000 leader entries (name/ip/asn/geoip per slot); sending
					// them all on a "no new data" response is 100s of KB of waste per poll.
					if len(cached.SlotLeaders) > 0 {
						trimmedLeaders := make(map[string]*EdgeScoreboardLeader, len(trimmed))
						for _, s := range trimmed {
							key := fmt.Sprintf("%d", s.Slot)
							if leader, ok := cached.SlotLeaders[key]; ok {
								trimmedLeaders[key] = leader
							}
						}
						resp.SlotLeaders = trimmedLeaders
					}
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("X-Cache", "HIT")
					writeJSON(w, &resp)
					return
				}
			}
		}
	}

	resp, err := a.FetchEdgeScoreboardData(ctx, window, leadersOnly, sinceSlot, beforeSlot, slotLimit)
	if err != nil {
		log.Printf("EdgeScoreboard error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, resp)
}

// FetchEdgeScoreboardData performs the actual edge scoreboard queries.
// When leadersOnly is true, results are scoped to slots where the leader published via DZ.
// sinceSlot > 0 or beforeSlot > 0 enables cursor mode: only recent_slots are returned
// and heavy query groups (feed stats, metros, etc.) are skipped.
// sinceSlot returns slots > sinceSlot in ASC order; beforeSlot returns slots < beforeSlot in DESC order.
// slotLimit controls how many recent slots to return (default 200, max 1000).
func (a *API) FetchEdgeScoreboardData(ctx context.Context, window string, leadersOnly bool, sinceSlot uint64, beforeSlot uint64, slotLimit int) (*EdgeScoreboardResponse, error) {
	if slotLimit <= 0 {
		slotLimit = 200
	}
	interval := validWindows[window]
	var timeFilter string
	if interval != "" {
		timeFilter = fmt.Sprintf("AND event_ts >= now() - INTERVAL %s", interval)
	}

	shredderDB := fmt.Sprintf("`%s`", a.ShredderDB)
	publisherDB := fmt.Sprintf("`%s`", a.PublisherDB)

	// rangeFilter combines the time filter with a slot lower bound. The table is
	// ORDER BY (host, slot, …) with monthly partitions by event_ts, so an event_ts
	// filter alone can't prune via the primary index — every query has to scan
	// the entire current month. Deriving a min slot from max(slot) - slotsPerWindow
	// lets the index seek directly to the ~24h range. max(slot) is O(1) against
	// the primary key so the preamble is effectively free.
	rangeFilter := timeFilter
	var dataLagMs uint64
	if slotWindow, ok := slotsPerWindow[window]; ok {
		var maxSlot uint64
		var lagSec float64
		start := time.Now()
		err := a.envDB(ctx).QueryRow(ctx, fmt.Sprintf(
			`SELECT max(slot), toFloat64(toUnixTimestamp(now()) - toUnixTimestamp(max(event_ts))) FROM %s.slot_feed_race_summary WHERE slot < %d`,
			shredderDB, maxValidSlot,
		)).Scan(&maxSlot, &lagSec)
		metrics.RecordClickHouseQuery(time.Since(start), err)
		if err != nil {
			return nil, fmt.Errorf("max slot: %w", err)
		}
		if lagSec > 0 {
			dataLagMs = uint64(lagSec * 1000)
		}
		if maxSlot > slotWindow {
			rangeFilter = fmt.Sprintf("%s AND slot >= %d", timeFilter, maxSlot-slotWindow)
		}
	}

	// In cursor mode the client only consumes recent_slots and slot_leaders, so we
	// skip the expensive aggregate queries (query1–query1d) that feed the full
	// scoreboard view. These run every 5s per live-page poller in prod and can
	// saturate ClickHouse, surfacing as "query1c rows: context deadline exceeded".
	cursorMode := sinceSlot > 0 || beforeSlot > 0

	const slotsPerEpoch = 432_000

	type nodeSlotInfo struct {
		totalSlots  uint64
		dzSlots     uint64
		maxEpoch    uint64
		minSlot     uint64
		maxSlot     uint64
		lastUpdated time.Time
	}
	nodeSlots := make(map[string]*nodeSlotInfo)

	var globalMaxEpoch, globalMaxSlot, globalMinSlot uint64
	dzLeaderSlotsByNode := make(map[string]uint64)
	var dzLeaderCTE string
	var totalDZLeaderSlots, globalTotalSlots uint64
	var publisherCount, publishingCount uint64
	var publishingStakePct float64

	if cursorMode {
		// Derive a slot window from the cursor. slotWindowMin/slotWindowMax (below)
		// are computed from globalMaxSlot; anchoring to the cursor keeps group D's
		// bounds tight without querying the shredder table.
		if sinceSlot > 0 {
			globalMaxSlot = sinceSlot
		} else {
			globalMaxSlot = beforeSlot
		}
		globalMinSlot = globalMaxSlot
		globalMaxEpoch = globalMaxSlot / slotsPerEpoch
	} else {

		// Query 1: Per-node slot counts from win-count rows (loser_feed = '').
		// dz_slots counts all Edge feed slots (dz + dz_rebop) for use as SlotsObserved in
		// all-slots mode. In leaders-only mode, query1b overrides this with DZ-leader slots only.
		// Includes feed count to filter out nodes that only record one feed (e.g. DZ-only nodes).
		query1 := fmt.Sprintf(`
		SELECT
			host,
			uniqExact(slot) AS total_slots,
			uniqExactIf(slot, feed IN ('dz', 'dz_rebop')) AS dz_slots,
			max(epoch) AS max_epoch,
			min(slot) AS min_slot,
			max(slot) AS max_slot,
			max(event_ts) AS last_updated,
			uniqExact(feed) AS feed_count
		FROM %s.slot_feed_race_summary
		WHERE feed_type = 'shred' AND loser_feed = '' %s
		GROUP BY host
	SETTINGS final=1
`, shredderDB, rangeFilter)

		start := time.Now()
		rows1, err := a.envDB(ctx).Query(ctx, query1)
		duration := time.Since(start)
		metrics.RecordClickHouseQuery(duration, err)
		if err != nil {
			return nil, fmt.Errorf("query1: %w", err)
		}
		defer rows1.Close()

		for rows1.Next() {
			var nodeID string
			var info nodeSlotInfo
			var feedCount uint64
			if err := rows1.Scan(&nodeID, &info.totalSlots, &info.dzSlots, &info.maxEpoch, &info.minSlot, &info.maxSlot, &info.lastUpdated, &feedCount); err != nil {
				return nil, fmt.Errorf("query1 scan: %w", err)
			}
			// Skip nodes that only record one feed in the time window — they can't produce
			// meaningful race comparisons. Note: a node can appear single-feed if a second
			// feed joined partway through the window.
			if feedCount < 2 {
				continue
			}
			nodeSlots[nodeID] = &info
		}
		if err := rows1.Err(); err != nil {
			return nil, fmt.Errorf("query1 rows: %w", err)
		}

		// Compute trusted max slot/epoch using the median of per-node values as a reference
		// to filter out corrupted outliers. A single bad row can cause max(slot) to be wildly
		// inflated; using the median anchors us to the real current Solana slot range.
		if len(nodeSlots) > 0 {
			maxSlots := make([]uint64, 0, len(nodeSlots))
			for _, info := range nodeSlots {
				maxSlots = append(maxSlots, info.maxSlot)
			}
			slices.Sort(maxSlots)
			median := maxSlots[len(maxSlots)/2]
			// Accept slots within 2 epochs of the median — generous enough for normal lag,
			// tight enough to exclude corrupted values that are orders of magnitude larger.
			upperBound := median + 2*slotsPerEpoch
			globalMinSlot = ^uint64(0) // max uint64, will be replaced below
			for _, info := range nodeSlots {
				if info.maxSlot <= upperBound && info.maxSlot > globalMaxSlot {
					globalMaxSlot = info.maxSlot
				}
				if info.maxSlot <= upperBound && info.minSlot < globalMinSlot {
					globalMinSlot = info.minSlot
				}
			}
			if globalMinSlot == ^uint64(0) {
				globalMinSlot = globalMaxSlot
			}
			// Use the max epoch reported by nodes (from DB), not derived from slot number.
			// Deriving from slot would be wrong when test data uses small slot numbers in high epochs.
			for _, info := range nodeSlots {
				if info.maxSlot <= upperBound && info.maxEpoch > globalMaxEpoch {
					globalMaxEpoch = info.maxEpoch
				}
			}
		}

		// If no data, return empty response
		if len(nodeSlots) == 0 {
			return &EdgeScoreboardResponse{
				Window:      window,
				GeneratedAt: time.Now().UTC(),
				Nodes:       []EdgeScoreboardNode{},
			}, nil
		}

		// DZ-leader slot filter: use publisher_shred_stats.is_scheduled_leader to identify
		// slots where the leader was publishing shreds via DZ. This is the authoritative
		// source — it comes from the shredder's own observation of DZ multicast traffic.
		dzLeaderCTE = fmt.Sprintf(`dz_leader_slots AS (
		SELECT DISTINCT slot
		FROM %s.publisher_shred_stats
		WHERE is_scheduled_leader = true %s
	)`, publisherDB, timeFilter)

		// Query 1b: DZ-leader slot counts per node — always run regardless of leadersOnly.
		// Counts slots where the dz feed was present (won shreds OR appeared as a pairwise loser),
		// intersected with dz_leader_slots from publisher_shred_stats. Using OR loser_feed='dz'
		// ensures nodes like tyo (where dz loses to dz_rebop every time) still count correctly.
		// In leaders-only mode, also overrides info.dzSlots so SlotsObserved reflects leader slots.
		{
			query1b := fmt.Sprintf(`
			WITH %s
			SELECT host, uniqExact(slot) AS dz_leader_slots
			FROM %s.slot_feed_race_summary
			WHERE feed_type = 'shred' AND (feed = 'dz' OR loser_feed = 'dz')
				AND slot IN (SELECT slot FROM dz_leader_slots)
				%s
			GROUP BY host
		SETTINGS final=1
`, dzLeaderCTE, shredderDB, rangeFilter)

			start = time.Now()
			rows1b, err := a.envDB(ctx).Query(ctx, query1b)
			duration = time.Since(start)
			metrics.RecordClickHouseQuery(duration, err)
			if err != nil {
				return nil, fmt.Errorf("query1b: %w", err)
			}
			defer rows1b.Close()

			for rows1b.Next() {
				var nodeID string
				var count uint64
				if err := rows1b.Scan(&nodeID, &count); err != nil {
					return nil, fmt.Errorf("query1b scan: %w", err)
				}
				dzLeaderSlotsByNode[nodeID] = count
			}
			if err := rows1b.Err(); err != nil {
				return nil, fmt.Errorf("query1b rows: %w", err)
			}

			// In leaders-only mode, SlotsObserved = DZ-leader slots (override query1 value).
			if leadersOnly {
				for _, info := range nodeSlots {
					info.dzSlots = 0
				}
				for nodeID, count := range dzLeaderSlotsByNode {
					if info, ok := nodeSlots[nodeID]; ok {
						info.dzSlots = count
					}
				}
			}
		}

		// Query 1c: DZ-leader slot count and total slot count.
		// dz_leader_slots comes directly from publisher_shred_stats (is_scheduled_leader=true) —
		// the authoritative count of slots where the scheduled leader published via DZ.
		// total_slots is the distinct slot count from slot_feed_race_summary aggregate rows.
		// completeness_pct = dz_leader_slots / total_slots — fraction of slots with a DZ leader.
		{
			query1c := fmt.Sprintf(`
			WITH %s
			SELECT
				(SELECT count() FROM dz_leader_slots) AS dz_leader_slots,
				uniqExact(slot) AS total_slots
			FROM %s.slot_feed_race_summary
			WHERE feed_type = 'shred' AND loser_feed = '' %s
		SETTINGS final=1
`, dzLeaderCTE, shredderDB, rangeFilter)
			start = time.Now()
			rows1c, err := a.envDB(ctx).Query(ctx, query1c)
			duration = time.Since(start)
			metrics.RecordClickHouseQuery(duration, err)
			if err != nil {
				return nil, fmt.Errorf("query1c: %w", err)
			}
			if rows1c.Next() {
				if err := rows1c.Scan(&totalDZLeaderSlots, &globalTotalSlots); err != nil {
					rows1c.Close()
					return nil, fmt.Errorf("query1c scan: %w", err)
				}
			}
			rows1c.Close()
			if err := rows1c.Err(); err != nil {
				return nil, fmt.Errorf("query1c rows: %w", err)
			}
		}

		// Query 1d: Publisher stats using the same method as the publisher check page.
		// publisher_count = activated DZ users with publishers and matched gossip+stake (same as total_publishers).
		// publishing_count / publishing_stake_pct = subset with leader_slots > 0 (same as "Publishing Shreds").
		{
			shredStatsTable := fmt.Sprintf("`%s`.publisher_shred_stats", a.PublisherDB)
			start = time.Now()
			err = a.envDB(ctx).QueryRow(ctx, fmt.Sprintf(`
			WITH current_epoch AS (
				SELECT max(epoch) AS epoch FROM %s
			),
			leader_pubkeys AS (
				SELECT DISTINCT dz_user_pubkey
				FROM %s
				WHERE epoch >= (SELECT epoch FROM current_epoch) - 1
				  AND is_scheduled_leader = true
			),
			total_network_stake AS (
				SELECT sum(activated_stake_lamports) AS stake
				FROM solana_vote_accounts_current
				WHERE epoch_vote_account = 'true' AND activated_stake_lamports > 0
			)
			SELECT
				countIf(v.vote_pubkey != '' AND g.pubkey != '') AS publisher_count,
				countIf(v.vote_pubkey != '' AND g.pubkey != '' AND l.dz_user_pubkey != '') AS publishing_count,
				COALESCE(sumIf(v.activated_stake_lamports, v.vote_pubkey != '' AND g.pubkey != '' AND l.dz_user_pubkey != ''), 0)
					/ greatest(COALESCE((SELECT stake FROM total_network_stake), 0), 1) * 100 AS publishing_stake_pct
			FROM dz_users_current u
			LEFT JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip AND u.client_ip != ''
			LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
			LEFT JOIN leader_pubkeys l ON u.pk = l.dz_user_pubkey
			WHERE u.status = 'activated'
			  AND JSONLength(u.publishers) > 0
		`, shredStatsTable, shredStatsTable),
			).Scan(&publisherCount, &publishingCount, &publishingStakePct)
			duration = time.Since(start)
			metrics.RecordClickHouseQuery(duration, err)
			if err != nil {
				log.Printf("EdgeScoreboard query1d error: %v", err)
			}
		}

	} // end !cursorMode aggregate queries

	// Build node ID list and location codes for parallel queries below.
	type feedKey struct {
		nodeID string
		feed   string
	}

	locationCodes := make(map[string]bool)
	nodeLocations := make(map[string]string)
	for nodeID := range nodeSlots {
		parts := strings.SplitN(nodeID, "-", 2)
		loc := strings.ToUpper(parts[0])
		locationCodes[loc] = true
		nodeLocations[nodeID] = loc
	}

	validNodeIDs := make([]string, 0, len(nodeSlots))
	for id := range nodeSlots {
		validNodeIDs = append(validNodeIDs, "'"+id+"'")
	}
	nodeList := strings.Join(validNodeIDs, ",")

	slotWindowMax := globalMaxSlot + 2*slotsPerEpoch

	type metroInfo struct {
		name      string
		latitude  float64
		longitude float64
	}
	type stakeInfo struct {
		stakeSol   float64
		validators uint64
	}

	type nodeGeoInfo struct {
		ip      string
		asn     int64
		asnOrg  string
		city    string
		country string
		pubkey  string
	}

	// Run query groups in parallel:
	//   A: feed win rates (q2) + lead times (q2b)
	//   B: metro coordinates (q3)
	//   C: stake by metro (q4)
	//   D: recent slot races (q5) + slot leader enrichment (q6a, q6b)
	//   E: bucketed slot win rates (q7)
	//   F: node geoip enrichment (q8)
	// In cursor mode (sinceSlot > 0 or beforeSlot > 0) only group D runs — the other groups are
	// expensive and only needed for the full scoreboard view.
	var (
		feedStats      map[feedKey]*EdgeScoreboardFeedStats
		leadTimeStats  map[feedKey][]EdgeScoreboardLeadTime
		metros         = make(map[string]*metroInfo)
		stakeByMetro   = make(map[string]*stakeInfo)
		recentSlots    []EdgeScoreboardSlotRace
		slotBuckets    []EdgeScoreboardSlotBucket
		slotBucketSize uint64
		slotLeaders    = make(map[string]*EdgeScoreboardLeader)
		nodeGeo        = make(map[string]*nodeGeoInfo)
	)

	g, gctx := errgroup.WithContext(ctx)

	// Groups A–C and E–F are skipped in cursor mode (sinceSlot > 0 or beforeSlot > 0) since the caller
	// only needs recent_slots and those queries are expensive.
	if !cursorMode {

		// Group A: feed win rates → lead times
		g.Go(func() error {
			localFeedStats := make(map[feedKey]*EdgeScoreboardFeedStats)

			// q2: per-host share-of-wins across the scoreboard feeds.
			// denom = sum(shreds_won) across all scoreboard feeds for in-scope slots —
			// only races one of the tracked feeds won first. Every feed's win_rate_pct is
			// its share of those races, so dz + dz_rebop + jito + turbine sum to 100%
			// and dz_edge (synthesized below from dz + dz_rebop) is additive.
			// This excludes shreds won first by untracked feeds (regional retransmits,
			// provider_one, etc.) so the scoreboard isn't diluted by new feeds we don't track.
			var q2 string
			if leadersOnly {
				q2 = fmt.Sprintf(`
				WITH %[1]s,
				feed_totals AS (
					SELECT host, feed, sum(shreds_won) AS shreds_won
					FROM %[2]s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = '' AND feed IN (%[3]s)
						AND slot IN (SELECT slot FROM dz_leader_slots)
						%[4]s
					GROUP BY host, feed
				),
				host_denom AS (
					SELECT host, sum(shreds_won) AS denom
					FROM feed_totals
					GROUP BY host
				)
				SELECT
					ft.host, ft.feed, ft.shreds_won, hd.denom AS total_shreds,
					if(hd.denom > 0, round(ft.shreds_won / hd.denom * 100, 1), 0) AS win_rate_pct
				FROM feed_totals ft
				INNER JOIN host_denom hd ON ft.host = hd.host
			SETTINGS final=1
`, dzLeaderCTE, shredderDB, scoreboardFeeds, rangeFilter)
			} else {
				q2 = fmt.Sprintf(`
				WITH feed_totals AS (
					SELECT host, feed, sum(shreds_won) AS shreds_won
					FROM %[1]s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = '' AND feed IN (%[2]s) %[3]s
					GROUP BY host, feed
				),
				host_denom AS (
					SELECT host, sum(shreds_won) AS denom
					FROM feed_totals
					GROUP BY host
				)
				SELECT
					ft.host, ft.feed, ft.shreds_won, hd.denom AS total_shreds,
					if(hd.denom > 0, round(ft.shreds_won / hd.denom * 100, 1), 0) AS win_rate_pct
				FROM feed_totals ft
				INNER JOIN host_denom hd ON ft.host = hd.host
			SETTINGS final=1
`, shredderDB, scoreboardFeeds, rangeFilter)
			}

			t := time.Now()
			rows, err := a.envDB(gctx).Query(gctx, q2)
			metrics.RecordClickHouseQuery(time.Since(t), err)
			if err != nil {
				return fmt.Errorf("query2: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var nodeID, feed string
				var shredsWon, totalShreds uint64
				var winRatePct float64
				if err := rows.Scan(&nodeID, &feed, &shredsWon, &totalShreds, &winRatePct); err != nil {
					return fmt.Errorf("query2 scan: %w", err)
				}
				localFeedStats[feedKey{nodeID, feed}] = &EdgeScoreboardFeedStats{
					ShredsWon:   shredsWon,
					TotalShreds: totalShreds,
					WinRatePct:  winRatePct,
				}
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("query2 rows: %w", err)
			}

			// Synthesize dz_edge = dz + dz_rebop on the shared per-host denominator.
			// Because both dz and dz_rebop carry the same host_denom as TotalShreds,
			// dz_edge.win_rate_pct == dz.win_rate_pct + dz_rebop.win_rate_pct exactly.
			hosts := make(map[string]struct{})
			for k := range localFeedStats {
				hosts[k.nodeID] = struct{}{}
			}
			for nodeID := range hosts {
				dz := localFeedStats[feedKey{nodeID, "dz"}]
				rebop := localFeedStats[feedKey{nodeID, "dz_rebop"}]
				if dz == nil && rebop == nil {
					continue
				}
				var denom, won uint64
				if dz != nil {
					denom = dz.TotalShreds
					won += dz.ShredsWon
				}
				if rebop != nil {
					if denom == 0 {
						denom = rebop.TotalShreds
					}
					won += rebop.ShredsWon
				}
				var rate float64
				if denom > 0 {
					rate = float64(int(float64(won)/float64(denom)*1000+0.5)) / 10
				}
				localFeedStats[feedKey{nodeID, "dz_edge"}] = &EdgeScoreboardFeedStats{
					ShredsWon:   won,
					TotalShreds: denom,
					WinRatePct:  rate,
				}
			}

			feedStats = localFeedStats
			return nil
		})

		// Group A2: pairwise lead times (q2b) for the synthetic dz_edge feed vs competitors.
		// Runs in parallel with Group A so it is never starved by q2+q2c consuming
		// the 45s budget, which caused intermittent empty lead-time columns.
		// Leaders-only uses feed='dz' (leader path only). All-slots pools dz + dz_rebop
		// so retransmit wins are represented — direct quantile over the combined rows
		// (no per-slot argMin CTE, which was too expensive over the full table).
		g.Go(func() error {
			var q2b string
			if leadersOnly {
				q2b = fmt.Sprintf(`
				WITH %s
				SELECT
					host, loser_feed,
					count() AS slot_count,
					quantile(0.5)(lead_time_p50_ms) AS p50_ms,
					quantile(0.95)(lead_time_p95_ms) AS p95_ms
				FROM %s.slot_feed_race_summary
				WHERE feed_type = 'shred' AND feed = 'dz' AND loser_feed IN (%s)
					AND slot IN (SELECT slot FROM dz_leader_slots)
					AND lead_time_p50_ms <= 500
					%s
				GROUP BY host, loser_feed
			SETTINGS final=1
`, dzLeaderCTE, shredderDB, scoreboardLoserFeeds, rangeFilter)
			} else {
				q2b = fmt.Sprintf(`
				SELECT
					host, loser_feed,
					count() AS slot_count,
					quantile(0.5)(lead_time_p50_ms) AS p50_ms,
					quantile(0.95)(lead_time_p95_ms) AS p95_ms
				FROM %s.slot_feed_race_summary
				WHERE feed_type = 'shred' AND feed IN ('dz', 'dz_rebop') AND loser_feed IN (%s)
					AND lead_time_p50_ms <= 500
					%s
				GROUP BY host, loser_feed
			SETTINGS final=1
`, shredderDB, scoreboardLoserFeeds, rangeFilter)
			}
			t := time.Now()
			rows2b, err := a.envDB(gctx).Query(gctx, q2b)
			metrics.RecordClickHouseQuery(time.Since(t), err)
			if err != nil {
				return fmt.Errorf("query2b: %w", err)
			}
			defer rows2b.Close()
			local := make(map[feedKey][]EdgeScoreboardLeadTime)
			for rows2b.Next() {
				var nodeID, loserFeed string
				var slotCount uint64
				var p50, p95 float64
				if err := rows2b.Scan(&nodeID, &loserFeed, &slotCount, &p50, &p95); err != nil {
					return fmt.Errorf("query2b scan: %w", err)
				}
				key := feedKey{nodeID, "dz_edge"}
				local[key] = append(local[key], EdgeScoreboardLeadTime{
					LoserFeed: loserFeed,
					P50Ms:     p50,
					P95Ms:     p95,
					SlotCount: slotCount,
				})
			}
			if err := rows2b.Err(); err != nil {
				return fmt.Errorf("query2b rows: %w", err)
			}
			leadTimeStats = local
			return nil
		})

		// Group B: metro coordinates
		g.Go(func() error {
			if len(locationCodes) == 0 {
				return nil
			}
			codes := make([]string, 0, len(locationCodes))
			for code := range locationCodes {
				codes = append(codes, strings.ToLower(code))
			}
			t := time.Now()
			rows, err := a.envDB(gctx).Query(gctx, `SELECT code, name, latitude, longitude FROM dz_metros_current WHERE code IN (?)`, codes)
			metrics.RecordClickHouseQuery(time.Since(t), err)
			if err != nil {
				return fmt.Errorf("query3: %w", err)
			}
			defer rows.Close()
			localMetros := make(map[string]*metroInfo)
			for rows.Next() {
				var code, name string
				var lat, lon float64
				if err := rows.Scan(&code, &name, &lat, &lon); err != nil {
					return fmt.Errorf("query3 scan: %w", err)
				}
				localMetros[strings.ToUpper(code)] = &metroInfo{name: name, latitude: lat, longitude: lon}
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("query3 rows: %w", err)
			}
			metros = localMetros
			return nil
		})

		// Group C: stake by metro (non-fatal)
		g.Go(func() error {
			query4 := `
			WITH validator_locations AS (
				SELECT
					va.vote_pubkey,
					va.activated_stake_lamports / 1000000000.0 AS stake_sol,
					g.latitude AS vlat,
					g.longitude AS vlon
				FROM solana_vote_accounts_current va
				JOIN solana_gossip_nodes_current gn ON va.node_pubkey = gn.pubkey
				JOIN geoip_records_current g ON gn.gossip_ip = g.ip
				WHERE va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
			),
			nearest_metro AS (
				SELECT
					v.vote_pubkey,
					v.stake_sol,
					arrayElement(
						arraySort(
							(x, y) -> y,
							groupArray(m.code),
							groupArray(
								sqrt(pow(v.vlat - m.latitude, 2) + pow(v.vlon - m.longitude, 2))
							)
						), 1
					) AS metro_code
				FROM validator_locations v
				CROSS JOIN dz_metros_current m
				GROUP BY v.vote_pubkey, v.stake_sol
			)
			SELECT upper(metro_code) AS metro, count() AS validators, sum(stake_sol) AS total_stake_sol
			FROM nearest_metro
			GROUP BY metro_code`
			t := time.Now()
			rows, err := a.envDB(gctx).Query(gctx, query4)
			metrics.RecordClickHouseQuery(time.Since(t), err)
			if err != nil && gctx.Err() == nil {
				log.Printf("EdgeScoreboard query4 error: %v", err)
				return nil
			} else if err != nil {
				return nil
			}
			defer rows.Close()
			localStake := make(map[string]*stakeInfo)
			for rows.Next() {
				var code string
				var si stakeInfo
				if err := rows.Scan(&code, &si.validators, &si.stakeSol); err != nil {
					log.Printf("EdgeScoreboard query4 scan error: %v", err)
					break
				}
				localStake[strings.ToUpper(code)] = &si
			}
			stakeByMetro = localStake
			return nil
		})

	} // end !cursorMode (groups A–C)

	// Group D: recent slot races (q5) → leader enrichment (q6a, q6b) — all non-fatal
	g.Go(func() error {
		var localSlots []EdgeScoreboardSlotRace
		localLeaders := make(map[string]*EdgeScoreboardLeader)

		// Slot window bounds derived from the trusted max slot computed in query1.
		// Using Go-side literals instead of a max(slot) subquery makes the window
		// resilient to corrupted outlier rows that could inflate max(slot).
		slotWindowMin := globalMaxSlot - 10000

		// For recent slots, dz_leader_slots must NOT use the time window filter.
		// The time filter is based on event_ts in publisher_shred_stats, which has much
		// longer history than slot_feed_race_summary. A wide window (e.g. 7d) would pull
		// in old leader slots that don't exist in the recent slot range, shrinking results.
		// Instead, scope to the recent slot range directly so the intersection is correct.
		dzLeaderCTEForRecent := fmt.Sprintf(`dz_leader_slots AS (
			SELECT DISTINCT slot
			FROM %s.publisher_shred_stats
			WHERE is_scheduled_leader = true
			AND slot BETWEEN %d AND %d
		)`, publisherDB, slotWindowMin, slotWindowMax)

		// slotFilter restricts slots for cursor-based fetches. sinceSlot fetches forward (ASC),
		// beforeSlot fetches backward (DESC, the default).
		slotFilter := ""
		orderDir := "DESC"
		if sinceSlot > 0 {
			slotFilter = fmt.Sprintf("AND slot > %d", sinceSlot)
			orderDir = "ASC"
		} else if beforeSlot > 0 {
			slotFilter = fmt.Sprintf("AND slot < %d", beforeSlot)
		}

		// Recent slots queries use slot-range bounds only (not timeFilter/nodeList/nodeCount),
		// so the chart always shows the same N slots regardless of the selected time window.
		var query5 string
		if leadersOnly {
			query5 = fmt.Sprintf(`
				WITH %s,
				active_hosts AS (
					SELECT host
					FROM %s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = ''
						AND slot BETWEEN %d AND %d
					GROUP BY host
					HAVING uniqExact(feed) >= 2
				),
				dz_slots AS (
					SELECT DISTINCT slot
					FROM dz_leader_slots
					WHERE slot BETWEEN %d AND %d
				),
				common_slots AS (
					SELECT slot
					FROM (
						SELECT DISTINCT host, slot
						FROM %s.slot_feed_race_summary
						WHERE feed_type = 'shred' AND loser_feed = ''
							AND host IN (SELECT host FROM active_hosts)
							AND slot IN (SELECT slot FROM dz_slots)
							%s
					)
					GROUP BY slot
					HAVING count(DISTINCT host) = (SELECT count(*) FROM active_hosts)
					ORDER BY slot %s
					LIMIT %d
				)
				SELECT r.host, r.slot, r.feed, r.shreds_won,
					round(r.shreds_won / greatest(r.total_shreds, 1) * 100, 1) AS win_pct
				FROM %s.slot_feed_race_summary AS r
				INNER JOIN common_slots cs ON r.slot = cs.slot
				WHERE r.feed_type = 'shred' AND r.loser_feed = '' AND r.feed IN (%s)
					AND r.slot BETWEEN %d AND %d
					AND r.host IN (SELECT host FROM active_hosts)
				ORDER BY r.host, r.slot, r.feed
			SETTINGS final=1
`, dzLeaderCTEForRecent, shredderDB, slotWindowMin, slotWindowMax, slotWindowMin, slotWindowMax, shredderDB, slotFilter, orderDir, slotLimit, shredderDB, scoreboardFeeds, slotWindowMin, slotWindowMax)
		} else {
			query5 = fmt.Sprintf(`
				WITH active_hosts AS (
					SELECT host
					FROM %s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = ''
						AND slot BETWEEN %d AND %d
					GROUP BY host
					HAVING uniqExact(feed) >= 2
				),
				common_slots AS (
					SELECT slot
					FROM (
						SELECT DISTINCT host, slot
						FROM %s.slot_feed_race_summary
						WHERE feed_type = 'shred' AND loser_feed = ''
							AND host IN (SELECT host FROM active_hosts)
							AND slot BETWEEN %d AND %d
							%s
					)
					GROUP BY slot
					HAVING count(DISTINCT host) = (SELECT count(*) FROM active_hosts)
					ORDER BY slot %s
					LIMIT %d
				)
				SELECT r.host, r.slot, r.feed, r.shreds_won,
					round(r.shreds_won / greatest(r.total_shreds, 1) * 100, 1) AS win_pct
				FROM %s.slot_feed_race_summary AS r
				INNER JOIN common_slots cs ON r.slot = cs.slot
				WHERE r.feed_type = 'shred' AND r.loser_feed = '' AND r.feed IN (%s)
					AND r.slot BETWEEN %d AND %d
					AND r.host IN (SELECT host FROM active_hosts)
				ORDER BY r.host, r.slot, r.feed
			SETTINGS final=1
`, shredderDB, slotWindowMin, slotWindowMax, shredderDB, slotWindowMin, slotWindowMax, slotFilter, orderDir, slotLimit, shredderDB, scoreboardFeeds, slotWindowMin, slotWindowMax)
		}
		t := time.Now()
		rows5, err := a.envDB(gctx).Query(gctx, query5)
		metrics.RecordClickHouseQuery(time.Since(t), err)
		if err != nil {
			if gctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("query5: %w", err)
		}
		defer rows5.Close()
		for rows5.Next() {
			var sr EdgeScoreboardSlotRace
			if err := rows5.Scan(&sr.Host, &sr.Slot, &sr.Feed, &sr.ShredsWon, &sr.WinPct); err != nil {
				return fmt.Errorf("query5 scan: %w", err)
			}
			localSlots = append(localSlots, sr)
		}
		if err := rows5.Err(); err != nil {
			return fmt.Errorf("query5 rows: %w", err)
		}

		if len(localSlots) > 0 {
			const slotsPerEpoch uint64 = 432_000

			// Group slots by epoch — recent slots may span an epoch boundary, so we
			// cannot assume all slots belong to globalMaxEpoch.
			type epochRelSlot struct {
				epoch uint64
				rel   uint64
			}
			slotSet := make(map[uint64]bool)
			for _, sr := range localSlots {
				slotSet[sr.Slot] = true
			}
			// relToAbs: (epoch, relSlot) → absSlot
			relByEpoch := make(map[uint64][]uint64)   // epoch → []relSlot
			relToAbs := make(map[epochRelSlot]uint64) // (epoch, rel) → absSlot
			for s := range slotSet {
				epoch := s / slotsPerEpoch
				rel := s - epoch*slotsPerEpoch
				relByEpoch[epoch] = append(relByEpoch[epoch], rel)
				relToAbs[epochRelSlot{epoch, rel}] = s
			}

			slotPubkeys := make(map[uint64]string)
			pubkeySet := make(map[string]bool)
			for epoch, relSlots := range relByEpoch {
				query6a := fmt.Sprintf(`
					SELECT
						arrayJoin(JSONExtract(slots, 'Array(UInt64)')) AS slot,
						node_pubkey
					FROM dim_solana_leader_schedule_history
					WHERE epoch = %d
					HAVING slot IN (?)
				`, epoch)

				t = time.Now()
				rows6a, err := a.envDB(gctx).Query(gctx, query6a, relSlots)
				metrics.RecordClickHouseQuery(time.Since(t), err)
				if err != nil {
					if gctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("query6a (epoch %d): %w", epoch, err)
				}
				for rows6a.Next() {
					var relSlot uint64
					var pubkey string
					if err := rows6a.Scan(&relSlot, &pubkey); err != nil {
						rows6a.Close()
						return fmt.Errorf("query6a scan: %w", err)
					}
					absSlot := relToAbs[epochRelSlot{epoch, relSlot}]
					slotPubkeys[absSlot] = pubkey
					pubkeySet[pubkey] = true
				}
				if err := rows6a.Err(); err != nil {
					rows6a.Close()
					return fmt.Errorf("query6a rows: %w", err)
				}
				rows6a.Close()
			}

			if len(pubkeySet) > 0 {
				pubkeys := make([]string, 0, len(pubkeySet))
				for pk := range pubkeySet {
					pubkeys = append(pubkeys, pk)
				}
				query6b := `
					SELECT
						v.account,
						COALESCE(v.name, ''),
						COALESCE(v.ip, ''),
						COALESCE(g.asn_org, ''),
						COALESCE(g.city, ''),
						COALESCE(g.country, '')
					FROM validatorsapp_validators_current v
					LEFT JOIN geoip_records_current g ON g.ip = v.ip
					WHERE v.account IN (?)
				`
				t = time.Now()
				rows6b, err := a.envDB(gctx).Query(gctx, query6b, pubkeys)
				metrics.RecordClickHouseQuery(time.Since(t), err)
				if err != nil {
					if gctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("query6b: %w", err)
				}
				defer rows6b.Close()

				type leaderInfo struct {
					name, ip, asnOrg, city, country string
				}
				infoByPubkey := make(map[string]*leaderInfo)
				for rows6b.Next() {
					var pubkey string
					var li leaderInfo
					if err := rows6b.Scan(&pubkey, &li.name, &li.ip, &li.asnOrg, &li.city, &li.country); err != nil {
						return fmt.Errorf("query6b scan: %w", err)
					}
					infoByPubkey[pubkey] = &li
				}
				if err := rows6b.Err(); err != nil {
					return fmt.Errorf("query6b rows: %w", err)
				}

				for absSlot, pubkey := range slotPubkeys {
					leader := &EdgeScoreboardLeader{Pubkey: pubkey}
					if li, ok := infoByPubkey[pubkey]; ok {
						leader.Name = li.name
						leader.IP = li.ip
						leader.ASNOrg = li.asnOrg
						leader.City = li.city
						leader.Country = li.country
					}
					localLeaders[fmt.Sprintf("%d", absSlot)] = leader
				}
			}
		}

		if localSlots == nil {
			localSlots = []EdgeScoreboardSlotRace{}
		}
		recentSlots = localSlots
		slotLeaders = localLeaders
		return nil
	})

	if !cursorMode {

		// Group E: bucketed slot win rates across the full time window (q7) — non-fatal
		g.Go(func() error {
			// Compute bucket size from the expected window slot range, targeting ~500 fine-grained
			// buckets. Using the window (not observed data range) ensures consistent bucket
			// boundaries regardless of data sparsity. The frontend fills in the full expected
			// range from current_slot so all nodes share the same x-axis.
			const slotsPerSec = 2.5
			const targetBuckets = 500
			windowSlotRange := map[string]uint64{
				"1h":  uint64(3600 * slotsPerSec),
				"24h": uint64(86400 * slotsPerSec),
				"3d":  uint64(3 * 86400 * slotsPerSec),
				"7d":  uint64(7 * 86400 * slotsPerSec),
				"30d": uint64(30 * 86400 * slotsPerSec),
			}
			targetRange, ok := windowSlotRange[window]
			if !ok {
				targetRange = globalMaxSlot - globalMinSlot // "all"
			}
			bucketSize := uint64(1)
			if targetRange > targetBuckets {
				bucketSize = targetRange / targetBuckets
			}
			slotBucketSize = bucketSize

			// The correct denominator for bucketed win rate is the total shreds across ALL
			// feeds per bucket, not per-feed total_shreds. Some feeds only have rows when
			// they win shreds (0-win slots are absent), so sum(total_shreds) per feed has
			// a smaller denominator than the shared total, inflating win rates.
			// Instead: bucket_total = sum(shreds_won across all feeds) = sum(total_shreds
			// per slot), since every shred in a race is won by exactly one feed.
			var query7 string
			if leadersOnly {
				query7 = fmt.Sprintf(`
				WITH %s,
				per_feed AS (
					SELECT
						host,
						intDiv(slot, %d) * %d AS slot_bucket,
						feed,
						sum(shreds_won) AS feed_won
					FROM %s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = '' AND feed IN (%s)
						AND host IN (%s)
						AND slot IN (SELECT slot FROM dz_leader_slots)
						AND slot <= %d
						%s
					GROUP BY host, slot_bucket, feed
				),
				bucket_totals AS (
					SELECT host, slot_bucket, sum(feed_won) AS bucket_total
					FROM per_feed
					GROUP BY host, slot_bucket
				)
				SELECT f.host, f.slot_bucket, f.feed, f.feed_won, bt.bucket_total
				FROM per_feed f
				JOIN bucket_totals bt ON f.host = bt.host AND f.slot_bucket = bt.slot_bucket
				ORDER BY f.host, f.slot_bucket, f.feed
			SETTINGS final=1
`, dzLeaderCTE, bucketSize, bucketSize, shredderDB, scoreboardFeeds, nodeList, slotWindowMax, rangeFilter)
			} else {
				query7 = fmt.Sprintf(`
				WITH per_feed AS (
					SELECT
						host,
						intDiv(slot, %d) * %d AS slot_bucket,
						feed,
						sum(shreds_won) AS feed_won
					FROM %s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = '' AND feed IN (%s)
						AND host IN (%s)
						AND slot <= %d
						%s
					GROUP BY host, slot_bucket, feed
				),
				bucket_totals AS (
					SELECT host, slot_bucket, sum(feed_won) AS bucket_total
					FROM per_feed
					GROUP BY host, slot_bucket
				)
				SELECT f.host, f.slot_bucket, f.feed, f.feed_won, bt.bucket_total
				FROM per_feed f
				JOIN bucket_totals bt ON f.host = bt.host AND f.slot_bucket = bt.slot_bucket
				ORDER BY f.host, f.slot_bucket, f.feed
			SETTINGS final=1
`, bucketSize, bucketSize, shredderDB, scoreboardFeeds, nodeList, slotWindowMax, rangeFilter)
			}

			t := time.Now()
			rows7, err := a.envDB(gctx).Query(gctx, query7)
			metrics.RecordClickHouseQuery(time.Since(t), err)
			if err != nil && gctx.Err() == nil {
				log.Printf("EdgeScoreboard query7 error: %v", err)
				return nil
			}
			if err == nil {
				defer rows7.Close()
				var localBuckets []EdgeScoreboardSlotBucket
				for rows7.Next() {
					var sb EdgeScoreboardSlotBucket
					if err := rows7.Scan(&sb.Host, &sb.SlotBucket, &sb.Feed, &sb.FeedWon, &sb.BucketTotal); err != nil {
						log.Printf("EdgeScoreboard query7 scan error: %v", err)
						break
					}
					localBuckets = append(localBuckets, sb)
				}
				slotBuckets = localBuckets
			}
			return nil
		})

		// Group F: node geoip enrichment via hardcoded host→IP map (non-fatal)
		g.Go(func() error {
			ips := make([]string, 0, len(nodeSlots))
			ipToHost := make(map[string]string)
			for nodeID := range nodeSlots {
				if ip, ok := edgeNodeIPs[nodeID]; ok {
					ips = append(ips, "'"+ip+"'")
					ipToHost[ip] = nodeID
				}
			}
			if len(ips) == 0 {
				return nil
			}
			ipList := strings.Join(ips, ",")
			query8 := fmt.Sprintf(`
			SELECT
				g.ip,
				COALESCE(g.asn, 0),
				COALESCE(g.asn_org, ''),
				COALESCE(g.city, ''),
				COALESCE(g.country, ''),
				COALESCE(gn.pubkey, '')
			FROM geoip_records_current g
			LEFT JOIN solana_gossip_nodes_current gn ON gn.gossip_ip = g.ip
			WHERE g.ip IN (%s)
		`, ipList)
			rows8, err := a.envDB(gctx).Query(gctx, query8)
			if err != nil {
				log.Printf("EdgeScoreboard query8 (geoip) error: %v", err)
				return nil
			}
			defer rows8.Close()
			localGeo := make(map[string]*nodeGeoInfo)
			for rows8.Next() {
				var gi nodeGeoInfo
				if err := rows8.Scan(&gi.ip, &gi.asn, &gi.asnOrg, &gi.city, &gi.country, &gi.pubkey); err != nil {
					log.Printf("EdgeScoreboard query8 scan error: %v", err)
					break
				}
				if host, ok := ipToHost[gi.ip]; ok {
					localGeo[host] = &gi
				}
			}
			nodeGeo = localGeo
			return nil
		})

	} // end !cursorMode (groups E–F)

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Merge lead times from Group A2 into feedStats.
	for key, lts := range leadTimeStats {
		if feedStats == nil {
			break
		}
		fs, ok := feedStats[key]
		if !ok {
			fs = &EdgeScoreboardFeedStats{}
			feedStats[key] = fs
		}
		fs.LeadTimes = lts
	}

	// Assemble response
	var totalSlots, dzSlots uint64
	nodes := make([]EdgeScoreboardNode, 0, len(nodeSlots))

	for nodeID, info := range nodeSlots {
		totalSlots += info.totalSlots
		dzSlots += info.dzSlots

		loc := nodeLocations[nodeID]
		node := EdgeScoreboardNode{
			Host:          nodeID,
			Location:      loc,
			TotalSlots:    info.totalSlots,
			SlotsObserved: info.dzSlots,
			LastUpdated:   info.lastUpdated,
			Feeds:         make(map[string]*EdgeScoreboardFeedStats),
		}

		if m, ok := metros[loc]; ok {
			node.MetroName = m.name
			node.Latitude = m.latitude
			node.Longitude = m.longitude
		}

		if si, ok := stakeByMetro[loc]; ok {
			node.StakeSol = si.stakeSol
			node.Validators = si.validators
		}

		// Attach feed stats
		for key, fs := range feedStats {
			if key.nodeID == nodeID {
				if fs.LeadTimes == nil {
					fs.LeadTimes = []EdgeScoreboardLeadTime{}
				}
				node.Feeds[key.feed] = fs
			}
		}

		node.DZLeaderSlots = dzLeaderSlotsByNode[nodeID]

		if gi, ok := nodeGeo[nodeID]; ok {
			node.GossipIP = gi.ip
			node.GossipPubkey = gi.pubkey
			node.ASN = gi.asn
			node.ASNOrg = gi.asnOrg
			node.City = gi.city
			node.Country = gi.country
		}

		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Host < nodes[j].Host
	})

	// completeness_pct: fraction of all slots where the leader was publishing via DZ.
	var completenessPct float64
	if globalTotalSlots > 0 {
		completenessPct = float64(totalDZLeaderSlots) / float64(globalTotalSlots) * 100
	}

	return &EdgeScoreboardResponse{
		Window:             window,
		LeadersOnly:        leadersOnly,
		GeneratedAt:        time.Now().UTC(),
		CurrentEpoch:       globalMaxEpoch,
		CurrentSlot:        globalMaxSlot,
		TotalSlots:         totalSlots,
		GlobalTotalSlots:   globalTotalSlots,
		DZSlots:            dzSlots,
		TotalDZLeaderSlots: totalDZLeaderSlots,
		CompletenessPct:    completenessPct,
		PublisherCount:     publisherCount,
		PublishingCount:    publishingCount,
		PublishingStakePct: publishingStakePct,
		Nodes:              nodes,
		RecentSlots:        recentSlots,
		SlotBuckets:        slotBuckets,
		SlotBucketSize:     slotBucketSize,
		SlotLeaders:        slotLeaders,
		DataLagMs:          dataLagMs,
	}, nil
}

// FetchEdgeScoreboardLatest fetches just the latest N slots (recent_slots + slot_leaders).
// Skips the heavy aggregate queries. Intended for the fast page-cache refresher so live-tail
// polls can be served from cache rather than hitting ClickHouse every few seconds.
func (a *API) FetchEdgeScoreboardLatest(ctx context.Context, leadersOnly bool, slotLimit int) (*EdgeScoreboardResponse, error) {
	if slotLimit <= 0 {
		slotLimit = 1000
	}
	shredderDB := fmt.Sprintf("`%s`", a.ShredderDB)
	var maxSlot uint64
	var lagSec float64
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, fmt.Sprintf(
		`SELECT max(slot), toFloat64(toUnixTimestamp(now()) - toUnixTimestamp(max(event_ts))) FROM %s.slot_feed_race_summary WHERE slot < %d`,
		shredderDB, maxValidSlot,
	)).Scan(&maxSlot, &lagSec)
	metrics.RecordClickHouseQuery(time.Since(start), err)
	if err != nil {
		return nil, fmt.Errorf("max slot: %w", err)
	}
	if maxSlot == 0 {
		return &EdgeScoreboardResponse{LeadersOnly: leadersOnly, GeneratedAt: time.Now().UTC()}, nil
	}
	// beforeSlot = maxSlot+1 triggers cursor mode and returns the latest slotLimit slots DESC.
	resp, err := a.FetchEdgeScoreboardData(ctx, "", leadersOnly, 0, maxSlot+1, slotLimit)
	if err != nil {
		return nil, err
	}
	if lagSec > 0 {
		resp.DataLagMs = uint64(lagSec * 1000)
	}
	return resp, nil
}
