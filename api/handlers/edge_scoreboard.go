package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
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
	DZLeaderSlots uint64                              `json:"dz_leader_slots"` // always feed='dz' leader slots — used for Edge Leaders Completeness
	LastUpdated   time.Time                           `json:"last_updated"`
	Name          string                              `json:"name,omitempty"`
	GossipPubkey  string                              `json:"gossip_pubkey,omitempty"`
	GossipIP      string                              `json:"gossip_ip,omitempty"`
	ASN           int64                               `json:"asn,omitempty"`
	ASNOrg        string                              `json:"asn_org,omitempty"`
	City          string                              `json:"city,omitempty"`
	Country       string                              `json:"country,omitempty"`
}

// EdgeScoreboardSlotRace holds per-slot per-feed win data for recent slots.
type EdgeScoreboardSlotRace struct {
	Host      string  `json:"host"`
	Slot      uint64  `json:"slot"`
	Feed      string  `json:"feed"`
	ShredsWon uint64  `json:"shreds_won"`
	WinPct    float64 `json:"win_pct"`
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
	Nodes              []EdgeScoreboardNode             `json:"nodes"`
	RecentSlots        []EdgeScoreboardSlotRace         `json:"recent_slots"`
	SlotLeaders        map[string]*EdgeScoreboardLeader `json:"slot_leaders,omitempty"`
}

// validWindows maps window parameter values to ClickHouse interval expressions.
var validWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
	"30d": "30 DAY",
	"all": "",
}

// edgeScoreboardCacheKey returns the page cache key for a request, or "" if the request
// is not eligible for caching (non-default window).
func edgeScoreboardCacheKey(r *http.Request) string {
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window != "" && window != "1h" {
		return ""
	}
	leadersOnly := strings.TrimSpace(r.URL.Query().Get("leaders_only")) != "false"
	if leadersOnly {
		return "edge_scoreboard"
	}
	return "edge_scoreboard:all"
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
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if _, ok := validWindows[window]; !ok {
		window = "1h"
	}

	leadersOnly := strings.TrimSpace(r.URL.Query().Get("leaders_only")) != "false"

	resp, err := a.FetchEdgeScoreboardData(ctx, window, leadersOnly)
	if err != nil {
		log.Printf("EdgeScoreboard error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, resp)
}

// FetchEdgeScoreboardData performs the actual edge scoreboard queries.
// When leadersOnly is true, results are scoped to slots where the leader published via DZ.
func (a *API) FetchEdgeScoreboardData(ctx context.Context, window string, leadersOnly bool) (*EdgeScoreboardResponse, error) {
	interval := validWindows[window]
	var timeFilter string
	if interval != "" {
		timeFilter = fmt.Sprintf("AND event_ts >= now() - INTERVAL %s", interval)
	}

	shredderDB := fmt.Sprintf("`%s`", a.ShredderDB)

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
			max(slot) AS max_slot,
			max(event_ts) AS last_updated,
			uniqExact(feed) AS feed_count
		FROM %s.slot_feed_race_summary
		WHERE feed_type = 'shred' AND loser_feed = '' %s
		GROUP BY host
	`, shredderDB, timeFilter)

	start := time.Now()
	rows1, err := a.envDB(ctx).Query(ctx, query1)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		return nil, fmt.Errorf("query1: %w", err)
	}
	defer rows1.Close()

	type nodeSlotInfo struct {
		totalSlots  uint64
		dzSlots     uint64
		maxEpoch    uint64
		maxSlot     uint64
		lastUpdated time.Time
	}
	nodeSlots := make(map[string]*nodeSlotInfo)
	var globalMaxEpoch, globalMaxSlot uint64

	for rows1.Next() {
		var nodeID string
		var info nodeSlotInfo
		var feedCount uint64
		if err := rows1.Scan(&nodeID, &info.totalSlots, &info.dzSlots, &info.maxEpoch, &info.maxSlot, &info.lastUpdated, &feedCount); err != nil {
			return nil, fmt.Errorf("query1 scan: %w", err)
		}
		// Skip nodes that only record one feed in the time window — they can't produce
		// meaningful race comparisons. Note: a node can appear single-feed if a second
		// feed joined partway through the window.
		if feedCount < 2 {
			continue
		}
		nodeSlots[nodeID] = &info
		if info.maxEpoch > globalMaxEpoch {
			globalMaxEpoch = info.maxEpoch
		}
		if info.maxSlot > globalMaxSlot {
			globalMaxSlot = info.maxSlot
		}
	}
	if err := rows1.Err(); err != nil {
		return nil, fmt.Errorf("query1 rows: %w", err)
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
	dzLeaderCTE := fmt.Sprintf(`dz_leader_slots AS (
		SELECT DISTINCT slot
		FROM %s.publisher_shred_stats
		WHERE is_scheduled_leader = true %s
	)`, shredderDB, timeFilter)

	// Query 1b: DZ-leader slot counts per node — always run regardless of leadersOnly.
	// Populates dzLeaderSlots (used for Edge Leaders Completeness, always consistent).
	// In leaders-only mode, also overrides info.dzSlots so SlotsObserved reflects leader slots.
	dzLeaderSlotsByNode := make(map[string]uint64)
	{
		query1b := fmt.Sprintf(`
			WITH %s
			SELECT host, uniqExact(slot) AS dz_leader_slots
			FROM %s.slot_feed_race_summary
			WHERE feed_type = 'shred' AND feed = 'dz' AND loser_feed = ''
				AND slot IN (SELECT slot FROM dz_leader_slots)
				%s
			GROUP BY host
		`, dzLeaderCTE, shredderDB, timeFilter)

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

	// Query 1c: DZ-leader slot count and total slot count from slot_feed_race_summary.
	// feed='dz' rows only appear when the leader is a DZ publisher, so uniqExactIf(slot, feed='dz')
	// gives DZ-leader slots. Both values come from the same table/window so the ratio is consistent.
	// completeness_pct = dz_leader_slots / total_slots — fraction of slots with a DZ leader.
	var totalDZLeaderSlots, globalTotalSlots uint64
	{
		query1c := fmt.Sprintf(`
			WITH %s
			SELECT
				uniqExactIf(slot, feed = 'dz' AND slot IN (SELECT slot FROM dz_leader_slots)) AS dz_leader_slots,
				uniqExact(slot) AS total_slots
			FROM %s.slot_feed_race_summary
			WHERE feed_type = 'shred' AND loser_feed = '' %s
		`, dzLeaderCTE, shredderDB, timeFilter)
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
	nodeCount := len(nodeSlots)

	type metroInfo struct {
		name      string
		latitude  float64
		longitude float64
	}
	type stakeInfo struct {
		stakeSol   float64
		validators uint64
	}

	// Run four independent query groups in parallel:
	//   A: feed win rates (q2) + lead times (q2b)
	//   B: metro coordinates (q3)
	//   C: stake by metro (q4)
	//   D: recent slot races (q5) + slot leader enrichment (q6a, q6b)
	var (
		feedStats    map[feedKey]*EdgeScoreboardFeedStats
		metros       = make(map[string]*metroInfo)
		stakeByMetro = make(map[string]*stakeInfo)
		recentSlots  []EdgeScoreboardSlotRace
		slotLeaders  = make(map[string]*EdgeScoreboardLeader)
	)

	g, gctx := errgroup.WithContext(ctx)

	// Group A: feed win rates → lead times
	g.Go(func() error {
		localFeedStats := make(map[feedKey]*EdgeScoreboardFeedStats)

		var q2 string
		if leadersOnly {
			q2 = fmt.Sprintf(`
				WITH %s
				SELECT
					host, feed, shreds_won, total_shreds,
					round(shreds_won / max_total * 100, 1) AS win_rate_pct
				FROM (
					SELECT
						r.host, r.feed,
						SUM(r.shreds_won) AS shreds_won,
						SUM(r.total_shreds) AS total_shreds,
						MAX(SUM(r.total_shreds)) OVER (PARTITION BY r.host) AS max_total
					FROM %s.slot_feed_race_summary AS r
					INNER JOIN (
						SELECT DISTINCT host, slot
						FROM %s.slot_feed_race_summary
						WHERE feed_type = 'shred' AND feed = 'dz' AND loser_feed = ''
							AND slot IN (SELECT slot FROM dz_leader_slots)
							%s
					) dz ON r.host = dz.host AND r.slot = dz.slot
					WHERE r.feed_type = 'shred' AND r.loser_feed = '' %s
					GROUP BY r.host, r.feed
				)
			`, dzLeaderCTE, shredderDB, shredderDB, timeFilter, timeFilter)
		} else {
			q2 = fmt.Sprintf(`
				SELECT
					host, feed, shreds_won, total_shreds,
					round(shreds_won / max_total * 100, 1) AS win_rate_pct
				FROM (
					SELECT
						host, feed, shreds_won, total_shreds,
						MAX(total_shreds) OVER (PARTITION BY host) AS max_total
					FROM (
						SELECT
							host, feed,
							SUM(shreds_won) AS shreds_won,
							SUM(total_shreds) AS total_shreds
						FROM %s.slot_feed_race_summary
						WHERE feed_type = 'shred' AND loser_feed = '' %s
						GROUP BY host, feed
					)
				)
			`, shredderDB, timeFilter)
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

		// q2b: pairwise lead times.
		// p50_ms = median of per-slot p50 lead times.
		// p95_ms = 95th percentile of per-slot p95 lead times (conservative tail estimate).
		var q2b string
		if leadersOnly {
			q2b = fmt.Sprintf(`
				WITH %s
				SELECT
					r.host, r.feed, r.loser_feed,
					count() AS slot_count,
					quantile(0.5)(r.lead_time_p50_ms) AS p50_ms,
					quantile(0.95)(r.lead_time_p95_ms) AS p95_ms
				FROM %s.slot_feed_race_summary AS r
				INNER JOIN (
					SELECT DISTINCT host, slot
					FROM %s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND feed = 'dz' AND loser_feed = ''
						AND slot IN (SELECT slot FROM dz_leader_slots)
						%s
				) dz ON r.host = dz.host AND r.slot = dz.slot
				WHERE r.feed_type = 'shred' AND r.loser_feed != ''
					AND r.lead_time_p50_ms <= 500
					%s
				GROUP BY r.host, r.feed, r.loser_feed
			`, dzLeaderCTE, shredderDB, shredderDB, timeFilter, timeFilter)
		} else {
			q2b = fmt.Sprintf(`
				SELECT
					host, feed, loser_feed,
					count() AS slot_count,
					quantile(0.5)(lead_time_p50_ms) AS p50_ms,
					quantile(0.95)(lead_time_p95_ms) AS p95_ms
				FROM %s.slot_feed_race_summary
				WHERE feed_type = 'shred' AND loser_feed != ''
					AND lead_time_p50_ms <= 500
					%s
				GROUP BY host, feed, loser_feed
			`, shredderDB, timeFilter)
		}

		t = time.Now()
		rows2b, err := a.envDB(gctx).Query(gctx, q2b)
		metrics.RecordClickHouseQuery(time.Since(t), err)
		if err != nil && gctx.Err() == nil {
			log.Printf("EdgeScoreboard query2b error: %v", err)
		} else if err == nil {
			defer rows2b.Close()
			for rows2b.Next() {
				var nodeID, feed, loserFeed string
				var slotCount uint64
				var p50, p95 float64
				if err := rows2b.Scan(&nodeID, &feed, &loserFeed, &slotCount, &p50, &p95); err != nil {
					log.Printf("EdgeScoreboard query2b scan error: %v", err)
					break
				}
				key := feedKey{nodeID, feed}
				fs, ok := localFeedStats[key]
				if !ok {
					fs = &EdgeScoreboardFeedStats{}
					localFeedStats[key] = fs
				}
				fs.LeadTimes = append(fs.LeadTimes, EdgeScoreboardLeadTime{
					LoserFeed: loserFeed,
					P50Ms:     p50,
					P95Ms:     p95,
					SlotCount: slotCount,
				})
			}
		}

		feedStats = localFeedStats
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

	// Group D: recent slot races (q5) → leader enrichment (q6a, q6b) — all non-fatal
	g.Go(func() error {
		var localSlots []EdgeScoreboardSlotRace
		localLeaders := make(map[string]*EdgeScoreboardLeader)

		var query5 string
		if leadersOnly {
			query5 = fmt.Sprintf(`
				WITH %s,
				dz_slots AS (
					SELECT DISTINCT slot
					FROM %s.slot_feed_race_summary
					WHERE feed_type = 'shred' AND loser_feed = '' AND feed = 'dz'
						AND slot IN (SELECT slot FROM dz_leader_slots)
						AND host IN (%s)
						AND slot >= (SELECT max(slot) - 10000 FROM %s.slot_feed_race_summary WHERE feed_type = 'shred' AND loser_feed = '')
				),
				common_slots AS (
					SELECT slot
					FROM (
						SELECT DISTINCT host, slot
						FROM %s.slot_feed_race_summary
						WHERE feed_type = 'shred' AND loser_feed = ''
							AND host IN (%s)
							AND slot IN (SELECT slot FROM dz_slots)
					)
					GROUP BY slot
					HAVING count(DISTINCT host) >= %d
					ORDER BY slot DESC
					LIMIT 100
				)
				SELECT r.host, r.slot, r.feed, r.shreds_won,
					round(r.shreds_won / greatest(r.total_shreds, 1) * 100, 1) AS win_pct
				FROM %s.slot_feed_race_summary AS r
				INNER JOIN common_slots cs ON r.slot = cs.slot
				WHERE r.feed_type = 'shred' AND r.loser_feed = ''
					AND r.host IN (%s)
				ORDER BY r.host, r.slot, r.feed
			`, dzLeaderCTE, shredderDB, nodeList, shredderDB, shredderDB, nodeList, nodeCount, shredderDB, nodeList)
		} else {
			query5 = fmt.Sprintf(`
				WITH common_slots AS (
					SELECT slot
					FROM (
						SELECT DISTINCT host, slot
						FROM %s.slot_feed_race_summary
						WHERE feed_type = 'shred' AND loser_feed = ''
							AND host IN (%s)
							AND slot >= (SELECT max(slot) - 10000 FROM %s.slot_feed_race_summary WHERE feed_type = 'shred' AND loser_feed = '')
					)
					GROUP BY slot
					HAVING count(DISTINCT host) >= %d
					ORDER BY slot DESC
					LIMIT 100
				)
				SELECT r.host, r.slot, r.feed, r.shreds_won,
					round(r.shreds_won / greatest(r.total_shreds, 1) * 100, 1) AS win_pct
				FROM %s.slot_feed_race_summary AS r
				INNER JOIN common_slots cs ON r.slot = cs.slot
				WHERE r.feed_type = 'shred' AND r.loser_feed = ''
					AND r.host IN (%s)
				ORDER BY r.host, r.slot, r.feed
			`, shredderDB, nodeList, shredderDB, nodeCount, shredderDB, nodeList)
		}
		t := time.Now()
		rows5, err := a.envDB(gctx).Query(gctx, query5)
		metrics.RecordClickHouseQuery(time.Since(t), err)
		if err != nil && gctx.Err() == nil {
			log.Printf("EdgeScoreboard query5 error: %v", err)
		} else if err == nil {
			defer rows5.Close()
			for rows5.Next() {
				var sr EdgeScoreboardSlotRace
				if err := rows5.Scan(&sr.Host, &sr.Slot, &sr.Feed, &sr.ShredsWon, &sr.WinPct); err != nil {
					log.Printf("EdgeScoreboard query5 scan error: %v", err)
					break
				}
				localSlots = append(localSlots, sr)
			}
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
				if err != nil && gctx.Err() == nil {
					log.Printf("EdgeScoreboard query6a error (epoch %d): %v", epoch, err)
					continue
				} else if err != nil {
					continue
				}
				for rows6a.Next() {
					var relSlot uint64
					var pubkey string
					if err := rows6a.Scan(&relSlot, &pubkey); err != nil {
						log.Printf("EdgeScoreboard query6a scan error: %v", err)
						rows6a.Close()
						break
					}
					absSlot := relToAbs[epochRelSlot{epoch, relSlot}]
					slotPubkeys[absSlot] = pubkey
					pubkeySet[pubkey] = true
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

				type leaderInfo struct {
					name, ip, asnOrg, city, country string
				}
				infoByPubkey := make(map[string]*leaderInfo)
				if err != nil && gctx.Err() == nil {
					log.Printf("EdgeScoreboard query6b error: %v", err)
				} else if err == nil {
					defer rows6b.Close()
					for rows6b.Next() {
						var pubkey string
						var li leaderInfo
						if err := rows6b.Scan(&pubkey, &li.name, &li.ip, &li.asnOrg, &li.city, &li.country); err != nil {
							log.Printf("EdgeScoreboard query6b scan error: %v", err)
							break
						}
						infoByPubkey[pubkey] = &li
					}
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

	if err := g.Wait(); err != nil {
		return nil, err
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
		Nodes:              nodes,
		RecentSlots:        recentSlots,
		SlotLeaders:        slotLeaders,
	}, nil
}
