package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

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
	SlotsObserved uint64                              `json:"slots_observed"`
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
	Window          string                           `json:"window"`
	LeadersOnly     bool                             `json:"leaders_only"`
	GeneratedAt     time.Time                        `json:"generated_at"`
	CurrentEpoch    uint64                           `json:"current_epoch"`
	CurrentSlot     uint64                           `json:"current_slot"`
	TotalSlots      uint64                           `json:"total_slots"`
	DZSlots         uint64                           `json:"dz_slots"`
	CompletenessPct float64                          `json:"completeness_pct"`
	Nodes           []EdgeScoreboardNode             `json:"nodes"`
	RecentSlots     []EdgeScoreboardSlotRace         `json:"recent_slots"`
	SlotLeaders     map[string]*EdgeScoreboardLeader `json:"slot_leaders,omitempty"`
}

// validWindows maps window parameter values to ClickHouse interval expressions.
var validWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
	"30d": "30 DAY",
	"all": "",
}

// isDefaultEdgeScoreboardRequest returns true if the request uses default parameters (window=1h, leaders_only=true).
func isDefaultEdgeScoreboardRequest(r *http.Request) bool {
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	leadersOnly := strings.TrimSpace(r.URL.Query().Get("leaders_only"))
	return (window == "" || window == "1h") && (leadersOnly == "" || leadersOnly == "true")
}

// GetEdgeScoreboard returns aggregated win rate / completeness data for DZ Edge nodes.
func (a *API) GetEdgeScoreboard(w http.ResponseWriter, r *http.Request) {
	// Try to serve from cache for default requests
	if isMainnet(r.Context()) && isDefaultEdgeScoreboardRequest(r) {
		if data, err := a.readPageCache(r.Context(), "edge_scoreboard"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
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

	// Query 1: Per-node slot counts from win-count rows (loser_feed = '')
	// Includes feed count to filter out nodes that only record one feed (e.g. DZ-only nodes)
	query1 := fmt.Sprintf(`
		SELECT
			host,
			uniqExact(slot) AS total_slots,
			uniqExactIf(slot, feed = 'dz') AS dz_slots,
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
		// Skip nodes that only record one feed — they can't produce meaningful race data
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
	// When leadersOnly is false, we skip this filter and include all slots.
	dzLeaderCTE := fmt.Sprintf(`dz_leader_slots AS (
		SELECT DISTINCT slot
		FROM %s.publisher_shred_stats
		WHERE is_scheduled_leader = true %s
	)`, shredderDB, timeFilter)

	if leadersOnly {
		// Query 1b: DZ-leader slot counts per node (overrides dz_slots from query 1)
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

		// Reset dz_slots and update with DZ-leader counts
		for _, info := range nodeSlots {
			info.dzSlots = 0
		}
		for rows1b.Next() {
			var nodeID string
			var dzLeaderSlots uint64
			if err := rows1b.Scan(&nodeID, &dzLeaderSlots); err != nil {
				return nil, fmt.Errorf("query1b scan: %w", err)
			}
			if info, ok := nodeSlots[nodeID]; ok {
				info.dzSlots = dzLeaderSlots
			}
		}
		if err := rows1b.Err(); err != nil {
			return nil, fmt.Errorf("query1b rows: %w", err)
		}
	}

	// Query 2: Per-node per-feed win counts from summary rows (loser_feed = '')
	// When leadersOnly, scoped to DZ-leader slots (where the leader published via DZ).
	// Win rate uses MAX(SUM(total_shreds)) OVER (PARTITION BY host) as the
	// denominator so all feeds share the same base per node.
	var query2 string
	if leadersOnly {
		query2 = fmt.Sprintf(`
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
		query2 = fmt.Sprintf(`
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

	start = time.Now()
	rows2, err := a.envDB(ctx).Query(ctx, query2)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		return nil, fmt.Errorf("query2: %w", err)
	}
	defer rows2.Close()

	type feedKey struct {
		nodeID string
		feed   string
	}
	feedStats := make(map[feedKey]*EdgeScoreboardFeedStats)

	for rows2.Next() {
		var nodeID, feed string
		var shredsWon, totalShreds uint64
		var winRatePct float64
		if err := rows2.Scan(&nodeID, &feed, &shredsWon, &totalShreds, &winRatePct); err != nil {
			return nil, fmt.Errorf("query2 scan: %w", err)
		}
		feedStats[feedKey{nodeID, feed}] = &EdgeScoreboardFeedStats{
			ShredsWon:   shredsWon,
			TotalShreds: totalShreds,
			WinRatePct:  winRatePct,
		}
	}
	if err := rows2.Err(); err != nil {
		return nil, fmt.Errorf("query2 rows: %w", err)
	}

	// Query 2b: Pairwise lead times from lead-time rows (loser_feed != '')
	// Uses quantile() to aggregate per-slot percentiles across slots — never AVG.
	var query2b string
	if leadersOnly {
		query2b = fmt.Sprintf(`
			WITH %s
			SELECT
				r.host, r.feed, r.loser_feed,
				count() AS slot_count,
				quantile(0.5)(r.lead_time_p50_ms) AS p50_ms,
				quantile(0.5)(r.lead_time_p95_ms) AS p95_ms
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
		query2b = fmt.Sprintf(`
			SELECT
				host, feed, loser_feed,
				count() AS slot_count,
				quantile(0.5)(lead_time_p50_ms) AS p50_ms,
				quantile(0.5)(lead_time_p95_ms) AS p95_ms
			FROM %s.slot_feed_race_summary
			WHERE feed_type = 'shred' AND loser_feed != ''
				AND lead_time_p50_ms <= 500
				%s
			GROUP BY host, feed, loser_feed
		`, shredderDB, timeFilter)
	}

	start = time.Now()
	rows2b, err := a.envDB(ctx).Query(ctx, query2b)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		return nil, fmt.Errorf("query2b: %w", err)
	}
	defer rows2b.Close()

	for rows2b.Next() {
		var nodeID, feed, loserFeed string
		var slotCount uint64
		var p50, p95 float64
		if err := rows2b.Scan(&nodeID, &feed, &loserFeed, &slotCount, &p50, &p95); err != nil {
			return nil, fmt.Errorf("query2b scan: %w", err)
		}
		key := feedKey{nodeID, feed}
		fs, ok := feedStats[key]
		if !ok {
			fs = &EdgeScoreboardFeedStats{}
			feedStats[key] = fs
		}
		fs.LeadTimes = append(fs.LeadTimes, EdgeScoreboardLeadTime{
			LoserFeed: loserFeed,
			P50Ms:     p50,
			P95Ms:     p95,
			SlotCount: slotCount,
		})
	}
	if err := rows2b.Err(); err != nil {
		return nil, fmt.Errorf("query2b rows: %w", err)
	}

	// Build location code set from node IDs (first segment before '-', uppercased)
	locationCodes := make(map[string]bool)
	nodeLocations := make(map[string]string) // node_id -> location code
	for nodeID := range nodeSlots {
		parts := strings.SplitN(nodeID, "-", 2)
		loc := strings.ToUpper(parts[0])
		locationCodes[loc] = true
		nodeLocations[nodeID] = loc
	}

	// Query 3: Resolve metro coordinates
	type metroInfo struct {
		name      string
		latitude  float64
		longitude float64
	}
	metros := make(map[string]*metroInfo)

	if len(locationCodes) > 0 {
		codes := make([]string, 0, len(locationCodes))
		for code := range locationCodes {
			codes = append(codes, strings.ToLower(code))
		}

		query3 := `SELECT code, name, latitude, longitude FROM dz_metros_current WHERE code IN (?)`
		start = time.Now()
		rows3, err := a.envDB(ctx).Query(ctx, query3, codes)
		duration = time.Since(start)
		metrics.RecordClickHouseQuery(duration, err)
		if err != nil {
			return nil, fmt.Errorf("query3: %w", err)
		}
		defer rows3.Close()

		for rows3.Next() {
			var code, name string
			var lat, lon float64
			if err := rows3.Scan(&code, &name, &lat, &lon); err != nil {
				return nil, fmt.Errorf("query3 scan: %w", err)
			}
			metros[strings.ToUpper(code)] = &metroInfo{name: name, latitude: lat, longitude: lon}
		}
		if err := rows3.Err(); err != nil {
			return nil, fmt.Errorf("query3 rows: %w", err)
		}
	}

	// Query 4: Total network stake per nearest DZ metro
	// Assigns each Solana validator to its nearest DZ metro by geo distance,
	// giving a view of total network stake (not just DZ-connected) per region.
	type stakeInfo struct {
		stakeSol   float64
		validators uint64
	}
	stakeByMetro := make(map[string]*stakeInfo)

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
	start = time.Now()
	rows4, err := a.envDB(ctx).Query(ctx, query4)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil && ctx.Err() == nil {
		log.Printf("EdgeScoreboard query4 error: %v", err)
		// Non-fatal: stake data is optional
	} else {
		defer rows4.Close()
		for rows4.Next() {
			var code string
			var si stakeInfo
			if err := rows4.Scan(&code, &si.validators, &si.stakeSol); err != nil {
				log.Printf("EdgeScoreboard query4 scan error: %v", err)
				break
			}
			stakeByMetro[strings.ToUpper(code)] = &si
		}
	}

	// Build the node ID list used by queries 4b and 5.
	validNodeIDs := make([]string, 0, len(nodeSlots))
	for id := range nodeSlots {
		validNodeIDs = append(validNodeIDs, "'"+id+"'")
	}
	nodeList := strings.Join(validNodeIDs, ",")
	nodeCount := len(nodeSlots)

	// Query 4b: Resolve node identity (pubkey, name, IP, geo) via publisher_shred_stats.
	// dz_device_code in publisher_shred_stats matches slot_feed_race_summary.host.
	// We pick the most-recent node_pubkey per host, then join with validatorsapp for name/IP.
	type nodeIdentity struct {
		name, gossipPubkey, gossipIP, asnOrg, city, country string
		asn                                                 int64
	}
	nodeIdentities := make(map[string]*nodeIdentity)

	if len(validNodeIDs) > 0 {
		query4b := fmt.Sprintf(`
			SELECT
				p.dz_device_code,
				p.node_pubkey,
				COALESCE(v.name, '') AS name,
				COALESCE(v.ip, '') AS gossip_ip,
				COALESCE(g.asn, 0) AS asn,
				COALESCE(g.asn_org, '') AS asn_org,
				COALESCE(g.city, '') AS city,
				COALESCE(g.country, '') AS country
			FROM (
				SELECT dz_device_code, argMax(node_pubkey, event_ts) AS node_pubkey
				FROM %s.publisher_shred_stats
				WHERE dz_device_code IN (%s)
				GROUP BY dz_device_code
			) p
			LEFT JOIN validatorsapp_validators_current v ON v.account = p.node_pubkey
			LEFT JOIN geoip_records_current g ON g.ip = v.ip
		`, shredderDB, nodeList)

		start = time.Now()
		rows4b, err := a.envDB(ctx).Query(ctx, query4b)
		duration = time.Since(start)
		metrics.RecordClickHouseQuery(duration, err)
		if err != nil && ctx.Err() == nil {
			log.Printf("EdgeScoreboard query4b error: %v", err)
		} else {
			defer rows4b.Close()
			for rows4b.Next() {
				var host string
				var ni nodeIdentity
				if err := rows4b.Scan(&host, &ni.gossipPubkey, &ni.name, &ni.gossipIP, &ni.asn, &ni.asnOrg, &ni.city, &ni.country); err != nil {
					log.Printf("EdgeScoreboard query4b scan error: %v", err)
					break
				}
				nodeIdentities[host] = &ni
			}
		}
	}

	// Query 5: Recent slot-level race data (last 100 slots across all nodes)
	// Returns per-slot per-feed win percentage for a time-series chart.
	var recentSlots []EdgeScoreboardSlotRace

	// Find the most recent 100 slots where all valid nodes reported data.
	// When leadersOnly, further restrict to slots where the leader published via DZ.
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
	start = time.Now()
	rows5, err := a.envDB(ctx).Query(ctx, query5)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil && ctx.Err() == nil {
		log.Printf("EdgeScoreboard query5 error: %v", err)
		// Non-fatal
	} else {
		defer rows5.Close()
		for rows5.Next() {
			var sr EdgeScoreboardSlotRace
			if err := rows5.Scan(&sr.Host, &sr.Slot, &sr.Feed, &sr.ShredsWon, &sr.WinPct); err != nil {
				log.Printf("EdgeScoreboard query5 scan error: %v", err)
				break
			}
			recentSlots = append(recentSlots, sr)
		}
	}
	if recentSlots == nil {
		recentSlots = []EdgeScoreboardSlotRace{}
	}

	// Query 6: Resolve leader validators for recent slots.
	slotLeaders := make(map[string]*EdgeScoreboardLeader)

	if len(recentSlots) > 0 && globalMaxEpoch > 0 {
		// Leader schedule uses epoch-relative slot indices (0–431999).
		// Convert absolute slots to epoch-relative for the query,
		// then map results back using the absolute slot as the key.
		const slotsPerEpoch uint64 = 432_000
		epochStart := globalMaxEpoch * slotsPerEpoch

		slotSet := make(map[uint64]bool)
		for _, sr := range recentSlots {
			slotSet[sr.Slot] = true
		}
		relativeSlots := make([]uint64, 0, len(slotSet))
		relToAbs := make(map[uint64]uint64) // epoch-relative -> absolute
		for s := range slotSet {
			rel := s - epochStart
			relativeSlots = append(relativeSlots, rel)
			relToAbs[rel] = s
		}

		// Step 1: Resolve slot -> leader pubkey.
		// Use dim_solana_leader_schedule_history instead of _current because _current only
		// holds the next epoch's schedule (Solana publishes N+1 during epoch N), so the
		// shredder's current-epoch slots would never match.
		query6a := fmt.Sprintf(`
			SELECT
				arrayJoin(JSONExtract(slots, 'Array(UInt64)')) AS slot,
				node_pubkey
			FROM dim_solana_leader_schedule_history
			WHERE epoch = %d
			HAVING slot IN (?)
		`, globalMaxEpoch)

		start = time.Now()
		rows6a, err := a.envDB(ctx).Query(ctx, query6a, relativeSlots)
		duration = time.Since(start)
		metrics.RecordClickHouseQuery(duration, err)

		// slot -> pubkey mapping
		slotPubkeys := make(map[uint64]string)
		pubkeySet := make(map[string]bool)
		if err != nil && ctx.Err() == nil {
			log.Printf("EdgeScoreboard query6a error: %v", err)
		} else {
			defer rows6a.Close()
			for rows6a.Next() {
				var relSlot uint64
				var pubkey string
				if err := rows6a.Scan(&relSlot, &pubkey); err != nil {
					log.Printf("EdgeScoreboard query6a scan error: %v", err)
					break
				}
				absSlot := relToAbs[relSlot]
				slotPubkeys[absSlot] = pubkey
				pubkeySet[pubkey] = true
			}
		}

		// Step 2: Enrich unique pubkeys with name/ip/geo
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

			start = time.Now()
			rows6b, err := a.envDB(ctx).Query(ctx, query6b, pubkeys)
			duration = time.Since(start)
			metrics.RecordClickHouseQuery(duration, err)

			type leaderInfo struct {
				name, ip, asnOrg, city, country string
			}
			infoByPubkey := make(map[string]*leaderInfo)
			if err != nil && ctx.Err() == nil {
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

			// Assemble slot leaders
			for absSlot, pubkey := range slotPubkeys {
				leader := &EdgeScoreboardLeader{Pubkey: pubkey}
				if li, ok := infoByPubkey[pubkey]; ok {
					leader.Name = li.name
					leader.IP = li.ip
					leader.ASNOrg = li.asnOrg
					leader.City = li.city
					leader.Country = li.country
				}
				slotLeaders[fmt.Sprintf("%d", absSlot)] = leader
			}
		}
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

		if ni, ok := nodeIdentities[nodeID]; ok {
			node.Name = ni.name
			node.GossipPubkey = ni.gossipPubkey
			node.GossipIP = ni.gossipIP
			node.ASN = ni.asn
			node.ASNOrg = ni.asnOrg
			node.City = ni.city
			node.Country = ni.country
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

		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Host < nodes[j].Host
	})

	var completenessPct float64
	if totalSlots > 0 {
		completenessPct = float64(dzSlots) / float64(totalSlots) * 100
	}

	resp := EdgeScoreboardResponse{
		Window:          window,
		LeadersOnly:     leadersOnly,
		GeneratedAt:     time.Now().UTC(),
		CurrentEpoch:    globalMaxEpoch,
		CurrentSlot:     globalMaxSlot,
		TotalSlots:      totalSlots,
		DZSlots:         dzSlots,
		CompletenessPct: completenessPct,
		Nodes:           nodes,
		RecentSlots:     recentSlots,
		SlotLeaders:     slotLeaders,
	}

	return &resp, nil
}
