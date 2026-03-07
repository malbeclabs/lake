package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/config"
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
	ShredsWon   uint64                    `json:"shreds_won"`
	TotalShreds uint64                    `json:"total_shreds"`
	WinRatePct  float64                   `json:"win_rate_pct"`
	LeadTimes   []EdgeScoreboardLeadTime  `json:"lead_times"`
}

// EdgeScoreboardNode holds aggregated stats for a single edge node.
type EdgeScoreboardNode struct {
	NodeID        string                              `json:"node_id"`
	Location      string                              `json:"location"`
	MetroName     string                              `json:"metro_name"`
	Latitude      float64                             `json:"latitude"`
	Longitude     float64                             `json:"longitude"`
	Feeds         map[string]*EdgeScoreboardFeedStats `json:"feeds"`
	TotalSlots    uint64                              `json:"total_slots"`
	SlotsObserved uint64                              `json:"slots_observed"`
	LastUpdated   time.Time                           `json:"last_updated"`
}

// EdgeScoreboardResponse is the response for the edge scoreboard endpoint.
type EdgeScoreboardResponse struct {
	Window          string               `json:"window"`
	GeneratedAt     time.Time            `json:"generated_at"`
	CurrentEpoch    uint64               `json:"current_epoch"`
	CurrentSlot     uint64               `json:"current_slot"`
	TotalSlots      uint64               `json:"total_slots"`
	DZSlots         uint64               `json:"dz_slots"`
	CompletenessPct float64              `json:"completeness_pct"`
	Nodes           []EdgeScoreboardNode `json:"nodes"`
}

// validWindows maps window parameter values to ClickHouse interval expressions.
var validWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
	"30d": "30 DAY",
	"all": "",
}

// GetEdgeScoreboard returns aggregated win rate / completeness data for DZ Edge nodes.
func GetEdgeScoreboard(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if _, ok := validWindows[window]; !ok {
		window = "24h"
	}

	interval := validWindows[window]
	var timeFilter string
	if interval != "" {
		timeFilter = fmt.Sprintf("AND event_ts >= now() - INTERVAL %s", interval)
	}

	shredderDB := fmt.Sprintf("`%s`", config.ShredderDB)

	// Query 1: Per-node slot counts from win-count rows (loser_feed = '')
	// Uses FINAL to handle ReplacingMergeTree pre-merge duplicates.
	// Includes feed count to filter out nodes that only record one feed (e.g. DZ-only nodes)
	query1 := fmt.Sprintf(`
		SELECT
			node_id,
			uniqExact(slot) AS total_slots,
			uniqExactIf(slot, feed = 'dz') AS dz_slots,
			max(epoch) AS max_epoch,
			max(slot) AS max_slot,
			max(event_ts) AS last_updated,
			uniqExact(feed) AS feed_count
		FROM %s.slot_feed_races FINAL
		WHERE feed_type = 'shred' AND loser_feed = '' %s
		GROUP BY node_id
	`, shredderDB, timeFilter)

	start := time.Now()
	rows1, err := envDB(ctx).Query(ctx, query1)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		log.Printf("EdgeScoreboard query1 error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
			log.Printf("EdgeScoreboard query1 scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
		log.Printf("EdgeScoreboard query1 rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// If no data, return empty response
	if len(nodeSlots) == 0 {
		writeJSON(w, EdgeScoreboardResponse{
			Window:      window,
			GeneratedAt: time.Now().UTC(),
			Nodes:       []EdgeScoreboardNode{},
		})
		return
	}

	// Query 2: Per-node per-feed win counts from summary rows (loser_feed = '')
	// Only DZ-participating slots. Uses FINAL for dedup safety.
	// Win rate uses MAX(SUM(total_shreds)) OVER (PARTITION BY node_id) as the
	// denominator so all feeds share the same base per node.
	query2 := fmt.Sprintf(`
		SELECT
			node_id,
			feed,
			shreds_won,
			total_shreds,
			round(shreds_won / max_total * 100, 1) AS win_rate_pct
		FROM (
			SELECT
				r.node_id,
				r.feed,
				SUM(r.shreds_won) AS shreds_won,
				SUM(r.total_shreds) AS total_shreds,
				MAX(SUM(r.total_shreds)) OVER (PARTITION BY r.node_id) AS max_total
			FROM %s.slot_feed_races AS r FINAL
			INNER JOIN (
				SELECT DISTINCT node_id, slot
				FROM %s.slot_feed_races FINAL
				WHERE feed_type = 'shred' AND feed = 'dz' AND loser_feed = '' %s
			) dz ON r.node_id = dz.node_id AND r.slot = dz.slot
			WHERE r.feed_type = 'shred' AND r.loser_feed = '' %s
			GROUP BY r.node_id, r.feed
		)
	`, shredderDB, shredderDB, timeFilter, timeFilter)

	start = time.Now()
	rows2, err := envDB(ctx).Query(ctx, query2)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		log.Printf("EdgeScoreboard query2 error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
			log.Printf("EdgeScoreboard query2 scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		feedStats[feedKey{nodeID, feed}] = &EdgeScoreboardFeedStats{
			ShredsWon:   shredsWon,
			TotalShreds: totalShreds,
			WinRatePct:  winRatePct,
		}
	}
	if err := rows2.Err(); err != nil {
		log.Printf("EdgeScoreboard query2 rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Query 2b: Pairwise lead times from lead-time rows (loser_feed != '')
	// Uses quantile() to aggregate per-slot percentiles across slots — never AVG.
	// No DZ-slot join needed: lead time rows only exist when both feeds delivered
	// the same shred, so sample counts naturally reflect coverage overlap.
	// Uses FINAL for dedup safety.
	query2b := fmt.Sprintf(`
		SELECT
			node_id,
			feed,
			loser_feed,
			count() AS slot_count,
			quantile(0.5)(lead_time_p50_ms) AS p50_ms,
			quantile(0.5)(lead_time_p95_ms) AS p95_ms
		FROM %s.slot_feed_races FINAL
		WHERE feed_type = 'shred' AND loser_feed != '' %s
		GROUP BY node_id, feed, loser_feed
	`, shredderDB, timeFilter)

	start = time.Now()
	rows2b, err := envDB(ctx).Query(ctx, query2b)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		log.Printf("EdgeScoreboard query2b error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows2b.Close()

	for rows2b.Next() {
		var nodeID, feed, loserFeed string
		var slotCount uint64
		var p50, p95 float64
		if err := rows2b.Scan(&nodeID, &feed, &loserFeed, &slotCount, &p50, &p95); err != nil {
			log.Printf("EdgeScoreboard query2b scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
		log.Printf("EdgeScoreboard query2b rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
		rows3, err := envDB(ctx).Query(ctx, query3, codes)
		duration = time.Since(start)
		metrics.RecordClickHouseQuery(duration, err)
		if err != nil {
			log.Printf("EdgeScoreboard query3 error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows3.Close()

		for rows3.Next() {
			var code, name string
			var lat, lon float64
			if err := rows3.Scan(&code, &name, &lat, &lon); err != nil {
				log.Printf("EdgeScoreboard query3 scan error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			metros[strings.ToUpper(code)] = &metroInfo{name: name, latitude: lat, longitude: lon}
		}
		if err := rows3.Err(); err != nil {
			log.Printf("EdgeScoreboard query3 rows error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
			NodeID:        nodeID,
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

	var completenessPct float64
	if totalSlots > 0 {
		completenessPct = float64(dzSlots) / float64(totalSlots) * 100
	}

	resp := EdgeScoreboardResponse{
		Window:          window,
		GeneratedAt:     time.Now().UTC(),
		CurrentEpoch:    globalMaxEpoch,
		CurrentSlot:     globalMaxSlot,
		TotalSlots:      totalSlots,
		DZSlots:         dzSlots,
		CompletenessPct: completenessPct,
		Nodes:           nodes,
	}

	writeJSON(w, resp)
}
