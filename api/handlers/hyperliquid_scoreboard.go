package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// hyperliquidCompetitors lists the non-DoubleZero feeds shown on the scoreboard,
// in display order, mapping each raw feed name to its label.
var hyperliquidCompetitors = []struct{ Feed, Label string }{
	{"hyperliquid_public_bbo", "Public API"},
	{"hydromancer_bbo", "Hydromancer"},
	{"hyperpc_shared_bbo", "HypeRPC"},
	{"quicknode_l2book_bbo", "QuickNode"},
}

// hyperliquidWindows maps window params to ClickHouse interval expressions.
var hyperliquidWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
}

// raceKeyTuple is the summary table's ReplacingMergeTree sorting key. The remote
// materialized view refreshes on overlapping windows, so each logical race appears as
// several rows that only FINAL (or a uniq over this key) collapses. Counting distinct keys
// dedups without paying FINAL's merge cost, which dominated query time against the full
// production table. Win rates are ratios and lead-time percentiles are duplicate-insensitive,
// so dropping FINAL keeps them correct.
//
// The dedup uses uniqCombined (not uniqExact) and lead percentiles use quantileTDigest (not
// quantileExact): the exact variants buffer per-group state proportional to the number of
// races in the window (~7.5M matchup rows/hour at current volume, ~750 MiB of aggregation
// state), which tripped the ClickHouse OvercommitTracker and got the page-cache refresh killed
// under concurrent load. The approximate variants use bounded (~KB/group) memory; at scoreboard
// scale their sub-1% error is invisible, and they are exact at the small cardinalities the
// unit tests assert.
const raceKeyTuple = "(measurement_node_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed)"

// hyperliquidLiquidSymbols is the curated universe of the most-liquid Hyperliquid markets
// (highest open interest) the scoreboard races on. Thin symbols add noise and aren't
// representative, so the aggregations are restricted to this set by default.
var hyperliquidLiquidSymbols = []string{
	// xyz: synthetic equity/commodity perps
	"xyz:SP500", "xyz:XYZ100", "xyz:MU", "xyz:SKHX", "xyz:SPCX", "xyz:CL", "xyz:NVDA", "xyz:BRENTOIL",
	// native crypto perps
	"BTC", "ETH", "SOL", "HYPE", "ZEC",
}

// hyperliquidLiquidSymbolFilter returns the SQL clause restricting races to the liquid universe.
func hyperliquidLiquidSymbolFilter() string {
	return hyperliquidSymbolInClause(hyperliquidLiquidSymbols)
}

// hyperliquidRecentRaceSymbols are the top-4-by-volume native perps and top-4 HIP-3 (xyz:) DEX
// perps shown in the recent-races grid. Only a subset (currently BTC/ETH) have competitor feeds;
// the rest render as DoubleZero-exclusive coverage in the UI.
var hyperliquidRecentRaceSymbols = []string{
	"BTC", "ETH", "SOL", "HYPE", // native perps
	"xyz:SP500", "xyz:XYZ100", "xyz:MU", "xyz:SKHX", // HIP-3 DEX perps
}

func hyperliquidRecentRaceSymbolFilter() string {
	return hyperliquidSymbolInClause(hyperliquidRecentRaceSymbols)
}

func hyperliquidSymbolInClause(symbols []string) string {
	quoted := make([]string, len(symbols))
	for i, s := range symbols {
		quoted[i] = "'" + s + "'"
	}
	return "AND symbol IN (" + strings.Join(quoted, ", ") + ")"
}

var hyperliquidSymbolRe = regexp.MustCompile(`^[A-Za-z0-9:_.-]{1,32}$`)

// sanitizeHyperliquidSymbol returns the symbol if safe to inline, else "".
func sanitizeHyperliquidSymbol(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "all") || !hyperliquidSymbolRe.MatchString(s) {
		return ""
	}
	return s
}

// HyperliquidCompetitor is DoubleZero's head-to-head record vs one competitor feed.
type HyperliquidCompetitor struct {
	Feed      string  `json:"feed"`
	Label     string  `json:"label"`
	DZWinPct  float64 `json:"dz_win_pct"`
	LeadP50Ms float64 `json:"lead_p50_ms"`
	LeadP95Ms float64 `json:"lead_p95_ms"`
	Races     uint64  `json:"races"`
}

// HyperliquidNode is the per-vantage breakdown.
type HyperliquidNode struct {
	MeasurementNodeID string                  `json:"measurement_node_id"`
	LocationCode      string                  `json:"location_code"`
	DZWinSharePct     float64                 `json:"dz_win_share_pct"`
	TotalRaces        uint64                  `json:"total_races"`
	Competitors       []HyperliquidCompetitor `json:"competitors"`
}

// HyperliquidRace is one recent race for the live strip.
type HyperliquidRace struct {
	EventTs       time.Time `json:"event_ts"`
	Symbol        string    `json:"symbol"`
	LocationCode  string    `json:"location_code"`
	WinnerFeed    string    `json:"winner_feed"`
	WinnerLabel   string    `json:"winner_label"`
	IsDZ          bool      `json:"is_dz"`
	RunnerUpFeed  string    `json:"runner_up_feed"`
	RunnerUpLabel string    `json:"runner_up_label"`
	LeadMs        float64   `json:"lead_ms"`
}

// HyperliquidScoreboardResponse is the API response.
type HyperliquidScoreboardResponse struct {
	Window        string                  `json:"window"`
	Symbol        string                  `json:"symbol,omitempty"`
	GeneratedAt   time.Time               `json:"generated_at"`
	FeedType      string                  `json:"feed_type"`
	DZWinSharePct float64                 `json:"dz_win_share_pct"`
	TotalRaces    uint64                  `json:"total_races"`
	Competitors   []HyperliquidCompetitor `json:"competitors"`
	Nodes         []HyperliquidNode       `json:"nodes"`
	RecentRaces   []HyperliquidRace       `json:"recent_races"`
	// Prices is the latest BBO mid price per recent-race symbol (live, for the grid).
	Prices map[string]float64 `json:"prices,omitempty"`
	// CompositeLatency is DoubleZero's composite first-arrival feed latency (24h); nil until the
	// background refresher computes it (the query is too heavy for the request path).
	CompositeLatency *HyperliquidCompositeLatency `json:"composite_latency,omitempty"`
}

// HyperliquidCompositeLatency is DoubleZero's composite first-arrival latency (blocktime ->
// receive, earliest across all tob_ DZ feeds) over a fixed 24h window, p50/p90/p99 in ms.
type HyperliquidCompositeLatency struct {
	Window      string    `json:"window"`
	P50Ms       float64   `json:"p50_ms"`
	P90Ms       float64   `json:"p90_ms"`
	P99Ms       float64   `json:"p99_ms"`
	GeneratedAt time.Time `json:"generated_at"`
}

const hyperliquidCompositeLatencyCacheKey = "hyperliquid_composite_latency"

// labelForFeed maps a raw competitor feed to its display label (falls back to the raw name).
func labelForFeed(feed string) string {
	for _, c := range hyperliquidCompetitors {
		if c.Feed == feed {
			return c.Label
		}
	}
	return feed
}

// hyperliquidFeedDisplay returns "DoubleZero" for any tob_ DZ feed, else the competitor label.
// Used so a competitor-won recent race reads "Hydromancer … vs DoubleZero", not a raw tob_ id.
func hyperliquidFeedDisplay(feed string) string {
	if strings.HasPrefix(feed, "tob_") {
		return "DoubleZero"
	}
	return labelForFeed(feed)
}

// FetchHyperliquidScoreboardData computes the aggregated scoreboard for a window and
// optional symbol. Empty symbol means all symbols.
func (a *API) FetchHyperliquidScoreboardData(ctx context.Context, window, symbol string) (*HyperliquidScoreboardResponse, error) {
	interval, ok := hyperliquidWindows[window]
	if !ok {
		window = "1h"
		interval = hyperliquidWindows[window]
	}
	symbol = sanitizeHyperliquidSymbol(symbol)

	// Guard: if the feeds summary table doesn't exist (e.g. local dev without the
	// proxy/seed), return an empty-but-valid response so the page-cache refresher
	// caches a clean empty payload instead of logging an error every cycle.
	if !a.hyperliquidFeedsTableExists(ctx) {
		return &HyperliquidScoreboardResponse{
			Window:      window,
			GeneratedAt: time.Now().UTC(),
			FeedType:    "bbo",
			Competitors: []HyperliquidCompetitor{},
			Nodes:       []HyperliquidNode{},
			RecentRaces: []HyperliquidRace{},
		}, nil
	}

	symbolFilter := hyperliquidLiquidSymbolFilter()
	if symbol != "" {
		symbolFilter = fmt.Sprintf("AND symbol = '%s'", symbol)
	}
	db := fmt.Sprintf("`%s`", a.FeedsDB)

	resp := &HyperliquidScoreboardResponse{
		Window:      window,
		Symbol:      symbol,
		GeneratedAt: time.Now().UTC(),
		FeedType:    "bbo",
		Competitors: []HyperliquidCompetitor{},
		Nodes:       []HyperliquidNode{},
		RecentRaces: []HyperliquidRace{},
	}

	// The headline (DZ win share + total races) is derived from the per-node aggregation
	// below rather than its own scan — counts partition cleanly by competitor, so summing the
	// per-node cells gives the same totals while saving a full scan of the (very large) table.

	// Per-competitor: win%, lead p50/p95 (over DZ wins), and total comparable races.
	compQ := fmt.Sprintf(`
		SELECT competitor,
			uniqCombinedIf(rk, dz_won = 1) AS dz_wins,
			uniqCombinedIf(rk, dz_won = 0) AS dz_losses,
			toFloat64(quantileTDigestIf(0.5)(lead_ms, dz_won = 1)) AS lead_p50,
			toFloat64(quantileTDigestIf(0.95)(lead_ms, dz_won = 1)) AS lead_p95
		FROM (
			SELECT
				%[1]s AS rk,
				if(startsWith(feed,'tob_'), loser_feed, feed) AS competitor,
				if(startsWith(feed,'tob_'), 1, 0) AS dz_won,
				lead_time_p50_ms AS lead_ms
			FROM %[2]s.hyperliquid_bbo_feed_race_summary
			WHERE feed != loser_feed AND loser_feed != ''
			  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %[3]s
			  AND event_ts >= now() - INTERVAL %[4]s
		)
		GROUP BY competitor`, raceKeyTuple, db, symbolFilter, interval)
	rows, err := a.envDB(ctx).Query(ctx, compQ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type stat struct {
		wins, losses     uint64
		leadP50, leadP95 float64
	}
	byFeed := map[string]stat{}
	for rows.Next() {
		var competitor string
		var s stat
		if err := rows.Scan(&competitor, &s.wins, &s.losses, &s.leadP50, &s.leadP95); err != nil {
			return nil, err
		}
		byFeed[competitor] = s
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Emit competitors in configured order.
	for _, c := range hyperliquidCompetitors {
		s, ok := byFeed[c.Feed]
		if !ok {
			continue
		}
		races := s.wins + s.losses
		var winPct float64
		if races > 0 {
			winPct = 100.0 * float64(s.wins) / float64(races)
		}
		resp.Competitors = append(resp.Competitors, HyperliquidCompetitor{
			Feed:      c.Feed,
			Label:     c.Label,
			DZWinPct:  winPct,
			LeadP50Ms: s.leadP50,
			LeadP95Ms: s.leadP95,
			Races:     races,
		})
	}

	// Per-node: same comparison, grouped by measurement node + competitor.
	nodeQ := fmt.Sprintf(`
		SELECT measurement_node_id, location_code, competitor,
			uniqCombinedIf(rk, dz_won = 1) AS dz_wins,
			uniqCombinedIf(rk, dz_won = 0) AS dz_losses,
			toFloat64(quantileTDigestIf(0.5)(lead_ms, dz_won = 1)) AS lead_p50,
			toFloat64(quantileTDigestIf(0.95)(lead_ms, dz_won = 1)) AS lead_p95
		FROM (
			SELECT measurement_node_id, location_code,
				%[1]s AS rk,
				if(startsWith(feed,'tob_'), loser_feed, feed) AS competitor,
				if(startsWith(feed,'tob_'), 1, 0) AS dz_won,
				lead_time_p50_ms AS lead_ms
			FROM %[2]s.hyperliquid_bbo_feed_race_summary
			WHERE feed != loser_feed AND loser_feed != ''
			  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %[3]s
			  AND event_ts >= now() - INTERVAL %[4]s
		)
		GROUP BY measurement_node_id, location_code, competitor`, raceKeyTuple, db, symbolFilter, interval)
	nrows, err := a.envDB(ctx).Query(ctx, nodeQ)
	if err != nil {
		return nil, err
	}
	defer nrows.Close()

	type nodeAgg struct {
		loc    string
		byFeed map[string]stat
	}
	nodeMap := map[string]*nodeAgg{}
	var nodeOrder []string
	for nrows.Next() {
		var node, loc, competitor string
		var s stat
		if err := nrows.Scan(&node, &loc, &competitor, &s.wins, &s.losses, &s.leadP50, &s.leadP95); err != nil {
			return nil, err
		}
		na, ok := nodeMap[node]
		if !ok {
			na = &nodeAgg{loc: loc, byFeed: map[string]stat{}}
			nodeMap[node] = na
			nodeOrder = append(nodeOrder, node)
		}
		na.byFeed[competitor] = s
	}
	if err := nrows.Err(); err != nil {
		return nil, err
	}

	var globalWins, globalRaces uint64
	for _, node := range nodeOrder {
		na := nodeMap[node]
		n := HyperliquidNode{
			MeasurementNodeID: node,
			LocationCode:      na.loc,
			Competitors:       []HyperliquidCompetitor{},
		}
		var wins, races uint64
		for _, c := range hyperliquidCompetitors {
			s, ok := na.byFeed[c.Feed]
			if !ok {
				continue
			}
			r := s.wins + s.losses
			var winPct float64
			if r > 0 {
				winPct = 100.0 * float64(s.wins) / float64(r)
			}
			n.Competitors = append(n.Competitors, HyperliquidCompetitor{
				Feed: c.Feed, Label: c.Label, DZWinPct: winPct,
				LeadP50Ms: s.leadP50, LeadP95Ms: s.leadP95, Races: r,
			})
			wins += s.wins
			races += r
		}
		n.TotalRaces = races
		if races > 0 {
			n.DZWinSharePct = 100.0 * float64(wins) / float64(races)
		}
		resp.Nodes = append(resp.Nodes, n)
		globalWins += wins
		globalRaces += races
	}

	// Headline derived from the per-node totals (no extra scan).
	resp.TotalRaces = globalRaces
	if globalRaces > 0 {
		resp.DZWinSharePct = 100.0 * float64(globalWins) / float64(globalRaces)
	}

	recent, err := a.fetchHyperliquidRecentRaces(ctx, time.Time{}, 10)
	if err != nil {
		return nil, err
	}
	resp.RecentRaces = recent

	// Live prices per symbol for the recent-races grid (best-effort; cheap latest-BBO lookup).
	if prices, err := a.fetchHyperliquidPrices(ctx); err == nil {
		resp.Prices = prices
	}

	// Attach the background-refreshed 24h composite latency (best-effort; absent until the slow
	// refresher has populated the cache).
	if raw, err := a.readPageCache(ctx, hyperliquidCompositeLatencyCacheKey); err == nil && len(raw) > 0 {
		var cl HyperliquidCompositeLatency
		if json.Unmarshal(raw, &cl) == nil {
			resp.CompositeLatency = &cl
		}
	}

	return resp, nil
}

// fetchHyperliquidRecentRaces returns the most recent races (one row per race,
// winner + closest competitor + lead). sinceTs zero -> last 5 minutes.
func (a *API) fetchHyperliquidRecentRaces(ctx context.Context, sinceTs time.Time, perSymbol int) ([]HyperliquidRace, error) {
	if perSymbol <= 0 || perSymbol > 50 {
		perSymbol = 10
	}
	// 15 min so the covered symbols fill a full column despite the ~50-90s MV lag; LIMIT n BY
	// symbol then keeps only the newest n per symbol.
	timeFilter := "AND event_ts >= now() - INTERVAL 15 MINUTE"
	if !sinceTs.IsZero() {
		timeFilter = fmt.Sprintf("AND event_ts > toDateTime64(%d, 9)", sinceTs.Unix())
	}
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT
			max(event_ts) AS max_event_ts,
			symbol,
			location_code,
			feed AS winner_feed,
			startsWith(feed,'tob_') AS is_dz,
			argMin(loser_feed, lead_time_p50_ms) AS runner_up_feed,
			min(lead_time_p50_ms) AS lead_ms
		FROM %s.hyperliquid_bbo_feed_race_summary
		-- Only DoubleZero-vs-competitor matchups (exactly one side is a tob_* DZ feed) — exclude
		-- DZ-vs-DZ races (e.g. tob_gcp_tyo vs tob_aws_tyo), which otherwise flood the feed.
		WHERE loser_feed != '' AND feed != loser_feed
		  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %s %s
		GROUP BY capture_run_id, measurement_node_id, symbol, source_ts_ms, bbo_hash, location_code, feed
		ORDER BY max_event_ts DESC
		LIMIT %d BY symbol`, db, hyperliquidRecentRaceSymbolFilter(), timeFilter, perSymbol)
	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []HyperliquidRace{}
	for rows.Next() {
		var r HyperliquidRace
		var isDZ uint8
		if err := rows.Scan(&r.EventTs, &r.Symbol, &r.LocationCode, &r.WinnerFeed, &isDZ, &r.RunnerUpFeed, &r.LeadMs); err != nil {
			return nil, err
		}
		r.IsDZ = isDZ == 1
		r.WinnerLabel = hyperliquidFeedDisplay(r.WinnerFeed)
		r.RunnerUpLabel = hyperliquidFeedDisplay(r.RunnerUpFeed)
		out = append(out, r)
	}
	return out, rows.Err()
}

// hyperliquidScoreboardCacheKey returns the cache key for the default request shape, or "".
func hyperliquidScoreboardCacheKey(r *http.Request) string {
	if r.URL.Query().Get("since_ts") != "" || r.URL.Query().Get("symbol") != "" {
		return ""
	}
	// 1h is the cacheable default — a 24h/7d aggregation over the full proxied table is too
	// slow to refresh within the cache deadline (the summary table has no time-based index),
	// so only the 1h view is cached; longer windows fall through to a (slower) live query.
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window != "" && window != "1h" {
		return ""
	}
	return "hyperliquid_scoreboard"
}

// hyperliquidFeedsTableExists reports whether the proxied summary table is queryable.
func (a *API) hyperliquidFeedsTableExists(ctx context.Context) bool {
	var n uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.hyperliquid_bbo_feed_race_summary", a.FeedsDB)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

// fetchHyperliquidPrices returns the latest BBO mid price per recent-race symbol — a cheap
// latest-observation lookup (~0.4s) used to show a live price next to each symbol's races.
// Price = (bid_px_raw + ask_px_raw)/2 * 10^price_exp.
func (a *API) fetchHyperliquidPrices(ctx context.Context) (map[string]float64, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT symbol, argMax((bid_px_raw + ask_px_raw) / 2 * pow(10, price_exp), recv_ts_ns) AS price
		FROM %s.hyperliquid_bbo_observations
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(1))) %s
		GROUP BY symbol`, db, hyperliquidRecentRaceSymbolFilter())
	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var sym string
		var price float64
		if err := rows.Scan(&sym, &price); err != nil {
			return nil, err
		}
		out[sym] = price
	}
	return out, rows.Err()
}

// FetchHyperliquidCompositeLatency computes DoubleZero's composite first-arrival latency
// (p50/p90/p99 in ms) over the last 24h across the liquid symbols and all vantage points, from
// the raw observations table: per (metro, symbol, block) the earliest receive across DZ tob_
// feeds, restricted to blocks where >=2 DZ feeds delivered. This is a heavy full-day scan over
// the proxied table (~tens of seconds) — it must run on a slow background cadence, never in the
// request path or the 60s page-cache loop.
func (a *API) FetchHyperliquidCompositeLatency(ctx context.Context) (*HyperliquidCompositeLatency, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	// Single GROUP BY over (metro, symbol, block): min(recv) is associative, so the earlier
	// per-source pre-aggregation was redundant, and uniqExact(source) >= 2 enforces ">=2 distinct
	// DZ feeds delivered" directly. Filtering to tob_ feeds in WHERE (not after grouping) drops
	// competitor rows before they enter the hash table. The prior two-CTE form grouped every
	// source by a source-keyed tuple over 24h of the 667 GiB observations table, which on the
	// memory-constrained proxy ClickHouse (feeds is a remoteSecure() proxy, so the GROUP BY runs
	// locally) built a hash table that tripped the OvercommitTracker (code 241). This form keeps
	// tiny per-group state; max_bytes_before_external_group_by spills to disk as a safety net so
	// high block cardinality degrades to a slower query instead of an OOM.
	q := fmt.Sprintf(`
		WITH c AS (
			SELECT location_code, symbol, source_ts_ms, min(recv_ts_ns) AS r
			FROM %[1]s.hyperliquid_bbo_observations
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(24)))
			  AND startsWith(source, 'tob_') %[2]s
			GROUP BY location_code, symbol, source_ts_ms
			HAVING uniqExact(source) >= 2
		)
		SELECT
			toFloat64(quantileTDigest(0.5)((toInt64(r) - toInt64(source_ts_ms) * 1000000) / 1e6)),
			toFloat64(quantileTDigest(0.9)((toInt64(r) - toInt64(source_ts_ms) * 1000000) / 1e6)),
			toFloat64(quantileTDigest(0.99)((toInt64(r) - toInt64(source_ts_ms) * 1000000) / 1e6))
		FROM c
		SETTINGS max_bytes_before_external_group_by = 2000000000`, db, hyperliquidLiquidSymbolFilter())
	var p50, p90, p99 float64
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&p50, &p90, &p99); err != nil {
		return nil, err
	}
	return &HyperliquidCompositeLatency{
		Window: "24h", P50Ms: p50, P90Ms: p90, P99Ms: p99, GeneratedAt: time.Now().UTC(),
	}, nil
}

// StartHyperliquidCompositeLatencyRefresher periodically computes the composite latency and
// writes it to the page cache (Postgres) so all replicas share one value. The query is far too
// slow for the 60s page-cache worker, so it runs here on its own slow cadence.
func (a *API) StartHyperliquidCompositeLatencyRefresher(ctx context.Context) {
	const interval = 10 * time.Minute
	const runTimeout = 3 * time.Minute
	refresh := func() {
		rctx, cancel := context.WithTimeout(ctx, runTimeout)
		defer cancel()
		val, err := a.FetchHyperliquidCompositeLatency(rctx)
		if err != nil {
			slog.Warn("hyperliquid composite latency refresh failed", "error", err)
			return
		}
		if err := a.WritePageCache(ctx, hyperliquidCompositeLatencyCacheKey, val); err != nil {
			slog.Warn("hyperliquid composite latency cache write failed", "error", err)
		}
	}
	go func() {
		refresh()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refresh()
			}
		}
	}()
}

// GetHyperliquidScoreboard serves the Hyperliquid BBO scoreboard.
func (a *API) GetHyperliquidScoreboard(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if key := hyperliquidScoreboardCacheKey(r); key != "" {
			if data, err := a.readPageCache(r.Context(), key); err == nil {
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
	if _, ok := hyperliquidWindows[window]; !ok {
		window = "1h"
	}
	symbol := r.URL.Query().Get("symbol")

	// Degrade gracefully if the proxy table isn't available (e.g. local dev).
	if !a.hyperliquidFeedsTableExists(ctx) {
		writeJSON(w, &HyperliquidScoreboardResponse{
			Window: window, GeneratedAt: time.Now().UTC(), FeedType: "bbo",
			Competitors: []HyperliquidCompetitor{}, Nodes: []HyperliquidNode{}, RecentRaces: []HyperliquidRace{},
		})
		return
	}

	resp, err := a.FetchHyperliquidScoreboardData(ctx, window, symbol)
	if err != nil {
		logError("HyperliquidScoreboard error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

