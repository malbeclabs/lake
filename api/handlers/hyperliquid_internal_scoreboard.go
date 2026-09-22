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

	"golang.org/x/sync/errgroup"
)

var hyperliquidCompetitors = []struct{ Feed, Label string }{
	{"hyperliquid_public_bbo", "Public API"},
	{"hydromancer_bbo", "Hydromancer"},
	{"dwellir_l2book_bbo", "Dwellir"},
	{"quicknode_l2book_bbo", "QuickNode"},
}

// Withheld from every measurement: HypeRPC arrives a median ~100s stale during recurring
// backlog episodes, which would show DoubleZero winning by minutes. Re-add to
// hyperliquidCompetitors once the feed is fixed.
var hyperliquidExcludedFeeds = []string{"hyperpc_shared_bbo"}

func hyperliquidExcludedFeedsClause() string {
	if len(hyperliquidExcludedFeeds) == 0 {
		return ""
	}
	quoted := make([]string, len(hyperliquidExcludedFeeds))
	for i, f := range hyperliquidExcludedFeeds {
		quoted[i] = "'" + f + "'"
	}
	in := strings.Join(quoted, ", ")
	return fmt.Sprintf("AND feed NOT IN (%[1]s) AND loser_feed NOT IN (%[1]s)", in)
}

var hyperliquidWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
}

// The summary table's ReplacingMergeTree sorting key. The remote MV refreshes on overlapping
// windows, so counting distinct keys dedups without paying FINAL's merge cost. The approximate
// aggregates elsewhere (uniqCombined, quantileTDigest) are deliberate: the exact variants buffer
// ~750 MiB of per-group state at current volume and tripped ClickHouse's OvercommitTracker,
// killing the page-cache refresh under load.
const raceKeyTuple = "(measurement_node_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed)"

var hyperliquidLiquidSymbols = []string{
	"xyz:SP500", "xyz:XYZ100", "xyz:MU", "xyz:SKHX", "xyz:SPCX", "xyz:CL", "xyz:NVDA", "xyz:BRENTOIL",
	"BTC", "ETH", "SOL", "HYPE", "ZEC",
}

func hyperliquidLiquidSymbolFilter() string {
	return hyperliquidSymbolInClause(hyperliquidLiquidSymbols)
}

var hyperliquidRecentRaceSymbols = []string{
	"BTC", "ETH", "SOL", "HYPE",
	"xyz:SP500", "xyz:XYZ100", "xyz:MU", "xyz:SKHX",
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

func sanitizeHyperliquidSymbol(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "all") || !hyperliquidSymbolRe.MatchString(s) {
		return ""
	}
	return s
}

type HyperliquidCompetitor struct {
	Feed      string  `json:"feed"`
	Label     string  `json:"label"`
	DZWinPct  float64 `json:"dz_win_pct"`
	LeadP50Ms float64 `json:"lead_p50_ms"`
	LeadP95Ms float64 `json:"lead_p95_ms"`
	Races     uint64  `json:"races"`
}

type HyperliquidNode struct {
	MeasurementNodeID string                  `json:"measurement_node_id"`
	LocationCode      string                  `json:"location_code"`
	DZWinSharePct     float64                 `json:"dz_win_share_pct"`
	TotalRaces        uint64                  `json:"total_races"`
	Competitors       []HyperliquidCompetitor `json:"competitors"`
}

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

type HyperliquidInternalScoreboardResponse struct {
	Window           string                       `json:"window"`
	Symbol           string                       `json:"symbol,omitempty"`
	GeneratedAt      time.Time                    `json:"generated_at"`
	FeedType         string                       `json:"feed_type"`
	DZWinSharePct    float64                      `json:"dz_win_share_pct"`
	TotalRaces       uint64                       `json:"total_races"`
	Competitors      []HyperliquidCompetitor      `json:"competitors"`
	Nodes            []HyperliquidNode            `json:"nodes"`
	RecentRaces      []HyperliquidRace            `json:"recent_races"`
	Prices           map[string]float64           `json:"prices,omitempty"`
	CompositeLatency *HyperliquidCompositeLatency `json:"composite_latency,omitempty"`
}

type HyperliquidCompositeLatency struct {
	Window      string    `json:"window"`
	P50Ms       float64   `json:"p50_ms"`
	P90Ms       float64   `json:"p90_ms"`
	P99Ms       float64   `json:"p99_ms"`
	GeneratedAt time.Time `json:"generated_at"`
}

const hyperliquidCompositeLatencyCacheKey = "hyperliquid_composite_latency"

func labelForFeed(feed string) string {
	for _, c := range hyperliquidCompetitors {
		if c.Feed == feed {
			return c.Label
		}
	}
	return feed
}

func hyperliquidFeedDisplay(feed string) string {
	if strings.HasPrefix(feed, "tob_") {
		return "DoubleZero"
	}
	return labelForFeed(feed)
}

// An empty symbol means all symbols.
func (a *API) FetchHyperliquidInternalScoreboardData(ctx context.Context, window, symbol string) (*HyperliquidInternalScoreboardResponse, error) {
	interval, ok := hyperliquidWindows[window]
	if !ok {
		window = "1h"
		interval = hyperliquidWindows[window]
	}
	symbol = sanitizeHyperliquidSymbol(symbol)

	// An environment without the proxy table caches a clean empty payload rather than
	// logging an error every refresh cycle.
	if !a.hyperliquidFeedsTableExists(ctx) {
		return &HyperliquidInternalScoreboardResponse{
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
	symbolFilter = strings.TrimSpace(symbolFilter + " " + hyperliquidExcludedFeedsClause())
	db := fmt.Sprintf("`%s`", a.FeedsDB)

	resp := &HyperliquidInternalScoreboardResponse{
		Window:      window,
		Symbol:      symbol,
		GeneratedAt: time.Now().UTC(),
		FeedType:    "bbo",
		Competitors: []HyperliquidCompetitor{},
		Nodes:       []HyperliquidNode{},
		RecentRaces: []HyperliquidRace{},
	}

	// WITH ROLLUP gives the per-vantage and per-competitor grains from one scan. The counts stay
	// exact because a race key includes measurement_node_id, so races partition cleanly by node.
	q := fmt.Sprintf(`
		SELECT competitor, measurement_node_id, any(location_code) AS location_code,
			uniqCombinedIf(rk, dz_won = 1) AS dz_wins,
			uniqCombinedIf(rk, dz_won = 0) AS dz_losses,
			-- A cell DoubleZero never won quantiles over no rows and returns NaN, which fails JSON
			-- encoding of the whole response and poisons the page cache.
			ifNotFinite(toFloat64(quantileTDigestIf(0.5)(lead_ms, dz_won = 1)), 0) AS lead_p50,
			ifNotFinite(toFloat64(quantileTDigestIf(0.95)(lead_ms, dz_won = 1)), 0) AS lead_p95
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
		GROUP BY competitor, measurement_node_id WITH ROLLUP`, raceKeyTuple, db, symbolFilter, interval)

	type stat struct {
		wins, losses     uint64
		leadP50, leadP95 float64
	}
	type nodeAgg struct {
		loc    string
		byFeed map[string]stat
	}
	byFeed := map[string]stat{}
	nodeMap := map[string]*nodeAgg{}
	var nodeOrder []string
	var recent []HyperliquidRace
	var prices map[string]float64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := a.envDB(gctx).Query(gctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var competitor, node, loc string
			var s stat
			if err := rows.Scan(&competitor, &node, &loc, &s.wins, &s.losses, &s.leadP50, &s.leadP95); err != nil {
				return err
			}
			switch {
			case competitor == "":
				// Grand total; the headline is summed from the per-node cells instead.
				continue
			case node == "":
				byFeed[competitor] = s
			default:
				na, ok := nodeMap[node]
				if !ok {
					na = &nodeAgg{loc: loc, byFeed: map[string]stat{}}
					nodeMap[node] = na
					nodeOrder = append(nodeOrder, node)
				}
				na.byFeed[competitor] = s
			}
		}
		return rows.Err()
	})
	g.Go(func() error {
		r, err := a.fetchHyperliquidRecentRaces(gctx, time.Time{}, 10)
		if err != nil {
			return err
		}
		recent = r
		return nil
	})
	g.Go(func() error {
		// Best-effort: a live-price lookup failure must not fail the whole scoreboard.
		if p, err := a.fetchHyperliquidPrices(gctx); err == nil {
			prices = p
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

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

	resp.TotalRaces = globalRaces
	if globalRaces > 0 {
		resp.DZWinSharePct = 100.0 * float64(globalWins) / float64(globalRaces)
	}

	if recent != nil {
		resp.RecentRaces = recent
	}
	if prices != nil {
		resp.Prices = prices
	}

	// Best-effort: absent until the slow refresher has populated the cache.
	if raw, err := a.readPageCache(ctx, hyperliquidCompositeLatencyCacheKey); err == nil && len(raw) > 0 {
		var cl HyperliquidCompositeLatency
		if json.Unmarshal(raw, &cl) == nil {
			resp.CompositeLatency = &cl
		}
	}

	return resp, nil
}

func (a *API) fetchHyperliquidRecentRaces(ctx context.Context, sinceTs time.Time, perSymbol int) ([]HyperliquidRace, error) {
	if perSymbol <= 0 || perSymbol > 50 {
		perSymbol = 10
	}
	// 15 min so the covered symbols fill a column despite the ~50-90s MV lag.
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
		LIMIT %d BY symbol`, db,
		strings.TrimSpace(hyperliquidRecentRaceSymbolFilter()+" "+hyperliquidExcludedFeedsClause()), timeFilter, perSymbol)
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

// Returns "" for a request shape that is never cached: a symbol filter is a non-default view.
func hyperliquidInternalScoreboardCacheKey(r *http.Request) string {
	if r.URL.Query().Get("symbol") != "" {
		return ""
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window == "" {
		window = "1h"
	}
	if _, ok := hyperliquidWindows[window]; !ok {
		return ""
	}
	return hyperliquidInternalScoreboardWindowKey(window)
}

// The 1h key is populated by the page-cache worker; 24h/7d by the background refresher.
func hyperliquidInternalScoreboardWindowKey(window string) string {
	if window == "1h" {
		return "hyperliquid_internal_scoreboard"
	}
	return "hyperliquid_internal_scoreboard:" + window
}

func (a *API) hyperliquidFeedsTableExists(ctx context.Context) bool {
	var n uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.hyperliquid_bbo_feed_race_summary", a.FeedsDB)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

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

// Scoped to Tokyo so the headline reflects best-case DoubleZero delivery rather than an
// all-metro average, matching the Grafana "tob_* (first-arrival, all DZ)" panel. A full-day scan
// of the proxied table taking tens of seconds: background cadence only, never the request path.
func (a *API) FetchHyperliquidCompositeLatency(ctx context.Context) (*HyperliquidCompositeLatency, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	// The uniqExact(source) >= 2 floor is what keeps p99 realistic: without it a single laggy feed
	// inflates the tail. The spill setting degrades high block cardinality to a slower query
	// instead of an OOM on the memory-constrained proxy.
	q := fmt.Sprintf(`
		WITH c AS (
			SELECT symbol, source_ts_ms, min(recv_ts_ns) AS r
			FROM %[1]s.hyperliquid_bbo_observations
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(24)))
			  AND startsWith(source, 'tob_')
			  AND location_code = 'tyo' %[2]s
			GROUP BY symbol, source_ts_ms
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

// Computes the views too heavy for the 60s page-cache worker: the composite latency and the
// 24h/7d scoreboards (1h stays on the ordinary worker). The latency goes first so the scoreboards
// pick up its freshly-cached value.
func (a *API) StartHyperliquidBackgroundRefresher(ctx context.Context) {
	const interval = 10 * time.Minute
	const runTimeout = 3 * time.Minute
	refreshComposite := func() {
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
	refreshScoreboard := func(window string) {
		rctx, cancel := context.WithTimeout(ctx, runTimeout)
		defer cancel()
		val, err := a.FetchHyperliquidInternalScoreboardData(rctx, window, "")
		if err != nil {
			slog.Warn("hyperliquid scoreboard refresh failed", "window", window, "error", err)
			return
		}
		if err := a.WritePageCache(ctx, hyperliquidInternalScoreboardWindowKey(window), val); err != nil {
			slog.Warn("hyperliquid scoreboard cache write failed", "window", window, "error", err)
		}
	}
	refresh := func() {
		refreshComposite()
		refreshScoreboard("24h")
		refreshScoreboard("7d")
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

func (a *API) GetHyperliquidInternalScoreboard(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if key := hyperliquidInternalScoreboardCacheKey(r); key != "" {
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

	resp, err := a.FetchHyperliquidInternalScoreboardData(ctx, window, symbol)
	if err != nil {
		logError("HyperliquidScoreboard error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
