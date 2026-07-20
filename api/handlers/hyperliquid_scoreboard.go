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

// hyperliquidFeedRe bounds feed ids to characters safe to inline into ClickHouse SQL.
// Config rows are operator-managed rather than user input, but the values are concatenated
// into queries, so anything outside this set is dropped rather than trusted.
var hyperliquidFeedRe = regexp.MustCompile(`^[A-Za-z0-9:_.-]{1,64}$`)

// hyperliquidEntry is one configured scoreboard feed.
type hyperliquidEntry struct{ Feed, Label string }

// hyperliquidEntries is the scoreboard's configured feed allow-list, loaded from Postgres.
// Only feeds present here are raced, counted, or displayed, so a feed is added, removed, or
// reordered by changing rows — no code change and no deploy.
type hyperliquidEntries struct {
	ordered []hyperliquidEntry // display order
	labels  map[string]string  // feed -> label
}

// empty reports whether any feed is configured.
func (e hyperliquidEntries) empty() bool { return len(e.ordered) == 0 }

// inClause returns the SQL predicate restricting a race to configured feeds, or "" if none
// are configured. Every race pairs one tob_* DoubleZero feed with one competing feed, and DZ
// feeds are never configured entries, so requiring either side to be in the set is equivalent
// to requiring the non-DZ side to be configured.
func (e hyperliquidEntries) inClause() string {
	if e.empty() {
		return ""
	}
	quoted := make([]string, 0, len(e.ordered))
	for _, en := range e.ordered {
		quoted = append(quoted, "'"+en.Feed+"'")
	}
	in := strings.Join(quoted, ", ")
	return fmt.Sprintf("AND (feed IN (%[1]s) OR loser_feed IN (%[1]s))", in)
}

// label maps a raw feed to its configured display label (falls back to the raw name).
func (e hyperliquidEntries) label(feed string) string {
	if l, ok := e.labels[feed]; ok {
		return l
	}
	return feed
}

// display returns "DoubleZero" for any tob_ DZ feed, else the configured label. Used so a
// competitor-won recent race reads "<Label> … vs DoubleZero", not a raw tob_ id.
func (e hyperliquidEntries) display(feed string) string {
	if strings.HasPrefix(feed, "tob_") {
		return "DoubleZero"
	}
	return e.label(feed)
}

// loadHyperliquidScoreboardEntries reads the enabled scoreboard feeds from Postgres, in
// display order. Zero configured rows is not an error — it is the deliberate "nothing
// configured yet" state and returns a clean empty set with a nil error. A genuine load
// failure (query, scan, or row iteration) returns a non-nil error instead of degrading to an
// empty set: the caller must not treat a Postgres blip as "zero feeds configured", or the
// background refresher and page-cache worker would overwrite the last-good cached payload
// with an empty one. Logged at WARN, never ERROR — ERROR pages on-call.
func (a *API) loadHyperliquidScoreboardEntries(ctx context.Context) (hyperliquidEntries, error) {
	e := hyperliquidEntries{labels: map[string]string{}}
	if a.PgPool == nil {
		return e, nil
	}
	rows, err := a.PgPool.Query(ctx, `
		SELECT feed, label FROM hyperliquid_scoreboard_entry
		WHERE enabled ORDER BY display_order, feed`)
	if err != nil {
		slog.Warn("hyperliquid scoreboard entry load failed", "error", err)
		return hyperliquidEntries{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var feed, label string
		if err := rows.Scan(&feed, &label); err != nil {
			slog.Warn("hyperliquid scoreboard entry scan failed", "error", err)
			return hyperliquidEntries{}, err
		}
		// A malformed feed would be inlined into SQL; drop it and keep serving the rest.
		if !hyperliquidFeedRe.MatchString(feed) {
			slog.Warn("hyperliquid scoreboard entry skipped: unsafe feed id", "feed", feed)
			continue
		}
		// tob_ feeds are DoubleZero's own; the allow-list clause relies on them never being
		// configured entries (see inClause). A tob_ config row would broaden the clause to
		// match races against unconfigured competitors, leaking their raw feed ids into the
		// public payload, so it is dropped rather than trusted.
		if strings.HasPrefix(feed, "tob_") {
			slog.Warn("hyperliquid scoreboard entry skipped: tob_ feed not allowed", "feed", feed)
			continue
		}
		e.ordered = append(e.ordered, hyperliquidEntry{Feed: feed, Label: label})
		e.labels[feed] = label
	}
	if err := rows.Err(); err != nil {
		slog.Warn("hyperliquid scoreboard entry iteration failed", "error", err)
		return hyperliquidEntries{}, err
	}
	return e, nil
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
	// CompositeLatency is DoubleZero's Tokyo first-arrival feed latency (24h); nil until the
	// background refresher computes it (the query is too heavy for the request path).
	CompositeLatency *HyperliquidCompositeLatency `json:"composite_latency,omitempty"`
}

// HyperliquidCompositeLatency is DoubleZero's Tokyo first-arrival latency (blocktime -> receive,
// earliest across the tob_ DZ feeds at the Tokyo vantage point) over a fixed 24h window,
// p50/p90/p99 in ms.
type HyperliquidCompositeLatency struct {
	Window      string    `json:"window"`
	P50Ms       float64   `json:"p50_ms"`
	P90Ms       float64   `json:"p90_ms"`
	P99Ms       float64   `json:"p99_ms"`
	GeneratedAt time.Time `json:"generated_at"`
}

const hyperliquidCompositeLatencyCacheKey = "hyperliquid_composite_latency"

// emptyHyperliquidScoreboard is the empty-but-valid response served when the scoreboard has
// nothing to compute — the proxied summary table is absent (e.g. local dev) or no feeds are
// configured. Returning this instead of an error keeps the page-cache refresher caching a
// clean payload rather than logging every cycle.
func emptyHyperliquidScoreboard(window string) *HyperliquidScoreboardResponse {
	return &HyperliquidScoreboardResponse{
		Window:      window,
		GeneratedAt: time.Now().UTC(),
		FeedType:    "bbo",
		Competitors: []HyperliquidCompetitor{},
		Nodes:       []HyperliquidNode{},
		RecentRaces: []HyperliquidRace{},
	}
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
		return emptyHyperliquidScoreboard(window), nil
	}

	// The configured feed allow-list drives which feeds are raced, counted, and labelled.
	// A load failure propagates as an error so the caller (page-cache refresher, request
	// handler) does not mistake it for "nothing configured" and overwrite a last-good cached
	// payload with an empty one. Zero configured rows is not an error — with nothing
	// configured there is no scoreboard to compute, so serve the empty-but-valid payload
	// rather than scanning for feeds we would then discard.
	entries, err := a.loadHyperliquidScoreboardEntries(ctx)
	if err != nil {
		return nil, err
	}
	if entries.empty() {
		return emptyHyperliquidScoreboard(window), nil
	}

	symbolFilter := hyperliquidLiquidSymbolFilter()
	if symbol != "" {
		symbolFilter = fmt.Sprintf("AND symbol = '%s'", symbol)
	}
	// Restrict both aggregations to configured feeds. Flows into the scan via the shared
	// symbolFilter slot.
	symbolFilter = strings.TrimSpace(symbolFilter + " " + entries.inClause())
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

	// Single scan grouped per (competitor, node) WITH ROLLUP: the (competitor, node) rows give the
	// per-vantage breakdown and the rolled-up (competitor) rows give per-competitor totals, so the
	// per-competitor stats come from the same scan instead of a second full-table scan. A race key
	// includes measurement_node_id, so races partition cleanly by node — the ROLLUP counts are
	// exact and the headline is derivable by summing per-node cells. One query (vs two) also
	// refreshes atomically: on the memory-constrained proxy ClickHouse the refresh succeeds or
	// retries as a unit, instead of needing two independent scans to both survive the
	// OvercommitTracker when other page-cache queries have the server near its memory ceiling.
	q := fmt.Sprintf(`
		SELECT competitor, measurement_node_id, any(location_code) AS location_code,
			uniqCombinedIf(rk, dz_won = 1) AS dz_wins,
			uniqCombinedIf(rk, dz_won = 0) AS dz_losses,
			-- ifNotFinite guards the empty-set case: when DZ won zero races in a (competitor, node)
			-- cell, quantileTDigestIf over no rows returns NaN, which fails JSON encoding of the whole
			-- response (and poisons the page cache). Coalesce to 0 so a swept cell serializes cleanly.
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
	// byFeed holds per-competitor totals (ROLLUP rows where node == ""); nodeMap holds per-node cells.
	byFeed := map[string]stat{}
	nodeMap := map[string]*nodeAgg{}
	var nodeOrder []string
	var recent []HyperliquidRace
	var prices map[string]float64

	// The main ROLLUP scan, the recent-races scan, and the live-price lookup are three independent
	// ClickHouse reads; run them concurrently so their latencies overlap rather than stack under
	// the request timeout. gctx cancels the siblings if the main scan or recent-races hard-fails.
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
				// Grand-total ROLLUP row; the headline is derived from per-node cells instead.
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
		r, err := a.fetchHyperliquidRecentRaces(gctx, time.Time{}, 10, entries)
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

	// Emit competitors in configured order.
	for _, c := range entries.ordered {
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
		for _, c := range entries.ordered {
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

	if recent != nil {
		resp.RecentRaces = recent
	}
	if prices != nil {
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
func (a *API) fetchHyperliquidRecentRaces(ctx context.Context, sinceTs time.Time, perSymbol int, entries hyperliquidEntries) ([]HyperliquidRace, error) {
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
		LIMIT %d BY symbol`, db,
		strings.TrimSpace(hyperliquidRecentRaceSymbolFilter()+" "+entries.inClause()), timeFilter, perSymbol)
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
		r.WinnerLabel = entries.display(r.WinnerFeed)
		r.RunnerUpLabel = entries.display(r.RunnerUpFeed)
		out = append(out, r)
	}
	return out, rows.Err()
}

// hyperliquidScoreboardCacheKey returns the cache key for a cacheable request shape, or "".
// A symbol filter is a non-default view and is never cached. Each supported window has its own
// cached key: the 1h view is refreshed by the page-cache worker; the heavier 24h/7d views are
// refreshed on a slow background cadence (StartHyperliquidBackgroundRefresher) so they are served
// from cache instead of running a multi-day scan on the request path.
func hyperliquidScoreboardCacheKey(r *http.Request) string {
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
	return hyperliquidScoreboardWindowKey(window)
}

// hyperliquidScoreboardWindowKey is the page-cache key for a given window. The 1h key keeps its
// original name (populated by the page-cache worker); 24h/7d get suffixed keys populated by the
// background refresher.
func hyperliquidScoreboardWindowKey(window string) string {
	if window == "1h" {
		return "hyperliquid_scoreboard"
	}
	return "hyperliquid_scoreboard:" + window
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

// FetchHyperliquidCompositeLatency computes DoubleZero's Tokyo first-arrival feed latency
// (p50/p90/p99 in ms) over the last 24h across the liquid symbols, from the raw observations
// table: per (symbol, block) the earliest receive across DZ tob_ feeds at the Tokyo vantage
// point, restricted to blocks where >=2 DZ feeds delivered. Scoped to Tokyo (DZ's fastest metro,
// nearest Hyperliquid) so the headline reflects best-case DZ delivery rather than an all-metro
// average — this matches the Grafana "tob_* (first-arrival, all DZ)" panel. This is a heavy
// full-day scan over the proxied table (~tens of seconds) — it must run on a slow background
// cadence, never in the request path or the 60s page-cache loop.
func (a *API) FetchHyperliquidCompositeLatency(ctx context.Context) (*HyperliquidCompositeLatency, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	// Single GROUP BY over (symbol, block) at Tokyo: min(recv) is associative, so a per-source
	// pre-aggregation is redundant, and uniqExact(source) >= 2 enforces ">=2 distinct Tokyo DZ
	// feeds delivered" directly (this >=2 requirement is what keeps p99 realistic; without it a
	// single laggy feed inflates the tail). Filtering to tob_ feeds and Tokyo in WHERE (not after
	// grouping) drops other rows before they enter the hash table. This keeps tiny per-group state;
	// max_bytes_before_external_group_by spills to disk as a safety net so high block cardinality
	// degrades to a slower query instead of an OOM on the memory-constrained proxy ClickHouse.
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

// StartHyperliquidBackgroundRefresher periodically computes the Hyperliquid views that are too
// heavy for the 60s page-cache worker and writes them to the page cache (Postgres) so all replicas
// share them and the request path never runs a multi-day scan:
//   - the composite feed latency, and
//   - the 24h and 7d scoreboards (the 1h scoreboard stays on the ordinary page-cache worker).
//
// Each computation gets its own timeout so a slow one can't starve the others; the composite
// latency is refreshed first so the 24h/7d scoreboards pick up its freshly-cached value.
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
		val, err := a.FetchHyperliquidScoreboardData(rctx, window, "")
		if err != nil {
			slog.Warn("hyperliquid scoreboard refresh failed", "window", window, "error", err)
			return
		}
		if err := a.WritePageCache(ctx, hyperliquidScoreboardWindowKey(window), val); err != nil {
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

	// FetchHyperliquidScoreboardData already degrades to an empty-but-valid response when the
	// proxy table is absent (e.g. local dev), so no separate existence check is needed here —
	// that avoids a redundant EXISTS TABLE round-trip per uncached request.
	resp, err := a.FetchHyperliquidScoreboardData(ctx, window, symbol)
	if err != nil {
		logError("HyperliquidScoreboard error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
