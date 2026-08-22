package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// The Kalshi scoreboard is the sibling of the Hyperliquid one (hyperliquid_scoreboard.go) and
// reads tables written by the same capture fleet into the same `feeds` database. It is a
// separate implementation rather than a shared one because the two summary tables have
// different grains, and four differences drive every query in this file:
//
//  1. kalshi_bbo_feed_race_summary stores per-event `lead_time_ms` — one row per (race, losing
//     feed) — and has no total_events/events_won/lead_time_p50_ms/lead_time_p95_ms/feed_type
//     columns. Win rate is a count of distinct race keys; p50/p95 are quantiles over rows.
//  2. Its refreshable MV never emits winner-only rows, so there is no loser_feed = '' case.
//  3. capture_run_id is part of its sorting key, so the dedup tuple is one column wider than
//     Hyperliquid's raceKeyTuple.
//  4. source_ts_ms is a DIFFERENT clock per transport arm (WS carries the orderbook-delta
//     timestamp, FIX the header-52 SendingTime), so any venue-to-receive latency must be
//     scoped to one arm. See the Kalshi capture's 20260721000001_bbo_xtransport_race_mv.sql.

// kalshiFeedRe bounds feed ids to characters safe to inline into ClickHouse SQL. Config rows
// are operator-managed rather than user input, but the values are concatenated into queries,
// so anything outside this set is dropped rather than trusted.
var kalshiFeedRe = regexp.MustCompile(`^[A-Za-z0-9:_.-]{1,64}$`)

// kalshiEntry is one configured scoreboard feed.
type kalshiEntry struct{ Feed, Label string }

// kalshiEntries is the scoreboard's configured feed allow-list, loaded from Postgres. Only
// feeds present here are raced, counted, or displayed, so a feed is added, removed, or
// reordered by changing rows — no code change and no deploy.
type kalshiEntries struct {
	ordered []kalshiEntry     // display order
	labels  map[string]string // feed -> label
}

// empty reports whether any feed is configured.
func (e kalshiEntries) empty() bool { return len(e.ordered) == 0 }

// inClause returns the SQL predicate restricting a race to configured feeds, or "" if none are
// configured. Every counted race pairs one DoubleZero feed with one competing feed, and DZ
// feeds are never configured entries (loadKalshiScoreboardEntries rejects them), so requiring
// either side to be in the set is equivalent to requiring the non-DZ side to be configured.
func (e kalshiEntries) inClause() string {
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
func (e kalshiEntries) label(feed string) string {
	if l, ok := e.labels[feed]; ok {
		return l
	}
	return feed
}

// display returns "DoubleZero" for any tob_ DZ feed, else the configured label. Used so a
// competitor-won recent race reads "<Label> … vs DoubleZero", not a raw tob_ id.
func (e kalshiEntries) display(feed string) string {
	if isKalshiDZFeed(feed) {
		return "DoubleZero"
	}
	return e.label(feed)
}

// kalshiDZFeedPrefixes are the capture source-id prefixes for DoubleZero's own edge publisher
// lanes: tob_ is the top-of-book lane, mbp_ the market-by-price lanes.
//
// Both belong to DoubleZero, and both appear in the feed race — an MBP source emits the shared
// BBO observation on every derived top-of-book change, so it races the venue's public feed just
// as the top-of-book lane does. Matching only tob_ would silently drop every mbp_-vs-public
// race from the counts and, worse, classify an mbp_ lane as a competitor. This mirrors the
// operational dashboards' `dz_class` variable (`tob_,mbp_` in
// infra/grafana/dashboards/kalshi-bbo-feed-race.json), which is the definition the numbers on
// this page have to agree with.
var kalshiDZFeedPrefixes = []string{"tob_", "mbp_"}

// isKalshiDZFeed reports whether a feed is one of DoubleZero's own edge publisher lanes.
func isKalshiDZFeed(feed string) bool {
	for _, p := range kalshiDZFeedPrefixes {
		if strings.HasPrefix(feed, p) {
			return true
		}
	}
	return false
}

// kalshiIsDZSQL returns the SQL predicate matching DoubleZero's own feeds on a column, the
// exact counterpart of isKalshiDZFeed. Keeping the two derived from one prefix list stops the
// Go-side labelling and the SQL-side bucketing from drifting apart.
func kalshiIsDZSQL(col string) string {
	terms := make([]string, len(kalshiDZFeedPrefixes))
	for i, p := range kalshiDZFeedPrefixes {
		terms[i] = fmt.Sprintf("startsWith(%s,'%s')", col, p)
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

// loadKalshiScoreboardEntries reads the enabled scoreboard feeds from Postgres, in display
// order. Zero configured rows is not an error — it is the deliberate "nothing configured yet"
// state and returns a clean empty set with a nil error. A genuine load failure (query, scan,
// or row iteration) returns a non-nil error instead of degrading to an empty set: the caller
// must not treat a Postgres blip as "zero feeds configured", or the background refresher and
// page-cache worker would overwrite the last-good cached payload with an empty one. Logged at
// WARN, never ERROR — ERROR pages on-call.
func (a *API) loadKalshiScoreboardEntries(ctx context.Context) (kalshiEntries, error) {
	e := kalshiEntries{labels: map[string]string{}}
	if a.PgPool == nil {
		return e, nil
	}
	rows, err := a.PgPool.Query(ctx, `
		SELECT feed, label FROM kalshi_scoreboard_entry
		WHERE enabled ORDER BY display_order, feed`)
	if err != nil {
		slog.Warn("kalshi scoreboard entry load failed", "error", err)
		return kalshiEntries{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var feed, label string
		if err := rows.Scan(&feed, &label); err != nil {
			slog.Warn("kalshi scoreboard entry scan failed", "error", err)
			return kalshiEntries{}, err
		}
		// A malformed feed would be inlined into SQL; drop it and keep serving the rest.
		if !kalshiFeedRe.MatchString(feed) {
			slog.Warn("kalshi scoreboard entry skipped: unsafe feed id", "feed", feed)
			continue
		}
		// tob_/mbp_ feeds are DoubleZero's own; the allow-list clause relies on them never
		// being configured entries (see inClause). Such a config row would broaden the clause
		// to match races against unconfigured competitors, leaking their raw feed ids into the
		// payload, so it is dropped rather than trusted.
		if isKalshiDZFeed(feed) {
			slog.Warn("kalshi scoreboard entry skipped: DoubleZero feed not allowed", "feed", feed)
			continue
		}
		e.ordered = append(e.ordered, kalshiEntry{Feed: feed, Label: label})
		e.labels[feed] = label
	}
	if err := rows.Err(); err != nil {
		slog.Warn("kalshi scoreboard entry iteration failed", "error", err)
		return kalshiEntries{}, err
	}
	return e, nil
}

// kalshiWindows maps window params to ClickHouse interval expressions.
var kalshiWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
}

// kalshiRaceKeyTuple is the summary table's ReplacingMergeTree sorting key. The remote
// materialized view refreshes on overlapping windows, so each logical race appears as several
// rows that only FINAL (or a uniq over this key) collapses. Counting distinct keys dedups
// without paying FINAL's merge cost. Win rates are ratios and lead-time percentiles are
// duplicate-insensitive, so dropping FINAL keeps them correct.
//
// Unlike Hyperliquid's raceKeyTuple this includes capture_run_id, which is in the Kalshi
// table's ORDER BY: a capture restart mints a new run id for the same (symbol, source_ts_ms,
// bbo_hash), and those are genuinely distinct races, not duplicates to collapse.
//
// It deliberately excludes feed and loser_feed, which are in the table's ORDER BY but are NOT
// part of the race identity. The summary writes one pairwise row per (race, winner, LOSER) —
// the winner is related to each loser, losers are never related to each other. With two
// DoubleZero lanes racing (tob_ and mbp_), that asymmetry counts a DoubleZero loss once per
// lane (public > tob_, public > mbp_ are two rows) and a DoubleZero win once per race (the
// tob_ > mbp_ row is dropped as DZ-vs-DZ), so keying on the feed pair systematically
// understates the win rate. Keying on the race alone collapses both loss rows into the one
// race they describe.
//
// uniqCombined (not uniqExact) and quantileTDigest (not quantileExact) keep aggregation state
// bounded at ~KB per group; the exact variants buffer state proportional to the window's race
// count and trip the ClickHouse OvercommitTracker on the memory-constrained proxy instance.
// At scoreboard scale their sub-1% error is invisible, and they are exact at the small
// cardinalities the unit tests assert.
const kalshiRaceKeyTuple = "(measurement_node_id, capture_run_id, symbol, source_ts_ms, bbo_hash)"

// kalshiRecentRaceSymbols are the perps tickers shown in the recent-races grid, in display
// order. The scoreboard aggregations race every symbol the configured feeds carry; this list
// only bounds the live grid, which needs a stable, small set of columns.
var kalshiRecentRaceSymbols = []string{
	"KXBTCPERP", "KXETHPERP", "KXSOLPERP", "KXHYPEPERP",
	"KXXRPPERP", "KXDOGEPERP", "KXLTCPERP", "KXLINKPERP",
}

func kalshiRecentRaceSymbolFilter() string {
	quoted := make([]string, len(kalshiRecentRaceSymbols))
	for i, s := range kalshiRecentRaceSymbols {
		quoted[i] = "'" + s + "'"
	}
	return "AND symbol IN (" + strings.Join(quoted, ", ") + ")"
}

var kalshiSymbolRe = regexp.MustCompile(`^[A-Za-z0-9:_.-]{1,32}$`)

// sanitizeKalshiSymbol returns the symbol if safe to inline, else "".
func sanitizeKalshiSymbol(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "all") || !kalshiSymbolRe.MatchString(s) {
		return ""
	}
	return s
}

// KalshiCompetitor is DoubleZero's head-to-head record vs one competing feed.
type KalshiCompetitor struct {
	Feed      string  `json:"feed"`
	Label     string  `json:"label"`
	DZWinPct  float64 `json:"dz_win_pct"`
	LeadP50Ms float64 `json:"lead_p50_ms"`
	LeadP95Ms float64 `json:"lead_p95_ms"`
	Races     uint64  `json:"races"`
}

// KalshiNode is the per-vantage breakdown.
type KalshiNode struct {
	MeasurementNodeID string             `json:"measurement_node_id"`
	LocationCode      string             `json:"location_code"`
	DZWinSharePct     float64            `json:"dz_win_share_pct"`
	TotalRaces        uint64             `json:"total_races"`
	Competitors       []KalshiCompetitor `json:"competitors"`
}

// KalshiRace is one recent race for the live grid.
type KalshiRace struct {
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

// KalshiFeedLatency is one feed's venue-to-receive path latency at one vantage point
// (p50/p90/p99 in ms).
type KalshiFeedLatency struct {
	Feed         string  `json:"feed"`
	Label        string  `json:"label"`
	LocationCode string  `json:"location_code"`
	IsDZ         bool    `json:"is_dz"`
	P50Ms        float64 `json:"p50_ms"`
	P90Ms        float64 `json:"p90_ms"`
	P99Ms        float64 `json:"p99_ms"`
	Samples      uint64  `json:"samples"`
}

// KalshiPathLatency is the venue-timestamp-to-receive latency of each feed over a fixed 24h
// window — the page's headline comparison.
//
// This is the methodology-independent measurement, and it is the headline for a reason. The
// obvious alternative — the recv-to-recv margin behind the race win rate — is contaminated:
// the venue's public perps WebSocket delivers on a batched cadence of roughly a second, so the
// difference between the two feeds' arrival times largely measures THAT cadence rather than
// any path advantage. kalshi#40 measured ~500 ms margins with a p05-p95 spread of ~2 ms: two
// tight clusters, which is the signature of a fixed offset, not of a propagation race.
//
// Path latency avoids the problem entirely because it never pairs the two feeds: each side is
// measured against the venue's own timestamp for the same update, so the number does not
// depend on the race pairing at all. See the "How to read this dashboard" panel in
// infra/grafana/dashboards/kalshi-edge-advantage.json.
type KalshiPathLatency struct {
	Window      string              `json:"window"`
	Feeds       []KalshiFeedLatency `json:"feeds"`
	GeneratedAt time.Time           `json:"generated_at"`
}

// KalshiScoreboardResponse is the API response.
type KalshiScoreboardResponse struct {
	Window        string             `json:"window"`
	Symbol        string             `json:"symbol,omitempty"`
	GeneratedAt   time.Time          `json:"generated_at"`
	DZWinSharePct float64            `json:"dz_win_share_pct"`
	TotalRaces    uint64             `json:"total_races"`
	Competitors   []KalshiCompetitor `json:"competitors"`
	Nodes         []KalshiNode       `json:"nodes"`
	RecentRaces   []KalshiRace       `json:"recent_races"`
	// Prices is the latest BBO mid price per recent-race symbol (live, for the grid).
	Prices map[string]float64 `json:"prices,omitempty"`
	// PathLatency is the per-feed venue-to-receive latency (24h) — the headline comparison;
	// nil until the background refresher computes it (too heavy for the request path).
	PathLatency *KalshiPathLatency `json:"path_latency,omitempty"`
	// Unconfigured reports that no comparison feed is configured in this environment, as
	// distinct from a configured one that simply had no races in the window. The UI cannot
	// tell those apart from empty slices, and guessing turns a capture outage into "nothing
	// is configured yet" — a wrong diagnosis at the worst moment.
	Unconfigured bool `json:"unconfigured,omitempty"`
}

const (
	kalshiPathLatencyCacheKey = "kalshi_path_latency"
	kalshiScoreboardCacheBase = "kalshi_scoreboard"
)

// emptyKalshiScoreboard is the empty-but-valid response served when the scoreboard has nothing
// to compute — the proxied summary table is absent (e.g. local dev) or no feeds are
// configured. Returning this instead of an error keeps the page-cache refresher caching a
// clean payload rather than logging every cycle.
func emptyKalshiScoreboard(window string, unconfigured bool) *KalshiScoreboardResponse {
	return &KalshiScoreboardResponse{
		Window:       window,
		GeneratedAt:  time.Now().UTC(),
		Competitors:  []KalshiCompetitor{},
		Nodes:        []KalshiNode{},
		RecentRaces:  []KalshiRace{},
		Unconfigured: unconfigured,
	}
}

// FetchKalshiScoreboardData computes the aggregated scoreboard for a window and optional
// symbol. Empty symbol means all symbols.
func (a *API) FetchKalshiScoreboardData(ctx context.Context, window, symbol string) (*KalshiScoreboardResponse, error) {
	interval, ok := kalshiWindows[window]
	if !ok {
		window = "1h"
		interval = kalshiWindows[window]
	}
	symbol = sanitizeKalshiSymbol(symbol)

	// Guard: if the feeds summary table doesn't exist (e.g. local dev without the proxy),
	// return an empty-but-valid response so the page-cache refresher caches a clean empty
	// payload instead of logging an error every cycle. A failed probe is an error, not an
	// absent table — see kalshiTableExists.
	exists, err := a.kalshiFeedsTableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return emptyKalshiScoreboard(window, false), nil
	}

	entries, err := a.loadKalshiScoreboardEntries(ctx)
	if err != nil {
		return nil, err
	}
	if entries.empty() {
		// No competitor to race, but DoubleZero's own path latency is still measured and
		// still meaningful, so attach it rather than dropping the one number that survives.
		resp := emptyKalshiScoreboard(window, true)
		a.attachKalshiPathLatency(ctx, resp)
		return resp, nil
	}

	filter := entries.inClause()
	if symbol != "" {
		filter += fmt.Sprintf(" AND symbol = '%s'", symbol)
	}
	db := fmt.Sprintf("`%s`", a.FeedsDB)

	resp := &KalshiScoreboardResponse{
		Window:      window,
		Symbol:      symbol,
		GeneratedAt: time.Now().UTC(),
		Competitors: []KalshiCompetitor{},
		Nodes:       []KalshiNode{},
		RecentRaces: []KalshiRace{},
	}

	// Single scan grouped per (competitor, node) WITH CUBE: the (competitor, node) rows give the
	// per-vantage breakdown, the (competitor) rows the per-competitor totals, the (node) rows
	// the per-vantage totals and the grand-total row the headline — all from one scan instead of
	// one per level. One query (vs several) also refreshes atomically on the memory-constrained
	// proxy ClickHouse: it succeeds or retries as a unit.
	//
	// CUBE rather than ROLLUP because the node and headline totals must be counted, not summed
	// from the per-competitor cells. The summary relates a race winner to each loser and never
	// two losers to each other, so with more than one configured competitor the per-competitor
	// cells overlap asymmetrically: a race DoubleZero wins against N competitors leaves N rows
	// (one per competitor) while a race it loses leaves one (the competitor-vs-competitor pairs
	// are dropped by the DZ-vs-competitor filter). Summing the cells would therefore count a win
	// once per competitor and a loss once per race, overstating the win rate — the mirror of the
	// dual-lane undercount kalshiRaceKeyTuple documents. uniqCombinedIf over the race key at each
	// level counts each race once no matter how many competitors it involved.
	q := fmt.Sprintf(`
		SELECT competitor, measurement_node_id, any(location_code) AS location_code,
			uniqCombinedIf(rk, dz_won = 1) AS dz_wins,
			uniqCombinedIf(rk, dz_won = 0) AS dz_losses,
			-- ifNotFinite guards the empty-set case: when DZ won zero races in a
			-- (competitor, node) cell, quantileTDigestIf over no rows returns NaN, which fails
			-- JSON encoding of the whole response (and poisons the page cache). Coalesce to 0
			-- so a swept cell serializes cleanly.
			ifNotFinite(toFloat64(quantileTDigestIf(0.5)(lead_ms, dz_won = 1)), 0) AS lead_p50,
			ifNotFinite(toFloat64(quantileTDigestIf(0.95)(lead_ms, dz_won = 1)), 0) AS lead_p95
		FROM (
			SELECT measurement_node_id, location_code,
				%[1]s AS rk,
				if(%[5]s, loser_feed, feed) AS competitor,
				if(%[5]s, 1, 0) AS dz_won,
				-- Per-event lead, NOT a window quantile: the percentiles above are computed
				-- here rather than read from the table.
				lead_time_ms AS lead_ms
			FROM %[2]s.kalshi_bbo_feed_race_summary
			WHERE feed != loser_feed
			  AND (%[5]s != %[6]s) %[3]s
			  AND event_ts >= now() - INTERVAL %[4]s
		)
		GROUP BY competitor, measurement_node_id WITH CUBE`,
		kalshiRaceKeyTuple, db, filter, interval, kalshiIsDZSQL("feed"), kalshiIsDZSQL("loser_feed"))

	type stat struct {
		wins, losses     uint64
		leadP50, leadP95 float64
	}
	type nodeAgg struct {
		loc string
		// total is the node's own CUBE row (races counted once across competitors), not the sum
		// of byFeed.
		total  stat
		byFeed map[string]stat
	}
	// byFeed holds per-competitor totals (CUBE rows where node == ""); nodeMap holds the
	// per-node cells and each node's total; grand is the all-competitors, all-nodes row.
	byFeed := map[string]stat{}
	nodeMap := map[string]*nodeAgg{}
	var nodeOrder []string
	var grand stat
	var recent []KalshiRace
	var prices map[string]float64

	// The main CUBE scan, the recent-races scan, and the live-price lookup are three
	// independent ClickHouse reads; run them concurrently so their latencies overlap rather
	// than stack under the request timeout. gctx cancels the siblings if either hard-fails.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := a.envDB(gctx).Query(gctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()
		// A node's total row can arrive before any of its cells, so both branches create it.
		nodeFor := func(node, loc string) *nodeAgg {
			na, ok := nodeMap[node]
			if !ok {
				na = &nodeAgg{loc: loc, byFeed: map[string]stat{}}
				nodeMap[node] = na
				nodeOrder = append(nodeOrder, node)
			}
			return na
		}
		for rows.Next() {
			var competitor, node, loc string
			var s stat
			if err := rows.Scan(&competitor, &node, &loc, &s.wins, &s.losses, &s.leadP50, &s.leadP95); err != nil {
				return err
			}
			switch {
			case competitor == "" && node == "":
				grand = s
			case competitor == "":
				nodeFor(node, loc).total = s
			case node == "":
				byFeed[competitor] = s
			default:
				nodeFor(node, loc).byFeed[competitor] = s
			}
		}
		return rows.Err()
	})
	g.Go(func() error {
		r, err := a.fetchKalshiRecentRaces(gctx, entries, 10)
		if err != nil {
			return err
		}
		recent = r
		return nil
	})
	g.Go(func() error {
		// Best-effort: a live-price lookup failure must not fail the whole scoreboard.
		if p, err := a.fetchKalshiPrices(gctx); err == nil {
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
		resp.Competitors = append(resp.Competitors, KalshiCompetitor{
			Feed:      c.Feed,
			Label:     c.Label,
			DZWinPct:  winPct,
			LeadP50Ms: s.leadP50,
			LeadP95Ms: s.leadP95,
			Races:     races,
		})
	}

	for _, node := range nodeOrder {
		na := nodeMap[node]
		n := KalshiNode{
			MeasurementNodeID: node,
			LocationCode:      na.loc,
			Competitors:       []KalshiCompetitor{},
		}
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
			n.Competitors = append(n.Competitors, KalshiCompetitor{
				Feed: c.Feed, Label: c.Label, DZWinPct: winPct,
				LeadP50Ms: s.leadP50, LeadP95Ms: s.leadP95, Races: r,
			})
		}
		// From the node's own CUBE row: a race against several competitors counts once here,
		// though it appears in each competitor's cell above.
		n.TotalRaces = na.total.wins + na.total.losses
		if n.TotalRaces > 0 {
			n.DZWinSharePct = 100.0 * float64(na.total.wins) / float64(n.TotalRaces)
		}
		resp.Nodes = append(resp.Nodes, n)
	}

	// Headline from the grand-total CUBE row, for the same reason.
	resp.TotalRaces = grand.wins + grand.losses
	if resp.TotalRaces > 0 {
		resp.DZWinSharePct = 100.0 * float64(grand.wins) / float64(resp.TotalRaces)
	}

	if recent != nil {
		resp.RecentRaces = recent
	}
	if prices != nil {
		resp.Prices = prices
	}

	a.attachKalshiPathLatency(ctx, resp)

	return resp, nil
}

// attachKalshiPathLatency copies the background-refreshed 24h path latency onto a response.
// Best-effort: absent until the slow refresher has populated the cache.
func (a *API) attachKalshiPathLatency(ctx context.Context, resp *KalshiScoreboardResponse) {
	raw, err := a.readPageCache(ctx, kalshiPathLatencyCacheKey)
	if err != nil || len(raw) == 0 {
		return
	}
	var pl KalshiPathLatency
	if json.Unmarshal(raw, &pl) == nil {
		resp.PathLatency = &pl
	}
}

// fetchKalshiRecentRaces returns the most recent races per symbol (one row per race, winner +
// closest competitor + lead) for the live grid.
func (a *API) fetchKalshiRecentRaces(ctx context.Context, entries kalshiEntries, perSymbol int) ([]KalshiRace, error) {
	if perSymbol <= 0 || perSymbol > 50 {
		perSymbol = 10
	}
	// 15 min so the covered symbols fill a full column despite the MV's refresh lag; LIMIT n
	// BY symbol then keeps only the newest n per symbol.
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT
			max(event_ts) AS max_event_ts,
			symbol,
			location_code,
			feed AS winner_feed,
			%[5]s AS is_dz,
			argMin(loser_feed, lead_time_ms) AS runner_up_feed,
			min(lead_time_ms) AS lead_ms
		FROM %[1]s.kalshi_bbo_feed_race_summary
		-- Only DoubleZero-vs-competitor matchups (exactly one side is a DZ feed) — a DZ-vs-DZ
		-- pairing would otherwise flood the grid with lane-against-lane comparisons, which are
		-- a transport question, not a race against the venue's public feed.
		WHERE feed != loser_feed
		  AND (%[5]s != %[6]s) %[2]s %[3]s
		  AND event_ts >= now() - INTERVAL 15 MINUTE
		GROUP BY capture_run_id, measurement_node_id, symbol, source_ts_ms, bbo_hash, location_code, feed
		ORDER BY max_event_ts DESC
		LIMIT %[4]d BY symbol`,
		db, kalshiRecentRaceSymbolFilter(), entries.inClause(), perSymbol,
		kalshiIsDZSQL("feed"), kalshiIsDZSQL("loser_feed"))
	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []KalshiRace{}
	for rows.Next() {
		var r KalshiRace
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

// kalshiScoreboardCacheKey returns the cache key for a cacheable request shape, or "". A symbol
// filter is a non-default view and is never cached. Each supported window has its own cached
// key: the 1h view is refreshed by the page-cache worker; the heavier 24h/7d views are
// refreshed on a slow background cadence (StartKalshiBackgroundRefresher) so they are served
// from cache instead of running a multi-day scan on the request path.
//
// Cacheability is decided on the SANITIZED symbol, the same value the query is built from, not
// on the raw param: `symbol=all` (and a symbol of whitespace, or one the pattern rejects) is
// the all-symbols view the cache holds, so testing the raw param would push exactly that view
// onto the request path — a 7-day scan the background refresher exists to keep off it.
func kalshiScoreboardCacheKey(r *http.Request) string {
	if sanitizeKalshiSymbol(r.URL.Query().Get("symbol")) != "" {
		return ""
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window == "" {
		window = "1h"
	}
	if _, ok := kalshiWindows[window]; !ok {
		return ""
	}
	return kalshiScoreboardWindowKey(window)
}

// kalshiScoreboardWindowKey is the page-cache key for a given window. The 1h key is populated
// by the page-cache worker; 24h/7d get suffixed keys populated by the background refresher.
func kalshiScoreboardWindowKey(window string) string {
	if window == "1h" {
		return kalshiScoreboardCacheBase
	}
	return kalshiScoreboardCacheBase + ":" + window
}

// kalshiTableExists reports whether a proxied table is queryable.
//
// A probe failure is returned, never folded into "absent". The absent case degrades to an
// empty-but-valid payload with a nil error, which the background refresher and the page-cache
// worker then write over the last-good entry — so swallowing a connection blip here would
// blank the 24h/7d and L2 views for a full refresh interval, with nothing logged and the page
// reporting "no data" rather than an error. Propagating the error leaves the previous entry
// in place instead.
func (a *API) kalshiTableExists(ctx context.Context, table string) (bool, error) {
	var n uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.%s", a.FeedsDB, table)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return false, err
	}
	return n == 1, nil
}

// kalshiFeedsTableExists reports whether the proxied summary table is queryable.
func (a *API) kalshiFeedsTableExists(ctx context.Context) (bool, error) {
	return a.kalshiTableExists(ctx, "kalshi_bbo_feed_race_summary")
}

// kalshiObservationsTableExists reports whether the proxied observations table is queryable.
func (a *API) kalshiObservationsTableExists(ctx context.Context) (bool, error) {
	return a.kalshiTableExists(ctx, "kalshi_bbo_observations")
}

// fetchKalshiPrices returns the latest BBO mid price per recent-race symbol — a cheap
// latest-observation lookup used to show a live price next to each symbol's races.
// Price = (bid_px_raw + ask_px_raw)/2 * 10^price_exp.
func (a *API) fetchKalshiPrices(ctx context.Context) (map[string]float64, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT symbol, argMax((bid_px_raw + ask_px_raw) / 2 * pow(10, price_exp), recv_ts_ns) AS price
		FROM %s.kalshi_bbo_observations
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(1))) %s
		GROUP BY symbol`, db, kalshiRecentRaceSymbolFilter())
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

// kalshiEdgeWSArmFilter restricts DoubleZero's side of the latency comparison to the WebSocket
// arm of the edge publisher.
//
// This is load-bearing, not a detail: the two prod perps arms egress to the same multicast
// group and share a `source` and `source_id`, differing only by channel_id, and their
// source_ts_ms values come from DIFFERENT clocks — WS carries the venue's orderbook-delta
// timestamp, FIX the header-52 SendingTime. Only the WS arm's clock is the same quantity the
// public feed stamps, which is the entire reason this comparison is methodology-independent; a
// latency averaged across both arms silently mixes two clocks and means nothing.
//
// The WS host moved from channel_id 2 to 101 on 2026-08-09, when the perps fleet adopted
// `publisher index = channel_id / 100, instrument set = channel_id % 100`. Rows written before
// that carry 2, so a window spanning the cut reads both values for one host and the predicate
// has to be IN (2, 101) — exactly what the capture's
// 20260809000001_xtransport_race_mv_ws_channel_101.sql migration prescribes. FIX is unchanged
// at 1. The assignment is operator-set publisher config (Ansible host_vars), not
// schema-enforced, so a further renumbering must be followed here.
// The startsWith(source,'tob_') term is NOT redundant with source_id: production carries
// source_id = 3 on BOTH the top-of-book and the market-by-price perps lanes, so source_id
// alone selects two feeds, not one. The prefix is what actually discriminates them.
const kalshiEdgeWSArmFilter = "startsWith(source, 'tob_') AND source_id = 3 AND channel_id IN (2, 101)"

// FetchKalshiPathLatency computes each feed's venue-to-receive latency (p50/p90/p99 in ms) over
// the last 24h from the raw observations table: for one venue update, how long until this feed
// delivered it. See KalshiPathLatency for why this, not the race margin, is the headline.
//
// Only feeds whose source_ts_ms is known to carry the venue's own timestamp are included —
// DoubleZero's WS arm (see kalshiEdgeWSArmFilter) and the configured comparison feeds. The FIX
// arm and the market-by-price lanes are deliberately absent: they are DoubleZero's feeds and
// they count in the race, but their source-timestamp provenance is not the same quantity, so
// including them would reintroduce exactly the clock-mixing this metric exists to avoid.
//
// This is a heavy full-day scan over the proxied table — it must run on a slow background
// cadence, never in the request path or the 60s page-cache loop.
func (a *API) FetchKalshiPathLatency(ctx context.Context) (*KalshiPathLatency, error) {
	// Same degrade-to-empty guard as the scoreboard and L2 views: without it an environment
	// that does not proxy the observations table logs a refresh failure every 10 minutes
	// forever, which is the noise those guards exist to prevent. A failed probe still
	// propagates, so a blip cannot blank the cached hero — see kalshiTableExists.
	exists, err := a.kalshiObservationsTableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return &KalshiPathLatency{Window: "24h", Feeds: []KalshiFeedLatency{}, GeneratedAt: time.Now().UTC()}, nil
	}
	entries, err := a.loadKalshiScoreboardEntries(ctx)
	if err != nil {
		return nil, err
	}
	db := fmt.Sprintf("`%s`", a.FeedsDB)

	// Competitor sides come from the allow-list; with none configured there is nothing to
	// compare DoubleZero against, but its own latency is still worth reporting.
	competitorPredicate := "0"
	if !entries.empty() {
		quoted := make([]string, 0, len(entries.ordered))
		for _, en := range entries.ordered {
			quoted = append(quoted, "'"+en.Feed+"'")
		}
		competitorPredicate = "source IN (" + strings.Join(quoted, ", ") + ")"
	}

	// Grouped by location_code, not just by feed. Without the metro in the key the inner
	// min(recv_ts_ns) collapses all vantage points into one row per update and silently
	// measures FLEET-WIDE FIRST ARRIVAL — "whichever recorder saw it first" — which is a
	// different quantity that flatters whichever feed happens to have the best-connected
	// vantage. Latency is a property of a path, and a path ends somewhere.
	//
	// The inner GROUP BY still collapses repeated observations of one venue update at one
	// vantage to its earliest arrival, so a feed that redelivers the same update cannot pad
	// its own distribution. This mirrors the `d` CTE in the operational dashboard's latency
	// table.
	q := fmt.Sprintf(`
		WITH d AS (
			SELECT source, location_code, symbol, source_ts_ms, min(recv_ts_ns) AS r
			FROM %[1]s.kalshi_bbo_observations
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(24)))
			  AND source_ts_ms > 0
			  AND ((%[2]s) OR (%[3]s))
			GROUP BY source, location_code, symbol, source_ts_ms
		)
		SELECT source, location_code,
			ifNotFinite(toFloat64(quantileTDigest(0.5)((toInt64(r) - toInt64(source_ts_ms) * 1000000) / 1e6)), 0),
			ifNotFinite(toFloat64(quantileTDigest(0.9)((toInt64(r) - toInt64(source_ts_ms) * 1000000) / 1e6)), 0),
			ifNotFinite(toFloat64(quantileTDigest(0.99)((toInt64(r) - toInt64(source_ts_ms) * 1000000) / 1e6)), 0),
			count() AS samples
		FROM d
		GROUP BY source, location_code
		SETTINGS max_bytes_before_external_group_by = 2000000000`,
		db, kalshiEdgeWSArmFilter, competitorPredicate)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := &KalshiPathLatency{Window: "24h", Feeds: []KalshiFeedLatency{}, GeneratedAt: time.Now().UTC()}
	for rows.Next() {
		var f KalshiFeedLatency
		if err := rows.Scan(&f.Feed, &f.LocationCode, &f.P50Ms, &f.P90Ms, &f.P99Ms, &f.Samples); err != nil {
			return nil, err
		}
		f.IsDZ = isKalshiDZFeed(f.Feed)
		if f.IsDZ {
			f.Label = "DoubleZero"
		} else {
			f.Label = entries.label(f.Feed)
		}
		out.Feeds = append(out.Feeds, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Grouped by vantage, DoubleZero first within each so the two paths sit adjacent and are
	// read against each other rather than across metros — the comparison is only meaningful
	// between feeds measured at the same place.
	order := map[string]int{}
	for i, en := range entries.ordered {
		order[en.Feed] = i + 1
	}
	sort.Slice(out.Feeds, func(i, j int) bool {
		a, b := out.Feeds[i], out.Feeds[j]
		if a.LocationCode != b.LocationCode {
			return a.LocationCode < b.LocationCode
		}
		if order[a.Feed] != order[b.Feed] {
			return order[a.Feed] < order[b.Feed]
		}
		return a.Feed < b.Feed
	})
	return out, nil
}

// StartKalshiBackgroundRefresher periodically computes the Kalshi views that are too heavy for
// the page-cache worker and writes them to the page cache (Postgres) so all replicas share
// them and the request path never runs a multi-day scan:
//   - the per-feed path latency,
//   - the 24h and 7d scoreboards (the 1h scoreboard stays on the ordinary page-cache worker),
//   - the sports L2 coverage view.
//
// Each computation gets its own timeout so a slow one can't starve the others; the path
// latency is refreshed first so the 24h/7d scoreboards pick up its freshly-cached value.
func (a *API) StartKalshiBackgroundRefresher(ctx context.Context) {
	const interval = 10 * time.Minute
	const runTimeout = 3 * time.Minute
	refreshLatency := func() {
		rctx, cancel := context.WithTimeout(ctx, runTimeout)
		defer cancel()
		val, err := a.FetchKalshiPathLatency(rctx)
		if err != nil {
			slog.Warn("kalshi path latency refresh failed", "error", err)
			return
		}
		if err := a.WritePageCache(ctx, kalshiPathLatencyCacheKey, val); err != nil {
			slog.Warn("kalshi path latency cache write failed", "error", err)
		}
	}
	refreshScoreboard := func(window string) {
		rctx, cancel := context.WithTimeout(ctx, runTimeout)
		defer cancel()
		val, err := a.FetchKalshiScoreboardData(rctx, window, "")
		if err != nil {
			slog.Warn("kalshi scoreboard refresh failed", "window", window, "error", err)
			return
		}
		if err := a.WritePageCache(ctx, kalshiScoreboardWindowKey(window), val); err != nil {
			slog.Warn("kalshi scoreboard cache write failed", "window", window, "error", err)
		}
	}
	refreshL2 := func() {
		rctx, cancel := context.WithTimeout(ctx, runTimeout)
		defer cancel()
		val, err := a.FetchKalshiL2Coverage(rctx)
		if err != nil {
			slog.Warn("kalshi l2 coverage refresh failed", "error", err)
			return
		}
		if err := a.WritePageCache(ctx, kalshiL2CoverageCacheKey, val); err != nil {
			slog.Warn("kalshi l2 coverage cache write failed", "error", err)
		}
	}
	// The top-of-book sequence leg of /dz/edge/multicast. Same cadence and same window as the
	// L2 coverage one so the two halves of that column describe the same span, and last in the
	// cycle because it is the only one no page falls back to a live query for.
	refreshTOBSequence := func() {
		rctx, cancel := context.WithTimeout(ctx, runTimeout)
		defer cancel()
		val, err := a.FetchEdgeMulticastTOBSequence(rctx)
		if err != nil {
			slog.Warn("edge multicast tob sequence refresh failed", "error", err)
			return
		}
		if err := a.WritePageCache(ctx, edgeMulticastTOBSequenceCacheKey, val); err != nil {
			slog.Warn("edge multicast tob sequence cache write failed", "error", err)
		}
	}
	refresh := func() {
		refreshLatency()
		refreshScoreboard("24h")
		refreshScoreboard("7d")
		refreshL2()
		refreshTOBSequence()
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

// GetKalshiScoreboard serves the Kalshi BBO scoreboard.
func (a *API) GetKalshiScoreboard(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if key := kalshiScoreboardCacheKey(r); key != "" {
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
	if _, ok := kalshiWindows[window]; !ok {
		window = "1h"
	}
	symbol := r.URL.Query().Get("symbol")

	// FetchKalshiScoreboardData already degrades to an empty-but-valid response when the proxy
	// table is absent or no feeds are configured, so no separate existence check is needed
	// here — that avoids a redundant EXISTS TABLE round-trip per uncached request.
	resp, err := a.FetchKalshiScoreboardData(ctx, window, symbol)
	if err != nil {
		logError("KalshiScoreboard error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
