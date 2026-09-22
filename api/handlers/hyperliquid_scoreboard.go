package handlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// The Hyperliquid scoreboard. Sibling of the internal one
// (hyperliquid_internal_scoreboard.go), and deliberately a different measurement —
// the two will not agree cell for cell and are not meant to:
//
//   - It reads hyperliquid_bbo_observations, not hyperliquid_bbo_feed_race_summary.
//     The summary is winner-centric: it writes a cross-camp row only for the update's
//     overall winner, so a feed appears as winner only when it WAS the fastest and as
//     loser only when a competitor beat everyone. Measured over one hour at Tokyo, the
//     Tokyo mirror read 96.71% on 79,684 rows there against 66.52% on 132,281 rows here.
//     Per-feed rates from the summary are conditional on that feed having won and are
//     not comparable across feeds; observations carry one row per feed per update, so
//     every pairing exists.
//   - DoubleZero's arrival is the EARLIEST of its publishers on that book state, which
//     is what a subscriber actually receives, rather than one publisher at a time.
//   - Margins are signed. A race DoubleZero loses carries a negative value into the
//     percentiles instead of being dropped, so p50/p95/p99 describe every race rather
//     than only the wins.
//   - Competitor identities never reach the payload. Competitors are numbered by median
//     margin ascending; only the venue's own free endpoint is named, because it is not
//     a competitor's product.
//
// A race is one (recording site, symbol, bbo_hash): the same book state, scored on
// whenever each feed first delivered it. source_ts_ms is deliberately NOT in the key —
// a book state that recurs is one race, not several. Measured over 48h at Tokyo that
// is 1.084 occurrences per state, and the two grains agree to ~0.2pt.

// hyperliquidDZSourcePrefix marks DoubleZero's own top-of-book publishers in the
// observations table, matching the `tob_` convention used across the feeds schema.
const hyperliquidDZSourcePrefix = "tob_"

// hyperliquidRetiredPublishers are DoubleZero publishers excluded from every figure on
// this page. tob_aws_tyo_mirror1 was a Tokyo/NYC mirror that stopped publishing at
// 2026-09-21 20:00 UTC. It is not merely dropped as a row: it was the fastest DoubleZero
// publisher on 63.6% of the 4.89M book states it appeared in, so leaving it in would
// credit the service with a publisher it no longer runs. Excluding it also drops the
// ~1.0% of states no other DoubleZero publisher observed, which is the honest treatment —
// there is no arrival to report there.
var hyperliquidRetiredPublishers = []string{"tob_aws_tyo_mirror1"}

// hyperliquidVenueFeed is the venue's own free endpoint. It is shown under its own name
// because it is not a rival product, and it is listed first.
const hyperliquidVenueFeed = "hyperliquid_public_bbo"

// hyperliquidScoreboardSites maps the recording-site codes in the feeds schema to the
// labels the page shows, in display order.
var hyperliquidScoreboardSites = []struct{ Code, Label, Display string }{
	{"tyo", "TYO", "Tokyo"},
	{"chi", "CHI", "Chicago"},
	{"nyc", "NYC", "New York"},
}

type hyperliquidMarketCategory struct {
	Name    string
	Symbols []string
}

type hyperliquidMarketGroup struct {
	Name string
	Cats []hyperliquidMarketCategory
}

// hyperliquidMarketGroups is the contract-type breakdown. It is deliberately coarser than
// the symbol list: the page names categories, never individual contracts. Commodities and
// energy are carried and raced, and are counted in the headline, but have no category row.
var hyperliquidMarketGroups = []hyperliquidMarketGroup{
	{Name: "Native Perpetuals", Cats: []hyperliquidMarketCategory{
		{Name: "Major crypto", Symbols: []string{"BTC", "ETH", "SOL"}},
		{Name: "Platform & high-beta", Symbols: []string{"HYPE", "ZEC"}},
	}},
	{Name: "HIP-3 Builder DEX Perpetuals", Cats: []hyperliquidMarketCategory{
		{Name: "Equity index", Symbols: []string{"xyz:SP500", "xyz:XYZ100"}},
		{Name: "Single-name equity", Symbols: []string{"xyz:MU", "xyz:SKHX", "xyz:SPCX", "xyz:NVDA"}},
	}},
}

// hyperliquidScoreboardWindowHours is the measured window. The observations table is
// row-per-feed-per-update and this scans three sites of it, so the page is served from
// the page cache and never runs the query on the request path.
const hyperliquidScoreboardWindowHours = 24

// HyperliquidScoreboardCacheKey is the page-cache key for the scoreboard.
const HyperliquidScoreboardCacheKey = "hyperliquid_scoreboard"

// HyperliquidScoreboardStat is one measured cell: how often DoubleZero arrived first and
// how far ahead it was. Margins are signed, so a cell whose win rate is below 50% will
// report a negative median.
type HyperliquidScoreboardStat struct {
	WinPct float64 `json:"win_pct"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
}

// HyperliquidScoreboardSite is one recording site's numbers.
type HyperliquidScoreboardSite struct {
	Code  string `json:"code"`
	Label string `json:"label"`
	HyperliquidScoreboardStat
}

// HyperliquidScoreboardFeed is one feed DoubleZero is raced against. Label is either
// "Public API" or "Competitor N" — the underlying feed name is never serialised.
type HyperliquidScoreboardFeed struct {
	Label string `json:"label"`
	Venue bool   `json:"venue"`
	HyperliquidScoreboardStat
	Sites []HyperliquidScoreboardSite `json:"sites"`
}

// HyperliquidScoreboardCategory is one contract type within a market.
type HyperliquidScoreboardCategory struct {
	Name string `json:"name"`
	HyperliquidScoreboardStat
}

// HyperliquidScoreboardMarket is a market and the contract types broken out under it.
type HyperliquidScoreboardMarket struct {
	Name    string                          `json:"name"`
	Carried int                             `json:"carried"`
	Cats    []HyperliquidScoreboardCategory `json:"cats"`
}

// HyperliquidScoreboardResponse is the API response.
type HyperliquidScoreboardResponse struct {
	WindowLabel string                        `json:"window_label"`
	Races       uint64                        `json:"races"`
	Instruments int                           `json:"instruments"`
	SiteCount   int                           `json:"site_count"`
	FeedCount   int                           `json:"feed_count"`
	All         HyperliquidScoreboardStat     `json:"all"`
	Sites       []HyperliquidScoreboardSite   `json:"sites"`
	Feeds       []HyperliquidScoreboardFeed   `json:"feeds"`
	Markets     []HyperliquidScoreboardMarket `json:"markets"`
	AsOf        time.Time                     `json:"as_of"`
}

// hyperliquidDZArrivalExpr is the predicate selecting DoubleZero's own publishers,
// minus the retired ones.
func hyperliquidDZArrivalExpr() string {
	pred := fmt.Sprintf("startsWith(source, '%s')", hyperliquidDZSourcePrefix)
	if len(hyperliquidRetiredPublishers) > 0 {
		quoted := make([]string, len(hyperliquidRetiredPublishers))
		for i, s := range hyperliquidRetiredPublishers {
			quoted[i] = "'" + s + "'"
		}
		pred += fmt.Sprintf(" AND source NOT IN (%s)", strings.Join(quoted, ", "))
	}
	return pred
}

// hyperliquidScoreboardSymbols is the symbol set the scoreboard measures over — the same
// liquid set the internal scoreboard uses, so the two describe the same markets.
func hyperliquidScoreboardSymbols() string {
	quoted := make([]string, len(hyperliquidLiquidSymbols))
	for i, s := range hyperliquidLiquidSymbols {
		quoted[i] = "'" + s + "'"
	}
	return strings.Join(quoted, ", ")
}

// hyperliquidCompetitorArrivals builds the per-competitor arrival/​presence projections and
// the ARRAY JOIN tuple list that unpivots them into one row per (state, competitor).
func hyperliquidCompetitorArrivals() (projection, arrayJoin string) {
	proj := make([]string, 0, len(hyperliquidCompetitors))
	tuples := make([]string, 0, len(hyperliquidCompetitors))
	for i, c := range hyperliquidCompetitors {
		proj = append(proj, fmt.Sprintf(
			"minIf(recv_ts_ns, source = '%[1]s') AS t%[2]d, countIf(source = '%[1]s') AS n%[2]d",
			c.Feed, i))
		tuples = append(tuples, fmt.Sprintf("('%s', n%d, t%d)", c.Feed, i, i))
	}
	return strings.Join(proj, ",\n            "), strings.Join(tuples, ",\n                        ")
}

// FetchHyperliquidScoreboardData computes the whole scoreboard. Three scans: the
// site×competitor matrix, the contract-type breakdown, and the carried-instrument counts.
func (a *API) FetchHyperliquidScoreboardData(ctx context.Context) (*HyperliquidScoreboardResponse, error) {
	resp := &HyperliquidScoreboardResponse{
		WindowLabel: fmt.Sprintf("last %d hours", hyperliquidScoreboardWindowHours),
		SiteCount:   len(hyperliquidScoreboardSites),
		FeedCount:   len(hyperliquidCompetitors),
		Sites:       []HyperliquidScoreboardSite{},
		Feeds:       []HyperliquidScoreboardFeed{},
		Markets:     []HyperliquidScoreboardMarket{},
		AsOf:        time.Now().UTC(),
	}
	// Degrade to an empty-but-valid payload where the proxy table is absent (local dev),
	// the same way the internal scoreboard does.
	if !a.hyperliquidObservationsTableExists(ctx) {
		return resp, nil
	}

	matrix, err := a.fetchHyperliquidScoreboardMatrix(ctx)
	if err != nil {
		return nil, err
	}
	markets, err := a.fetchHyperliquidScoreboardMarkets(ctx)
	if err != nil {
		return nil, err
	}
	carried, err := a.fetchHyperliquidCarriedInstruments(ctx)
	if err != nil {
		return nil, err
	}

	// Grand total.
	if total, ok := matrix[hyperliquidMatrixKey{}]; ok {
		resp.All = total.stat
		resp.Races = total.races
	}
	// Per site.
	for _, s := range hyperliquidScoreboardSites {
		cell := matrix[hyperliquidMatrixKey{loc: s.Code}]
		resp.Sites = append(resp.Sites, HyperliquidScoreboardSite{
			Code: s.Label, Label: s.Display, HyperliquidScoreboardStat: cell.stat,
		})
	}

	// Per feed. Competitors are numbered by median margin ascending; the venue endpoint
	// keeps its name and leads the list. The number is positional only and is recomputed
	// every refresh, so it is not a stable identifier for anyone reading the page.
	type feedRow struct {
		feed  string
		stat  HyperliquidScoreboardStat
		sites []HyperliquidScoreboardSite
	}
	rows := make([]feedRow, 0, len(hyperliquidCompetitors))
	for _, c := range hyperliquidCompetitors {
		fr := feedRow{feed: c.Feed, stat: matrix[hyperliquidMatrixKey{feed: c.Feed}].stat}
		for _, s := range hyperliquidScoreboardSites {
			cell := matrix[hyperliquidMatrixKey{loc: s.Code, feed: c.Feed}]
			fr.sites = append(fr.sites, HyperliquidScoreboardSite{
				Code: s.Label, Label: s.Display, HyperliquidScoreboardStat: cell.stat,
			})
		}
		rows = append(rows, fr)
	}
	ranked := make([]feedRow, 0, len(rows))
	for _, r := range rows {
		if r.feed != hyperliquidVenueFeed {
			ranked = append(ranked, r)
		}
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].stat.P50Ms < ranked[j].stat.P50Ms })
	for _, r := range rows {
		if r.feed == hyperliquidVenueFeed {
			resp.Feeds = append(resp.Feeds, HyperliquidScoreboardFeed{
				Label: "Public API", Venue: true,
				HyperliquidScoreboardStat: r.stat, Sites: r.sites,
			})
		}
	}
	for i, r := range ranked {
		resp.Feeds = append(resp.Feeds, HyperliquidScoreboardFeed{
			Label:                     fmt.Sprintf("Competitor %d", i+1),
			HyperliquidScoreboardStat: r.stat, Sites: r.sites,
		})
	}

	// Per market.
	for _, g := range hyperliquidMarketGroups {
		m := HyperliquidScoreboardMarket{Name: g.Name, Carried: carried[g.Name]}
		for _, c := range g.Cats {
			m.Cats = append(m.Cats, HyperliquidScoreboardCategory{
				Name: c.Name, HyperliquidScoreboardStat: markets[c.Name],
			})
		}
		resp.Markets = append(resp.Markets, m)
	}
	for _, n := range carried {
		resp.Instruments += n
	}
	return resp, nil
}

type hyperliquidMatrixKey struct{ loc, feed string }

type hyperliquidMatrixCell struct {
	races uint64
	stat  HyperliquidScoreboardStat
}

// fetchHyperliquidScoreboardMatrix returns every cell of the site × feed matrix plus the
// per-site and grand totals, from one scan. WITH CUBE gives all four groupings; the
// location and feed sentinels are safe because neither column is ever empty in the rows
// that reach it.
func (a *API) fetchHyperliquidScoreboardMatrix(ctx context.Context) (map[hyperliquidMatrixKey]hyperliquidMatrixCell, error) {
	proj, tuples := hyperliquidCompetitorArrivals()
	q := fmt.Sprintf(`
		WITH agg AS (
		    SELECT
		        location_code, symbol, bbo_hash,
		        minIf(recv_ts_ns, %[2]s) AS dz,
		        countIf(%[2]s)           AS n_dz,
		        %[3]s
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(%[5]d)))
		      AND symbol IN (%[4]s)
		    GROUP BY location_code, symbol, bbo_hash
		),
		sv AS (
		    SELECT location_code, c.1 AS feed,
		           (toInt64(c.3) - toInt64(dz)) / 1e6 AS signed_ms
		    FROM agg
		    ARRAY JOIN [%[6]s] AS c
		    WHERE c.2 > 0 AND n_dz > 0
		)
		SELECT
		    location_code, feed,
		    count() AS races,
		    100 * countIf(signed_ms > 0) / greatest(count(), 1)   AS win_pct,
		    toFloat64(quantileTDigest(0.50)(signed_ms))           AS p50,
		    toFloat64(quantileTDigest(0.95)(signed_ms))           AS p95,
		    toFloat64(quantileTDigest(0.99)(signed_ms))           AS p99
		FROM sv
		GROUP BY location_code, feed WITH CUBE
		SETTINGS max_bytes_before_external_group_by = 8000000000`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), proj,
		hyperliquidScoreboardSymbols(), hyperliquidScoreboardWindowHours, tuples)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid scoreboard matrix: %w", err)
	}
	defer rows.Close()

	out := map[hyperliquidMatrixKey]hyperliquidMatrixCell{}
	for rows.Next() {
		var loc, feed string
		var races uint64
		var win, p50, p95, p99 float64
		if err := rows.Scan(&loc, &feed, &races, &win, &p50, &p95, &p99); err != nil {
			return nil, err
		}
		out[hyperliquidMatrixKey{loc: loc, feed: feed}] = hyperliquidMatrixCell{
			races: races,
			stat:  HyperliquidScoreboardStat{WinPct: win, P50Ms: p50, P95Ms: p95, P99Ms: p99},
		}
	}
	return out, rows.Err()
}

// fetchHyperliquidScoreboardMarkets returns one stat per contract type, pooled across sites.
func (a *API) fetchHyperliquidScoreboardMarkets(ctx context.Context) (map[string]HyperliquidScoreboardStat, error) {
	proj, tuples := hyperliquidCompetitorArrivals()

	var cases []string
	var symbols []string
	for _, g := range hyperliquidMarketGroups {
		for _, c := range g.Cats {
			quoted := make([]string, len(c.Symbols))
			for i, s := range c.Symbols {
				quoted[i] = "'" + s + "'"
			}
			cases = append(cases, fmt.Sprintf("symbol IN (%s), '%s'", strings.Join(quoted, ", "), c.Name))
			symbols = append(symbols, quoted...)
		}
	}
	q := fmt.Sprintf(`
		WITH agg AS (
		    SELECT
		        location_code, symbol, bbo_hash,
		        minIf(recv_ts_ns, %[2]s) AS dz,
		        countIf(%[2]s)           AS n_dz,
		        %[3]s
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(%[5]d)))
		      AND symbol IN (%[4]s)
		    GROUP BY location_code, symbol, bbo_hash
		),
		sv AS (
		    SELECT multiIf(%[7]s, '') AS cat,
		           (toInt64(c.3) - toInt64(dz)) / 1e6 AS signed_ms
		    FROM agg
		    ARRAY JOIN [%[6]s] AS c
		    WHERE c.2 > 0 AND n_dz > 0
		)
		SELECT
		    cat,
		    100 * countIf(signed_ms > 0) / greatest(count(), 1) AS win_pct,
		    toFloat64(quantileTDigest(0.50)(signed_ms))         AS p50,
		    toFloat64(quantileTDigest(0.95)(signed_ms))         AS p95,
		    toFloat64(quantileTDigest(0.99)(signed_ms))         AS p99
		FROM sv
		WHERE cat != ''
		GROUP BY cat
		SETTINGS max_bytes_before_external_group_by = 8000000000`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), proj,
		strings.Join(symbols, ", "), hyperliquidScoreboardWindowHours, tuples,
		strings.Join(cases, ", "))

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid scoreboard markets: %w", err)
	}
	defer rows.Close()

	out := map[string]HyperliquidScoreboardStat{}
	for rows.Next() {
		var cat string
		var win, p50, p95, p99 float64
		if err := rows.Scan(&cat, &win, &p50, &p95, &p99); err != nil {
			return nil, err
		}
		out[cat] = HyperliquidScoreboardStat{WinPct: win, P50Ms: p50, P95Ms: p95, P99Ms: p99}
	}
	return out, rows.Err()
}

// fetchHyperliquidCarriedInstruments counts the distinct symbols the live DoubleZero fleet
// publishes, split by market. This counts everything carried, not only the liquid set the
// races are measured over — it answers "what does the feed cover", not "what was raced".
func (a *API) fetchHyperliquidCarriedInstruments(ctx context.Context) (map[string]int, error) {
	q := fmt.Sprintf(`
		SELECT startsWith(symbol, 'xyz:') AS hip3, count() AS n
		FROM (
		    SELECT DISTINCT symbol
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(%[3]d)))
		      AND %[2]s
		)
		GROUP BY hip3`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), hyperliquidScoreboardWindowHours)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid carried instruments: %w", err)
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var hip3 uint8
		var n uint64
		if err := rows.Scan(&hip3, &n); err != nil {
			return nil, err
		}
		if hip3 == 1 {
			out["HIP-3 Builder DEX Perpetuals"] = int(n)
		} else {
			out["Native Perpetuals"] = int(n)
		}
	}
	return out, rows.Err()
}

// hyperliquidObservationsTableExists reports whether the proxied observations table is
// queryable, so an environment without the proxy renders an empty page instead of an error.
func (a *API) hyperliquidObservationsTableExists(ctx context.Context) bool {
	var n uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.hyperliquid_bbo_observations", a.FeedsDB)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

// GetHyperliquidScoreboard serves the Hyperliquid scoreboard. The query scans three sites
// of a row-per-feed-per-update table, so a cache miss falls through to the live query with
// a generous deadline rather than being served stale.
func (a *API) GetHyperliquidScoreboard(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), HyperliquidScoreboardCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	resp, err := a.FetchHyperliquidScoreboardData(ctx)
	if err != nil {
		logError("HyperliquidScoreboard error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
