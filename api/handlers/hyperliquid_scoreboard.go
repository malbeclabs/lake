package handlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// The public Hyperliquid scoreboard. Unlike its sibling in
// hyperliquid_internal_scoreboard.go it reads hyperliquid_bbo_observations rather than
// hyperliquid_bbo_feed_race_summary, so the two do not agree cell for cell: the summary
// writes a cross-camp row only for an update's overall winner, which makes its per-feed
// rates conditional on that feed having won. DoubleZero's arrival is the earliest of its
// publishers, and margins are signed so a lost race carries a negative value into the
// percentiles.
//
// A race is one venue emission: (site, symbol, source_ts_ms, bbo_hash). Keyed on the book
// state alone a feed that missed an occurrence borrows a later arrival, making the margin the
// gap between emissions rather than a latency.

const hyperliquidDZSourcePrefix = "tob_"

// Excluded as a PUBLISHER, not as a row: it was the fastest DoubleZero publisher on 63.6%
// of the states it appeared in, so DoubleZero's arrival has to fall back to the next
// publisher rather than keep the mirror's. tob_aws_tyo_mirror1 stopped publishing on
// 2026-09-21.
var hyperliquidRetiredPublishers = []string{"tob_aws_tyo_mirror1"}

// The venue's own free endpoint, shown under its own name because it is not a rival product.
const hyperliquidVenueFeed = "hyperliquid_public_bbo"

type hyperliquidSite struct {
	Code    string // location_code in the feeds schema, e.g. "tyo"
	Short   string // column header, e.g. "TYO"
	Display string // city name, e.g. "Tokyo"
}

// A label map, not the set of sites shown: that comes from the data, so a new vantage appears
// without a deploy here and an unnamed one renders under its uppercased code. Display names
// are the ledger's, from lake.dz_metros_current.
var hyperliquidNamedSites = []hyperliquidSite{
	{Code: "tyo", Short: "TYO", Display: "Tokyo"},
	{Code: "chi", Short: "CHI", Display: "Chicago"},
	{Code: "nyc", Short: "NYC", Display: "New York"},
	{Code: "was", Short: "WAS", Display: "Washington DC"},
	{Code: "cmh", Short: "CMH", Display: "Columbus"},
}

func hyperliquidSitesFrom(matrix map[hyperliquidMatrixKey]hyperliquidMatrixCell) []hyperliquidSite {
	present := map[string]bool{}
	for k := range matrix {
		// The per-site CUBE cells: an empty loc is a total, and the feed-keyed cells repeat
		// the same locations.
		if k.loc != "" && k.feed == "" {
			present[k.loc] = true
		}
	}
	out := make([]hyperliquidSite, 0, len(present))
	for _, s := range hyperliquidNamedSites {
		if present[s.Code] {
			out = append(out, s)
			delete(present, s.Code)
		}
	}
	rest := make([]string, 0, len(present))
	for code := range present {
		rest = append(rest, code)
	}
	sort.Strings(rest)
	for _, code := range rest {
		up := strings.ToUpper(code)
		out = append(out, hyperliquidSite{Code: code, Short: up, Display: up})
	}
	return out
}

type hyperliquidMarketCategory struct {
	Name    string
	Symbols []string
}

type hyperliquidMarketGroup struct {
	Name string
	Cats []hyperliquidMarketCategory
}

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

const hyperliquidScoreboardWindowHours = 24

// Deliberately not "hyperliquid_scoreboard", which the internal board used for its 1h view
// before this split. Cached bytes are served without unmarshalling, so reusing the name would
// hand this page the old board's shape until the first refresh overwrote it.
const HyperliquidScoreboardCacheKey = "hyperliquid_public_scoreboard"

// HyperliquidScoreboardStat is one measured cell. Margins are signed, so a cell whose win
// rate is below 50% reports a negative median.
type HyperliquidScoreboardStat struct {
	WinPct float64 `json:"win_pct"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
}

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

type HyperliquidScoreboardCategory struct {
	Name string `json:"name"`
	HyperliquidScoreboardStat
}

type HyperliquidScoreboardMarket struct {
	Name    string                          `json:"name"`
	Carried int                             `json:"carried"`
	Cats    []HyperliquidScoreboardCategory `json:"cats"`
}

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

func hyperliquidScoreboardSymbols() string {
	quoted := make([]string, len(hyperliquidLiquidSymbols))
	for i, s := range hyperliquidLiquidSymbols {
		quoted[i] = "'" + s + "'"
	}
	return strings.Join(quoted, ", ")
}

// hyperliquidCompetitorArrivals builds the per-competitor arrival/presence projections and
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

func (a *API) FetchHyperliquidScoreboardData(ctx context.Context) (*HyperliquidScoreboardResponse, error) {
	resp := &HyperliquidScoreboardResponse{
		WindowLabel: fmt.Sprintf("last %d hours", hyperliquidScoreboardWindowHours),
		FeedCount:   len(hyperliquidCompetitors),
		Sites:       []HyperliquidScoreboardSite{},
		Feeds:       []HyperliquidScoreboardFeed{},
		Markets:     []HyperliquidScoreboardMarket{},
		AsOf:        time.Now().UTC(),
	}
	// An environment without the proxy table renders an empty page rather than an error.
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

	if total, ok := matrix[hyperliquidMatrixKey{}]; ok {
		resp.All = total.stat
		resp.Races = total.races
	}
	sites := hyperliquidSitesFrom(matrix)
	resp.SiteCount = len(sites)
	for _, s := range sites {
		cell := matrix[hyperliquidMatrixKey{loc: s.Code}]
		resp.Sites = append(resp.Sites, HyperliquidScoreboardSite{
			Code: s.Short, Label: s.Display, HyperliquidScoreboardStat: cell.stat,
		})
	}

	type feedRow struct {
		feed  string
		stat  HyperliquidScoreboardStat
		sites []HyperliquidScoreboardSite
	}
	rows := make([]feedRow, 0, len(hyperliquidCompetitors))
	for _, c := range hyperliquidCompetitors {
		fr := feedRow{feed: c.Feed, stat: matrix[hyperliquidMatrixKey{feed: c.Feed}].stat}
		for _, s := range sites {
			cell := matrix[hyperliquidMatrixKey{loc: s.Code, feed: c.Feed}]
			fr.sites = append(fr.sites, HyperliquidScoreboardSite{
				Code: s.Short, Label: s.Display, HyperliquidScoreboardStat: cell.stat,
			})
		}
		rows = append(rows, fr)
	}
	// Competitor numbering is positional and recomputed every refresh, so it is not a
	// stable identifier for anyone reading the page.
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
// per-site and grand totals from one scan. WITH CUBE gives all four groupings, keyed by
// empty-string sentinels that are safe because neither column is empty in the rows
// reaching it.
func (a *API) fetchHyperliquidScoreboardMatrix(ctx context.Context) (map[hyperliquidMatrixKey]hyperliquidMatrixCell, error) {
	proj, tuples := hyperliquidCompetitorArrivals()
	q := fmt.Sprintf(`
		WITH agg AS (
		    SELECT
		        location_code, symbol, source_ts_ms, bbo_hash,
		        minIf(recv_ts_ns, %[2]s) AS dz,
		        countIf(%[2]s)           AS n_dz,
		        %[3]s
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(%[5]d)))
		      AND symbol IN (%[4]s)
		    GROUP BY location_code, symbol, source_ts_ms, bbo_hash
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
		        location_code, symbol, source_ts_ms, bbo_hash,
		        minIf(recv_ts_ns, %[2]s) AS dz,
		        countIf(%[2]s)           AS n_dz,
		        %[3]s
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(%[5]d)))
		      AND symbol IN (%[4]s)
		    GROUP BY location_code, symbol, source_ts_ms, bbo_hash
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

// fetchHyperliquidCarriedInstruments counts everything the live fleet publishes, not only
// the liquid set the races are measured over: it answers what the feed covers, not what
// was raced.
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

func (a *API) hyperliquidObservationsTableExists(ctx context.Context) bool {
	var n uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.hyperliquid_bbo_observations", a.FeedsDB)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

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

	// A miss scans three sites of a row-per-feed-per-update table, so it falls through to
	// the live query with a generous deadline rather than being served stale.
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
