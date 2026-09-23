package handlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const hyperliquidDZSourcePrefix = "tob_"

var hyperliquidRetiredPublishers = []string{"tob_aws_tyo_mirror1"}

const hyperliquidVenueFeed = "hyperliquid_public_bbo"

type hyperliquidSite struct {
	Code    string // location_code in the feeds schema, e.g. "tyo"
	Short   string // column header, e.g. "TYO"
	Display string // city name, e.g. "Tokyo"
}

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

const HyperliquidScoreboardCacheKey = "hyperliquid_public_scoreboard"

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

func hyperliquidCompetitorArrivals() (projection, arrayJoin string) {
	proj := make([]string, 0, len(hyperliquidCompetitors))
	tuples := make([]string, 0, len(hyperliquidCompetitors))
	for i, c := range hyperliquidCompetitors {
		proj = append(proj, fmt.Sprintf("minIf(recv_ts_ns, source = '%[1]s') AS t%[2]d", c.Feed, i))
		tuples = append(tuples, fmt.Sprintf("('%s', t%d)", c.Feed, i))
	}
	return strings.Join(proj, ",\n            "), strings.Join(tuples, ",\n                        ")
}

func newHyperliquidScoreboardResponse() *HyperliquidScoreboardResponse {
	return &HyperliquidScoreboardResponse{
		WindowLabel: fmt.Sprintf("last %d hours", hyperliquidScoreboardWindowHours),
		FeedCount:   len(hyperliquidCompetitors),
		Sites:       []HyperliquidScoreboardSite{},
		Feeds:       []HyperliquidScoreboardFeed{},
		Markets:     []HyperliquidScoreboardMarket{},
		AsOf:        time.Now().UTC(),
	}
}

func (a *API) FetchHyperliquidScoreboardData(ctx context.Context) (*HyperliquidScoreboardResponse, error) {
	resp := newHyperliquidScoreboardResponse()
	if !a.hyperliquidObservationsTableExists(ctx) {
		return resp, nil
	}

	matrix, markets, err := a.fetchHyperliquidScoreboardCells(ctx)
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

const hyperliquidUncategorised = "other"

func (a *API) fetchHyperliquidScoreboardCells(ctx context.Context) (
	map[hyperliquidMatrixKey]hyperliquidMatrixCell, map[string]HyperliquidScoreboardStat, error,
) {
	proj, tuples := hyperliquidCompetitorArrivals()

	var cases []string
	for _, g := range hyperliquidMarketGroups {
		for _, c := range g.Cats {
			quoted := make([]string, len(c.Symbols))
			for i, sym := range c.Symbols {
				quoted[i] = "'" + sym + "'"
			}
			cases = append(cases, fmt.Sprintf("symbol IN (%s), '%s'", strings.Join(quoted, ", "), c.Name))
		}
	}

	q := fmt.Sprintf(`
		WITH agg AS (
		    SELECT
		        location_code, symbol, source_ts_ms, bbo_hash,
		        minIf(recv_ts_ns, %[2]s) AS dz,
		        %[3]s
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalHour(%[5]d)))
		      AND symbol IN (%[4]s)
		    GROUP BY location_code, symbol, source_ts_ms, bbo_hash
		),
		sv AS (
		    SELECT location_code, c.1 AS feed,
		           multiIf(%[7]s, '%[8]s') AS cat,
		           (toInt64(c.2) - toInt64(dz)) / 1e6 AS signed_ms
		    FROM agg
		    ARRAY JOIN [%[6]s] AS c
		    WHERE c.2 > 0 AND dz > 0
		)
		SELECT
		    location_code, feed, cat,
		    count() AS races,
		    100 * countIf(signed_ms > 0) / greatest(count(), 1)   AS win_pct,
		    toFloat64(quantileTDigest(0.50)(signed_ms))           AS p50,
		    toFloat64(quantileTDigest(0.95)(signed_ms))           AS p95,
		    toFloat64(quantileTDigest(0.99)(signed_ms))           AS p99
		FROM sv
		GROUP BY location_code, feed, cat WITH CUBE
		SETTINGS max_bytes_before_external_group_by = 8000000000`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), proj,
		hyperliquidScoreboardSymbols(), hyperliquidScoreboardWindowHours, tuples,
		strings.Join(cases, ", "), hyperliquidUncategorised)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, nil, fmt.Errorf("hyperliquid scoreboard cells: %w", err)
	}
	defer rows.Close()

	matrix := map[hyperliquidMatrixKey]hyperliquidMatrixCell{}
	markets := map[string]HyperliquidScoreboardStat{}
	for rows.Next() {
		var loc, feed, cat string
		var races uint64
		var win, p50, p95, p99 float64
		if err := rows.Scan(&loc, &feed, &cat, &races, &win, &p50, &p95, &p99); err != nil {
			return nil, nil, err
		}
		stat := HyperliquidScoreboardStat{WinPct: win, P50Ms: p50, P95Ms: p95, P99Ms: p99}
		switch {
		case cat == "":
			matrix[hyperliquidMatrixKey{loc: loc, feed: feed}] = hyperliquidMatrixCell{races: races, stat: stat}
		case cat != hyperliquidUncategorised && loc == "" && feed == "":
			markets[cat] = stat
		}
	}
	return matrix, markets, rows.Err()
}

const hyperliquidCarriedWindowMinutes = 5

func (a *API) fetchHyperliquidCarriedInstruments(ctx context.Context) (map[string]int, error) {
	q := fmt.Sprintf(`
		SELECT startsWith(symbol, 'xyz:') AS hip3, count() AS n
		FROM (
		    SELECT DISTINCT symbol
		    FROM %[1]s.hyperliquid_bbo_observations
		    WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[3]d)))
		      AND %[2]s
		)
		GROUP BY hip3`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), hyperliquidCarriedWindowMinutes)

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

// hyperliquidScoreboardRetryAfter is what a miss tells the client to wait. The entry is due
// the moment its key is unwritten, so the wait is a worker cycle, not the 24h cadence.
const hyperliquidScoreboardRetryAfter = 30 * time.Second

func (a *API) GetHyperliquidScoreboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Read for EVERY environment, not just mainnet. The payload does not vary by one: FeedsDB
	// is a single config value and envDB falls back to the same connection, so testnet would
	// have computed the identical board. What gating the read on isMainnet actually did was
	// leave `X-DZ-Env: testnet` as a way for any caller to skip the cache.
	if data, err := a.readPageCache(ctx, HyperliquidScoreboardCacheKey); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		_, _ = w.Write(data)
		return
	}
	w.Header().Set("X-Cache", "MISS")

	// No capture tables here (local dev): an empty board, not an error. This is an EXISTS,
	// not a scan.
	if !a.hyperliquidObservationsTableExists(ctx) {
		writeJSON(w, newHyperliquidScoreboardResponse())
		return
	}

	// A miss is never computed in the request path. One compute is a ~10 GiB, ~15s scan of
	// hyperliquid_bbo_observations; this route is reachable by anyone, there is no
	// singleflight, and the miss path never wrote back — so the next request ran it again.
	// The rate limiter bounds requests per client, not what one request costs the cluster.
	// The worker owns the compute; a miss says so and stops.
	w.Header().Set("Retry-After", strconv.Itoa(int(hyperliquidScoreboardRetryAfter.Seconds())))
	http.Error(w, "hyperliquid scoreboard is not computed yet", http.StatusServiceUnavailable)
}
