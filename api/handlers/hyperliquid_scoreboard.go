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

// The feeds DoubleZero is raced against. The venue's own public feed is one of them; the rest
// are competitors, whose ids never reach the payload (see HyperliquidRacedFeedIDs).
var hyperliquidCompetitors = []struct{ Feed string }{
	{"hyperliquid_public_bbo"},
	{"hydromancer_bbo"},
	{"dwellir_l2book_bbo"},
	{"quicknode_l2book_bbo"},
}

// Withheld from every measurement: HypeRPC arrives a median ~100s stale during recurring
// backlog episodes, which would show DoubleZero winning by minutes. Re-add to
// hyperliquidCompetitors once the feed is fixed.
var hyperliquidExcludedFeeds = []string{"hyperpc_shared_bbo"}

var hyperliquidLiquidSymbols = []string{
	"xyz:SP500", "xyz:XYZ100", "xyz:MU", "xyz:SKHX", "xyz:SPCX", "xyz:CL", "xyz:NVDA", "xyz:BRENTOIL",
	"BTC", "ETH", "SOL", "HYPE", "ZEC",
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

type HyperliquidScoreboardResponse struct {
	WindowLabel string `json:"window_label"`
	// Races is distinct venue emissions, which is what the page labels "Updates raced".
	// Comparisons is the (emission, feed) count behind the rates — about 4x larger, because
	// most emissions are seen by several feeds.
	Races       uint64 `json:"races"`
	Comparisons uint64 `json:"comparisons"`
	// DZAbsent is emissions a competitor delivered and DoubleZero did not. Excluded from every
	// rate here, so it is reported rather than silently dropped.
	DZAbsent    uint64                      `json:"dz_absent"`
	Instruments int                         `json:"instruments"`
	SiteCount   int                         `json:"site_count"`
	FeedCount   int                         `json:"feed_count"`
	All         HyperliquidScoreboardStat   `json:"all"`
	Sites       []HyperliquidScoreboardSite `json:"sites"`
	Feeds       []HyperliquidScoreboardFeed `json:"feeds"`
	AsOf        time.Time                   `json:"as_of"`
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

// HyperliquidRacedFeedIDs returns every competitor feed id this package knows about, raced or
// withheld. Exported for the test that asserts none of them reaches the public payload: that
// test is the only thing between a competitor's identity and an externally-facing route, and
// hardcoding the list there meant a fifth feed shipped uncovered.
func HyperliquidRacedFeedIDs() []string {
	out := make([]string, 0, len(hyperliquidCompetitors)+len(hyperliquidExcludedFeeds))
	for _, c := range hyperliquidCompetitors {
		if c.Feed != hyperliquidVenueFeed {
			out = append(out, c.Feed)
		}
	}
	out = append(out, hyperliquidExcludedFeeds...)
	return out
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
	// FeedCount is deliberately absent here: it counts feeds actually measured in the window,
	// not len(hyperliquidCompetitors), and is filled once the rows are known.
	//
	// The window closes when the blob is computed, which on this entry's cadence is 09:00 UTC
	// — so by late in the day "last 24 hours" names a window that closed hours ago. The label
	// carries the closing time rather than asking the reader to reconcile it with the
	// freshness pill.
	now := time.Now().UTC()
	return &HyperliquidScoreboardResponse{
		WindowLabel: fmt.Sprintf("%d hours to %s UTC", hyperliquidScoreboardWindowHours, now.Format("2006-01-02 15:04")),
		Sites:       []HyperliquidScoreboardSite{},
		Feeds:       []HyperliquidScoreboardFeed{},
		AsOf:        now,
	}
}

func (a *API) FetchHyperliquidScoreboardData(ctx context.Context) (*HyperliquidScoreboardResponse, error) {
	resp := newHyperliquidScoreboardResponse()
	queryable, err := a.hyperliquidObservationsQueryable(ctx)
	if err != nil {
		return nil, err
	}
	if !queryable {
		return resp, nil
	}

	matrix, err := a.fetchHyperliquidScoreboardCells(ctx)
	if err != nil {
		return nil, err
	}
	resp.Instruments, err = a.fetchHyperliquidCarriedInstruments(ctx)
	if err != nil {
		return nil, err
	}

	if total, ok := matrix[hyperliquidMatrixKey{}]; ok {
		resp.All = total.stat
		resp.Races = total.emissions
		resp.Comparisons = total.races
		resp.DZAbsent = total.emissionsDZAbsent
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
		cell := matrix[hyperliquidMatrixKey{feed: c.Feed}]
		// A feed with nothing in the window is not a competitor at 0.0% — it is a feed we have
		// no measurement of. Rendering it anyway put a phantom row at the TOP of the board,
		// because ranking is by median margin ascending and its zero sorts ahead of every real
		// competitor DoubleZero beats.
		if cell.races == 0 {
			continue
		}
		fr := feedRow{feed: c.Feed, stat: cell.stat}
		for _, s := range sites {
			cell := matrix[hyperliquidMatrixKey{loc: s.Code, feed: c.Feed}]
			fr.sites = append(fr.sites, HyperliquidScoreboardSite{
				Code: s.Short, Label: s.Display, HyperliquidScoreboardStat: cell.stat,
			})
		}
		rows = append(rows, fr)
	}
	resp.FeedCount = len(rows)
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

	return resp, nil
}

type hyperliquidMatrixKey struct{ loc, feed string }

type hyperliquidMatrixCell struct {
	// races counts (emission, feed) comparisons, so it is Σ over feeds on any cell that rolls
	// feeds up. Correct per feed; never the number of book states. Use emissions for that.
	races uint64
	// emissions counts distinct venue emissions, so a cell rolled up over four feeds does not
	// report an update four times. Approximate (uniqCombined, measured 0.18% against
	// uniqExact) because the exact form cost 3.7 GB more on a query already near its limit.
	emissions uint64
	// emissionsDZAbsent counts emissions a competitor delivered and DoubleZero did not. They
	// are excluded from every rate above — the win rate is conditional on DoubleZero having
	// delivered — so the payload carries the denominator it is not measuring.
	emissionsDZAbsent uint64
	stat              HyperliquidScoreboardStat
}

func (a *API) fetchHyperliquidScoreboardCells(ctx context.Context) (map[hyperliquidMatrixKey]hyperliquidMatrixCell, error) {
	proj, tuples := hyperliquidCompetitorArrivals()

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
		    SELECT location_code, symbol, source_ts_ms, bbo_hash, dz, c.1 AS feed,
		           (toInt64(c.2) - toInt64(dz)) / 1e6 AS signed_ms
		    FROM agg
		    ARRAY JOIN [%[6]s] AS c
		    WHERE c.2 > 0
		)
		SELECT
		    location_code, feed,
		    countIf(dz > 0) AS races,
		    100 * countIf(signed_ms > 0 AND dz > 0) / greatest(countIf(dz > 0), 1)  AS win_pct,
		    ifNotFinite(toFloat64(quantileTDigestIf(0.50)(signed_ms, dz > 0)), 0)   AS p50,
		    ifNotFinite(toFloat64(quantileTDigestIf(0.95)(signed_ms, dz > 0)), 0)   AS p95,
		    ifNotFinite(toFloat64(quantileTDigestIf(0.99)(signed_ms, dz > 0)), 0)   AS p99,
		    uniqCombinedIf((location_code, symbol, source_ts_ms, bbo_hash), dz > 0) AS emissions,
		    uniqCombinedIf((location_code, symbol, source_ts_ms, bbo_hash), dz = 0) AS emissions_dz_absent
		FROM sv
		GROUP BY location_code, feed WITH CUBE
		SETTINGS max_bytes_before_external_group_by = 8000000000`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), proj,
		hyperliquidScoreboardSymbols(), hyperliquidScoreboardWindowHours, tuples)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid scoreboard cells: %w", err)
	}
	defer rows.Close()

	matrix := map[hyperliquidMatrixKey]hyperliquidMatrixCell{}
	for rows.Next() {
		var loc, feed string
		var races, emissions, emissionsDZAbsent uint64
		var win, p50, p95, p99 float64
		if err := rows.Scan(&loc, &feed, &races, &win, &p50, &p95, &p99, &emissions, &emissionsDZAbsent); err != nil {
			return nil, err
		}
		matrix[hyperliquidMatrixKey{loc: loc, feed: feed}] = hyperliquidMatrixCell{
			races: races, emissions: emissions, emissionsDZAbsent: emissionsDZAbsent,
			stat: HyperliquidScoreboardStat{WinPct: win, P50Ms: p50, P95Ms: p95, P99Ms: p99},
		}
	}
	return matrix, rows.Err()
}

const hyperliquidCarriedWindowMinutes = 5

func (a *API) fetchHyperliquidCarriedInstruments(ctx context.Context) (int, error) {
	q := fmt.Sprintf(`
		SELECT uniqExact(symbol)
		FROM %[1]s.hyperliquid_bbo_observations
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[3]d)))
		  AND %[2]s`,
		fmt.Sprintf("`%s`", a.FeedsDB), hyperliquidDZArrivalExpr(), hyperliquidCarriedWindowMinutes)

	var n uint64
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("hyperliquid carried instruments: %w", err)
	}
	return int(n), nil
}

// hyperliquidObservationsQueryable answers whether the scan can run against this connection —
// and, which is the point of the error return, tells a NO apart from a DON'T KNOW.
//
// Two answers are no, and both mean the empty payload rather than a failure. The table may be
// absent, which is local dev. Or it may be present only as a remoteSecure() proxy into another
// service: EXISTS TABLE cannot tell that from a real table, and the engine can, because a table
// created AS remoteSecure(...) reports StorageProxy. That case needs catching because no budget
// rescues it — the scan aggregates in a CTE, ARRAY JOINs, then aggregates again, so there is no
// merge to push down and the initiator dies with NETWORK_ERROR after ~7.6 GiB. Answering with
// the empty payload lets the refresh SUCCEED, and succeeding is what stops it re-running on
// every cycle with the escalator pinned at ERROR.
//
// A probe that could not run is none of those. Reported as false it would let a single
// connection blip at the 09:00 refresh write the empty blob and advance updated_at — and under
// dailyAtUTC the entry is then not due again until tomorrow, so the board reads "no races
// recorded" for 24 hours with nothing logged anywhere. The error goes back to the caller
// instead: nothing is written, the escalator sees it, and the entry stays due.
//
// any() rather than the bare column so the probe always returns exactly one row. An absent
// table comes back as the empty string, rather than as a no-rows error this would then have to
// tell apart from a real one — which is the same conflation one level down.
func (a *API) hyperliquidObservationsQueryable(ctx context.Context) (bool, error) {
	var engine string
	q := fmt.Sprintf(
		"SELECT any(engine) FROM system.tables WHERE database = '%s' AND name = 'hyperliquid_bbo_observations'",
		a.FeedsDB)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&engine); err != nil {
		return false, fmt.Errorf("hyperliquid observations probe: %w", err)
	}
	// Production is SharedMergeTree, local dev is MergeTree, a proxy is StorageProxy. Matched
	// on the family so a service that replicates differently still reads as local.
	//
	// system.tables is filtered by grant, which is what makes this safe under a restricted
	// user: a row is visible exactly when the caller holds SELECT on that table, so "cannot
	// read the engine" and "cannot run the scan" are the same condition. Verified against
	// 25.12 — a user granted SELECT on one table reads its engine and sees no row at all for
	// its neighbour.
	return strings.HasSuffix(engine, "MergeTree"), nil
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

	// Nothing to compute from here (local dev, or a proxy): an empty board, not an error. This
	// is a system.tables lookup, not a scan. A probe that fails falls through to the 503 below
	// rather than rendering zeros — an empty board asserts a measurement, and we have not made
	// one.
	if queryable, err := a.hyperliquidObservationsQueryable(ctx); err == nil && !queryable {
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
