package handlers

import (
	"context"
	"fmt"
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
	{"hyperpc_shared_bbo", "HyperPC"},
	{"quicknode_l2book_bbo", "QuickNode"},
}

// hyperliquidWindows maps window params to ClickHouse interval expressions.
var hyperliquidWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
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
}

// labelForFeed maps a raw competitor feed to its display label (falls back to the raw name).
func labelForFeed(feed string) string {
	for _, c := range hyperliquidCompetitors {
		if c.Feed == feed {
			return c.Label
		}
	}
	return feed
}

// FetchHyperliquidScoreboardData computes the aggregated scoreboard for a window and
// optional symbol. Empty symbol means all symbols.
func (a *API) FetchHyperliquidScoreboardData(ctx context.Context, window, symbol string) (*HyperliquidScoreboardResponse, error) {
	interval, ok := hyperliquidWindows[window]
	if !ok {
		window = "24h"
		interval = hyperliquidWindows[window]
	}
	symbol = sanitizeHyperliquidSymbol(symbol)
	symbolFilter := ""
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

	// Headline: DZ win share over all DZ-vs-competitor pairwise comparisons.
	headlineQ := fmt.Sprintf(`
		SELECT
			ifNull(100.0 * countIf(startsWith(feed,'tob_') AND NOT startsWith(loser_feed,'tob_'))
			      / nullIf(countIf(startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')), 0), 0) AS dz_win_share_pct,
			countIf(startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) AS total_races
		FROM %s.hyperliquid_bbo_feed_race_summary FINAL
		WHERE feed != loser_feed AND loser_feed != '' %s
		  AND event_ts >= now() - INTERVAL %s`, db, symbolFilter, interval)
	var winShare float64
	var totalRaces uint64
	if err := a.envDB(ctx).QueryRow(ctx, headlineQ).Scan(&winShare, &totalRaces); err != nil {
		return nil, err
	}
	resp.DZWinSharePct = winShare
	resp.TotalRaces = totalRaces

	// Per-competitor: win%, lead p50/p95 (over DZ wins), and total comparable races.
	compQ := fmt.Sprintf(`
		SELECT competitor,
			countIf(dz_won = 1) AS dz_wins,
			countIf(dz_won = 0) AS dz_losses,
			quantileExactIf(0.5)(lead_ms, dz_won = 1) AS lead_p50,
			quantileExactIf(0.95)(lead_ms, dz_won = 1) AS lead_p95
		FROM (
			SELECT
				if(startsWith(feed,'tob_'), loser_feed, feed) AS competitor,
				if(startsWith(feed,'tob_'), 1, 0) AS dz_won,
				lead_time_p50_ms AS lead_ms
			FROM %s.hyperliquid_bbo_feed_race_summary FINAL
			WHERE feed != loser_feed AND loser_feed != ''
			  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %s
			  AND event_ts >= now() - INTERVAL %s
		)
		GROUP BY competitor`, db, symbolFilter, interval)
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
			countIf(dz_won = 1) AS dz_wins,
			countIf(dz_won = 0) AS dz_losses,
			quantileExactIf(0.5)(lead_ms, dz_won = 1) AS lead_p50,
			quantileExactIf(0.95)(lead_ms, dz_won = 1) AS lead_p95
		FROM (
			SELECT measurement_node_id, location_code,
				if(startsWith(feed,'tob_'), loser_feed, feed) AS competitor,
				if(startsWith(feed,'tob_'), 1, 0) AS dz_won,
				lead_time_p50_ms AS lead_ms
			FROM %s.hyperliquid_bbo_feed_race_summary FINAL
			WHERE feed != loser_feed AND loser_feed != ''
			  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %s
			  AND event_ts >= now() - INTERVAL %s
		)
		GROUP BY measurement_node_id, location_code, competitor`, db, symbolFilter, interval)
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
	}

	recent, err := a.fetchHyperliquidRecentRaces(ctx, symbol, time.Time{}, 50)
	if err != nil {
		return nil, err
	}
	resp.RecentRaces = recent

	return resp, nil
}

// fetchHyperliquidRecentRaces returns the most recent races (one row per race,
// winner + closest competitor + lead). sinceTs zero -> last 2 minutes.
func (a *API) fetchHyperliquidRecentRaces(ctx context.Context, symbol string, sinceTs time.Time, limit int) ([]HyperliquidRace, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	symbol = sanitizeHyperliquidSymbol(symbol)
	symbolFilter := ""
	if symbol != "" {
		symbolFilter = fmt.Sprintf("AND symbol = '%s'", symbol)
	}
	timeFilter := "AND event_ts >= now() - INTERVAL 2 MINUTE"
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
		FROM %s.hyperliquid_bbo_feed_race_summary FINAL
		WHERE loser_feed != '' AND feed != loser_feed %s %s
		GROUP BY capture_run_id, measurement_node_id, symbol, source_ts_ms, bbo_hash, location_code, feed
		ORDER BY max_event_ts DESC
		LIMIT %d`, db, symbolFilter, timeFilter, limit)
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
		r.RunnerUpLabel = labelForFeed(r.RunnerUpFeed)
		out = append(out, r)
	}
	return out, rows.Err()
}

// hyperliquidScoreboardCacheKey returns the cache key for the default request shape, or "".
func hyperliquidScoreboardCacheKey(r *http.Request) string {
	if r.URL.Query().Get("since_ts") != "" || r.URL.Query().Get("symbol") != "" {
		return ""
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window != "" && window != "24h" {
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
		window = "24h"
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

// FetchHyperliquidScoreboardLatest returns only the recent-races strip (fast cache).
func (a *API) FetchHyperliquidScoreboardLatest(ctx context.Context, limit int) (*HyperliquidScoreboardResponse, error) {
	races, err := a.fetchHyperliquidRecentRaces(ctx, "", time.Time{}, limit)
	if err != nil {
		return nil, err
	}
	return &HyperliquidScoreboardResponse{
		GeneratedAt: time.Now().UTC(),
		FeedType:    "bbo",
		RecentRaces: races,
		Competitors: []HyperliquidCompetitor{},
		Nodes:       []HyperliquidNode{},
	}, nil
}
