package handlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// Sports L2 coverage: the health and shape of the market-by-price lanes DoubleZero's Kalshi
// edge publisher carries. Unlike the scoreboard this is not a race — Kalshi publishes no
// public sports feed to race against — so it reports what the lanes are actually delivering:
// message rates, instrument counts, real book depth, fault counters, and how long ago each
// lane was last heard from.
//
// The last-heard number is the point of the page. The failure mode this fleet actually hits is
// a lane going silent while the service still reports healthy: the DoubleZero tunnel
// re-establishes, the receive socket's IGMP membership is silently gone, and the process keeps
// running with a dead socket (observed twice in one day, recorded in the kalshi_feed_capture
// Ansible group_vars). Nothing pages on it. A frozen last-seen is what makes it visible.

// kalshiL2CoverageCacheKey is the page-cache key written by StartKalshiBackgroundRefresher.
const kalshiL2CoverageCacheKey = "kalshi_l2_coverage"

// kalshiL2WindowMinutes is the interval the rates are averaged over. Rates are derived from
// it, so changing it changes nothing about correctness.
//
// It does NOT bound the scan the way a leading-key predicate would. kalshi_mbp_levels sorts by
// (measurement_node_id, source, channel_id, symbol, instrument_id, recv_ts_ns) and partitions
// by toDate(recv_ts_ns), so a recv_ts_ns predicate prunes to the day's partition and no
// further: mid-day this reads most of a day of a level-grain, TTL-less table to answer a
// fifteen-minute question, over a remoteSecure() proxy. That is why this view is owned by the
// background refresher and served from cache rather than run per request.
const kalshiL2WindowMinutes = 15

// kalshiL2Lane describes a known market-by-price source. Order here is display order.
//
// The ids follow the capture's `[[sources]].id`, which since infra#2254 is named after the
// DoubleZero ledger group code with the plane suffix hoisted to the front
// (edge-kalshi-sports-mbp -> mbp_edge_kalshi_sports_<league>). They were `*_lashay_*` before
// that rename.
//
// An unlisted source is NOT dropped — it renders under its raw id in the "Other" category
// (see kalshiL2LaneFor). That is what keeps this list from being a correctness dependency: a
// new lane added by the publisher, or another round of renaming, shows up on the page by
// itself. Requiring a code change to see a lane would make the page quietly under-report
// exactly when someone is checking whether a new lane works.
var kalshiL2Lanes = []struct{ Source, Label, Category string }{
	{"mbp_edge_kalshi_perps", "Perpetual Futures", "Perps"},

	{"mbp_edge_kalshi_sports_nfl", "NFL", "Football"},
	{"mbp_edge_kalshi_sports_ncaaf", "NCAA Football", "Football"},
	{"mbp_edge_kalshi_sports_cfl", "CFL", "Football"},
	{"mbp_edge_kalshi_sports_football_other", "Football (other)", "Football"},

	{"mbp_edge_kalshi_sports_nba", "NBA", "Basketball"},
	{"mbp_edge_kalshi_sports_wnba", "WNBA", "Basketball"},
	{"mbp_edge_kalshi_sports_ncaamb", "NCAA Men's Basketball", "Basketball"},
	{"mbp_edge_kalshi_sports_ncaawb", "NCAA Women's Basketball", "Basketball"},
	{"mbp_edge_kalshi_sports_basketball_other", "Basketball (other)", "Basketball"},

	{"mbp_edge_kalshi_sports_mlb", "MLB", "Baseball"},
	{"mbp_edge_kalshi_sports_npb", "NPB", "Baseball"},
	{"mbp_edge_kalshi_sports_kbo", "KBO", "Baseball"},
	{"mbp_edge_kalshi_sports_baseball_other", "Baseball (other)", "Baseball"},

	{"mbp_edge_kalshi_sports_nhl", "NHL", "Hockey"},

	{"mbp_edge_kalshi_sports_epl", "Premier League", "Soccer"},
	{"mbp_edge_kalshi_sports_laliga", "LaLiga", "Soccer"},
	{"mbp_edge_kalshi_sports_seriea", "Serie A", "Soccer"},
	{"mbp_edge_kalshi_sports_bundesliga", "Bundesliga", "Soccer"},
	{"mbp_edge_kalshi_sports_ligue1", "Ligue 1", "Soccer"},
	{"mbp_edge_kalshi_sports_ucl", "Champions League", "Soccer"},
	{"mbp_edge_kalshi_sports_mls", "MLS", "Soccer"},
	{"mbp_edge_kalshi_sports_ligamx", "Liga MX", "Soccer"},
	{"mbp_edge_kalshi_sports_worldcup", "World Cup", "Soccer"},
	{"mbp_edge_kalshi_sports_soccer", "Soccer (other)", "Soccer"},

	{"mbp_edge_kalshi_sports_golf", "Golf", "Other"},
	{"mbp_edge_kalshi_sports_tennis", "Tennis", "Other"},
	{"mbp_edge_kalshi_sports_esports", "Esports", "Other"},
	{"mbp_edge_kalshi_sports_combat", "Combat Sports", "Other"},
	{"mbp_edge_kalshi_sports_cricket", "Cricket", "Other"},
	{"mbp_edge_kalshi_sports_motorsport", "Motorsport", "Other"},
	{"mbp_edge_kalshi_sports_other", "Other", "Other"},
}

// kalshiL2LaneFor returns the label, category, and display order for a source. Unknown sources
// sort last under "Other" but are still reported.
func kalshiL2LaneFor(source string) (label, category string, order int) {
	for i, l := range kalshiL2Lanes {
		if l.Source == source {
			return l.Label, l.Category, i
		}
	}
	return source, "Other", len(kalshiL2Lanes)
}

// KalshiL2Lane is one (source, channel_id) market-by-price lane over the coverage window.
type KalshiL2Lane struct {
	Source       string `json:"source"`
	Label        string `json:"label"`
	Category     string `json:"category"`
	ChannelID    uint8  `json:"channel_id"`
	LocationCode string `json:"location_code"`

	// MeasurementNodeID completes this row's identity. A lane recorded from several
	// vantages is several rows, one per vantage — see the GROUP BY in
	// FetchKalshiL2Coverage. location_code alone is not a key: two recorders can share a
	// metro, and the rates would merge with no way to see it had happened.
	MeasurementNodeID string `json:"measurement_node_id"`

	// Rates over the coverage window.
	MessagesPerSec     float64 `json:"messages_per_sec"`
	LevelUpdatesPerSec float64 `json:"level_updates_per_sec"`

	// Instruments is a distinct count within this channel. instrument_id is unique only
	// WITHIN a channel_id, so this is never counted across channels.
	Instruments uint64 `json:"instruments"`

	// Book depth observed on level-bearing messages.
	DepthP50 float64 `json:"depth_p50"`
	DepthP95 float64 `json:"depth_p95"`
	DepthMax uint32  `json:"depth_max"`

	// Fault and lifecycle counters.
	Gaps           uint64 `json:"gaps"`
	Resets         uint64 `json:"resets"`
	Clears         uint64 `json:"clears"`
	SnapshotCycles uint64 `json:"snapshot_cycles"`

	// Seen reports whether this lane produced any message inside the coverage window. A
	// configured lane that has gone silent is reported with Seen=false and zeroed stats
	// rather than being omitted — see the roster merge in FetchKalshiL2Coverage.
	Seen bool `json:"seen"`

	// LastSeen is the newest message in the window; the zero time when Seen is false.
	LastSeen time.Time `json:"last_seen"`
}

// KalshiL2CoverageResponse is the API response.
type KalshiL2CoverageResponse struct {
	GeneratedAt   time.Time      `json:"generated_at"`
	WindowMinutes int            `json:"window_minutes"`
	Lanes         []KalshiL2Lane `json:"lanes"`
}

func emptyKalshiL2Coverage() *KalshiL2CoverageResponse {
	return &KalshiL2CoverageResponse{
		GeneratedAt:   time.Now().UTC(),
		WindowMinutes: kalshiL2WindowMinutes,
		Lanes:         []KalshiL2Lane{},
	}
}

// kalshiL2TableExists reports whether the proxied level table is queryable. A probe failure is
// an error, not an absent table — see kalshiTableExists.
func (a *API) kalshiL2TableExists(ctx context.Context) (bool, error) {
	return a.kalshiTableExists(ctx, "kalshi_mbp_levels")
}

// FetchKalshiL2Coverage aggregates the market-by-price lanes over the coverage window.
//
// Grouped by (source, channel_id, measurement_node_id), and each of the three is
// load-bearing:
//
//   - never by source alone: prod's two publisher arms share one multicast group and one port
//     triple and differ only by channel_id, and instrument_id is unique only within a channel.
//     Collapsing the arms would merge two independent delta streams and double-count their
//     instruments.
//   - never without the vantage: one lane recorded at several vantages is several INDEPENDENT
//     observations of the same stream. Without measurement_node_id in the key they collapse
//     into one row whose rates are their SUM — three vantages of perps would report treble the
//     real message rate — and any(location_code) would name one of them arbitrarily, so the
//     merge is invisible in the output. Recording is cmh-only today precisely because this
//     was wrong (malbeclabs/infra, kalshi_feed_capture_cmh.yml), which is what makes adding a
//     vantage safe now.
//
// measurement_node_id rather than location_code: two recorders can share a metro, and that is
// the case the location column cannot represent.
//
// No latency is derived from source_ts_ns here. Its meaning is chosen by source_ts_kind
// (`venue` / `publisher_capture` / `none`), so an unfiltered delta silently reports
// publisher-clock differences as feed latency. If a latency column is ever added to this page
// it must filter source_ts_kind = 'venue'.
func (a *API) FetchKalshiL2Coverage(ctx context.Context) (*KalshiL2CoverageResponse, error) {
	exists, err := a.kalshiL2TableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return emptyKalshiL2Coverage(), nil
	}

	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT
			source,
			channel_id,
			measurement_node_id,
			any(location_code) AS location_code,
			count() AS messages,
			countIf(msg_type = 'level_update') AS level_updates,
			uniqCombined(instrument_id) AS instruments,
			-- Depth is only meaningful on level-bearing messages; ifNotFinite keeps a lane
			-- that carried only resets/clears from emitting NaN and failing JSON encoding.
			ifNotFinite(toFloat64(quantileTDigestIf(0.5)(book_levels_after, msg_type = 'level_update')), 0) AS depth_p50,
			ifNotFinite(toFloat64(quantileTDigestIf(0.95)(book_levels_after, msg_type = 'level_update')), 0) AS depth_p95,
			maxIf(book_levels_after, msg_type = 'level_update') AS depth_max,
			countIf(status_after = 'gap') AS gaps,
			countIf(msg_type = 'instrument_reset') AS resets,
			countIf(msg_type = 'book_clear') AS clears,
			countIf(msg_type = 'snapshot_end') AS snapshot_cycles,
			max(recv_ts_ns) AS last_recv_ts_ns
		FROM %[1]s.kalshi_mbp_levels
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[2]d)))
		GROUP BY source, channel_id, measurement_node_id`, db, kalshiL2WindowMinutes)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	windowSecs := float64(kalshiL2WindowMinutes * 60)
	resp := emptyKalshiL2Coverage()
	type ordered struct {
		lane  KalshiL2Lane
		order int
	}
	var out []ordered
	for rows.Next() {
		var l KalshiL2Lane
		var messages, levelUpdates uint64
		var lastRecvNs uint64
		if err := rows.Scan(
			&l.Source, &l.ChannelID, &l.MeasurementNodeID, &l.LocationCode,
			&messages, &levelUpdates, &l.Instruments,
			&l.DepthP50, &l.DepthP95, &l.DepthMax,
			&l.Gaps, &l.Resets, &l.Clears, &l.SnapshotCycles,
			&lastRecvNs,
		); err != nil {
			return nil, err
		}
		l.MessagesPerSec = float64(messages) / windowSecs
		l.LevelUpdatesPerSec = float64(levelUpdates) / windowSecs
		l.LastSeen = time.Unix(0, int64(lastRecvNs)).UTC()
		l.Seen = true
		label, category, order := kalshiL2LaneFor(l.Source)
		l.Label, l.Category = label, category
		out = append(out, ordered{lane: l, order: order})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Merge in the configured roster. Lanes are discovered from rows inside the window, so a
	// lane that stops publishing does not go quiet in this list — it DISAPPEARS from it, and
	// the page then looks healthy (fewer lanes, nothing flagged) in precisely the failure mode
	// this view exists to catch: the capture process still reports healthy while its multicast
	// membership is gone and the counters have frozen. Emitting every known lane, with Seen
	// false when the window held nothing, keeps the row on screen to be noticed.
	//
	// This covers the lanes named in kalshiL2Lanes. A lane that is neither listed there nor
	// present in the window is still invisible; the roster is the only record of what ought to
	// be publishing, since the capture's source list lives in Ansible, not in the data.
	if len(out) > 0 {
		present := map[string]bool{}
		for _, o := range out {
			present[o.lane.Source] = true
		}
		for i, known := range kalshiL2Lanes {
			if present[known.Source] {
				continue
			}
			out = append(out, ordered{
				order: i,
				lane: KalshiL2Lane{
					Source:   known.Source,
					Label:    known.Label,
					Category: known.Category,
					Seen:     false,
				},
			})
		}
	}

	// Stable display order: configured lane order, then source (so unknown lanes, which all
	// share the fallback order, stay deterministic), then channel id so the two arms of one
	// lane sit together.
	sort.Slice(out, func(i, j int) bool {
		if out[i].order != out[j].order {
			return out[i].order < out[j].order
		}
		if out[i].lane.Source != out[j].lane.Source {
			return out[i].lane.Source < out[j].lane.Source
		}
		if out[i].lane.ChannelID != out[j].lane.ChannelID {
			return out[i].lane.ChannelID < out[j].lane.ChannelID
		}
		return out[i].lane.MeasurementNodeID < out[j].lane.MeasurementNodeID
	})
	for _, o := range out {
		resp.Lanes = append(resp.Lanes, o.lane)
	}
	return resp, nil
}

// GetKalshiL2Coverage serves the Kalshi sports L2 coverage view.
//
// Cache-first: the background refresher owns this view. The live fallback keeps local dev and
// a cold cache working — the scan is bounded to the coverage window and grouped, so it is not
// the multi-day scan the scoreboard's heavy windows are — but the cached path is the normal
// one in production.
func (a *API) GetKalshiL2Coverage(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), kalshiL2CoverageCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	resp, err := a.FetchKalshiL2Coverage(ctx)
	if err != nil {
		logError("KalshiL2Coverage error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
