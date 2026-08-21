package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Per-day completeness of the level-grain market-by-price record: for each day, whether the
// captured stream is good enough to replay a book from.
//
// This is the catalog view, not the delivery path. Selling history (malbeclabs/kalshi#212)
// needs an answer to "which days are sellable", and presence of rows is not that answer. A day
// is only replayable if every book that moved also stayed anchored, and if a replay has a
// snapshot inside the day to start from. Both are countable here and neither is visible on the
// live coverage page, which averages a fifteen-minute window and says nothing about yesterday.
//
// What this deliberately does NOT report is capture downtime. A lane is legitimately silent
// when no game is on, and nothing in the data separates "no market" from "socket died", so a
// wall-clock coverage percentage here would be a number with no meaning. First and last message
// per day are reported instead, and the operator reads them.

// kalshiL2CompletenessCacheKey is the page-cache key written by StartKalshiBackgroundRefresher.
//
// Bump the version in the same commit as any change to the payload shape. The entry lives in
// Postgres and outlives the deploy, so an unbumped key hands a new bundle a row written by the
// old binary — see the same note on kalshiL2CoverageCacheKey for what that costs.
const kalshiL2CompletenessCacheKey = "kalshi_l2_completeness:v1"

// kalshiL2CompletenessDays is how many days back the view reports, today included.
//
// ponytail: this rescans every day in the window on every refresh, over a level-grain table
// through a remoteSecure() proxy. It is affordable while recording is one perps lane and it is
// not affordable at sports scale (27.46 Mbps on the wire, malbeclabs/infra
// kalshi_feed_capture_cmh.yml). A completed day never changes, so the upgrade is to compute
// each day once and keep the row — an AggregatingMergeTree fed on insert in the capture's
// schema, or a daily job writing Postgres here. Do that before sports recording is enabled
// (malbeclabs/kalshi#214), not after.
const kalshiL2CompletenessDays = 14

// KalshiL2Day is one day of captured level data, aggregated across lanes.
type KalshiL2Day struct {
	// Day is the UTC calendar day, YYYY-MM-DD. It is the table's partition key, so it is also
	// the unit an export would ship.
	Day string `json:"day"`

	// Lanes is the count of distinct (source, channel_id) pairs that recorded anything.
	Lanes uint64 `json:"lanes"`

	// Instruments is summed across lanes, never counted across them: instrument_id is unique
	// only within a channel_id, so a global distinct count would merge two channels' books
	// into one number. Within a lane it is the widest vantage rather than the sum, because two
	// recorders of one lane are two observations of the same instrument set.
	Instruments uint64 `json:"instruments"`

	// GappedInstruments is how many books ran un-anchored at some point in the day. Any value
	// above zero means a replay of that book has a hole, so the day is not clean.
	//
	// This is a count of books and not of gap events, for the reason recorded on
	// KalshiL2Lane.GapMessages: the message-grain count is a recovery duration scaled by
	// traffic, so it makes a busy lane look thousands of times worse at identical loss.
	GappedInstruments uint64 `json:"gapped_instruments"`

	// UnanchoredInstruments is how many books moved during the day with no snapshot_end inside
	// it. A replay of those cannot start from this day's data alone; it has to carry state in
	// from an earlier day, which is what makes a single-day export of them unusable on its own.
	UnanchoredInstruments uint64 `json:"unanchored_instruments"`

	// Messages is the row count, the size proxy for what a day's export would carry.
	Messages uint64 `json:"messages"`

	// First and last message of the day, from the capture host's clock (recv_ts_ns). Reported
	// rather than turned into a coverage percentage — see the header on capture downtime.
	FirstMessage time.Time `json:"first_message"`
	LastMessage  time.Time `json:"last_message"`

	// GapLanes names the lanes holding the gapped books, so a bad day points at what to look
	// at. Display labels, resolved through kalshiL2LaneFor.
	GapLanes []string `json:"gap_lanes"`
}

// KalshiL2CompletenessResponse is the API response, newest day first.
type KalshiL2CompletenessResponse struct {
	GeneratedAt time.Time     `json:"generated_at"`
	DayCount    int           `json:"day_count"`
	Days        []KalshiL2Day `json:"days"`
}

func emptyKalshiL2Completeness() *KalshiL2CompletenessResponse {
	return &KalshiL2CompletenessResponse{
		GeneratedAt: time.Now().UTC(),
		DayCount:    kalshiL2CompletenessDays,
		Days:        []KalshiL2Day{},
	}
}

// FetchKalshiL2Completeness aggregates captured level data by day.
//
// Three grains, and the order of the two roll-ups is the whole correctness argument:
//
//  1. (day, source, channel_id, measurement_node_id) — the raw grain, one row per lane per
//     vantage. Counting instruments any higher than this merges channels, and instrument_id is
//     unique only within a channel.
//  2. (day, source, channel_id) — collapse the vantages by MAX, not by sum. Several recorders
//     of one lane see one publisher, so the widest is the best observation of it and a lossy
//     recorder cannot inflate the day. Summing here would report the recording fan-out as
//     traffic, which is the same mistake FetchKalshiL2Coverage avoids by keeping the vantage in
//     its key.
//  3. (day) — sum the lanes. Different channels carry different instrument sets, so summing is
//     right at this step and only at this step.
func (a *API) FetchKalshiL2Completeness(ctx context.Context) (*KalshiL2CompletenessResponse, error) {
	exists, err := a.kalshiL2TableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return emptyKalshiL2Completeness(), nil
	}

	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		WITH per_vantage AS (
			SELECT
				toDate(fromUnixTimestamp64Nano(toInt64(recv_ts_ns))) AS day,
				source,
				channel_id,
				measurement_node_id,
				count() AS messages,
				uniqExact(instrument_id) AS instruments,
				uniqExactIf(instrument_id, status_after = 'gap') AS gapped,
				uniqExactIf(instrument_id, msg_type = 'snapshot_end') AS anchored,
				min(recv_ts_ns) AS first_ns,
				max(recv_ts_ns) AS last_ns
			FROM %[1]s.kalshi_mbp_levels
			-- toStartOfDay() returns DateTime, and toUnixTimestamp64Nano() rejects it, so the
			-- start of the window is widened back to DateTime64 before it is converted.
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(toDateTime64(
				toStartOfDay(now64(9)) - toIntervalDay(%[2]d - 1), 9)))
			GROUP BY day, source, channel_id, measurement_node_id
		),
		per_lane AS (
			SELECT
				day,
				source,
				channel_id,
				max(messages) AS messages,
				max(instruments) AS instruments,
				max(gapped) AS gapped,
				-- anchored <= instruments holds per vantage, so the max of each keeps the
				-- difference below non-negative. greatest() guards it anyway: the two maxima
				-- can come from different vantages.
				max(anchored) AS anchored,
				min(first_ns) AS first_ns,
				max(last_ns) AS last_ns
			FROM per_vantage
			GROUP BY day, source, channel_id
		)
		SELECT
			day,
			uniqExact((source, channel_id)) AS lanes,
			-- The output aliases carry a suffix on purpose: an alias that repeats the column
			-- it aggregates resolves to itself here, and ClickHouse rejects the query as an
			-- aggregate inside an aggregate.
			sum(messages) AS messages_total,
			sum(instruments) AS instruments_total,
			sum(gapped) AS gapped_total,
			sum(greatest(toInt64(instruments) - toInt64(anchored), 0)) AS unanchored_total,
			min(first_ns) AS first_ns,
			max(last_ns) AS last_ns,
			groupUniqArrayIf(source, gapped > 0) AS gap_sources
		FROM per_lane
		GROUP BY day
		ORDER BY day DESC`, db, kalshiL2CompletenessDays)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resp := emptyKalshiL2Completeness()
	for rows.Next() {
		var (
			d          KalshiL2Day
			day        time.Time
			unanchored int64
			firstNs    uint64
			lastNs     uint64
			gapSources []string
		)
		if err := rows.Scan(
			&day, &d.Lanes, &d.Messages, &d.Instruments, &d.GappedInstruments,
			&unanchored, &firstNs, &lastNs, &gapSources,
		); err != nil {
			return nil, err
		}
		d.Day = day.Format(time.DateOnly)
		d.UnanchoredInstruments = uint64(unanchored)
		d.FirstMessage = time.Unix(0, int64(firstNs)).UTC()
		d.LastMessage = time.Unix(0, int64(lastNs)).UTC()
		d.GapLanes = make([]string, 0, len(gapSources))
		for _, s := range gapSources {
			label, _, _ := kalshiL2LaneFor(s)
			d.GapLanes = append(d.GapLanes, label)
		}
		resp.Days = append(resp.Days, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetKalshiL2Completeness serves the per-day completeness view.
//
// Cache-first, and the cache is the normal path: the live query rescans the whole window. See
// kalshiL2CompletenessDays.
func (a *API) GetKalshiL2Completeness(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), kalshiL2CompletenessCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	resp, err := a.FetchKalshiL2Completeness(ctx)
	if err != nil {
		logError("KalshiL2Completeness error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
