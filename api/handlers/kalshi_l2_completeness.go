package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
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

// KalshiL2CompletenessCacheKey is the page-cache key. It is refreshed by the page-cache worker
// (api/worker, heavyEntries) rather than by StartKalshiBackgroundRefresher, so the cadence is
// gated on this row's own updated_at and two API replicas cannot each refresh it.
//
// Bump the version in the same commit as any change to the payload shape. The entry lives in
// Postgres and outlives the deploy, so an unbumped key hands a new bundle a row written by the
// old binary — see the same note on kalshiL2CoverageCacheKey for what that costs.
const KalshiL2CompletenessCacheKey = "kalshi_l2_completeness:v1"

// kalshiL2CompletenessDays is how many days back the view reports, today included.
//
// It is 7 because feeds.kalshi_mbp_levels carries `TTL toDate(recv_ts_ns) + toIntervalDay(7)`,
// so seven days is the whole record that exists. The two numbers have to stay equal: reporting
// a longer window renders rows for days the capture already deleted, which reads as "we never
// recorded it" rather than "it is gone", and reporting a shorter one hides days that are still
// sellable. If the capture's TTL moves, move this with it.
//
// The scan is the expensive thing on this page: sports recording is on (malbeclabs/infra#2309,
// cmh, perps and all thirty-one sports lanes since 2026-08-18), that roster is 27.46 Mbps on the
// wire, and the window holds ~90B rows. Measured on prod 2026-08-24, a single-column count()
// over the window read 215 GB in 13.2s, and this query reads seven columns and can skip no
// granule. So a refresh reads only the days that can still change and carries the rest forward
// — see kalshiL2CompletenessRefreshDays and mergeKalshiL2Days.
const kalshiL2CompletenessDays = 7

// kalshiL2CompletenessRefreshDays is how many days an incremental refresh re-reads: today and
// the two before it. The days behind it are carried forward from the cached payload unchanged,
// which is what keeps the hourly cost at three partitions instead of seven.
//
// Two days behind today rather than one, because a day's partition can still gain rows after
// midnight — a recorder catching up, or a replay written late. Past that the row is frozen: a
// day older than the lookback is never re-read, so if it was wrong when it was written it stays
// wrong until the capture's 7-day TTL trims it off the end of the window. That is the trade the
// flat cost buys, and the TTL is what bounds it.
//
// A refresh that has no usable cached payload to merge onto scans the whole window instead —
// see completenessMergeBase.
const kalshiL2CompletenessRefreshDays = 3

// completenessLiveTimeout bounds a collapsed live scan. It matches the refresher's budget
// rather than a browser's patience: the scan is the same one either way, and a caller that
// gives up first leaves it running for the next.
const completenessLiveTimeout = 10 * time.Minute

// KalshiL2Day is one day of captured level data, aggregated across lanes.
type KalshiL2Day struct {
	// Day is the UTC calendar day, YYYY-MM-DD. It is the table's partition key, so it is also
	// the unit an export would ship.
	Day string `json:"day"`

	// Lanes is the count of distinct (source, channel_id) pairs that recorded anything.
	Lanes uint64 `json:"lanes"`

	// Instruments is summed across lanes, never counted across them: instrument_id is unique
	// only within a channel_id, so a global distinct count would merge two channels' books
	// into one number. Within a lane it is the distinct books seen by any vantage, not the sum
	// over vantages, because two recorders of one lane are two observations of one book set.
	Instruments uint64 `json:"instruments"`

	// GappedInstruments is how many books ran un-anchored at some point in the day, in every
	// vantage that recorded them. Any value above zero means a replay of that book has a hole
	// no recorder can fill, so the day is not clean. A book that one recorder gapped and
	// another held is not counted: the replay takes it from the recorder that held it.
	//
	// This is a count of books and not of gap events, for the reason recorded on
	// KalshiL2Lane.GapMessages: the message-grain count is a recovery duration scaled by
	// traffic, so it makes a busy lane look thousands of times worse at identical loss.
	GappedInstruments uint64 `json:"gapped_instruments"`

	// UnanchoredInstruments is how many books moved during the day with no snapshot_end inside
	// it in any vantage. A replay of those cannot start from this day's data alone; it has to
	// carry state in from an earlier day, which is what makes a single-day export of them
	// unusable on its own.
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
// Four grains, and the order of the roll-ups is the whole correctness argument:
//
//  1. (day, source, channel_id, measurement_node_id, instrument_id) — one row per book per
//     vantage. The fault flags are set here, where they describe one recorder's capture of one
//     book, which is the only grain at which they mean anything.
//  2. (day, source, channel_id, instrument_id) — collapse the vantages per book. A book counts
//     as gapped only when EVERY vantage gapped it, and as anchored when ANY vantage anchored
//     it, because a replay can take each book from whichever recorder captured it cleanly.
//     Collapsing faults at the lane grain instead would let one lossy redundant recorder mark a
//     lane incomplete that another recorder captured whole.
//  3. (day, source, channel_id) — count the books. Counting instruments any higher than this
//     merges channels, and instrument_id is unique only within a channel.
//  4. (day) — sum the lanes. Different channels carry different instrument sets, so summing is
//     right at this step and only at this step. Summing vantages is never right, which is why
//     they are gone by step 3.
//
// days is how far back to scan, today included. It is the scan's width, not the view's: the
// returned DayCount always reports the window the page shows, because a partial payload is only
// ever an argument to mergeKalshiL2Days and is never served on its own.
func (a *API) FetchKalshiL2Completeness(ctx context.Context, days int) (*KalshiL2CompletenessResponse, error) {
	exists, err := a.kalshiL2TableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return emptyKalshiL2Completeness(), nil
	}

	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		WITH per_book_vantage AS (
			SELECT
				toDate(fromUnixTimestamp64Nano(toInt64(recv_ts_ns))) AS day,
				source,
				channel_id,
				measurement_node_id,
				instrument_id,
				count() AS messages,
				max(status_after = 'gap') AS gapped,
				max(msg_type = 'snapshot_end') AS anchored,
				min(recv_ts_ns) AS first_ns,
				max(recv_ts_ns) AS last_ns
			FROM %[1]s.kalshi_mbp_levels
			-- toStartOfDay() returns DateTime, and toUnixTimestamp64Nano() rejects it, so the
			-- start of the window is widened back to DateTime64 before it is converted.
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(toDateTime64(
				toStartOfDay(now64(9)) - toIntervalDay(%[2]d - 1), 9)))
			GROUP BY day, source, channel_id, measurement_node_id, instrument_id
		),
		per_book AS (
			SELECT
				day,
				source,
				channel_id,
				instrument_id,
				-- min over the vantages: gapped only if no recorder held the book anchored.
				min(gapped) AS gapped,
				-- max over the vantages: one snapshot anywhere starts a replay.
				max(anchored) AS anchored,
				-- The widest vantage of this book, not the sum: several recorders of one lane
				-- are several observations of one publisher, so summing would report the
				-- recording fan-out as traffic.
				max(messages) AS messages,
				min(first_ns) AS first_ns,
				max(last_ns) AS last_ns
			FROM per_book_vantage
			GROUP BY day, source, channel_id, instrument_id
		),
		per_lane AS (
			SELECT
				day,
				source,
				channel_id,
				count() AS instruments,
				countIf(gapped > 0) AS gapped,
				countIf(anchored = 0) AS unanchored,
				sum(messages) AS messages,
				min(first_ns) AS first_ns,
				max(last_ns) AS last_ns
			FROM per_book
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
			sum(unanchored) AS unanchored_total,
			min(first_ns) AS first_ns,
			max(last_ns) AS last_ns,
			groupUniqArrayIf(source, gapped > 0) AS gap_sources
		FROM per_lane
		GROUP BY day
		ORDER BY day DESC`, db, days)

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
			firstNs    uint64
			lastNs     uint64
			gapSources []string
		)
		if err := rows.Scan(
			&day, &d.Lanes, &d.Messages, &d.Instruments, &d.GappedInstruments,
			&d.UnanchoredInstruments, &firstNs, &lastNs, &gapSources,
		); err != nil {
			return nil, err
		}
		d.Day = day.Format(time.DateOnly)
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

// completenessMergeBase returns the cached payload when an incremental refresh may build on
// it, and nil when the refresh has to scan the whole window instead.
//
// Three things disqualify it, and each one means the cheap scan would leave a hole rather than
// save work:
//
//   - No row at all: a cold Postgres, or a bumped cache key. There is nothing to carry forward.
//   - Older than a day: the partial scan starts at today-2, so any day between that and the
//     cached payload's newest was scanned by neither. The refresher runs hourly, so a cache
//     this old means the process was down, and a full scan is the answer that cannot be wrong.
//   - A different DayCount: the window constant moved without a key bump, so the cached days
//     describe a window this payload no longer reports.
func (a *API) completenessMergeBase(ctx context.Context) *KalshiL2CompletenessResponse {
	data, updatedAt, err := a.readPageCacheWithAge(ctx, KalshiL2CompletenessCacheKey)
	if err != nil {
		return nil
	}
	if time.Since(updatedAt) > 24*time.Hour {
		return nil
	}
	var cached KalshiL2CompletenessResponse
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil
	}
	if cached.DayCount != kalshiL2CompletenessDays {
		return nil
	}
	return &cached
}

// mergeKalshiL2Days lays a partial scan over the previous payload: a freshly scanned day
// replaces the cached one for the same date, and a cached day the scan did not cover carries
// forward unchanged. Newest first, like the query's own order.
//
// Both sides are filtered by date, not just trimmed by count. A day that has fallen out of the
// window is a day whose rows the capture has already deleted, and trimming by count alone would
// keep it whenever the payload holds fewer than a full window — leaving the page reporting a
// clean, "replayable" day whose data no longer exists. That is the one failure this view must
// not have, so the cutoff is applied to the fresh side too, cheap as that check is.
func mergeKalshiL2Days(fresh, cached *KalshiL2CompletenessResponse) *KalshiL2CompletenessResponse {
	cutoff := fresh.GeneratedAt.UTC().
		AddDate(0, 0, -(kalshiL2CompletenessDays - 1)).Format(time.DateOnly)

	out := &KalshiL2CompletenessResponse{
		GeneratedAt: fresh.GeneratedAt,
		DayCount:    kalshiL2CompletenessDays,
		Days:        make([]KalshiL2Day, 0, len(fresh.Days)+len(cached.Days)),
	}
	scanned := make(map[string]bool, len(fresh.Days))
	for _, d := range fresh.Days {
		scanned[d.Day] = true
		if d.Day >= cutoff {
			out.Days = append(out.Days, d)
		}
	}
	for _, d := range cached.Days {
		if !scanned[d.Day] && d.Day >= cutoff {
			out.Days = append(out.Days, d)
		}
	}
	sort.Slice(out.Days, func(i, j int) bool { return out.Days[i].Day > out.Days[j].Day })
	if len(out.Days) > kalshiL2CompletenessDays {
		out.Days = out.Days[:kalshiL2CompletenessDays]
	}
	return out
}

// RefreshKalshiL2Completeness computes the payload the cache entry should hold. The page-cache
// worker calls it and writes what it returns (api/worker, heavyEntries).
//
// It reads the previous payload first, because that decides how wide the scan has to be. Six of
// the seven partitions in the window are finished and immutable, so with a payload to carry them
// forward this reads three days instead of seven. Without one it reads the whole window — a
// first run, a bumped key, or a gap longer than a day. A deploy is NOT one of those: page_cache
// lives in Postgres and outlives the process, so the entry keeps its base across a restart.
func (a *API) RefreshKalshiL2Completeness(ctx context.Context) (*KalshiL2CompletenessResponse, error) {
	base := a.completenessMergeBase(ctx)
	days := kalshiL2CompletenessDays
	if base != nil {
		days = kalshiL2CompletenessRefreshDays
	}
	fresh, err := a.FetchKalshiL2Completeness(ctx, days)
	if err != nil {
		return nil, err
	}
	if base == nil {
		return fresh, nil
	}
	return mergeKalshiL2Days(fresh, base), nil
}

// GetKalshiL2Completeness serves the per-day completeness view.
//
// Cache-first in every environment, and the cache is the only path that should normally run:
// the live query rescans the whole window (see kalshiL2CompletenessDays). The cached row is not
// env-specific — envDB falls back to the mainnet connection for every other env, so a
// non-mainnet request would run the identical scan and get the identical answer. Gating the
// read on mainnet, as the coverage handler does, would send every staging and preview request
// live.
//
// The refresher fills the cache at process start, so a miss means no Postgres, a just-bumped
// key, or an unreadable row. Those still run live rather than returning empty, so local dev
// without Postgres shows real data, but they are collapsed: one scan serves every waiter.
func (a *API) GetKalshiL2Completeness(w http.ResponseWriter, r *http.Request) {
	if data, err := a.readPageCache(r.Context(), KalshiL2CompletenessCacheKey); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		_, _ = w.Write(data)
		return
	}
	w.Header().Set("X-Cache", "MISS")

	// Detached from the winning caller's context for the reason recorded on pubCheckSF: a
	// plain Do would let one caller's disconnect cancel the shared scan and 500 every waiter.
	ch := a.l2CompletenessSF.DoChan("", func() (any, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), completenessLiveTimeout)
		defer cancel()
		return a.FetchKalshiL2Completeness(ctx, kalshiL2CompletenessDays)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			logError("KalshiL2Completeness error", "error", res.Err)
			http.Error(w, res.Err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, res.Val.(*KalshiL2CompletenessResponse))
	case <-r.Context().Done():
	}
}
