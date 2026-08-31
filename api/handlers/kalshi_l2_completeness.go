package handlers

import (
	"context"
	"encoding/json"
	"errors"
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
const KalshiL2CompletenessCacheKey = "kalshi_l2_completeness:v2"

// kalshiL2CompletenessDays is how many days back the view reports, today included.
//
// It is 7 because feeds.kalshi_mbp_levels carries `TTL toDate(recv_ts_ns) + toIntervalDay(7)`,
// so seven days is the whole record that exists. The two numbers have to stay equal: reporting
// a longer window renders rows for days the capture already deleted, which reads as "we never
// recorded it" rather than "it is gone", and reporting a shorter one hides days that are still
// sellable. If the capture's TTL moves, move this with it.
//
// The window is never read in one query. Sports recording is on (malbeclabs/infra#2309, cmh,
// perps and all thirty-one sports lanes since 2026-08-18), that roster is 27.46 Mbps on the
// wire, and the window holds ~90B rows. Measured on prod: a single-column count() over the
// window read 215 GB in 13.2s (2026-08-24), but the real query over all seven partitions ran
// past 250s and was killed (2026-08-25) — the cost is the per-book GROUP BY, not the scan. So
// the unit of work is one day, which is one partition and one query, and a refresh reads three
// of them. See completenessScanDays and mergeKalshiL2Days.
const kalshiL2CompletenessDays = 7

// kalshiL2CompletenessLiveDays is how many of the newest days every refresh re-reads: today
// and yesterday. Both can still gain rows — a recorder catching up, a replay written late —
// so neither is settled enough to carry forward.
//
// The rest of the window is covered by rotation, one day per refresh, so nothing in it is ever
// frozen: see completenessScanDays.
const kalshiL2CompletenessLiveDays = 2

// completenessLiveTimeout bounds a collapsed live read, which is the whole window a day at a
// time. It is set from what the work costs rather than from a browser's patience: the scan is
// the same one either way, and a caller that gives up first leaves it running for the next.
//
// It is longer than the refresher's own budget on purpose. The refresher reads three days per
// pass and builds the window up over several passes; a live read has no earlier payload to
// carry forward, so it has to read every day of the window itself, and it stops short rather
// than overrunning this.
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

	// GapMessages is how many of those rows were recorded while their own book was out of
	// sync, so they cannot be applied to a replay. Over Messages it is the share of the day's
	// record that is unusable.
	//
	// This is the one place a gap is counted in messages rather than in books, and it does not
	// contradict the rule recorded on KalshiL2Lane.GapMessages. What that rule rejects is a
	// message count used as a fault MAGNITUDE, where it is a recovery duration scaled by
	// traffic and makes a busy lane look thousands of times worse at identical loss. As a
	// share of the same lane's own traffic it is scale-free, and it is the only figure here
	// that says how much of a day was lost rather than how many books were touched.
	//
	// It exists because GappedInstruments on its own misleads, and by a lot. Measured on
	// mainnet for 2026-08-24: 40.4% of books were touched by a gap and 0.300% of the record
	// was lost, so the day is wide-and-shallow — a book loses ~700 messages out of ~94,000.
	// The book count alone reads as "the day is 41% broken" when the day is 99.7% intact.
	// Neither number is the answer on its own, so the page carries both.
	GapMessages uint64 `json:"gap_messages"`

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

// fetchKalshiL2CompletenessDay aggregates one day of captured level data.
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
// One day per call, and nil when the day recorded nothing. The day is a whole partition, which
// is what bounds the GROUP BY at step 1 — the step that decides what this query costs.
func (a *API) fetchKalshiL2CompletenessDay(ctx context.Context, day string) (*KalshiL2Day, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		WITH per_book_vantage AS (
			SELECT
				toDate(fromUnixTimestamp64Nano(toInt64(recv_ts_ns))) AS day,
				measurement_node_id,
				source,
				channel_id,
				instrument_id,
				count() AS messages,
				countIf(status_after = 'gap') AS gap_messages,
				max(status_after = 'gap') AS gapped,
				max(msg_type = 'snapshot_end') AS anchored,
				min(recv_ts_ns) AS first_ns,
				max(recv_ts_ns) AS last_ns
			FROM %[1]s.kalshi_mbp_levels
			-- One day, written as the table's own partition expression so it prunes to one
			-- partition and reads nothing else. A range over the window would prune the same
			-- rows but build one hash table over all of them at once, which is the thing this
			-- query cannot afford.
			WHERE toDate(fromUnixTimestamp64Nano(toInt64(recv_ts_ns))) = toDate('%[2]s')
			-- The key order follows the table's own sort key,
			-- (measurement_node_id, source, channel_id, symbol, instrument_id, recv_ts_ns),
			-- so ClickHouse can aggregate in order over the (node, source, channel) prefix and
			-- hold a hash table for one channel's instruments at a time. Key order carries no
			-- meaning in a GROUP BY, but it decides this: leading with source instead reads the
			-- same rows and then builds one hash table over every distinct
			-- (node, source, channel, instrument) in the window at once. symbol is not in the
			-- key, so the in-order run stops at channel_id, which is far enough.
			GROUP BY day, measurement_node_id, source, channel_id, instrument_id
		),
		per_book AS (
			SELECT
				day,
				source,
				channel_id,
				instrument_id,
				-- max over the vantages, with no span test: a snapshot is an instant and not a
				-- span, so any recorder's snapshot is a place a replay can start.
				max(anchored) AS anchored,
				-- The widest vantage of this book, not the sum: several recorders of one lane
				-- are several observations of one publisher, so summing would report the
				-- recording fan-out as traffic.
				max(messages) AS messages,
				-- The least lossy vantage's loss, to pair with the most complete vantage's
				-- record above. Both are "best available", which is how a replay reads a book:
				-- it takes it from whichever recorder holds it best. They can be different
				-- recorders, so the ratio these two feed is a best case and not one vantage's
				-- record. Measured at three parts in a thousand of a day, with one recorder on
				-- every sports lane, that is below anything the page distinguishes.
				min(gap_messages) AS gap_messages,
				-- The book's whole life, over every vantage.
				min(first_ns) AS book_first_ns,
				max(last_ns) AS book_last_ns,
				-- A recorder clears this book's gap only if its own rows span the book's whole
				-- life. One that died before the gap, or joined after it, carries gapped = 0
				-- because it was not there to see it, and letting absence veto another
				-- recorder's marked gap calls a day replayable when nothing holds it end to
				-- end. So the clean vantages are carried up as (start, -end) under one min():
				-- that picks the earliest start and, among those, the latest end, and a
				-- covering vantage has to hold both ends of the book, so if one exists it is
				-- this one. The span test is settled a level up, where these are plain columns.
				--
				-- Two consequences worth naming. A lane with one recorder still reads on that
				-- recorder's own record, since a single vantage covers itself and its downtime
				-- is not in the data. And two clean part-day recorders that between them cover
				-- the book still read gapped: stitching a replay across them would be sound
				-- only if their spans met with no hole, which min and max over the pair cannot
				-- tell apart from a hole. This view errs towards unsellable.
				countIf(gapped = 0) AS clean_vantages,
				minIf((first_ns, -toInt64(last_ns)), gapped = 0) AS widest_clean
			FROM per_book_vantage
			GROUP BY day, source, channel_id, instrument_id
		),
		per_lane AS (
			SELECT
				day,
				source,
				channel_id,
				count() AS instruments,
				countIf(
					clean_vantages = 0
					OR widest_clean.1 > book_first_ns
					OR -widest_clean.2 < book_last_ns
				) AS gapped,
				countIf(anchored = 0) AS unanchored,
				sum(messages) AS messages,
				sum(gap_messages) AS gap_messages,
				min(book_first_ns) AS first_ns,
				max(book_last_ns) AS last_ns
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
			sum(gap_messages) AS gap_messages_total,
			sum(instruments) AS instruments_total,
			sum(gapped) AS gapped_total,
			sum(unanchored) AS unanchored_total,
			min(first_ns) AS first_ns,
			max(last_ns) AS last_ns,
			groupUniqArrayIf(source, gapped > 0) AS gap_sources
		FROM per_lane
		GROUP BY day
		SETTINGS optimize_aggregation_in_order = 1`, db, day)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out *KalshiL2Day
	for rows.Next() {
		var (
			d          KalshiL2Day
			scanned    time.Time
			firstNs    uint64
			lastNs     uint64
			gapSources []string
		)
		if err := rows.Scan(
			&scanned, &d.Lanes, &d.Messages, &d.GapMessages, &d.Instruments,
			&d.GappedInstruments, &d.UnanchoredInstruments, &firstNs, &lastNs, &gapSources,
		); err != nil {
			return nil, err
		}
		d.Day = scanned.Format(time.DateOnly)
		d.FirstMessage = time.Unix(0, int64(firstNs)).UTC()
		d.LastMessage = time.Unix(0, int64(lastNs)).UTC()
		d.GapLanes = make([]string, 0, len(gapSources))
		for _, s := range gapSources {
			label, _, _ := kalshiL2LaneFor(s)
			d.GapLanes = append(d.GapLanes, label)
		}
		out = &d
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// FetchKalshiL2Completeness reads the newest `days` days, one query per day, newest first.
//
// days is the scan's width, not the view's: the returned DayCount always reports the window the
// page shows, because a partial payload is only ever an argument to mergeKalshiL2Days and is
// never served on its own.
func (a *API) FetchKalshiL2Completeness(ctx context.Context, days int) (*KalshiL2CompletenessResponse, error) {
	list := make([]string, 0, days)
	now := time.Now().UTC()
	for i := range days {
		list = append(list, now.AddDate(0, 0, -i).Format(time.DateOnly))
	}
	return a.fetchKalshiL2CompletenessDays(ctx, list)
}

// fetchKalshiL2CompletenessDays scans the named days in order and stops before a day the
// previous day's cost predicts will not fit the caller's deadline — or at the day the deadline
// actually cut off, since that estimate misses when day sizes are skewed (a thin "today"
// predicting a full Friday).
//
// It stops rather than failing because the caller always merges: the days already kept are
// written and the rest wait for the next pass. A pass holding no days still fails, keeping a
// genuinely stuck entry visible to the escalator — an empty day completes a query but keeps
// nothing, so it cannot license writing (and serving) an empty payload.
func (a *API) fetchKalshiL2CompletenessDays(ctx context.Context, days []string) (*KalshiL2CompletenessResponse, error) {
	exists, err := a.kalshiL2TableExists(ctx)
	if err != nil {
		return nil, err
	}
	resp := emptyKalshiL2Completeness()
	if !exists {
		return resp, nil
	}

	var last time.Duration
	for i, day := range days {
		if i > 0 {
			if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < last {
				break
			}
		}
		start := time.Now()
		d, err := a.fetchKalshiL2CompletenessDay(ctx, day)
		if err != nil {
			// The estimate missed: keep what was kept, fail with nothing to keep.
			if len(resp.Days) > 0 && errors.Is(err, context.DeadlineExceeded) {
				logWarn("kalshi l2 completeness pass truncated", "cut_day", day, "kept_days", len(resp.Days), "error", err)
				break
			}
			return nil, err
		}
		last = time.Since(start)
		if d != nil {
			resp.Days = append(resp.Days, *d)
		}
	}
	sort.Slice(resp.Days, func(i, j int) bool { return resp.Days[i].Day > resp.Days[j].Day })
	return resp, nil
}

// completenessMergeBase returns the cached payload when a refresh may carry days forward from
// it, and nil when it may not.
//
// Three things disqualify it:
//
//   - No row at all: a cold Postgres, or a bumped cache key. There is nothing to carry forward.
//   - Older than a day: the refresh reads three days and carries the rest, so a base this stale
//     would hand days forward that nothing has re-read since. Better to report the days this
//     pass read and let the rotation fill the rest in.
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

// completenessScanDays is the list of days one refresh reads, newest first: today and
// yesterday, plus one older day of the window on rotation.
//
// A fixed three days per pass is what keeps this inside a bounded activity budget whatever the
// table's size, because the window is never read in one query (see kalshiL2CompletenessDays).
// The cost of that is that a cold cache reports three days and fills the window over the next
// few hours, which is the right way round: a payload that grows is readable at every step,
// where one pass that has to read everything either fits or produces nothing.
//
// The rotation replaces the older design's frozen tail. Every day in the window is re-read
// every kalshiL2CompletenessDays - kalshiL2CompletenessLiveDays passes, so a row that was wrong
// when it was written heals in hours rather than waiting out the capture's TTL.
//
// It is derived from the clock and not stored. Nothing has to be kept in the payload, and the
// two API replicas pick the same day within an hour of each other, so neither can spend its
// pass on a day the other just read.
func completenessScanDays(now time.Time) []string {
	now = now.UTC()
	days := make([]string, 0, kalshiL2CompletenessLiveDays+1)
	for i := range kalshiL2CompletenessLiveDays {
		days = append(days, now.AddDate(0, 0, -i).Format(time.DateOnly))
	}
	rotating := kalshiL2CompletenessDays - kalshiL2CompletenessLiveDays
	back := kalshiL2CompletenessLiveDays + int(now.Unix()/int64(time.Hour/time.Second))%rotating
	return append(days, now.AddDate(0, 0, -back).Format(time.DateOnly))
}

// RefreshKalshiL2Completeness computes the payload the cache entry should hold. The page-cache
// worker calls it and writes what it returns (api/worker, heavyEntries).
//
// It reads three days and carries the rest forward from the previous payload. A deploy does not
// reset that: page_cache lives in Postgres and outlives the process, so the entry keeps its base
// across a restart.
func (a *API) RefreshKalshiL2Completeness(ctx context.Context) (*KalshiL2CompletenessResponse, error) {
	base := a.completenessMergeBase(ctx)
	fresh, err := a.fetchKalshiL2CompletenessDays(ctx, completenessScanDays(time.Now()))
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
