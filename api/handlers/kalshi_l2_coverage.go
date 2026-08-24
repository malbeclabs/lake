package handlers

import (
	"context"
	"fmt"
	"log/slog"
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
//
// The key carries a version, and it MUST be bumped in the same commit as any change to the
// shape of the payload. The entry lives in Postgres and so outlives the deploy: without a bump
// the new bundle reads a row written by the old binary, and the table dereferences the fields
// it expects unguarded (lane.gap_books.toLocaleString()), which takes the whole SPA to the
// app-wide error boundary rather than this one page. Nothing ages the row out either — only
// the 10-minute background refresher rewrites it, last of its four refreshes, and it skips the
// write entirely while the L2 query is failing, so a stale-shaped row survives for exactly as
// long as the view it monitors is unhealthy. A miss falls through to the live query below.
//
// v2: `gaps` split into `gap_messages`/`gap_books`, `messages` added.
// v3: `publisher_source_ip` added, and the lane grain is the channel instance with it.
// v4: `gap_episodes` added, the per-instance loss timeline.
// v5: the per-instrument sequence loss counters added.
const kalshiL2CoverageCacheKey = "kalshi_l2_coverage:v5"

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

// kalshiL2GapSecondsCap bounds the array groupUniqArray builds per channel instance. One entry
// per WHOLE SECOND in which the instance recorded a gap-marked message, so the window's own
// second count is the true ceiling rather than an arbitrary truncation: a path losing data for
// every second of the window fills it exactly and drops nothing.
const kalshiL2GapSecondsCap = kalshiL2WindowMinutes * 60

// KalshiL2GapEpisode is one contiguous run of seconds in which a channel instance recorded
// gap-marked messages: when the loss started and how long it went on.
//
// This is the unit a timeline has to be drawn in, and neither counter on the lane can stand in
// for it. GapMessages is a duration that scales with traffic (see KalshiL2Lane). GapBooks
// SATURATES: perps carries 13 instruments and a lost datagram on the delta port un-anchors most
// of them at once, so it pins at 13/13 after a single episode and reads as total failure where
// the truth was ~10 losses of 3-8 seconds each. Measured 2026-08-24 on mainnet. Feeds with
// hundreds of instruments do not saturate, which is exactly why the count cannot be compared
// across feeds and the episode can.
//
// The (start, seconds) run encoding is LOSSLESS over whole seconds — the set it came from is
// recoverable by expanding each run. That is deliberate: asking "were BOTH paths of this feed
// losing in the same second", the only question whose answer means the feed itself lost data,
// is an intersection over those sets, and it stays computable from the cached payload without
// going back to the table.
type KalshiL2GapEpisode struct {
	// Start is the first second of the run, Unix seconds UTC. Not a formatted time: the
	// consumer draws it on an axis, and the payload's own window is the frame.
	Start int64 `json:"start"`

	// Seconds is the run length, never zero — a one-second episode is {Start, 1}.
	Seconds uint32 `json:"seconds"`
}

// collapseKalshiL2GapSeconds folds a set of seconds into contiguous runs.
//
// It sorts in place: groupUniqArray returns no order, and two adjacent seconds that arrive
// apart would otherwise become two episodes instead of one. Duplicates are tolerated rather
// than assumed away — the aggregate dedups today, and a caller that changes to groupArray
// should not silently start emitting zero-length runs.
func collapseKalshiL2GapSeconds(secs []uint32) []KalshiL2GapEpisode {
	if len(secs) == 0 {
		return nil
	}
	sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })

	out := make([]KalshiL2GapEpisode, 0, 8)
	start, prev := secs[0], secs[0]
	flush := func() {
		out = append(out, KalshiL2GapEpisode{Start: int64(start), Seconds: prev - start + 1})
	}
	for _, s := range secs[1:] {
		switch {
		case s == prev:
		case s == prev+1:
			prev = s
		default:
			flush()
			start, prev = s, s
		}
	}
	flush()
	return out
}

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

	// PublisherSourceIP is the tunnel address these datagrams were sent from, and it is what
	// makes this row a CHANNEL INSTANCE rather than a channel: a sequence series, a `Reset
	// Count` and a snapshot cycle are owned by "one path's view of one channel, keyed (source
	// IP address, Channel ID, destination port)", and a subscriber MUST key gap detection on
	// that rather than on the channel (edge-feed-spec/GLOSSARY.md, Transport). Empty on an
	// unseen lane, which was heard from nowhere, and empty on a row the capture wrote before
	// the column existed.
	PublisherSourceIP string `json:"publisher_source_ip"`

	// MeasurementNodeID completes this row's identity. A lane recorded from several
	// vantages is several rows, one per vantage — see the GROUP BY in
	// FetchKalshiL2Coverage. location_code alone is not a key: two recorders can share a
	// metro, and the rates would merge with no way to see it had happened.
	MeasurementNodeID string `json:"measurement_node_id"`

	// Messages is the raw count over the coverage window, from which MessagesPerSec is
	// derived. It is emitted because it is the denominator GapMessages is only meaningful
	// against: a gap-marked count means nothing without the total it is a share of, and
	// reconstructing that total from the rate makes the caller agree with this handler
	// about the window length.
	Messages uint64 `json:"messages"`

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
	//
	// **GapMessages is a DURATION, not a fault count, and must never be displayed as one.**
	// It counts every message that arrived while its book was un-anchored, so one gap
	// event inflates it by the message rate times the seconds until the next snapshot
	// re-anchors the book. Measured on perps: 22 real discontinuities in five minutes
	// produced 158,912 gap-marked messages — a factor of ~2,400. Worse than the size, it
	// scales with TRAFFIC rather than with reliability, so a busy lane looks thousands of
	// times worse than a quiet one at identical loss, which is exactly the comparison a
	// coverage page exists to support.
	//
	// GapBooks is the count to show: how many distinct books gapped at all in the window.
	// It is one number per affected instrument no matter how long the recovery took, and
	// it does not move with the message rate.
	//
	// Neither is the transition count, which is what a true event count would be. That
	// needs a per-instrument window function over every row in the window (~135M at
	// current rates), and this query is already the heavy one the background refresher
	// exists to keep off the request path. GapBooks is the cheap upper-bound-per-book
	// approximation: it undercounts a book that gapped repeatedly, and that is the
	// deliberate trade.
	GapMessages    uint64 `json:"gap_messages"`
	GapBooks       uint64 `json:"gap_books"`
	Resets         uint64 `json:"resets"`
	Clears         uint64 `json:"clears"`
	SnapshotCycles uint64 `json:"snapshot_cycles"`

	// GapEpisodes is the same loss, on a time axis: the contiguous runs of seconds this
	// instance was recording gap-marked messages. Empty on a clean instance, and empty on an
	// unseen one — see KalshiL2GapEpisode for why the counters above cannot be drawn instead.
	GapEpisodes []KalshiL2GapEpisode `json:"gap_episodes,omitempty"`

	// The per-instrument sequence loss counters: how many delta updates never arrived, measured
	// on the ONLY counter in this schema that can answer it.
	//
	// `per_instrument_seq` is dense per (path, instrument, reset generation), so a hole in it is
	// an update that did not arrive. Two other candidates were measured and rejected:
	//
	//   - `frame_sequence` looks like the obvious one and is not. It is dense per (publisher,
	//     channel, port), but measured on mainnet its holes are IDENTICAL on both redundant paths
	//     of every sports feed — 193 against 192, 180 against 180 — and two independent paths do
	//     not lose the same datagrams. Whatever those holes are, they are a property of the
	//     numbering. Reading them as loss reports ~12% on 29 healthy sports feeds.
	//   - `status_after = 'gap'`, which the timeline is built on, counts TIME a book spent
	//     un-anchored rather than updates lost, and has no denominator at all.
	//
	// The per-instrument counter passes the test the other two fail: golf reads zero on both
	// paths where frame_sequence claimed 11.6%, and the paths DIFFER wherever there is loss —
	// perps at 36 against 9 over the same window — which is what independent per-path loss looks
	// like.
	//
	// UpdatesReceived is the denominator and the two are only meaningful together: expected is
	// received + missing, and a lane with no updates at all has no rate rather than a rate of
	// zero. Level updates only — snapshot messages carry no per-instrument sequence to lose.
	UpdatesReceived uint64 `json:"updates_received"`
	UpdatesMissing  uint64 `json:"updates_missing"`

	// SeqGapEvents is the number of discontinuities, which is what a "gaps per hour" is built
	// from. It is NOT the episode count on the timeline: an episode is a stretch of wall-clock
	// time a book was un-anchored, this is a break in the numbering, and one break can leave a
	// book un-anchored for seconds while a whole burst of them lands inside one episode.
	SeqGapEvents uint64 `json:"seq_gap_events"`

	// The shape of the losses, in messages. MaxGapMessages is the worst single break and
	// P99GapMessages the same figure with one outlier unable to speak for the window. Measured
	// on mainnet these run 1-6 and 1-5.4 respectively; a sudden order-of-magnitude change is the
	// signal, not the absolute number.
	MaxGapMessages uint32  `json:"max_gap_messages"`
	P99GapMessages float64 `json:"p99_gap_messages"`

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

// kalshiL2LaneKey identifies one channel instance at one vantage: the grain both queries in this
// file group by, and the join key between them.
type kalshiL2LaneKey struct {
	source    string
	channelID uint8
	publisher string
	node      string
}

// kalshiL2SequenceLoss is what the loss query returns for one channel instance.
type kalshiL2SequenceLoss struct {
	received uint64
	missing  uint64
	events   uint64
	maxGap   uint32
	p99Gap   float64
}

// fetchKalshiL2SequenceLoss measures update loss from the per-instrument sequence.
//
// A SECOND query rather than more aggregates on the first, because the two do not share a shape:
// this one filters to level updates, needs a window function to see each instrument's numbering in
// order, and would otherwise drag the depth and lifecycle counters through a sort they do not need.
// Measured on mainnet it costs 1.5s against the coverage query's 1.8s, on the same ten-minute
// refresher — cheap enough not to be worth folding them into one unreadable pass.
//
// Two things in the PARTITION BY are load-bearing:
//
//   - instrument_id, because the counter is per book. Without it the numbering of thirteen books
//     interleaves into one sequence full of holes that are not losses.
//   - reset_count, because a reset starts the numbering again. Without it the step back to zero
//     reads as a jump of the whole sequence space.
//
// The delta is taken in Int64 on purpose. per_instrument_seq is UInt32 and an unsigned subtraction
// underflows to ~4 billion the moment a value goes backwards, which would then be counted as four
// billion missing messages. In Int64 a backwards step is negative, fails the d > 1 test, and is
// dropped — which is the right answer for a reset the partition key did not already separate.
func (a *API) fetchKalshiL2SequenceLoss(ctx context.Context) (map[kalshiL2LaneKey]kalshiL2SequenceLoss, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT
			source,
			channel_id,
			publisher_source_ip,
			measurement_node_id,
			count() AS updates_received,
			toUInt64(sum(if(d > 1, d - 1, 0))) AS updates_missing,
			toUInt64(countIf(d > 1)) AS seq_gap_events,
			toUInt32(max(if(d > 1, d - 1, 0))) AS max_gap_messages,
			toFloat64(ifNotFinite(quantileIf(0.99)(d - 1, d > 1), 0)) AS p99_gap_messages
		FROM (
			SELECT
				source,
				channel_id,
				publisher_source_ip,
				measurement_node_id,
				toInt64(assumeNotNull(per_instrument_seq)) - lagInFrame(
					toInt64(assumeNotNull(per_instrument_seq)), 1,
					toInt64(assumeNotNull(per_instrument_seq))
				) OVER (
					PARTITION BY source, channel_id, publisher_source_ip,
						measurement_node_id, instrument_id, reset_count
					ORDER BY per_instrument_seq
				) AS d
			FROM %[1]s.kalshi_mbp_levels
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[2]d)))
			  AND msg_type = 'level_update'
			  AND per_instrument_seq IS NOT NULL
		)
		GROUP BY source, channel_id, publisher_source_ip, measurement_node_id`, db, kalshiL2WindowMinutes)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[kalshiL2LaneKey]kalshiL2SequenceLoss{}
	for rows.Next() {
		var k kalshiL2LaneKey
		var v kalshiL2SequenceLoss
		if err := rows.Scan(&k.source, &k.channelID, &k.publisher, &k.node,
			&v.received, &v.missing, &v.events, &v.maxGap, &v.p99Gap); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// FetchKalshiL2Coverage aggregates the market-by-price lanes over the coverage window.
//
// Grouped by (source, channel_id, publisher_source_ip, measurement_node_id), and each of the four
// is load-bearing:
//
//   - never by source alone: prod's two publisher arms share one multicast group and one port
//     triple, and instrument_id is unique only within a channel. Collapsing the arms would merge
//     two independent delta streams and double-count their instruments.
//   - never without publisher_source_ip: that is the field the wire protocol says owns the
//     sequence series, and channel_id only happens to separate the arms today. It is the arm
//     axis by design in the capture schema (kalshi-capture's 20260805000001_mbp_levels.sql), so
//     keying on it means a deployment that puts both arms on one channel_id — the perps collapse
//     to a single id, settled in malbeclabs/kalshi#86 — stays two rows here instead of folding
//     into one whose gap count belongs to neither publisher.
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
// The three port roles of one channel are deliberately NOT in the key. Only `Sequence Number` is
// per port; `Reset Count`, `Manifest Seq` and the channel state they govern span the three ports
// one publisher serves a channel on, and this view carries those recovery counters rather than
// raw sequence numbers — splitting by port would scatter a book's gap, its reset and its snapshot
// cycle across three rows that each look like they are missing something.
//
// publisher_source_ip costs one more column read and one more GROUP BY key over rows this query
// already scans; it adds no scan and no pruning. It is NOT in the table's sort key — the header
// on 20260805000001_mbp_levels.sql says it is, and the ORDER BY there does not carry it — so it
// could not prune anything even if a predicate were put on it. Nothing here filters on it.
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
			publisher_source_ip,
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
			countIf(status_after = 'gap') AS gap_messages,
			-- Distinct books affected, which is what the page shows. instrument_id is unique
			-- only within a channel and this groups by channel, so the count is well defined.
			uniqCombinedIf(instrument_id, status_after = 'gap') AS gap_books,
			-- The same loss on a time axis, sparse: one entry per whole second that carried a
			-- gap-marked message. One more aggregate over rows this query already scans, so it
			-- adds no scan and no round trip — measured at 1.8s for every source on mainnet,
			-- against 3.8s for the top-of-book leg that runs beside it. Sparse because loss is
			-- rare: the worst instance on the fleet held 89 of 900 seconds.
			groupUniqArrayIf(%[3]d)(toUInt32(intDiv(recv_ts_ns, 1000000000)), status_after = 'gap') AS gap_seconds,
			countIf(msg_type = 'instrument_reset') AS resets,
			countIf(msg_type = 'book_clear') AS clears,
			countIf(msg_type = 'snapshot_end') AS snapshot_cycles,
			max(recv_ts_ns) AS last_recv_ts_ns
		FROM %[1]s.kalshi_mbp_levels
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[2]d)))
		GROUP BY source, channel_id, publisher_source_ip, measurement_node_id`, db, kalshiL2WindowMinutes, kalshiL2GapSecondsCap)

	// Before the scan, so a lane can be filled in as it is read. A failure here costs the loss
	// counters and not the page: they are additive to a view whose subject is coverage, and a lane
	// with a zero denominator reports no rate rather than a rate of zero.
	sequenceLoss, err := a.fetchKalshiL2SequenceLoss(ctx)
	if err != nil {
		slog.Warn("kalshi l2 coverage: sequence loss unavailable", "error", err)
		sequenceLoss = nil
	}

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
		var levelUpdates uint64
		var lastRecvNs uint64
		var gapSeconds []uint32
		if err := rows.Scan(
			&l.Source, &l.ChannelID, &l.PublisherSourceIP, &l.MeasurementNodeID, &l.LocationCode,
			&l.Messages, &levelUpdates, &l.Instruments,
			&l.DepthP50, &l.DepthP95, &l.DepthMax,
			&l.GapMessages, &l.GapBooks, &gapSeconds, &l.Resets, &l.Clears, &l.SnapshotCycles,
			&lastRecvNs,
		); err != nil {
			return nil, err
		}
		l.GapEpisodes = collapseKalshiL2GapSeconds(gapSeconds)
		l.MessagesPerSec = float64(l.Messages) / windowSecs
		l.LevelUpdatesPerSec = float64(levelUpdates) / windowSecs
		l.LastSeen = time.Unix(0, int64(lastRecvNs)).UTC()
		l.Seen = true
		if loss, ok := sequenceLoss[kalshiL2LaneKey{l.Source, l.ChannelID, l.PublisherSourceIP, l.MeasurementNodeID}]; ok {
			l.UpdatesReceived, l.UpdatesMissing = loss.received, loss.missing
			l.SeqGapEvents, l.MaxGapMessages, l.P99GapMessages = loss.events, loss.maxGap, loss.p99Gap
		}
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
	// share the fallback order, stay deterministic), then channel id and source address so the
	// arms of one lane sit together.
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
		if out[i].lane.PublisherSourceIP != out[j].lane.PublisherSourceIP {
			return out[i].lane.PublisherSourceIP < out[j].lane.PublisherSourceIP
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
