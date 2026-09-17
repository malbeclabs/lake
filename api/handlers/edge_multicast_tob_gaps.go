package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// Recorded sequence gaps on the top-of-book plane, from the feed-race recorder's own grain.
//
// # Why this exists, and why it could not before
//
// The Sequence column has two legs. The market-by-price one counts a gap marker the recorder
// writes onto every message that arrived while a book was un-anchored (`kalshi_l2_coverage.go`,
// `status_after = 'gap'`). The top-of-book one could not: `edge_multicast_observations.go` folds
// `kalshi_bbo_observations`, which carries no such marker, so its series land with
// `GapsMeasured = false` and are graded on staleness alone. That file records what closing the
// half would take:
//
//	Closing the gap half needs the producer to emit a gap marker for top-of-book the way it
//	does for market-by-price, and that is not work this repository can do.
//
// `dz_kalshi_recorder` now emits one. `kalshi_edge_book_top` carries `uncertain_reason`, and a
// top lowered by a hole in the publisher's own sequence reads `gap` — the same fault the
// market-by-price leg counts, on the plane that had no way to count it.
//
// # The substitute this does NOT use
//
// Diffing sequence numbers against the row count is wrong on both top-of-book tables by
// construction: a row exists only where the top of the book CHANGED, so a wire message that did
// not move the BBO legitimately leaves a hole in the numbering. `edge_multicast_observations.go`
// measured it on mainnet — one instance carried 23,846 rows across a sequence span of 24,553,
// about 3% "missing" on a feed with nothing wrong with it. This counts the marker and never the
// span.
//
// # The grain, and why the group is a column rather than a convention
//
// One series is one channel instance seen by one recorder: (destination address, Channel ID,
// source IP address, recording node). The destination address is what makes it attributable
// without decoding a capture source name — the same key `resolveMulticastIP` already prefers,
// for the reason it gives: a name "is a convention that has been renamed once already".
//
// `kalshi_edge_book_top` gained that column in malbeclabs/kalshi#287 and the fleet was deployed
// 2026-09-16. **Rows older than that carry `0.0.0.0`**, the type default an `ALTER` gives a
// table's history, and this query excludes them: the address resolves to no group, so folding
// them would produce a series attributed to nothing while its own recorder already reports one
// that is. The filter is what keeps the two from appearing as one feed recorded twice.
const edgeMulticastTOBGapsCacheKey = "edge_multicast_tob_gaps:v1"

// EdgeMulticastTOBGapSeries is one channel instance's recorded gap counters over the window.
type EdgeMulticastTOBGapSeries struct {
	MulticastGroup    string `json:"multicast_group"`
	PublisherSourceIP string `json:"publisher_source_ip"`
	ChannelID         uint8  `json:"channel_id"`
	Node              string `json:"node"`
	LocationCode      string `json:"location_code,omitempty"`

	// Messages is the row count over the window: tops, not datagrams. It is the denominator
	// GapMessages is a rate against, and it is NOT comparable with the market-by-price leg's
	// count of the same name — that one is level-grain messages. Two planes, two denominators;
	// the rate each expresses is about its own plane.
	Messages uint64 `json:"messages"`

	// GapMessages is how many of those tops were lowered by a hole in the sequence, and
	// GapBooks how many distinct instruments were affected at all. The same pair, with the same
	// meanings, as the market-by-price leg — GapBooks is the fault count to show and GapMessages
	// is a duration that scales with traffic.
	GapMessages uint64 `json:"gap_messages"`
	GapBooks    uint64 `json:"gap_books"`

	// GapSeconds is the same loss on a time axis: one entry per whole second that carried a
	// gap-marked top. Sparse, because loss is rare.
	GapSeconds []uint32 `json:"gap_seconds,omitempty"`

	// Resets is how far the era advanced across the window, which is the recovery side: a
	// series with gaps and no reset is not re-anchoring.
	Resets   uint64    `json:"resets"`
	LastSeen time.Time `json:"last_seen"`
}

// EdgeMulticastTOBGapsResponse is the cached payload the page folds.
type EdgeMulticastTOBGapsResponse struct {
	GeneratedAt   time.Time                   `json:"generated_at"`
	WindowMinutes int                         `json:"window_minutes"`
	Series        []EdgeMulticastTOBGapSeries `json:"series"`
}

// FetchEdgeMulticastTOBGaps scans the recorder's top-of-book grain for the window.
//
// Probe-guarded like every other leg: the proxied table is absent in local dev and in every test
// that does not create it, and an absent table has to leave the rest of the page intact.
func (a *API) FetchEdgeMulticastTOBGaps(ctx context.Context) (*EdgeMulticastTOBGapsResponse, error) {
	out := &EdgeMulticastTOBGapsResponse{
		GeneratedAt:   time.Now().UTC(),
		WindowMinutes: edgeMulticastObservationsWindowMinutes,
		Series:        []EdgeMulticastTOBGapSeries{},
	}

	exists, err := a.kalshiTableExists(ctx, "kalshi_edge_book_top")
	if err != nil {
		return nil, err
	}
	if !exists {
		return out, nil
	}

	q := fmt.Sprintf(`
		SELECT
			toString(dst_addr) AS multicast_group,
			toString(source_addr) AS publisher_source_ip,
			channel_id,
			recorder AS node,
			any(site) AS location_code,
			count() AS messages,
			countIf(uncertain_reason = 'gap') AS gap_messages,
			-- Distinct books affected, which is what the page shows. instrument_id is unique
			-- only within a channel instance and this groups by one, so the count is well
			-- defined.
			uniqCombinedIf(instrument_id, uncertain_reason = 'gap') AS gap_books,
			-- One entry per whole second that carried a gap-marked top. One more aggregate over
			-- rows this query already scans, so it adds no scan and no round trip.
			groupUniqArrayIf(%[3]d)(toUInt32(toUnixTimestamp(recv_ts)), uncertain_reason = 'gap') AS gap_seconds,
			-- Cast the DIFFERENCE, not the operands: ClickHouse promotes UInt8 - UInt8 to a
			-- signed type so the result can go negative, and the scan into a uint64 then fails
			-- and takes the whole refresh with it. max >= min here by construction.
			toUInt64(max(reset_count) - min(reset_count)) AS resets,
			toUInt64(toUnixTimestamp64Nano(max(recv_ts))) AS last_recv_ts_ns
		FROM %[1]s.kalshi_edge_book_top
		WHERE recv_ts >= now64(9) - toIntervalMinute(%[2]d)
			-- Rows older than malbeclabs/kalshi#287's rollout, which an ALTER gave the type
			-- default. The address names no group, so a series folded from them would be
			-- attributed to nothing beside the one its own recorder already reports.
			AND dst_addr != toIPv4('0.0.0.0')
		GROUP BY multicast_group, publisher_source_ip, channel_id, node
		SETTINGS max_execution_time = 120, timeout_before_checking_execution_speed = 0`,
		"`"+a.FeedsDB+"`", edgeMulticastObservationsWindowMinutes, kalshiL2GapSecondsCap)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("edge_multicast_tob_gaps", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s EdgeMulticastTOBGapSeries
		var lastRecvNs uint64
		if err := rows.Scan(&s.MulticastGroup, &s.PublisherSourceIP, &s.ChannelID, &s.Node,
			&s.LocationCode, &s.Messages, &s.GapMessages, &s.GapBooks, &s.GapSeconds,
			&s.Resets, &lastRecvNs); err != nil {
			return nil, err
		}
		if lastRecvNs > 0 {
			s.LastSeen = time.Unix(0, int64(lastRecvNs)).UTC()
		}
		out.Series = append(out.Series, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// foldEdgeMulticastTOBGaps overlays the recorder's measured gap counters onto the top-of-book
// series, and returns the payload's own clock.
//
// **It REPLACES rather than appends**, which is the whole of the care in this function. The
// observations leg has already folded a series for the same channel instance from
// `kalshi_bbo_observations`, and the two describe one feed recorded by one process at one node:
// appending would double every top-of-book row on the page, and half of each pair would carry
// `GapsMeasured = false` and inflate `GapsUnmeasured` while the other half reported the gaps.
//
// The match is (source IP address, Channel ID, node), and it lands because the two tables agree
// on those three: `kalshi_edge_book_top.recorder` is the same host name as
// `kalshi_bbo_observations.measurement_node_id`, and `site` the same token as `location_code`.
//
// The replaced series' capture source name is carried over. Nothing in this payload knows it —
// the recorder's grain names a feed, not a capture source — and the page reads that field for
// the quiet-source demotion and for its own sort key, so inventing a new spelling here would
// take a series out of the group it has always been sorted with.
//
// A series with nothing to replace keeps an empty one, and the two rollups keyed on the capture
// source — the quiet-source demotion and the all-paths intersection — skip it for that reason. An
// empty name is not a bucket of its own: every unnamed series falls into the same one, so two
// unrelated markets at one node would be read as two paths of one capture source, and each of
// those rollups says in its own doc comment that this is the failure the source is in its key to
// prevent. Before this leg the case could not arise, because the only instances without a source
// were top-of-book ones and `GapsMeasured = false` already excluded them; this leg measures that
// plane, so the exclusion is now explicit in both places.
//
// The series itself is still folded and still rendered — it has counters, a verdict and a badge,
// and what it lacks is a name to be compared under. Today the case is rare (the capture records
// the same feeds at the same nodes, so there is almost always a series to replace); it becomes
// the normal case if the capture's top-of-book leg is ever retired, and at that point the group
// needs a capture-source key from the recorder's own grain rather than this fallback.
// The window travels back with the clock for the same reason the market-by-price leg's does: the
// episodes this leg folds are placed on an axis of (as-of - window, as-of], and the page draws no
// timeline at all when it has no width for one. Reading the width from the other leg's cache alone
// made a miss there erase these episodes too.
func (a *API) foldEdgeMulticastTOBGaps(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) (time.Time, int) {
	data, err := a.readPageCache(ctx, edgeMulticastTOBGapsCacheKey)
	if err != nil {
		return time.Time{}, 0
	}

	var payload EdgeMulticastTOBGapsResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast tob gaps: cache did not parse", "error", err)
		return time.Time{}, 0
	}

	mergeEdgeMulticastTOBGaps(captureSources, payload.Series, payload.GeneratedAt, out)
	return payload.GeneratedAt.UTC(), payload.WindowMinutes * 60
}

// mergeEdgeMulticastTOBGaps is the fold itself, without the cache read around it: pure, so the
// replacement rule above can be tested without a database or a page cache standing in the way.
func mergeEdgeMulticastTOBGaps(captureSources edgeMulticastCaptureSourceMap, series []EdgeMulticastTOBGapSeries, generatedAt time.Time, out map[string]*EdgeMulticastSequenceHealth) {
	for _, series := range series {
		groupPK := captureSources.resolveMulticastIP(series.MulticastGroup)
		if groupPK == "" {
			// An address no group on this page carries. Dropped rather than bucketed, the same
			// rule `resolve` documents: a series with no group is not a group.
			continue
		}
		if out[groupPK] == nil {
			out[groupPK] = &EdgeMulticastSequenceHealth{}
		}
		health := out[groupPK]

		inst := EdgeMulticastChannelInstance{
			PublisherSourceIP: series.PublisherSourceIP,
			ChannelID:         series.ChannelID,
			Node:              series.Node,
			LocationCode:      series.LocationCode,
			Messages:          series.Messages,
			GapMessages:       series.GapMessages,
			GapBooks:          series.GapBooks,
			GapEpisodes:       collapseKalshiL2GapSeconds(series.GapSeconds),
			Resets:            series.Resets,
			LastSeen:          series.LastSeen.UTC(),
			// The reading this leg exists to make. Graded on the gap count as well as on
			// staleness, which is what the observations leg could not do.
			Status:       edgeMulticastSequenceStatus(series.GapBooks, series.LastSeen, generatedAt),
			GapsMeasured: true,
		}

		// The match is chosen by the LOWEST capture source name among the candidates, not by
		// position. This payload arrives from a GROUP BY with no ORDER BY, so slice order is not
		// stable between refreshes, and "the first match" would move the recorder's counters from
		// one row to another across a poll of unchanged data.
		//
		// More than one candidate means the recorder's grain cannot resolve which series the
		// counters belong to: it groups by (group, publisher, channel, node) and knows nothing
		// about capture sources, so one row is all it can offer for however many the capture
		// split that key into. One is replaced and the rest keep GapsMeasured = false, which
		// understates the measurement and never invents one — writing the same aggregate onto
		// each would multiply this group's gap books by the number of candidates. It should not
		// happen (a capture source has its own channel id on the group), so the count is logged
		// rather than assumed away: that is the only way the assumption is falsifiable from
		// production.
		match, candidates := -1, 0
		for i, existing := range health.Instances {
			if existing.PublisherSourceIP != inst.PublisherSourceIP ||
				existing.ChannelID != inst.ChannelID ||
				existing.Node != inst.Node {
				continue
			}
			candidates++
			if match < 0 || existing.CaptureSource < health.Instances[match].CaptureSource {
				match = i
			}
		}
		if candidates > 1 {
			slog.Warn("edge multicast tob gaps: channel instance matches several capture sources",
				"multicast_group", series.MulticastGroup, "publisher_source_ip", series.PublisherSourceIP,
				"channel_id", series.ChannelID, "node", series.Node, "candidates", candidates,
				"measured", health.Instances[match].CaptureSource)
		}
		if match >= 0 {
			inst.CaptureSource = health.Instances[match].CaptureSource
			health.Instances[match] = inst
			continue
		}
		health.Instances = append(health.Instances, inst)
	}
}
