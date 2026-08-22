package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// Top-of-book sequence health: the second source the Sequence column folds, and the one that
// makes the TOP rows say something instead of an em dash.
//
// # Why this is a separate read from the market-by-price one
//
// The market-by-price counters come from kalshi_mbp_levels, which is level grain and TTL-less,
// and kalshi_l2_coverage.go owns that scan for the reasons written down there. The top-of-book
// plane has no equivalent table. What it has is kalshi_bbo_observations — one row per change to
// the top of the book — and that table carries the wire protocol's own `sequence` and
// `reset_count` on every row, plus a `raw_meta` JSON object holding the channel instance's
// identity:
//
//	{"channel":"marketdata","multicast_group":"233.84.178.3","port":"31000",
//	 "publisher_source_ip":"148.51.120.6"}
//
// publisher_source_ip is the field that makes a series attributable to one publisher line, the
// same join the market-by-price leg makes against the ledger's dz_ip. multicast_group is better
// still for finding the group: it is the destination address itself rather than the capture
// source naming convention, which is a convention and has been renamed once already.
//
// # What this leg does NOT report, and why the distinction is load-bearing
//
// It does not count gaps. The market-by-price leg counts `status_after = 'gap'`, a marker the
// recorder writes onto the message that arrived while a book was un-anchored. There is no such
// marker here, and the obvious substitute — diffing sequence numbers against the row count — is
// wrong on this table by construction: a row exists only where the top of the book CHANGED, so a
// wire message that did not move the BBO legitimately leaves a hole in the numbering. Measured on
// mainnet over two minutes, one instance carried 23,846 rows across a sequence span of 24,553:
// about 3% "missing" on a feed with nothing wrong with it. A count-versus-span test would paint
// every healthy top-of-book series permanently red.
//
// So a series from this leg carries GapsMeasured = false, and the UI must not present its 'ok' as
// a gap-checked verdict. What it does report is real and worth having: the series is advancing,
// how far behind the recorder's own clock it is, and how many resets it took. Closing the gap
// half needs the producer to emit a gap marker for top-of-book the way it does for
// market-by-price, and that is not work this repository can do.
const edgeMulticastTOBSequenceCacheKey = "edge_multicast_tob_sequence:v1"

// edgeMulticastTOBWindowMinutes matches kalshiL2WindowMinutes so the two legs of one column
// describe the same span. Measured on mainnet: 3.8s over every tob_ capture source, which is why
// this rides the ten-minute background refresher rather than the page's own fetch.
const edgeMulticastTOBWindowMinutes = 15

// edgeMulticastTOBSourcePrefix is the capture source prefix for the top-of-book plane. It is the
// plane suffix hoisted to the front of the ledger group code, the same convention
// edgeMulticastPlanes drives everywhere else on this page.
const edgeMulticastTOBSourcePrefix = "tob_"

// EdgeMulticastTOBSeries is one recorded top-of-book series: one channel from one publisher as
// one recording node saw it.
type EdgeMulticastTOBSeries struct {
	Source string `json:"source"`

	// MulticastGroup is the destination address out of raw_meta, and the primary way a series
	// finds its group. Empty on a recorder that has not carried it yet, in which case the
	// capture source name is the fallback.
	MulticastGroup string `json:"multicast_group,omitempty"`

	// PublisherSourceIP is the address the datagrams came from, matched against the ledger's
	// dz_ip to find the publisher line. Empty means the series has no line to sit on and is
	// counted as unattributed on the group roll-up.
	PublisherSourceIP string `json:"publisher_source_ip,omitempty"`

	ChannelID    uint8  `json:"channel_id"`
	Node         string `json:"node"`
	LocationCode string `json:"location_code,omitempty"`

	// Messages is rows in the window, which on this table is top-of-book CHANGES rather than
	// wire messages. It is a liveness magnitude, never a denominator for a loss rate.
	Messages uint64 `json:"messages"`

	// Resets is how far the wire protocol's Reset Count advanced across the window. The column
	// is a UInt8 and wraps, so a series taking more than 255 resets inside one window
	// under-reports — a feed doing that has a louder problem than this number.
	Resets uint64 `json:"resets"`

	LastSeen time.Time `json:"last_seen"`
}

// EdgeMulticastTOBSequenceResponse is what the refresher caches.
type EdgeMulticastTOBSequenceResponse struct {
	GeneratedAt   time.Time                `json:"generated_at"`
	WindowMinutes int                      `json:"window_minutes"`
	Series        []EdgeMulticastTOBSeries `json:"series"`
}

// FetchEdgeMulticastTOBSequence aggregates the top-of-book series over the coverage window.
//
// Grouped by (source, multicast_group, publisher_source_ip, channel_id, measurement_node_id).
// The recording node is a key and not folded, for the same reason it is not folded on the
// market-by-price leg: two vantages of one instance are two independent observations, and merging
// them hides a recorder that is missing the feed.
//
// An absent observations table yields an empty payload rather than an error — local dev and any
// environment without the feeds proxy must not fail this refresh.
func (a *API) FetchEdgeMulticastTOBSequence(ctx context.Context) (*EdgeMulticastTOBSequenceResponse, error) {
	out := &EdgeMulticastTOBSequenceResponse{
		GeneratedAt:   time.Now().UTC(),
		WindowMinutes: edgeMulticastTOBWindowMinutes,
		Series:        []EdgeMulticastTOBSeries{},
	}

	exists, err := a.kalshiObservationsTableExists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return out, nil
	}

	q := fmt.Sprintf(`
		SELECT
			source,
			JSONExtractString(raw_meta, 'multicast_group') AS multicast_group,
			JSONExtractString(raw_meta, 'publisher_source_ip') AS publisher_source_ip,
			channel_id,
			measurement_node_id,
			any(location_code) AS location_code,
			count() AS messages,
			toUInt64(max(reset_count)) - toUInt64(min(reset_count)) AS resets,
			max(recv_ts_ns) AS last_recv_ts_ns
		FROM %[1]s.kalshi_bbo_observations
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[2]d)))
			AND startsWith(source, '%[3]s')
		GROUP BY source, multicast_group, publisher_source_ip, channel_id, measurement_node_id
		SETTINGS max_execution_time = 120, timeout_before_checking_execution_speed = 0`,
		"`"+a.FeedsDB+"`", edgeMulticastTOBWindowMinutes, edgeMulticastTOBSourcePrefix)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("edge_multicast_tob_sequence", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s EdgeMulticastTOBSeries
		var lastRecvNs uint64
		if err := rows.Scan(&s.Source, &s.MulticastGroup, &s.PublisherSourceIP, &s.ChannelID,
			&s.Node, &s.LocationCode, &s.Messages, &s.Resets, &lastRecvNs); err != nil {
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
