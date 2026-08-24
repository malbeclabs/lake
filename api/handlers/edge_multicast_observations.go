package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// The observations plane: everything /dz/edge/multicast reads out of kalshi_bbo_observations.
//
// One background read, two products. It is what makes the top-of-book rows say something in the
// Sequence column instead of an em dash, and it is what measures a publisher path against its
// redundant peer — a check that works where capture-node parity cannot.
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
const edgeMulticastObservationsCacheKey = "edge_multicast_observations:v1"

// edgeMulticastObservationsWindowMinutes matches kalshiL2WindowMinutes so the two legs of the
// Sequence column describe the same span. Measured on mainnet: 4.3s over every mbp_ and tob_
// capture source, which is why this rides the ten-minute background refresher rather than the
// page's own fetch.
const edgeMulticastObservationsWindowMinutes = 15

// The capture source prefixes this leg reads: the plane suffix hoisted to the front of the ledger
// group code, the same convention edgeMulticastPlanes drives everywhere else on this page.
//
// Both planes are read, and they are used differently. Top-of-book rows become Sequence series,
// because nothing else records that plane. Market-by-price rows do NOT — kalshi_l2_coverage.go owns
// those, with a gap marker this table does not carry, and two sources for one plane would let the
// column disagree with itself. Both planes feed the path-parity check, which needs no marker.
const (
	edgeMulticastTOBSourcePrefix = "tob_"
	edgeMulticastMBPSourcePrefix = "mbp_"
)

// EdgeMulticastObservationSeries is one recorded series: one channel from one publisher as one
// recording node saw it.
type EdgeMulticastObservationSeries struct {
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

// EdgeMulticastObservationsResponse is what the refresher caches.
type EdgeMulticastObservationsResponse struct {
	GeneratedAt   time.Time                        `json:"generated_at"`
	WindowMinutes int                              `json:"window_minutes"`
	Series        []EdgeMulticastObservationSeries `json:"series"`
}

// FetchEdgeMulticastObservations aggregates the recorded series over the coverage window.
//
// Grouped by (source, multicast_group, publisher_source_ip, channel_id, measurement_node_id).
// The recording node is a key and not folded, for the same reason it is not folded on the
// market-by-price leg: two vantages of one instance are two independent observations, and merging
// them hides a recorder that is missing the feed.
//
// An absent observations table yields an empty payload rather than an error — local dev and any
// environment without the feeds proxy must not fail this refresh.
func (a *API) FetchEdgeMulticastObservations(ctx context.Context) (*EdgeMulticastObservationsResponse, error) {
	out := &EdgeMulticastObservationsResponse{
		GeneratedAt:   time.Now().UTC(),
		WindowMinutes: edgeMulticastObservationsWindowMinutes,
		Series:        []EdgeMulticastObservationSeries{},
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
			-- Cast the DIFFERENCE, not the operands: ClickHouse promotes UInt64 - UInt64 to
			-- Int64 so the result can go negative, and the scan into a uint64 then fails and
			-- takes the whole refresh with it. max >= min here by construction.
			toUInt64(max(reset_count) - min(reset_count)) AS resets,
			max(recv_ts_ns) AS last_recv_ts_ns
		FROM %[1]s.kalshi_bbo_observations
		WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[2]d)))
			AND (startsWith(source, '%[3]s') OR startsWith(source, '%[4]s'))
		GROUP BY source, multicast_group, publisher_source_ip, channel_id, measurement_node_id
		SETTINGS max_execution_time = 120, timeout_before_checking_execution_speed = 0`,
		"`"+a.FeedsDB+"`", edgeMulticastObservationsWindowMinutes,
		edgeMulticastTOBSourcePrefix, edgeMulticastMBPSourcePrefix)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("edge_multicast_observations", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s EdgeMulticastObservationSeries
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

// edgeMulticastPathParityFloor is how far behind its redundant peer a publisher path may fall
// before the line is called `behind`: below 98% of the best path of the same feed, measured at the
// same recording node.
//
// Tight on purpose, and it can afford to be. Redundant paths carry the same feed, so measured on
// mainnet over fifteen minutes the two Kalshi paths agree to the message on all 29 sports capture
// sources and run 0.9985-1.0000 on perps. There is no legitimate spread here to leave room for —
// unlike capture-node parity, which folds recorders that started at different times and has to sit
// at half the median to avoid firing on all of it.
//
// This check exists because that one cannot reach most of this page. Capture-node parity needs two
// recorders (edgeMulticastMinParityNodes) and the sports capture runs on one, so it is inert on
// every sports group. Comparing paths AT one node needs no second node, and it asks the sharper
// question anyway: not "is this recorder keeping up" but "is this path delivering what its peer
// is".
const edgeMulticastPathParityFloor = 0.98

// edgeMulticastPathParityMinMessages is how much traffic the BEST path of a pair must have carried
// in the window before the ratio is allowed to produce a verdict.
//
// The 98% floor is only meaningful when a single message is small against it. At 500 messages one
// message is 0.2%, a tenth of the slack the floor leaves; at the other end of the scale the sports
// capture sources run 669 to 400,000 messages per fifteen minutes, and a market-by-price instance
// was measured at 4. Without a floor those 4 messages against a peer's 5 read as 'behind', and a
// line reads 'behind' when ANY of its pairs fails — the sports groups compare 29 to 33 capture
// sources at one node, so one off-hours league, or a path that came up inside the window, flips the
// whole line. Skipping the pair rather than passing it: nothing was measured, and the page says so
// by leaving Peer blank.
const edgeMulticastPathParityMinMessages = 500

// EdgeMulticastPathParity is one publisher path measured against the other paths of the same feed.
type EdgeMulticastPathParity struct {
	// Compared is how many (capture source, recording node) pairs this path could be measured
	// on — a pair only counts when another path carried the same feed at the same node. Zero
	// means there was nothing to compare against and no verdict is taken.
	Compared int `json:"compared"`

	// Behind is how many of those fell below the floor.
	Behind int `json:"behind"`

	// WorstRatio is this path's message count over the best path's, at its worst pair, and
	// WorstSource names that pair's capture source.
	WorstRatio  float64 `json:"worst_ratio"`
	WorstSource string  `json:"worst_source,omitempty"`
	WorstNode   string  `json:"worst_node,omitempty"`
}

// edgeMulticastPathParityKey identifies one comparison: the paths of one capture source as one
// recorder saw them.
//
// The channel is deliberately NOT in the key. The two paths of a feed publish it on DIFFERENT
// channel ids — mainnet runs a +100 offset, sports on 10-49 against 110-149 and perps on 1 against
// 101 — so keying on channel would put each path in a group of its own and compare nothing.
//
// The recording node IS in the key. Comparing two paths at one vantage removes the recorder as a
// variable: a node that is behind on everything cancels out of the ratio instead of reading as a
// fault in both paths.
type edgeMulticastPathParityKey struct {
	source string
	node   string
}

// edgeMulticastPathKey addresses one publisher line: a publisher is a path of ONE group, and the
// same box publishes into several. Keying results on the address alone would let a path that is
// behind on top-of-book paint that box's market-by-price line too, which is the same folding this
// page exists to undo one level up.
type edgeMulticastPathKey struct {
	groupPK string
	ip      string
}

// edgeMulticastPathParity measures every path against its peers, per (group, publisher).
//
// Compared against the BEST peer rather than the mean, because the question is "is anything
// delivering more of this feed than I am". A mean over a pair sinks with the faulty path and would
// report both of them at roughly 1.0 when one is broken.
func edgeMulticastPathParity(series []EdgeMulticastObservationSeries, captureSources edgeMulticastCaptureSourceMap) map[edgeMulticastPathKey]*EdgeMulticastPathParity {
	type tally struct{ messages uint64 }
	byPair := map[edgeMulticastPathParityKey]map[edgeMulticastPathKey]*tally{}
	for _, s := range series {
		if s.PublisherSourceIP == "" {
			continue
		}
		groupPK := edgeMulticastSeriesGroup(s, captureSources)
		if groupPK == "" {
			continue
		}
		key := edgeMulticastPathParityKey{source: s.Source, node: s.Node}
		if byPair[key] == nil {
			byPair[key] = map[edgeMulticastPathKey]*tally{}
		}
		pathKey := edgeMulticastPathKey{groupPK: groupPK, ip: s.PublisherSourceIP}
		if byPair[key][pathKey] == nil {
			byPair[key][pathKey] = &tally{}
		}
		// Summed across channels: one path serves a feed on several of them.
		byPair[key][pathKey].messages += s.Messages
	}

	out := map[edgeMulticastPathKey]*EdgeMulticastPathParity{}
	for key, paths := range byPair {
		if len(paths) < 2 {
			// One path on this feed at this node. Nothing to be measured against, and
			// calling that either healthy or behind would be a claim about a comparison
			// that was never made.
			continue
		}
		var best uint64
		for _, t := range paths {
			if t.messages > best {
				best = t.messages
			}
		}
		if best < edgeMulticastPathParityMinMessages {
			// Too little traffic for the ratio to mean anything — including best == 0,
			// where every path is silent here. That is the counter plane's finding, not
			// this check's, and a 0/0 ratio would report it as perfect parity.
			continue
		}
		for pathKey, t := range paths {
			ratio := float64(t.messages) / float64(best)
			p := out[pathKey]
			if p == nil {
				p = &EdgeMulticastPathParity{WorstRatio: 1}
				out[pathKey] = p
			}
			p.Compared++
			if ratio < edgeMulticastPathParityFloor {
				p.Behind++
			}
			if ratio < p.WorstRatio {
				p.WorstRatio = ratio
				p.WorstSource = key.source
				p.WorstNode = key.node
			}
		}
	}
	return out
}

// edgeMulticastSeriesGroup resolves one recorded series to its group: the destination address the
// datagrams carried, with the capture source name as the fallback for a recorder payload that
// predates raw_meta carrying the address. Same precedence as the sequence fold.
func edgeMulticastSeriesGroup(s EdgeMulticastObservationSeries, captureSources edgeMulticastCaptureSourceMap) string {
	if pk := captureSources.resolveMulticastIP(s.MulticastGroup); pk != "" {
		return pk
	}
	return captureSources.resolve(s.Source)
}

// edgeMulticastPathRates is each path's recorded message rate, per (group, publisher), in messages
// per second over the window.
//
// This is the rate the page can state without a caveat, and it is why it earns a column beside the
// counter bps. Interface counters are per tunnel, so a publisher feeding both planes of a product
// from one tunnel reports one figure against both groups and neither is attributable. These counts
// come from the recorders, keyed by the destination address the datagrams carried, so they are per
// group by construction.
//
// Taken as the MAX over recording nodes, not the sum and not the mean. The nodes are independent
// vantages of the same traffic, so summing would multiply the feed by the number of recorders
// watching it, and a mean would drag the figure down with any recorder that is behind — the thing
// path parity is there to report separately. The max is "what the best-placed recorder saw", which
// is the closest this plane gets to what the publisher sent.
func edgeMulticastPathRates(series []EdgeMulticastObservationSeries, captureSources edgeMulticastCaptureSourceMap, windowMinutes int) map[edgeMulticastPathKey]float64 {
	if windowMinutes <= 0 {
		return nil
	}
	// (path, node) -> messages, summed over the capture sources and channels of that group.
	type nodeKey struct {
		path edgeMulticastPathKey
		node string
	}
	byNode := map[nodeKey]uint64{}
	for _, s := range series {
		if s.PublisherSourceIP == "" {
			continue
		}
		groupPK := edgeMulticastSeriesGroup(s, captureSources)
		if groupPK == "" {
			continue
		}
		byNode[nodeKey{
			path: edgeMulticastPathKey{groupPK: groupPK, ip: s.PublisherSourceIP},
			node: s.Node,
		}] += s.Messages
	}

	best := map[edgeMulticastPathKey]uint64{}
	for k, messages := range byNode {
		if messages > best[k.path] {
			best[k.path] = messages
		}
	}

	seconds := float64(windowMinutes) * 60
	out := make(map[edgeMulticastPathKey]float64, len(best))
	for path, messages := range best {
		out[path] = float64(messages) / seconds
	}
	return out
}

// edgeMulticastObservationStats reads the cached observations payload once and returns both things
// the publisher lines take from it. One read, one parse: they come from the same rows.
//
// Nil maps on a miss or a shape mismatch, the same contract the sequence fold has — these signals
// are additive to the page and must not be able to fail it.
// The payload's own clock comes back with it. Both figures it produces are as old as the refresher
// left them, and the two columns they fill have to age against that rather than against the
// response they are folded into — the same contract the sequence legs already carry. It is a
// separate stamp from SequenceAsOf on purpose: that one is the OLDER of the two sequence legs, so
// borrowing it would let the market-by-price leg's staleness grey out figures it has nothing to
// do with.
func (a *API) edgeMulticastObservationStats(ctx context.Context, captureSources edgeMulticastCaptureSourceMap) (map[edgeMulticastPathKey]*EdgeMulticastPathParity, map[edgeMulticastPathKey]float64, time.Time) {
	data, err := a.readPageCache(ctx, edgeMulticastObservationsCacheKey)
	if err != nil {
		return nil, nil, time.Time{}
	}
	var payload EdgeMulticastObservationsResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast observation stats: cache did not parse", "error", err)
		return nil, nil, time.Time{}
	}
	return edgeMulticastPathParity(payload.Series, captureSources),
		edgeMulticastPathRates(payload.Series, captureSources, payload.WindowMinutes),
		payload.GeneratedAt.UTC()
}
