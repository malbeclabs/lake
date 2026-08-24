package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
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
// v2: `recorder_loss` added, each recording node measured against its peers.
const edgeMulticastObservationsCacheKey = "edge_multicast_observations:v2"

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

// EdgeMulticastRecorderLossSeries is one recording node's view of one path's numbering: which of
// the sequence numbers SOMEONE recorded that node did not.
//
// This is the only loss measurement on the top-of-book plane, and it works where an absolute one
// cannot. A row here exists only where the top of the book CHANGED, so a wire message that moved
// nothing legitimately leaves a hole — which is why this file refuses to count gaps against the
// span. Comparing recorders is immune to that: a message that never moved the BBO is absent at
// EVERY node, so it never enters the reference, while a datagram lost on one node's branch is
// present at the others and shows up as that node's alone.
//
// The reference is the UNION of what the nodes recorded, and that bounds the claim in one specific
// way: a message no node received is not in it and cannot be reported. So this measures loss
// BETWEEN recorders — a branch, a host, a receive path — and is structurally unable to see a loss
// upstream of where the paths fan out. Simultaneous loss at several nodes is the closest thing to
// that signal, and it is carried separately for exactly that reason.
type EdgeMulticastRecorderLossSeries struct {
	MulticastGroup    string `json:"multicast_group,omitempty"`
	PublisherSourceIP string `json:"publisher_source_ip,omitempty"`
	ChannelID         uint8  `json:"channel_id"`
	Node              string `json:"node"`
	LocationCode      string `json:"location_code,omitempty"`

	// Missing is how many reference sequences this node did not record, and ReferenceSeqs is
	// what it is a share of. Both are per (path, node) over the window.
	Missing       uint64 `json:"missing"`
	ReferenceSeqs uint64 `json:"reference_seqs"`

	// Episodes is when, at one entry per contiguous run of seconds. Stamped with the recv time
	// of the node that DID record the message — this node has no clock reading for something it
	// never received, and the recording node's is the only timestamp the loss has.
	Episodes []KalshiL2GapEpisode `json:"episodes,omitempty"`
}

// edgeMulticastRecorderLossCap bounds the seconds array per (path, node), one entry per second of
// the window. The window's own second count is the ceiling, so a node that lost something in every
// second of the window fills it exactly and drops nothing.
const edgeMulticastRecorderLossCap = edgeMulticastObservationsWindowMinutes * 60

// fetchEdgeMulticastRecorderLoss measures each recording node against its peers on the same path.
//
// The shape is a three-step: collapse to one row per (path, sequence) carrying which nodes saw it,
// derive each path's node universe from that, then emit one row per node that is missing from a
// sequence its peers recorded. The universe is derived rather than configured because it has to be
// what actually records this path today — a node added or removed is then a fact about the data
// rather than a deploy.
//
// A path recorded at ONE node produces nothing at all, which is correct and not a gap in coverage:
// with no peer there is no reference, and every hole in its numbering is the plane's own legitimate
// hole. Market-by-price is single-node on every group today, so this signal exists only for
// top-of-book — where perps runs three vantages.
func (a *API) fetchEdgeMulticastRecorderLoss(ctx context.Context) ([]EdgeMulticastRecorderLossSeries, error) {
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		WITH per_seq AS (
			SELECT
				JSONExtractString(raw_meta, 'multicast_group') AS multicast_group,
				JSONExtractString(raw_meta, 'publisher_source_ip') AS publisher_source_ip,
				channel_id,
				sequence,
				groupUniqArray(measurement_node_id) AS nodes,
				min(recv_ts_ns) AS first_recv,
				any(location_code) AS location_code
			FROM %[1]s.kalshi_bbo_observations
			WHERE recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(%[2]d)))
			  AND source LIKE '%[3]s%%'
			GROUP BY multicast_group, publisher_source_ip, channel_id, sequence
		),
		universe AS (
			SELECT
				multicast_group, publisher_source_ip, channel_id,
				arrayDistinct(arrayFlatten(groupArray(nodes))) AS all_nodes,
				count() AS reference_seqs
			FROM per_seq
			GROUP BY multicast_group, publisher_source_ip, channel_id
		)
		SELECT
			p.multicast_group,
			p.publisher_source_ip,
			p.channel_id,
			node,
			any(p.location_code) AS location_code,
			count() AS missing,
			any(u.reference_seqs) AS reference_seqs,
			groupUniqArray(%[4]d)(toUInt32(intDiv(p.first_recv, 1000000000))) AS seconds
		FROM per_seq AS p
		INNER JOIN universe AS u
			ON p.multicast_group = u.multicast_group
			AND p.publisher_source_ip = u.publisher_source_ip
			AND p.channel_id = u.channel_id
		ARRAY JOIN arrayFilter(x -> NOT has(p.nodes, x), u.all_nodes) AS node
		GROUP BY p.multicast_group, p.publisher_source_ip, p.channel_id, node`,
		db, edgeMulticastObservationsWindowMinutes, edgeMulticastTOBSourcePrefix, edgeMulticastRecorderLossCap)

	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EdgeMulticastRecorderLossSeries
	for rows.Next() {
		var s EdgeMulticastRecorderLossSeries
		var seconds []uint32
		if err := rows.Scan(&s.MulticastGroup, &s.PublisherSourceIP, &s.ChannelID, &s.Node,
			&s.LocationCode, &s.Missing, &s.ReferenceSeqs, &seconds); err != nil {
			return nil, err
		}
		s.Episodes = collapseKalshiL2GapSeconds(seconds)
		out = append(out, s)
	}
	return out, rows.Err()
}

// EdgeMulticastObservationsResponse is what the refresher caches.
type EdgeMulticastObservationsResponse struct {
	GeneratedAt   time.Time                        `json:"generated_at"`
	WindowMinutes int                              `json:"window_minutes"`
	Series        []EdgeMulticastObservationSeries `json:"series"`

	// RecorderLoss is each recording node measured against its peers on the same path. Empty for
	// a path with one recorder, which has no peer to be measured against.
	RecorderLoss []EdgeMulticastRecorderLossSeries `json:"recorder_loss,omitempty"`
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

	// Additive to this payload, and a failure costs the recorder strips rather than the series
	// every other column on the page is folded from. Not fatal for the same reason the columns it
	// feeds are optional: a missing measurement must never be able to take the page with it.
	loss, err := a.fetchEdgeMulticastRecorderLoss(ctx)
	if err != nil {
		slog.Warn("edge multicast recorder loss unavailable", "error", err)
	} else {
		out.RecorderLoss = loss
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

	// Faulted is whether that is enough to call the path behind, and it is not simply
	// Behind > 0. See edgeMulticastPathParityFaulted for why one failing pair out of thirty is
	// not a path finding. The page reads THIS rather than re-deriving it, so the verdict, the
	// badge and the colour of the ratio cannot disagree.
	Faulted bool `json:"faulted"`

	// WorstRatio is this path's message count over the best path's, at its worst pair, and
	// WorstSource names that pair's capture source.
	WorstRatio  float64 `json:"worst_ratio"`
	WorstSource string  `json:"worst_source,omitempty"`
	WorstNode   string  `json:"worst_node,omitempty"`
}

// edgeMulticastPathParityBehindShare is how much of a path's comparisons must fail before the path
// is called behind.
//
// One failing pair marks a whole line, and a sports node compares 29-33 capture sources, so without
// this the verdict is decided by the single flakiest market on the feed. That is the same
// one-instance sensitivity the stalled verdict had, and the same shape of fix: a reading is not a
// finding until it is more than an outlier.
//
// A quarter, and the two ends of the range are what set it. A path with ONE comparison — every
// perps group — still fires at 1 of 1, so nothing that was already reportable stops being
// reportable. And a genuine deficit is not shy: loss on a branch is indiscriminate, so a path
// dropping enough to clear the 2% floor at all clears it nearly everywhere, while a market opening
// or closing a few seconds out of step with its peer clears it at exactly one capture source.
// Measured on mainnet, sports read 0.988 and 0.967 on the two paths at once — arithmetically
// impossible from one systematic deficit, since the better path of each pair is 1.0 by
// construction, and the signature of per-source noise near the volume floor instead.
const edgeMulticastPathParityBehindShare = 0.25

// edgeMulticastPathParityFaulted is the gate above, applied. Compared == 0 is no comparison and
// never a fault.
func edgeMulticastPathParityFaulted(behind, compared int) bool {
	if compared == 0 || behind == 0 {
		return false
	}
	return float64(behind)/float64(compared) >= edgeMulticastPathParityBehindShare
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
	// Once every pair is in: the share is over the path's whole comparison set, so it cannot be
	// decided while that set is still being built.
	for _, p := range out {
		p.Faulted = edgeMulticastPathParityFaulted(p.Behind, p.Compared)
	}
	return out
}

// EdgeMulticastRecorderCoverage is the group's recording nodes measured against each other: how
// many there are, and which of them are recording less of the feed than the best-placed one.
//
// This is the receiver-side statement, and it belongs to the group rather than to any publisher
// line — a node that is short on every path is the vantage, not the feed, and no single path owns
// that fact. It is the same thing capture-node parity says on the counter plane, computed where
// the numbers are exact instead: recorded message counts per node, rather than sample counts
// against half the median.
type EdgeMulticastRecorderCoverage struct {
	// Nodes is how many recording nodes wrote anything for this group in the window.
	Nodes int `json:"nodes"`

	// Lagging is worst-first, and empty when every node keeps up. A node with nothing to be
	// compared against is absent from it rather than counted as passing.
	Lagging []EdgeMulticastLaggingRecorder `json:"lagging,omitempty"`
}

// EdgeMulticastLaggingRecorder is one recording node against the best-placed one.
type EdgeMulticastLaggingRecorder struct {
	Node string `json:"node"`

	// WorstRatio is its worst showing across the instances it was compared on, and
	// Behind/Compared how many of those it failed. One bad capture source out of thirty is a
	// different call to action from thirty out of thirty.
	WorstRatio  float64 `json:"worst_ratio"`
	Behind      int     `json:"behind"`
	Compared    int     `json:"compared"`
	WorstSource string  `json:"worst_source,omitempty"`
}

// edgeMulticastNodeCoverageFloor is how far below the best-placed recorder a recording node may sit
// before the group says so.
//
// Looser than the path floor, and not by taste. The window is fifteen minutes with no exclusion of
// its trailing edge and every node's rows are filtered by one clock, so a recorder whose ingest
// lags reads as a deficit of exactly that lag — at 0.98 an eighteen-second lag would report as
// loss. Five percent buys about forty-five seconds of it, and the floor still has room to work in:
// measured on mainnet two healthy recorders of one feed agreed to 0.1% over a minute (17,020
// against 17,020) while the one that was genuinely dropping sat at 0.915 sustained across every
// minute of the window.
const edgeMulticastNodeCoverageFloor = 0.95

// edgeMulticastNodeCoverageKey addresses one recorded instance every node should see identically:
// one capture source carried by one publisher path. It is the transpose of the path-parity key —
// that one fixes the vantage and compares the paths, this one fixes the path and compares the
// vantages — and the pair of them is why either result can be attributed at all. A deficit that
// shows up in both is a path that is short at one recorder; a deficit only here is the recorder.
type edgeMulticastNodeCoverageKey struct {
	source string
	path   edgeMulticastPathKey
}

// edgeMulticastNodeCoverage measures every recording node against its peers, per group.
//
// Same shape as edgeMulticastPathParity and the same three refusals: a node absent from an
// instance is not compared rather than counted behind, an instance with one node records nothing
// either way, and an instance under the message floor is skipped so a handful of messages cannot
// mint a ratio. Compared against the BEST node for the same reason: the question is whether
// anything recorded more of this feed than I did, and a mean sinks with the faulty node.
func edgeMulticastNodeCoverage(series []EdgeMulticastObservationSeries, captureSources edgeMulticastCaptureSourceMap) map[string]*EdgeMulticastRecorderCoverage {
	byInstance := map[edgeMulticastNodeCoverageKey]map[string]uint64{}
	nodesPerGroup := map[string]map[string]struct{}{}
	for _, s := range series {
		if s.PublisherSourceIP == "" || s.Node == "" {
			continue
		}
		groupPK := edgeMulticastSeriesGroup(s, captureSources)
		if groupPK == "" {
			continue
		}
		if nodesPerGroup[groupPK] == nil {
			nodesPerGroup[groupPK] = map[string]struct{}{}
		}
		nodesPerGroup[groupPK][s.Node] = struct{}{}

		key := edgeMulticastNodeCoverageKey{
			source: s.Source,
			path:   edgeMulticastPathKey{groupPK: groupPK, ip: s.PublisherSourceIP},
		}
		if byInstance[key] == nil {
			byInstance[key] = map[string]uint64{}
		}
		// Summed across channels, the same fold the path check makes: one node records all of
		// them and a per-channel split would compare a node against itself.
		byInstance[key][s.Node] += s.Messages
	}

	// The paths are tracked as sets and not just counted, because the claim this makes is not
	// "behind somewhere" — it is "behind on everything of this group it records", which is what
	// separates a bad vantage from a bad path. A deficit confined to one path IS the path's
	// finding and Peer already carries it on that line; repeating it here as a node fault would
	// name the recorder for something a publisher did.
	type nodeTally struct {
		worstRatio    float64
		worstSource   string
		behind        int
		compared      int
		comparedPaths map[string]struct{}
		behindPaths   map[string]struct{}
	}
	tallies := map[string]map[string]*nodeTally{}
	for key, nodes := range byInstance {
		if len(nodes) < 2 {
			continue
		}
		var best uint64
		for _, messages := range nodes {
			if messages > best {
				best = messages
			}
		}
		if best < edgeMulticastPathParityMinMessages {
			continue
		}
		for node, messages := range nodes {
			if tallies[key.path.groupPK] == nil {
				tallies[key.path.groupPK] = map[string]*nodeTally{}
			}
			t := tallies[key.path.groupPK][node]
			if t == nil {
				t = &nodeTally{
					worstRatio:    1,
					comparedPaths: map[string]struct{}{},
					behindPaths:   map[string]struct{}{},
				}
				tallies[key.path.groupPK][node] = t
			}
			ratio := float64(messages) / float64(best)
			t.compared++
			t.comparedPaths[key.path.ip] = struct{}{}
			if ratio < edgeMulticastNodeCoverageFloor {
				t.behind++
				t.behindPaths[key.path.ip] = struct{}{}
			}
			if ratio < t.worstRatio {
				t.worstRatio = ratio
				t.worstSource = key.source
			}
		}
	}

	out := map[string]*EdgeMulticastRecorderCoverage{}
	for groupPK, nodes := range nodesPerGroup {
		cov := &EdgeMulticastRecorderCoverage{Nodes: len(nodes)}
		for node, t := range tallies[groupPK] {
			// Two gates, and they answer different questions. Breadth — behind on every path
			// it records — is what separates a bad vantage from a bad path. Share is what
			// separates a fault from a transient: a node short at ONE capture source is short
			// on both paths there and clears the breadth test on two comparisons out of the
			// ~58 a sports group makes, which would turn the group row amber over a single
			// market while every publisher line stayed green. Same gate as the sibling check,
			// for the same reason: a reading is not a finding until it is more than an outlier.
			if len(t.behindPaths) == 0 || len(t.behindPaths) != len(t.comparedPaths) {
				continue
			}
			if !edgeMulticastPathParityFaulted(t.behind, t.compared) {
				continue
			}
			cov.Lagging = append(cov.Lagging, EdgeMulticastLaggingRecorder{
				Node:        node,
				WorstRatio:  t.worstRatio,
				Behind:      t.behind,
				Compared:    t.compared,
				WorstSource: t.worstSource,
			})
		}
		// Worst-first, then by name: the payload is polled every 30s and a list that reorders
		// itself under the reader is its own bug.
		sort.Slice(cov.Lagging, func(i, j int) bool {
			if cov.Lagging[i].WorstRatio != cov.Lagging[j].WorstRatio {
				return cov.Lagging[i].WorstRatio < cov.Lagging[j].WorstRatio
			}
			return cov.Lagging[i].Node < cov.Lagging[j].Node
		})
		out[groupPK] = cov
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
func (a *API) edgeMulticastObservationStats(ctx context.Context, captureSources edgeMulticastCaptureSourceMap) (edgeMulticastObservationStatsResult, time.Time) {
	data, err := a.readPageCache(ctx, edgeMulticastObservationsCacheKey)
	if err != nil {
		return edgeMulticastObservationStatsResult{}, time.Time{}
	}
	var payload EdgeMulticastObservationsResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast observation stats: cache did not parse", "error", err)
		return edgeMulticastObservationStatsResult{}, time.Time{}
	}
	loss, simultaneous := edgeMulticastRecorderLossFold(payload.RecorderLoss)
	return edgeMulticastObservationStatsResult{
		parity:            edgeMulticastPathParity(payload.Series, captureSources),
		rates:             edgeMulticastPathRates(payload.Series, captureSources, payload.WindowMinutes),
		recorder:          edgeMulticastNodeCoverage(payload.Series, captureSources),
		recorderLoss:      loss,
		recorderLossSimul: simultaneous,
	}, payload.GeneratedAt.UTC()
}

// edgeMulticastObservationStatsResult is what one read of the observations payload yields: the
// path check, the recorded rate, and the recorder check. Grouped into a struct because they are
// three views of one cache entry and returning them separately grew a signature nobody could read.
type edgeMulticastObservationStatsResult struct {
	parity   map[edgeMulticastPathKey]*EdgeMulticastPathParity
	rates    map[edgeMulticastPathKey]float64
	recorder map[string]*EdgeMulticastRecorderCoverage

	// Both keyed on the publisher's tunnel address, the same join the sequence series make
	// against the ledger's dz_ip.
	recorderLoss      map[string][]EdgeMulticastRecorderLoss
	recorderLossSimul map[string][]KalshiL2GapEpisode
}
