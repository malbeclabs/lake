package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"
)

// Sequence-counter health for the edge multicast overview: for the feeds that run the DoubleZero
// Edge wire protocol and have a recorder behind them, whether the recorded sequence series is
// intact.
//
// # The grain, and what the capture schema can actually key on
//
// Sequencing keys on the CHANNEL INSTANCE — (source IP address, Channel ID, destination port) —
// because redundant paths carrying one channel run as separate publishers on separate hosts and
// cannot share a counter (edge-feed-spec/GLOSSARY.md, Transport). Two of those three fields are
// not in kalshi_mbp_levels: it carries the capture source id, the Channel ID and the recording
// node, and no source IP or port anywhere. So the key here is (capture source, Channel ID,
// recording node).
//
// That resolves the same set today and it is not luck: the capture source id maps 1:1 onto a
// multicast group and its port role, and prod's two publisher paths for a group share the group,
// the port triple and the source-IP-bearing tunnel — they differ ONLY by Channel ID, which is
// exactly what kalshi_l2_coverage.go's GROUP BY documents. The limit worth writing down is the
// case that would break it: two paths sharing a Channel ID with different source IP addresses
// would fold into one row here, and a gap on one of them would read as a gap on both. Nothing in
// lake can resolve a source IP to a recorded series, the same gap that stops
// edge_multicast_class.go from deriving the market-data recorders.
//
// # Why this reads a cache and never queries
//
// The gap counters come from kalshi_mbp_levels, and that table is the heavy one: it is level
// grain, TTL-less, sorted by (measurement_node_id, source, channel_id, symbol, instrument_id,
// recv_ts_ns) and partitioned by day, so a fifteen-minute question reads most of a day through a
// remoteSecure() proxy — ~135M rows at current rates. kalshi_l2_coverage.go owns that query for
// exactly that reason and keeps it on a ten-minute background refresher.
//
// Adding a second copy of it to a page that refreshes every 30 seconds would be the same scan
// again, and worse, the two pages could then disagree about the same feed. This folds the
// refresher's own cached payload in instead: no query, no scan, and the number on this column is
// by construction the number /dz/kalshi/l2 shows. The cost is staleness, up to one refresher
// interval, which is why SequenceAsOf is in the payload rather than left implicit.
//
// A cache miss is the normal state in local dev and while the refresher has never run. It yields
// no column and is not an error — the same contract as the application-plane last-heard leg.

// edgeMulticastSequenceStaleSecs is how long a channel instance may go without a message before
// the series is called stalled rather than intact.
//
// Two minutes, against the recorder's own newest message. Deliberately far looser than a market
// feed's real cadence (perps run thousands of messages a second): this is not a liveness check —
// LastHeard already answers that on a fresher plane — it is here to stop a series that stopped
// advancing hours ago from being reported as 'ok' purely because it recorded no gap while it was
// dead. The window it is read over is fifteen minutes wide, so anything tighter than a couple of
// minutes would only be measuring the refresher's own lag.
const edgeMulticastSequenceStaleSecs = 120

// The three states a channel instance can be in. There is no 'unknown': an instance exists here
// only because a recorder wrote messages for it, so the absence of an instance is the absence of
// the whole column rather than a fourth state.
const (
	edgeMulticastSeqOK      = "ok"
	edgeMulticastSeqGapped  = "gapped"
	edgeMulticastSeqStalled = "stalled"
)

// EdgeMulticastChannelInstance is one recorded sequence series: one Channel ID of one capture
// source as one recording node saw it.
type EdgeMulticastChannelInstance struct {
	CaptureSource string `json:"capture_source"`
	ChannelID     uint8  `json:"channel_id"`
	Node          string `json:"node"`
	LocationCode  string `json:"location_code,omitempty"`

	// Messages is the count over the coverage window, and it is the denominator GapBooks is
	// only meaningful against.
	Messages uint64 `json:"messages"`

	// GapBooks is how many distinct books gapped at all in the window: the fault count to
	// show. NOT GapMessages, which counts every message that arrived while a book was
	// un-anchored and therefore scales with traffic rather than with reliability — 22 real
	// discontinuities produced 158,912 gap-marked messages on perps. See KalshiL2Lane, which
	// documents that decision at length; this column deliberately carries the same one so the
	// two pages cannot tell different stories about one feed.
	GapBooks uint64 `json:"gap_books"`

	// Resets and SnapshotCycles are the recovery side: an `instrument_reset` re-anchors one
	// book, a `snapshot_end` completes a cycle. A series with gaps and no cycles is not
	// recovering.
	Resets         uint64 `json:"resets"`
	SnapshotCycles uint64 `json:"snapshot_cycles"`

	LastSeen time.Time `json:"last_seen"`
	Status   string    `json:"status"`
}

// EdgeMulticastSequenceHealth is the group's roll-up over its channel instances.
type EdgeMulticastSequenceHealth struct {
	// Status is the worst of the instances: gapped, then stalled, then ok.
	Status string `json:"status"`

	// Gapped and Stalled are how many instances are in each state, so the badge can say "1 of
	// 4" rather than implying the whole group is broken.
	Gapped  int `json:"gapped"`
	Stalled int `json:"stalled"`

	// Instances are sorted worst-first, so a reader who only looks at the first one is looking
	// at the one that matters.
	Instances []EdgeMulticastChannelInstance `json:"instances"`
}

// edgeMulticastSequenceHealth folds the L2 coverage refresher's cached payload into per-group
// sequence health.
//
// Returns (nil, zero time, nil) when there is nothing to fold — no cache entry, an entry that
// does not parse, or no lane that resolves to a group on this page. All three are "no column",
// never an error: this signal is additive to the page and must not be able to fail it.
func (a *API) edgeMulticastSequenceHealth(ctx context.Context, captureSources edgeMulticastCaptureSourceMap) (map[string]*EdgeMulticastSequenceHealth, time.Time, error) {
	data, err := a.readPageCache(ctx, kalshiL2CoverageCacheKey)
	if err != nil {
		// A miss, which is the normal state before the refresher's first run and in local
		// dev. Not logged: the page says so by dropping the column.
		return nil, time.Time{}, nil
	}

	var coverage KalshiL2CoverageResponse
	if err := json.Unmarshal(data, &coverage); err != nil {
		// A shape mismatch means the cache key was not bumped alongside a payload change,
		// which is a deploy-time bug worth a line — but not this page's failure.
		slog.Warn("edge multicast sequence health: l2 coverage cache did not parse", "error", err)
		return nil, time.Time{}, nil
	}

	out := map[string]*EdgeMulticastSequenceHealth{}
	for _, lane := range coverage.Lanes {
		// An unseen lane is a configured capture source that produced nothing in the window,
		// carried by the coverage payload as a placeholder with zeroed stats. It says
		// nothing about a sequence series, so it must not become an 'ok' instance.
		if !lane.Seen {
			continue
		}
		groupPK := captureSources.resolve(lane.Source)
		if groupPK == "" {
			continue
		}
		inst := EdgeMulticastChannelInstance{
			CaptureSource:  lane.Source,
			ChannelID:      lane.ChannelID,
			Node:           lane.MeasurementNodeID,
			LocationCode:   lane.LocationCode,
			Messages:       lane.Messages,
			GapBooks:       lane.GapBooks,
			Resets:         lane.Resets,
			SnapshotCycles: lane.SnapshotCycles,
			LastSeen:       lane.LastSeen.UTC(),
			Status:         edgeMulticastSequenceStatus(lane.GapBooks, lane.LastSeen, coverage.GeneratedAt),
		}
		if out[groupPK] == nil {
			out[groupPK] = &EdgeMulticastSequenceHealth{}
		}
		out[groupPK].Instances = append(out[groupPK].Instances, inst)
	}
	if len(out) == 0 {
		return nil, time.Time{}, nil
	}

	for _, health := range out {
		finishEdgeMulticastSequenceHealth(health)
	}
	return out, coverage.GeneratedAt.UTC(), nil
}

// edgeMulticastSequenceStatus grades one series.
//
// Staleness is measured against the coverage payload's OWN clock, not wall clock. The entry is up
// to a refresher interval old, so reading its timestamps against now() would add the refresher's
// lag to every instance and mark healthy series stalled for most of every cycle — the same
// mistake the counter columns on this page document for their own ages.
func edgeMulticastSequenceStatus(gapBooks uint64, lastSeen, asOf time.Time) string {
	if gapBooks > 0 {
		return edgeMulticastSeqGapped
	}
	if lastSeen.IsZero() || asOf.Sub(lastSeen) > edgeMulticastSequenceStaleSecs*time.Second {
		return edgeMulticastSeqStalled
	}
	return edgeMulticastSeqOK
}

// finishEdgeMulticastSequenceHealth tallies the instance states and rolls them up worst-first.
func finishEdgeMulticastSequenceHealth(health *EdgeMulticastSequenceHealth) {
	rank := map[string]int{
		edgeMulticastSeqGapped:  0,
		edgeMulticastSeqStalled: 1,
		edgeMulticastSeqOK:      2,
	}
	for _, inst := range health.Instances {
		switch inst.Status {
		case edgeMulticastSeqGapped:
			health.Gapped++
		case edgeMulticastSeqStalled:
			health.Stalled++
		}
	}
	sort.SliceStable(health.Instances, func(i, j int) bool {
		a, b := health.Instances[i], health.Instances[j]
		if rank[a.Status] != rank[b.Status] {
			return rank[a.Status] < rank[b.Status]
		}
		// Most gapped books first within a state, then a stable key: the payload is polled
		// every 30s and a shifting order under the reader's cursor is its own bug.
		if a.GapBooks != b.GapBooks {
			return a.GapBooks > b.GapBooks
		}
		if a.CaptureSource != b.CaptureSource {
			return a.CaptureSource < b.CaptureSource
		}
		if a.ChannelID != b.ChannelID {
			return a.ChannelID < b.ChannelID
		}
		return a.Node < b.Node
	})
	switch {
	case health.Gapped > 0:
		health.Status = edgeMulticastSeqGapped
	case health.Stalled > 0:
		health.Status = edgeMulticastSeqStalled
	default:
		health.Status = edgeMulticastSeqOK
	}
}
