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
// # The grain: a sequence series belongs to one publisher
//
// Sequencing keys on the CHANNEL INSTANCE — "one path's view of one channel, keyed (source IP
// address, Channel ID, destination port)" — and a subscriber "MUST key gap detection and recovery
// state" on that tuple, "never the channel", because redundant paths carrying one channel run as
// separate processes on separate hosts and cannot share a counter (edge-feed-spec/GLOSSARY.md,
// Transport). market-by-price/spec.md says it per field at 3.1.0 — Sequence Number is minted "per
// channel instance", Reset Count per (source, channel) — in its Redundant Channel Instances section.
//
// kalshi_mbp_levels carries the source address, as `publisher_source_ip`: the arm axis is a column
// in that schema on purpose, so that the arms are separable by construction rather than by
// remembering to filter. So the key here is (source IP address, Channel ID, recording node), and
// each series is reported on the publisher line whose address the datagrams carried — matched
// against the ledger's dz_ip, which is the join the fabric's own source attribution already makes
// ("source_address matches the publisher user's dz_ip", enriched_ip_mroute).
//
// Two folds in that key are deliberate:
//
//   - The destination port is folded. Only `Sequence Number` is per port role; `Reset Count`, the
//     manifest and the channel state they govern span the three ports one publisher serves a
//     channel on, and what this column carries is the book-level fault and recovery counters, not
//     raw sequence numbers. Splitting by port would scatter a book's gap, its reset and its
//     snapshot cycle across three rows that each look like they are missing something.
//   - The recording node is NOT folded. Two vantages of one instance are two independent
//     observations, and merging them hides a recorder that is missing the feed.
//
// The limit worth writing down is now a narrow one: an instance whose source address matches no
// publisher in the ledger has no line to sit on. It is counted as Unattributed on the group
// roll-up rather than dropped, because silently discarding a recorded gap is the one outcome this
// column must not have. Two paths sharing a Channel ID are no longer that case — collapsing the
// perps arms onto a single channel id (malbeclabs/kalshi#86: settled, timing gated) is a non-event
// here, and on the sports plane, where channel_id names the league and never was an arm
// discriminator, there was nothing to fold in the first place.
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
// by construction the number /dz/kalshi/l2 shows. The source address rides along in that payload
// for the same reason — it is one more GROUP BY key on a scan that is already happening, not a
// second read. The cost is staleness, up to one refresher
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

// EdgeMulticastChannelInstance is one recorded sequence series: one Channel ID from one source
// address as one recording node saw it.
type EdgeMulticastChannelInstance struct {
	// PublisherSourceIP is the address the datagrams came from, and the field that makes this a
	// channel instance rather than a channel. Empty only on a payload written before the
	// refresher carried it, in which case the series has no publisher to be reported on.
	PublisherSourceIP string `json:"publisher_source_ip,omitempty"`

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

	// GapsMeasured says whether GapBooks is a reading or an absence. True on the
	// market-by-price plane, where the recorder writes a gap marker this can count. False on
	// top-of-book, where there is no marker and the row grain (one row per change to the top of
	// the book) makes a sequence-versus-row-count test structurally wrong — see
	// edge_multicast_tob_sequence.go. A zero GapBooks with this false is "not checked", and the
	// UI has to render it as something other than a clean bill of health.
	GapsMeasured bool `json:"gaps_measured"`

	LastSeen time.Time `json:"last_seen"`
	Status   string    `json:"status"`
}

// EdgeMulticastSequenceHealth is sequence health over a set of channel instances: one publisher's
// own series where it hangs off a publisher line, the group's roll-up over all of them where it
// hangs off the group.
type EdgeMulticastSequenceHealth struct {
	// Status is the worst of the instances: gapped, then stalled, then ok.
	Status string `json:"status"`

	// Gapped and Stalled are how many instances are in each state, so the badge can say "1 of
	// 4" rather than implying the whole group is broken.
	Gapped  int `json:"gapped"`
	Stalled int `json:"stalled"`

	// The same tally at the grain the verdict belongs to, set on the group roll-up only. A group
	// carrying one series per publisher makes these identical to the instance counts; a group
	// whose publishers each carry several channels does not, and "1 of 2 publishers" is then a
	// different call to action from "1 of 8 series".
	Publishers        int `json:"publishers,omitempty"`
	PublishersGapped  int `json:"publishers_gapped,omitempty"`
	PublishersStalled int `json:"publishers_stalled,omitempty"`

	// Unattributed is how many instances matched no publisher line, so their verdict has no row
	// of its own: a recorded source address the ledger does not carry as a publisher of this
	// group. Counted rather than dropped — the roll-up is the only place left that can report
	// them.
	Unattributed int `json:"unattributed,omitempty"`

	// GapsUnmeasured is how many of these instances came from a plane with no gap marker, so
	// their 'ok' means "advancing", not "lost nothing". Carried so the badge can say which kind
	// of ok it is rather than letting the top-of-book rows borrow the market-by-price rows'
	// stronger claim.
	GapsUnmeasured int `json:"gaps_unmeasured,omitempty"`

	// Instances are sorted worst-first, so a reader who only looks at the first one is looking
	// at the one that matters.
	Instances []EdgeMulticastChannelInstance `json:"instances"`
}

// edgeMulticastSequenceHealth folds the two cached refresher payloads into per-group sequence
// health: the L2 coverage one for market-by-price, and the top-of-book one.
//
// Returns (nil, zero time, nil) when there is nothing to fold — no cache entries, entries that do
// not parse, or nothing that resolves to a group on this page. All of those are "no column",
// never an error: this signal is additive to the page and must not be able to fail it. One leg
// missing costs that plane's rows and leaves the other's intact.
//
// The reported as-of is the OLDER of the two legs. They run on one refresher today, so the two
// stamps are seconds apart; taking the older one means that if they ever diverge, the column ages
// against the staler half rather than flattering itself with the fresher.
func (a *API) edgeMulticastSequenceHealth(ctx context.Context, captureSources edgeMulticastCaptureSourceMap) (map[string]*EdgeMulticastSequenceHealth, time.Time, error) {
	out := map[string]*EdgeMulticastSequenceHealth{}
	var asOf time.Time
	note := func(at time.Time) {
		if at.IsZero() {
			return
		}
		if asOf.IsZero() || at.Before(asOf) {
			asOf = at
		}
	}

	note(a.foldKalshiL2Coverage(ctx, captureSources, out))
	note(a.foldEdgeMulticastTOBSequence(ctx, captureSources, out))

	if len(out) == 0 {
		return nil, time.Time{}, nil
	}
	for _, health := range out {
		finishEdgeMulticastSequenceHealth(health)
	}
	return out, asOf.UTC(), nil
}

// foldKalshiL2Coverage adds the market-by-price series, and returns the payload's own clock.
func (a *API) foldKalshiL2Coverage(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) time.Time {
	data, err := a.readPageCache(ctx, kalshiL2CoverageCacheKey)
	if err != nil {
		// A miss, which is the normal state before the refresher's first run and in local
		// dev. Not logged: the page says so by dropping the column.
		return time.Time{}
	}

	var coverage KalshiL2CoverageResponse
	if err := json.Unmarshal(data, &coverage); err != nil {
		// A shape mismatch means the cache key was not bumped alongside a payload change,
		// which is a deploy-time bug worth a line — but not this page's failure.
		slog.Warn("edge multicast sequence health: l2 coverage cache did not parse", "error", err)
		return time.Time{}
	}

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
			PublisherSourceIP: lane.PublisherSourceIP,

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
			GapsMeasured:   true,
		}
		if out[groupPK] == nil {
			out[groupPK] = &EdgeMulticastSequenceHealth{}
		}
		out[groupPK].Instances = append(out[groupPK].Instances, inst)
	}
	return coverage.GeneratedAt.UTC()
}

// foldEdgeMulticastTOBSequence adds the top-of-book series, and returns the payload's own clock.
//
// Every instance it produces carries GapsMeasured = false. That is the whole difference between
// the two legs and it is not a shortfall to be papered over: this plane has no gap marker to
// count, so the series can say it is advancing and cannot say it lost nothing.
func (a *API) foldEdgeMulticastTOBSequence(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) time.Time {
	data, err := a.readPageCache(ctx, edgeMulticastTOBSequenceCacheKey)
	if err != nil {
		return time.Time{}
	}

	var payload EdgeMulticastTOBSequenceResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast sequence health: tob sequence cache did not parse", "error", err)
		return time.Time{}
	}

	for _, series := range payload.Series {
		// The destination address first: it is what the datagrams were addressed to. The
		// capture source name is the fallback, for a recorder payload that predates
		// raw_meta carrying the address.
		groupPK := captureSources.resolveMulticastIP(series.MulticastGroup)
		if groupPK == "" {
			groupPK = captureSources.resolve(series.Source)
		}
		if groupPK == "" {
			continue
		}
		inst := EdgeMulticastChannelInstance{
			PublisherSourceIP: series.PublisherSourceIP,
			CaptureSource:     series.Source,
			ChannelID:         series.ChannelID,
			Node:              series.Node,
			LocationCode:      series.LocationCode,
			Messages:          series.Messages,
			Resets:            series.Resets,
			LastSeen:          series.LastSeen.UTC(),
			// Graded on staleness alone. Passing a zero gap count is not a claim that the
			// count is zero — GapsMeasured is what carries that, and it is false here.
			Status:       edgeMulticastSequenceStatus(0, series.LastSeen, payload.GeneratedAt),
			GapsMeasured: false,
		}
		if out[groupPK] == nil {
			out[groupPK] = &EdgeMulticastSequenceHealth{}
		}
		out[groupPK].Instances = append(out[groupPK].Instances, inst)
	}
	return payload.GeneratedAt.UTC()
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
		if !inst.GapsMeasured {
			health.GapsUnmeasured++
		}
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
		if a.PublisherSourceIP != b.PublisherSourceIP {
			return a.PublisherSourceIP < b.PublisherSourceIP
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

// attachEdgeMulticastSequenceHealth reports each channel instance on the publisher line that
// emitted it, and leaves the group roll-up counting publishers rather than series.
//
// Matched on the recorded source address against the ledger's dz_ip. That is the publisher's
// tunnel address, which is what the datagrams carry and what the recorders' allow-lists are
// written in; client_ip is the box's own public address and never appears on the wire here.
//
// Runs over every publisher, before edgeMulticastPublisherLineCap truncates the list, so which
// lines the payload happens to carry cannot change any verdict. A faulted line the cap then hid
// would take its badge off screen with it, leaving only the roll-up — unreachable today, since the
// only groups with a recorded series have two publishers each against a cap of twelve.
func attachEdgeMulticastSequenceHealth(lines []EdgeMulticastPublisher, health *EdgeMulticastSequenceHealth) {
	if health == nil {
		return
	}

	byDZIP := make(map[string]int, len(lines))
	for i, line := range lines {
		// A publisher with no tunnel address cannot be the source of anything recorded, and
		// must not become the line an empty PublisherSourceIP lands on.
		if line.DZIP == "" {
			continue
		}
		// One tunnel address per publisher, so first-wins only fires on a ledger that has
		// issued one dz_ip twice.
		if _, dup := byDZIP[line.DZIP]; !dup {
			byDZIP[line.DZIP] = i
		}
	}

	per := map[int]*EdgeMulticastSequenceHealth{}
	for _, inst := range health.Instances {
		i, ok := byDZIP[inst.PublisherSourceIP]
		if !ok {
			health.Unattributed++
			continue
		}
		if per[i] == nil {
			per[i] = &EdgeMulticastSequenceHealth{}
		}
		per[i].Instances = append(per[i].Instances, inst)
	}

	for i, h := range per {
		finishEdgeMulticastSequenceHealth(h)
		lines[i].Sequence = h
		health.Publishers++
		switch h.Status {
		case edgeMulticastSeqGapped:
			health.PublishersGapped++
		case edgeMulticastSeqStalled:
			health.PublishersStalled++
		}
	}
}
