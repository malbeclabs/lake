package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
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

	// GapMessages is how many messages arrived while a book was un-anchored. It is a DURATION
	// and not a fault count — 22 real discontinuities produced 158,912 of them on perps — so it
	// is never displayed as one and never sizes a badge. It is carried for one thing: over
	// Messages it is a loss RATE, which is the only severity this column can express. GapBooks
	// saturates at the channel's instrument count, so on a perps channel carrying thirteen
	// books, thirteen gapped and one gapped print the same badge.
	GapMessages uint64 `json:"gap_messages,omitempty"`
	// GapEpisodes is the same loss on a time axis: the contiguous runs of seconds this series
	// was recording gap-marked messages, from the same cached payload GapBooks comes from.
	//
	// It is what makes the two paths of a feed comparable at all. GapBooks saturates on a
	// small-instrument feed and says nothing about WHEN, so two lines both reading "13 books"
	// look like one failure when measured on mainnet they were disjoint in time — 10 seconds of
	// loss on one path, 64 on the other, and not one second on both. The counter cannot express
	// that; a shared time axis shows it without a word.
	//
	// Empty on a clean series, and empty on the top-of-book plane, which has no gap marker at
	// all. Those two are NOT the same thing and GapsMeasured is what separates them: an empty
	// timeline drawn under a false GapsMeasured would be the same clean bill of health this
	// struct already refuses to give.
	GapEpisodes []KalshiL2GapEpisode `json:"gap_episodes,omitempty"`

	// The per-instrument sequence loss counters, folded from the same payload. This is the only
	// signal on the page that counts MESSAGES lost rather than time spent un-anchored, and it is
	// the one a loss rate can be built from — see KalshiL2Lane, which records the two counters
	// that look like they could stand in for it and cannot.
	//
	// UpdatesReceived is the denominator: expected is received + missing, and a series with no
	// updates at all has no rate rather than a rate of zero. Omitted (omitempty) on the
	// top-of-book plane, whose rows carry no per-instrument sequence — absent rather than a
	// measured zero, which is the distinction completeness() reads to withhold a ppm figure.
	UpdatesReceived uint64  `json:"updates_received,omitempty"`
	UpdatesMissing  uint64  `json:"updates_missing,omitempty"`
	SeqGapEvents    uint64  `json:"seq_gap_events,omitempty"`
	MaxGapMessages  uint32  `json:"max_gap_messages,omitempty"`
	P99GapMessages  float64 `json:"p99_gap_messages,omitempty"`

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

	// CaptureSourceQuiet marks a stalled series whose silence belongs to the capture source
	// rather than to this path: every other path recording that source at the same node went
	// quiet with it. Set by demoteEdgeMulticastQuietCaptureSources, which documents the rule.
	// The Status stays 'stalled' — the reading is unchanged and only its attribution is — and
	// the tally is what reads this flag.
	CaptureSourceQuiet bool `json:"capture_source_quiet,omitempty"`

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

	// CaptureSourceQuiet is how many instances are stalled only because their capture source
	// stopped producing on every path at once. Counted apart from Stalled and deliberately not
	// folded into it: it is a statement about the feed's upstream, not about a path, and the
	// verdict it would otherwise mint outranks the findings that ARE about the path.
	CaptureSourceQuiet int `json:"capture_source_quiet,omitempty"`

	// Unattributed is how many instances matched no publisher line, so their verdict has no row
	// of its own: a recorded source address the ledger does not carry as a publisher of this
	// group. Counted rather than dropped — the roll-up is the only place left that can report
	// them.
	Unattributed int `json:"unattributed,omitempty"`

	// GapNodes is how many distinct recording nodes contributed a gap-measured instance here.
	//
	// One means the gap finding has a single vantage, and that is a limit on what it can say
	// rather than a detail: comparing the two paths at one recorder exonerates the recorder's
	// HOST, and nothing more. It cannot tell a path that lost data end to end from a path whose
	// last hop into that one recorder lost it. Measured on mainnet — a publisher read 13 books
	// gapped at the only node recording market-by-price, while on the plane that does have three
	// vantages the same path arrived intact at a second one, which placed the loss on the branch
	// and not on the path. The verdict stays 'gapped' either way, because data was lost; what
	// this bounds is whose loss the page may call it.
	GapNodes int `json:"gap_nodes,omitempty"`

	// RecorderLoss is each recording node measured against its peers on the same path, and
	// RecorderLossSimultaneous the seconds two or more of them lost at once. Set on a publisher
	// line only, and only where the path has more than one recorder — market-by-price runs a
	// single node on every group, so this is a top-of-book signal today.
	//
	// It answers what the per-line gap timeline structurally cannot: whether a loss is the path's
	// or one recorder's. A mark on one node's line and clear track on its peers' is that node's
	// branch; a mark on several at the same second is not, and that is the only thing this plane
	// can say about loss upstream of the recorders.
	RecorderLoss             []EdgeMulticastRecorderLoss `json:"recorder_loss,omitempty"`
	RecorderLossSimultaneous []KalshiL2GapEpisode        `json:"recorder_loss_simultaneous,omitempty"`

	// RecorderLossUnavailable says the comparison was attempted and failed. Rendered as "not
	// measured" rather than as nothing: an absent strip and a failed one are different claims,
	// and conflating them is how a query that died on every cycle went unnoticed.
	RecorderLossUnavailable bool `json:"recorder_loss_unavailable,omitempty"`

	// AllPathsGapped is the seconds every path of this feed lost data at once, set on the GROUP
	// roll-up only. Non-empty means the redundancy failed and the feed itself lost data — the one
	// sequence statement no publisher line can make, since a line only ever sees its own loss.
	AllPathsGapped []KalshiL2GapEpisode `json:"all_paths_gapped,omitempty"`

	// GapsUnmeasured is how many of these instances came from a plane with no gap marker, so
	// their 'ok' means "advancing", not "lost nothing". Carried so the badge can say which kind
	// of ok it is rather than letting the top-of-book rows borrow the market-by-price rows'
	// stronger claim.
	GapsUnmeasured int `json:"gaps_unmeasured,omitempty"`

	// Instances are sorted worst-first, so a reader who only looks at the first one is looking
	// at the one that matters.
	Instances []EdgeMulticastChannelInstance `json:"instances"`
}

// EdgeMulticastRecorderLoss is one recording node's loss on a publisher line, folded across the
// channels that publisher carries on the group.
type EdgeMulticastRecorderLoss struct {
	Node         string `json:"node"`
	LocationCode string `json:"location_code,omitempty"`

	// Missing is reference sequences this node did not record, and ReferenceSeqs what it is a
	// share of. Summed over the line's channels.
	Missing       uint64 `json:"missing"`
	ReferenceSeqs uint64 `json:"reference_seqs"`

	Episodes []KalshiL2GapEpisode `json:"episodes,omitempty"`
}

// edgeMulticastRecorderLossLineKey identifies one publisher line: its destination group and the
// address it publishes from.
//
// The group is in it because a publisher serves several — the top-of-book and market-by-price
// halves of a feed are two addresses carried on one tunnel — and both planes are now compared, so
// keying on the publisher alone would total its losses across its groups and print that total on
// every one of its rows.
func edgeMulticastRecorderLossLineKey(multicastGroup, publisherSourceIP string) string {
	return multicastGroup + "|" + publisherSourceIP
}

// edgeMulticastRecorderLossFold turns the cached per-(path, node) series into per-publisher-line
// recorder loss, plus the line every reader actually asks for: where SEVERAL recorders lost at the
// same second.
//
// Simultaneity is computed per PATH and only then unioned. Merging the nodes' seconds first would
// call it simultaneous when one node lost on one channel and another node on a different one — two
// unrelated losses that happen to share a clock reading. Perps carries one channel per publisher so
// the two agree there; a publisher with several would not.
//
// Two or more is the threshold, and the ceiling is not "all of them". The reference is the union of
// what the nodes recorded, so a second in which EVERY node lost cannot exist: the message would be
// in nobody's set and therefore in no reference. What several nodes losing at once does say is that
// the cause is not one node's branch — which is the question the per-node lines leave open.
func edgeMulticastRecorderLossFold(series []EdgeMulticastRecorderLossSeries) (map[string][]EdgeMulticastRecorderLoss, map[string][]KalshiL2GapEpisode) {
	type pathKey struct {
		line    string
		group   string
		channel uint8
	}

	byLine := map[string]map[string]*EdgeMulticastRecorderLoss{}
	// Seconds each node lost in, per path, so simultaneity is asked at the grain it means
	// something at.
	byPath := map[pathKey]map[string]map[uint32]bool{}

	for _, s := range series {
		if s.PublisherSourceIP == "" || s.Node == "" {
			continue
		}
		// Keyed on (destination group, publisher), not on the publisher alone. One publisher
		// serves several groups — the tob and mbp halves of a feed are two addresses on one
		// tunnel — and this table now carries both planes, so keying on the address alone would
		// sum a publisher's losses across its groups and show the total on each of its rows.
		lk := edgeMulticastRecorderLossLineKey(s.MulticastGroup, s.PublisherSourceIP)
		if byLine[lk] == nil {
			byLine[lk] = map[string]*EdgeMulticastRecorderLoss{}
		}
		node := byLine[lk][s.Node]
		if node == nil {
			node = &EdgeMulticastRecorderLoss{Node: s.Node, LocationCode: s.LocationCode}
			byLine[lk][s.Node] = node
		}
		node.Missing += s.Missing
		node.ReferenceSeqs += s.ReferenceSeqs

		pk := pathKey{lk, s.MulticastGroup, s.ChannelID}
		if byPath[pk] == nil {
			byPath[pk] = map[string]map[uint32]bool{}
		}
		secs := map[uint32]bool{}
		for _, e := range s.Episodes {
			for i := uint32(0); i < e.Seconds; i++ {
				secs[uint32(e.Start)+i] = true
			}
		}
		byPath[pk][s.Node] = secs
	}

	// Per-node episodes, re-collapsed from the union across the line's channels.
	out := map[string][]EdgeMulticastRecorderLoss{}
	perLineSecs := map[string]map[string]map[uint32]bool{}
	for pk, nodes := range byPath {
		if perLineSecs[pk.line] == nil {
			perLineSecs[pk.line] = map[string]map[uint32]bool{}
		}
		for node, secs := range nodes {
			if perLineSecs[pk.line][node] == nil {
				perLineSecs[pk.line][node] = map[uint32]bool{}
			}
			for sec := range secs {
				perLineSecs[pk.line][node][sec] = true
			}
		}
	}
	for pub, nodes := range byLine {
		lines := make([]EdgeMulticastRecorderLoss, 0, len(nodes))
		for name, node := range nodes {
			flat := make([]uint32, 0, len(perLineSecs[pub][name]))
			for sec := range perLineSecs[pub][name] {
				flat = append(flat, sec)
			}
			node.Episodes = collapseKalshiL2GapSeconds(flat)
			lines = append(lines, *node)
		}
		// Worst first, then by name: a reader who looks at one line looks at the one that
		// matters, and the order cannot shuffle between polls of an unchanged payload.
		sort.Slice(lines, func(i, j int) bool {
			if lines[i].Missing != lines[j].Missing {
				return lines[i].Missing > lines[j].Missing
			}
			return lines[i].Node < lines[j].Node
		})
		out[pub] = lines
	}

	// The global line, per path and then unioned.
	simul := map[string][]KalshiL2GapEpisode{}
	simulSecs := map[string]map[uint32]bool{}
	for pk, nodes := range byPath {
		if len(nodes) < 2 {
			continue
		}
		count := map[uint32]int{}
		for _, secs := range nodes {
			for sec := range secs {
				count[sec]++
			}
		}
		for sec, n := range count {
			if n < 2 {
				continue
			}
			if simulSecs[pk.line] == nil {
				simulSecs[pk.line] = map[uint32]bool{}
			}
			simulSecs[pk.line][sec] = true
		}
	}
	for pub, secs := range simulSecs {
		flat := make([]uint32, 0, len(secs))
		for sec := range secs {
			flat = append(flat, sec)
		}
		simul[pub] = collapseKalshiL2GapSeconds(flat)
	}
	return out, simul
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
func (a *API) edgeMulticastSequenceHealth(ctx context.Context, captureSources edgeMulticastCaptureSourceMap) (map[string]*EdgeMulticastSequenceHealth, time.Time, int, error) {
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

	at, gapWindowSecs := a.foldKalshiL2Coverage(ctx, captureSources, out)
	note(at)
	note(a.foldEdgeMulticastTOBSequence(ctx, captureSources, out))

	if len(out) == 0 {
		return nil, time.Time{}, 0, nil
	}
	for _, health := range out {
		// Before the tally, and at the group grain on purpose: the demotion needs the other
		// paths of the group, which the per-publisher split below no longer has.
		demoteEdgeMulticastQuietCaptureSources(health)
		finishEdgeMulticastSequenceHealth(health)
	}
	return out, asOf.UTC(), gapWindowSecs, nil
}

// foldKalshiL2Coverage adds the market-by-price series, and returns the payload's own clock
// alongside the width of the window its gap episodes are stamped inside.
//
// The window travels with the episodes because a run of seconds is meaningless without the frame
// it is drawn in: the consumer needs (as-of - window, as-of] to place a start on an axis, and
// reading the width from a second copy of kalshiL2WindowMinutes on the far side would let the
// axis and the data disagree the first time the window changes.
func (a *API) foldKalshiL2Coverage(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) (time.Time, int) {
	data, err := a.readPageCache(ctx, kalshiL2CoverageCacheKey)
	if err != nil {
		// A miss, which is the normal state before the refresher's first run and in local
		// dev. Not logged: the page says so by dropping the column.
		return time.Time{}, 0
	}

	var coverage KalshiL2CoverageResponse
	if err := json.Unmarshal(data, &coverage); err != nil {
		// A shape mismatch means the cache key was not bumped alongside a payload change,
		// which is a deploy-time bug worth a line — but not this page's failure.
		slog.Warn("edge multicast sequence health: l2 coverage cache did not parse", "error", err)
		return time.Time{}, 0
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

			CaptureSource: lane.Source,
			ChannelID:     lane.ChannelID,
			Node:          lane.MeasurementNodeID,
			LocationCode:  lane.LocationCode,
			Messages:      lane.Messages,
			GapBooks:      lane.GapBooks,
			GapMessages:   lane.GapMessages,
			GapEpisodes:   lane.GapEpisodes,

			UpdatesReceived: lane.UpdatesReceived,
			UpdatesMissing:  lane.UpdatesMissing,
			SeqGapEvents:    lane.SeqGapEvents,
			MaxGapMessages:  lane.MaxGapMessages,
			P99GapMessages:  lane.P99GapMessages,

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
	return coverage.GeneratedAt.UTC(), coverage.WindowMinutes * 60
}

// foldEdgeMulticastTOBSequence adds the top-of-book series out of the observations payload, and
// returns that payload's own clock.
//
// Every instance it produces carries GapsMeasured = false. That is the whole difference between
// the two legs and it is not a shortfall to be papered over: this plane has no gap marker to
// count, so the series can say it is advancing and cannot say it lost nothing.
func (a *API) foldEdgeMulticastTOBSequence(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) time.Time {
	data, err := a.readPageCache(ctx, edgeMulticastObservationsCacheKey)
	if err != nil {
		return time.Time{}
	}

	var payload EdgeMulticastObservationsResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast sequence health: tob sequence cache did not parse", "error", err)
		return time.Time{}
	}

	for _, series := range payload.Series {
		// Top-of-book only. The market-by-price rows in this payload are here for the parity
		// check; their Sequence series comes from kalshi_l2_coverage.go, which has a gap
		// marker this table does not carry. Folding both would let one column disagree with
		// itself about one feed.
		if !strings.HasPrefix(series.Source, edgeMulticastTOBSourcePrefix) {
			continue
		}
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

// demoteEdgeMulticastQuietCaptureSources tells a path that stopped delivering apart from a capture
// source that stopped producing.
//
// A stalled instance says one thing on its own: this series recorded nothing over the last couple
// of minutes of the window. It does not say whose silence it is, and on a per-event feed it is
// usually nobody's — a sports capture source is one market, and a market that closes mid-window
// goes quiet on every path at once. Measured on mainnet: both paths of edge-kalshi-sports-tob read
// 'stalled 1/29' on the same instance, which is the signature of a market ending rather than of a
// path or a recorder dying. 'stalled' then outranked 'behind' on the publisher line and hid the one
// finding on that row which was about the path.
//
// So the call needs a peer, exactly as path parity does, and it keys the same way for the same
// reasons: the recording node IS in the key, because a node that went quiet on everything is the
// node's silence and not the source's; the channel is NOT, because the two paths of a feed publish
// it under different channel ids and keying on channel would put each path in a group of one.
//
// A path is quiet at a (capture source, node) when every instance it has there is stalled. When
// every path there is quiet the capture source is what went silent, and those instances are
// flagged; when one path is still delivering, the stalled ones are a fault of their own and keep
// it. A vantage with a single path records nothing either way — there is nothing there to tell a
// dead path from a quiet source with, and guessing in either direction is worse than letting the
// stall stand.
//
// The guard that makes the whole thing safe is aliveHere: a path may only be excused at a capture
// source if it is itself delivering at THAT VANTAGE. Two failures need it, and they need different
// halves of it. A feed that stopped everywhere — every market closed, or the venue down — would
// otherwise find every path quiet at every source and demote all of it, reading as advancing while
// nothing advanced. And a recording node that stops ingesting mid-window is the same shape one
// level down: every series it holds goes stale together, so every vantage-local pair is quiet on
// both paths. Keyed on the path alone the paths still look alive — they are delivering at the OTHER
// recorders — and a dead recorder would be excused as the venue going quiet, which is precisely the
// attribution this function exists to get right.
func demoteEdgeMulticastQuietCaptureSources(health *EdgeMulticastSequenceHealth) {
	type pathTally struct{ stalled, total int }

	// Keyed on (path, vantage), not on the path: see aliveHere in the comment above.
	type pathAtNode struct{ ip, node string }
	byVantage := map[edgeMulticastPathParityKey]map[string]*pathTally{}
	aliveHere := map[pathAtNode]bool{}
	for _, inst := range health.Instances {
		// No source address is no path: an instance that cannot be attributed to one cannot
		// be compared against the others, and must not stand in as a peer for them either.
		if inst.PublisherSourceIP == "" {
			continue
		}
		if inst.Status != edgeMulticastSeqStalled {
			aliveHere[pathAtNode{ip: inst.PublisherSourceIP, node: inst.Node}] = true
		}
		key := edgeMulticastPathParityKey{source: inst.CaptureSource, node: inst.Node}
		if byVantage[key] == nil {
			byVantage[key] = map[string]*pathTally{}
		}
		tally := byVantage[key][inst.PublisherSourceIP]
		if tally == nil {
			tally = &pathTally{}
			byVantage[key][inst.PublisherSourceIP] = tally
		}
		tally.total++
		if inst.Status == edgeMulticastSeqStalled {
			tally.stalled++
		}
	}

	quiet := map[edgeMulticastPathParityKey]bool{}
	for key, paths := range byVantage {
		if len(paths) < 2 {
			continue
		}
		all := true
		for _, tally := range paths {
			if tally.stalled != tally.total {
				all = false
				break
			}
		}
		quiet[key] = all
	}

	for i := range health.Instances {
		inst := &health.Instances[i]
		if inst.Status != edgeMulticastSeqStalled || !aliveHere[pathAtNode{ip: inst.PublisherSourceIP, node: inst.Node}] {
			continue
		}
		if quiet[edgeMulticastPathParityKey{source: inst.CaptureSource, node: inst.Node}] {
			inst.CaptureSourceQuiet = true
		}
	}
}

// edgeMulticastAllPathsGapped is the seconds in which EVERY path of a feed lost data at once.
//
// This is the one sequence finding that belongs to the group and to no line, which is why it lives
// here rather than on a publisher: a path can only report its own loss, and "A lost while B held"
// is the redundancy working. What no line can say is that A and B lost together — and that is the
// only case where the FEED lost data rather than one of its paths.
//
// Keyed on (capture source, recording node) before intersecting, for the same two reasons
// edgeMulticastPathParity is: the node has to be in the key or a recorder that stopped ingesting
// looks like every path failing at once, and the capture source has to be in it or two unrelated
// losses at two different markets in the same second read as one shared outage. The channel is NOT
// in the key — the paths of a feed publish it under different channel ids, so keying on it would
// put each path alone and intersect nothing.
//
// Measured on mainnet over six hours of edge-kalshi-perps-mbp: 22 seconds where both paths lost
// together, against 83 and 84 where only one did. Rare enough to mean something, common enough to
// be worth a badge.
func edgeMulticastAllPathsGapped(instances []EdgeMulticastChannelInstance) []KalshiL2GapEpisode {
	type vantage struct {
		source string
		node   string
	}
	// Per vantage, per publisher, the seconds that publisher was losing.
	byVantage := map[vantage]map[string]map[uint32]bool{}
	for _, inst := range instances {
		// Only the plane that measures gaps at all. A top-of-book series has no marker, so its
		// empty episode list is an absence of measurement and must not count as "held".
		if !inst.GapsMeasured || inst.PublisherSourceIP == "" {
			continue
		}
		v := vantage{inst.CaptureSource, inst.Node}
		if byVantage[v] == nil {
			byVantage[v] = map[string]map[uint32]bool{}
		}
		if byVantage[v][inst.PublisherSourceIP] == nil {
			byVantage[v][inst.PublisherSourceIP] = map[uint32]bool{}
		}
		for _, e := range inst.GapEpisodes {
			for i := uint32(0); i < e.Seconds; i++ {
				byVantage[v][inst.PublisherSourceIP][uint32(e.Start)+i] = true
			}
		}
	}

	shared := map[uint32]bool{}
	for _, publishers := range byVantage {
		// One path at a vantage cannot fail "together" with anything. Recording nothing here is
		// deliberate: a single-path group has no redundancy to lose, and claiming otherwise would
		// turn every ordinary gap into a feed outage.
		if len(publishers) < 2 {
			continue
		}
		var first map[uint32]bool
		for _, secs := range publishers {
			if first == nil {
				first = secs
				continue
			}
			next := map[uint32]bool{}
			for sec := range first {
				if secs[sec] {
					next[sec] = true
				}
			}
			first = next
		}
		for sec := range first {
			shared[sec] = true
		}
	}
	if len(shared) == 0 {
		return nil
	}
	flat := make([]uint32, 0, len(shared))
	for sec := range shared {
		flat = append(flat, sec)
	}
	return collapseKalshiL2GapSeconds(flat)
}

// finishEdgeMulticastSequenceHealth tallies the instance states and rolls them up worst-first.
func finishEdgeMulticastSequenceHealth(health *EdgeMulticastSequenceHealth) {
	// Computed over every instance the roll-up holds, before any per-publisher split. On a
	// per-publisher health this is always empty, which is correct — one path cannot fail together
	// with itself — so the same call is safe on both grains.
	health.AllPathsGapped = edgeMulticastAllPathsGapped(health.Instances)

	rank := map[string]int{
		edgeMulticastSeqGapped:  0,
		edgeMulticastSeqStalled: 1,
		edgeMulticastSeqOK:      2,
	}
	gapNodes := map[string]struct{}{}
	for _, inst := range health.Instances {
		if !inst.GapsMeasured {
			health.GapsUnmeasured++
		} else {
			gapNodes[inst.Node] = struct{}{}
		}
		switch inst.Status {
		case edgeMulticastSeqGapped:
			health.Gapped++
		case edgeMulticastSeqStalled:
			if inst.CaptureSourceQuiet {
				health.CaptureSourceQuiet++
			} else {
				health.Stalled++
			}
		}
	}
	health.GapNodes = len(gapNodes)
	sort.SliceStable(health.Instances, func(i, j int) bool {
		a, b := health.Instances[i], health.Instances[j]
		if rank[a.Status] != rank[b.Status] {
			return rank[a.Status] < rank[b.Status]
		}
		// A stall the capture source owns is not a finding about this path, so it reads under
		// the ones that are.
		if a.CaptureSourceQuiet != b.CaptureSourceQuiet {
			return !a.CaptureSourceQuiet
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
func attachEdgeMulticastSequenceHealth(lines []EdgeMulticastPublisher, health *EdgeMulticastSequenceHealth, multicastGroup string, recorderLoss map[string][]EdgeMulticastRecorderLoss, recorderLossSimul map[string][]KalshiL2GapEpisode, recorderLossUnavailable bool) {
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
		// Keyed on the tunnel address, the same join the instances above make. A line with no
		// entry keeps nil, which is "no peer to be measured against" and not "measured clean".
		lk := edgeMulticastRecorderLossLineKey(multicastGroup, lines[i].DZIP)
		h.RecorderLoss = recorderLoss[lk]
		h.RecorderLossSimultaneous = recorderLossSimul[lk]
		h.RecorderLossUnavailable = recorderLossUnavailable
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
