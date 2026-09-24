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
//
// # The third leg: the recorder's own rows
//
// edge_multicast_recorder_sequence.go folds a third cached payload, and it is the only one whose
// magnitude has had the OBSERVER's own loss taken out of it. The other two count what did not reach
// a decoded table, and a datagram the recorder's capture ring dropped is missing from that table
// exactly the way one the publisher never sent is. The recorder writes its admitted drop as a
// number per datagram and its deriver subtracts it, leaving `unexplained_count` — so where that leg
// has rows for an instance, its number replaces the others' and re-decides the loss half of the
// verdict. Where it has none it says nothing, and the instance keeps whatever the other two said.
//
// It also brings the two things a decoder-derived source structurally cannot: a feed is covered on
// the day it is first RECORDED rather than on the day a bot learns to fold its book, and the
// arithmetic is gated by the recorder's declared capture mode, so a fleet running AF_PACKET is told
// "unverifiable" rather than handed a number that may be its own ring's.

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

	// GapBooks is how many distinct books gapped at all in the window. It is a RECOVERY state
	// and not a loss count — how many books were left un-anchored and could not be trusted until
	// a snapshot re-anchored them, which is the same thing a venue feed handler reports when it
	// marks an instrument gapped and stops publishing it. UpdatesMissing is what says how much
	// was lost.
	//
	// It saturates at the channel's instrument count, so it can never size a loss: measured over
	// six hours of mainnet, perps read 13 books (of 13 instruments) against 3,439 updates lost,
	// while ncaaf read 1,934 books against 2,693 — 149x the books for 0.78x the loss. It is still
	// a trigger for the verdict, because a marker written is a loss observed.
	//
	// NOT GapMessages, which counts every message that arrived while a book was un-anchored and
	// therefore scales with traffic rather than with reliability — 22 real discontinuities
	// produced 158,912 gap-marked messages on perps. See KalshiL2Lane, which documents that
	// decision at length; this column deliberately carries the same one so the two pages cannot
	// tell different stories about one feed.
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
	//
	// **The two planes count different things into Resets**, and they share this field and the
	// tooltip that renders it. Market-by-price counts `instrument_reset` messages, one book
	// re-anchored each; the recorded-gap leg counts how far `reset_count` advanced, one per era
	// over a channel that carries every instrument. The reading both legs state — "a series with
	// gaps and no reset is not re-anchoring" — holds on either, but the magnitudes do not
	// compare across them: one era advance re-anchoring 20 books prints `1` from the recorder's
	// leg where market-by-price would print `20` for the same event.
	//
	// **Absent and zero are different readings**, and the sentence above is why: zero cycles
	// on a gapped series is a finding, so a plane that cannot count them must not print one.
	//
	// Carried as a plain number with a separate flag rather than as an omitted field, because
	// a deploy is not atomic: a tab still running the previous bundle dereferences
	// `snapshot_cycles` unguarded inside its `gaps_measured` branch, and the recorder's series
	// now reach that branch. Omitting the key would throw inside a render and take the whole
	// page down for anyone who had not reloaded. So the number is always there — the old
	// bundle reads 0 and prints what it always printed — and the flag is what the current one
	// reads to omit the clause.
	Resets                 uint64 `json:"resets"`
	SnapshotCycles         uint64 `json:"snapshot_cycles"`
	SnapshotCyclesMeasured bool   `json:"snapshot_cycles_measured"`

	// GapsMeasured says whether GapBooks is a reading or an absence. A zero GapBooks with this
	// false is "not checked", and the UI has to render it as something other than a clean bill
	// of health.
	//
	// True wherever a producer writes a gap marker this can count: the market-by-price plane
	// from `kalshi_mbp_levels.status_after`, and — since the feed-race recorder began writing
	// `uncertain_reason` — the top-of-book plane too, from `kalshi_edge_book_top`
	// (`edge_multicast_tob_gaps.go`).
	//
	// It stays false for a top-of-book series the recorder has not covered, which is the
	// observations leg's own reading: that table carries no marker, and the obvious substitute
	// is wrong on its grain by construction — a row exists only where the top CHANGED, so
	// diffing sequence numbers against the row count reports ~3% loss on a healthy feed. The
	// measurement is in `edge_multicast_observations.go`.
	GapsMeasured bool `json:"gaps_measured"`

	// LossGrain names the counter UpdatesReceived/UpdatesMissing are expressed in, because two
	// legs fill those fields in different units and nothing else on the row distinguishes them.
	// `levels` is the market-by-price leg's holes in `per_instrument_seq`; `datagrams` is the
	// recorder leg's datagram-header sequence values. Summing across the two is meaningless —
	// one break in the header can swallow many level updates — so a consumer that aggregates a
	// line's instances must group on this first. Empty means the fields are unset.
	LossGrain string `json:"loss_grain,omitempty"`

	// AttributionWithheld says a loss WAS observed on this instance and its size cannot be
	// charged to the publisher — capture-handle scope with a handle that admitted drops. It is a
	// marking and not an erasure: the counters beside it belong to whichever leg took them, and
	// a reader must not be shown "no updates to count" over a series that counted half a million.
	AttributionWithheld bool `json:"attribution_withheld,omitempty"`

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

	// RecorderLossSource names which measurement produced RecorderLoss: `recorder` for the
	// recording nodes' own sequence-loss rows, `peers` for each node measured against the others.
	//
	// It is on the payload rather than left to the reader's inference because the two measure
	// against different references. The peer comparison's is the UNION of what the nodes received,
	// so a datagram nobody received is in nobody's reference and an empty strip does not rule it
	// out; the recorder's is the publisher's own numbering, where an empty strip does.
	RecorderLossSource string `json:"recorder_loss_source,omitempty"`

	// RecorderLossPublisher is the runs the rule set attributed to the PUBLISHER — absent from
	// every site, no recorder overflow anywhere, coverage intact. Recorder leg only, and the
	// stronger form of what RecorderLossSimultaneous reaches for.
	RecorderLossPublisher []KalshiL2GapEpisode `json:"recorder_loss_publisher,omitempty"`

	// RecorderGapsUnavailable says the recorder rows exist and the read of them failed, so the
	// strip above is the peer comparison standing in. Deliberately not folded into
	// RecorderLossUnavailable: something WAS measured, and "not measured" over a strip carrying
	// marks is a worse answer than a note beside it.
	RecorderGapsUnavailable bool `json:"recorder_gaps_unavailable,omitempty"`

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

	// The rest is the recorder leg's and absent on the peer comparison, which cannot produce any
	// of it. See EdgeMulticastSequenceHealth.RecorderLossSource for which leg a line came from.

	// MissingRaw is Missing before this recorder's own admitted drops came off, and Admitted is
	// what came off. Carried so a tooltip can show the arithmetic rather than a number a reader
	// has to trust: the peer comparison can only infer that share, and a load spike reaches every
	// node at once.
	MissingRaw uint64 `json:"missing_raw,omitempty"`
	Admitted   uint64 `json:"admitted,omitempty"`

	// Runs is contiguous runs of missing sequence numbers — the count of episodes, never their
	// size. The size is Missing.
	Runs uint64 `json:"runs,omitempty"`

	// MissingByVerdict splits Missing by the rule set's attribution:
	// recorder | upstream | path | unverifiable | publisher.
	MissingByVerdict map[string]uint64 `json:"missing_by_verdict,omitempty"`

	// Unverifiable says the archive had a hole over this window, so a clean reading here is an
	// absence of evidence rather than a clean run — the object that would have carried the loss is
	// the one we do not hold. It qualifies a non-zero Missing too, where it makes the figure a
	// floor rather than a measurement.
	Unverifiable bool `json:"unverifiable,omitempty"`

	// Datagrams is what this node recorded on the line, from coverage.
	//
	// It is what a CLEAN row has instead of a rate: no gap row means no ReferenceSeqs either, so
	// without it the line the whole comparison rests on — "cmh lost 0" beside "was lost 267" —
	// described itself as "0 of 0 sequence numbers the publisher sent".
	Datagrams uint64 `json:"datagrams,omitempty"`
}

// sortEdgeMulticastRecorderLoss orders a line's nodes worst-first, then by name.
//
// Worst first so a reader who looks at one row looks at the one that matters; by name after so the
// order cannot shuffle between two polls of an unchanged payload. Shared by both legs, because a
// strip whose row order depended on which measurement produced it would be unreadable across a
// fallback.
func sortEdgeMulticastRecorderLoss(lines []EdgeMulticastRecorderLoss) {
	sort.Slice(lines, func(i, j int) bool {
		if lines[i].Missing != lines[j].Missing {
			return lines[i].Missing > lines[j].Missing
		}
		return lines[i].Node < lines[j].Node
	})
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
		sortEdgeMulticastRecorderLoss(lines)
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

// edgeMulticastSequenceHealth folds the three cached refresher payloads into per-group sequence
// health: the L2 coverage one for market-by-price, the top-of-book one, and the recorder's own
// sequence-loss rows.
//
// Returns (nil, zero time, nil) when there is nothing to fold — no cache entries, entries that do
// not parse, or nothing that resolves to a group on this page. All of those are "no column",
// never an error: this signal is additive to the page and must not be able to fail it. One leg
// missing costs that plane's rows and leaves the others' intact.
//
// ORDER MATTERS between the third leg and the first two. The recorder's magnitude is the only one
// with the observer's own loss subtracted out of it, so it runs LAST and overwrites what the other
// two said about an instance it has rows for — see foldEdgeMulticastRecorderSequence. It cannot run
// first: it would have nothing to overwrite and the level-grain leg would then land on top of it.
//
// The reported as-of is the OLDEST of the legs. They run on one refresher today, so the stamps are
// seconds apart; taking the oldest means that if they ever diverge, the column ages against the
// stalest leg rather than flattering itself with the freshest.
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

	// The axis width, taken from whichever measured leg reports one — and the WIDER of them if
	// both do. It is not the market-by-price leg's to supply alone: the page draws no timeline at
	// all without it, so reading it from one cache made an independent miss there erase every
	// top-of-book episode this run had measured, on a page whose whole rule is that one leg
	// missing costs that leg's rows and no more. Wider rather than narrower because the episodes
	// are placed by absolute start: a span too wide draws them further right than they need to
	// be, a span too narrow clamps everything older than it into a pile on the left edge.
	var gapWindowSecs int
	widen := func(secs int) {
		if secs > gapWindowSecs {
			gapWindowSecs = secs
		}
	}

	at, coverageWindow := a.foldKalshiL2Coverage(ctx, captureSources, out)
	note(at)
	widen(coverageWindow)
	note(a.foldEdgeMulticastTOBSequence(ctx, captureSources, out))
	// **After the observations leg, never before it.** This one replaces the series that leg
	// folded for the same channel instance, so it has to find them already there; run first, it
	// would append and then be overwritten by the staleness-only reading it exists to replace.
	gapsAt, gapsWindow := a.foldEdgeMulticastTOBGaps(ctx, captureSources, out)
	note(gapsAt)
	widen(gapsWindow)
	// **Last of the legs**, because it regrades rather than appends: it measures against the
	// publisher's own numbering, the only reference here that does not depend on what someone
	// recorded, so what it finds outranks what the capture planes inferred. Run before either of
	// them it would regrade rows that are not there yet.
	note(a.foldEdgeMulticastRecorderSequence(ctx, captureSources, out))

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
			// Holes in `per_instrument_seq`: level updates, not datagrams. See LossGrain.
			LossGrain:      edgeMulticastLossGrainLevels,
			SeqGapEvents:   lane.SeqGapEvents,
			MaxGapMessages: lane.MaxGapMessages,
			P99GapMessages: lane.P99GapMessages,

			Resets: lane.Resets,
			// Always a reading on this plane, zero included.
			SnapshotCycles:         lane.SnapshotCycles,
			SnapshotCyclesMeasured: true,
			LastSeen:               lane.LastSeen.UTC(),
			Status:                 edgeMulticastSequenceStatus(lane.GapBooks, lane.UpdatesMissing, lane.LastSeen, coverage.GeneratedAt),
			GapsMeasured:           true,
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
			// Graded on staleness alone. Passing zeroes for both loss counters is not a claim
			// that either is zero — GapsMeasured is what carries that, and it is false here.
			Status:       edgeMulticastSequenceStatus(0, 0, series.LastSeen, payload.GeneratedAt),
			GapsMeasured: false,
		}
		if out[groupPK] == nil {
			out[groupPK] = &EdgeMulticastSequenceHealth{}
		}
		out[groupPK].Instances = append(out[groupPK].Instances, inst)
	}
	return payload.GeneratedAt.UTC()
}

// foldEdgeMulticastRecorderSequence applies the recorder's own sequence-loss rows, and returns that
// payload's own clock.
//
// A miss is the normal state wherever the recorder's analysis tier is not loading rows yet, and it
// costs nothing: this leg has no opinion and the other two stand exactly as they were.
func (a *API) foldEdgeMulticastRecorderSequence(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) time.Time {
	data, err := a.readPageCache(ctx, edgeMulticastRecorderSequenceCacheKey)
	if err != nil {
		return time.Time{}
	}

	var payload EdgeMulticastRecorderSequenceResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast sequence health: recorder sequence cache did not parse", "error", err)
		return time.Time{}
	}

	applyEdgeMulticastRecorderSequence(payload, captureSources, out)
	return payload.GeneratedAt.UTC()
}

// applyEdgeMulticastRecorderSequence is the fold itself, over an already-read payload.
//
// # Where this leg has rows for an instance, it wins
//
// Its magnitude is `unexplained_count`: what was missing, less what the recorder admits losing
// itself. Neither other leg can make that subtraction — the level-grain leg counts holes in a
// decoded table, where a datagram the capture ring dropped is missing exactly the way one the
// publisher never sent is, and comparing recorders is blind to a loss they share, which is what a
// load spike on a shared host produces. So where a recorder row exists, its number replaces the
// other leg's on that instance, and the verdict's loss half is re-decided from it.
//
// What it replaces is the MAGNITUDE and nothing else. GapBooks, GapMessages and GapEpisodes stay:
// they are a recovery state and a time axis measured on another plane, they are not loss counts,
// and overwriting them with silence would take the gap timeline and the all-paths intersection off
// a feed that has both. GapsMeasured stays for the same reason — it says whether a gap MARKER was
// read, and this plane has no marker to read, only a count of values that never arrived.
//
// # An instance with no rows here gets no opinion from here
//
// The loop is over the payload's series, so an instance the recorder wrote nothing for is never
// touched and falls through to whatever the other legs said. That is the whole of the rule and it
// is worth stating: nothing in this system looks more like a healthy feed than a silence nobody
// claimed, and an absent recorder row is a silence — the recorder may be down, may never have been
// asked to join that group, may be loading late. FetchEdgeMulticastRecorderSequence already refuses
// to emit a row for a window with no coverage for the same reason.
//
// # A recording node is never folded into another
//
// A series is matched to at most ONE existing instance and each existing instance is claimed at
// most once, so two vantages of one channel instance stay two rows even where a recorder and a
// capture share a name. Two vantages of one instance are two observations, and merging them hides a
// recorder that is missing the feed.
func applyEdgeMulticastRecorderSequence(payload EdgeMulticastRecorderSequenceResponse, captureSources edgeMulticastCaptureSourceMap, out map[string]*EdgeMulticastSequenceHealth) {
	// Instances already claimed by an earlier series of this same payload, per group. Two series
	// can legitimately present the same (publisher, channel, node) — one recorder name hosted in
	// two of the recorder's own environments — and the second must become its own row rather
	// than overwrite the first's target.
	claimed := map[string]map[int]bool{}

	for _, s := range payload.Series {
		// The destination address, which is on the gap row for exactly this purpose. There is no
		// capture-source fallback here and there must not be: the recorder's `feed` is its own
		// spec name for a stream, a different namespace from the capture source ids the other
		// legs resolve by, and matching one against the other would attribute a series to a group
		// by coincidence of spelling.
		groupPK := captureSources.resolveMulticastIP(s.MulticastGroup)
		if groupPK == "" {
			continue
		}
		if out[groupPK] == nil {
			out[groupPK] = &EdgeMulticastSequenceHealth{}
		}
		if claimed[groupPK] == nil {
			claimed[groupPK] = map[int]bool{}
		}
		health := out[groupPK]

		received, missing := edgeMulticastRecorderLossFor(s)
		withheld := !s.verifiable()

		if i := edgeMulticastRecorderMatch(health.Instances, s, claimed[groupPK]); i >= 0 {
			claimed[groupPK][i] = true
			inst := &health.Instances[i]
			if withheld {
				// At capture-handle scope with a handle that admitted drops, the residue cannot
				// be charged to this instance and MUST NOT be reported as the publisher's. What
				// is withheld is THIS leg's number, not the finding and not the other leg's
				// reading: overwriting with zeroes denied counters the level-grain leg had
				// legitimately taken, and `omitempty` then rendered half a million counted
				// updates as "no level updates to count". Mark it and leave the row alone.
				inst.AttributionWithheld = true
			} else {
				inst.UpdatesReceived = received
				inst.UpdatesMissing = missing
				inst.LossGrain = edgeMulticastLossGrainDatagrams
				inst.SeqGapEvents = s.Gaps
				inst.MaxGapMessages = clampUint32(s.MaxRun)
				inst.P99GapMessages = s.P99Run
			}
			inst.Status = edgeMulticastRecorderRegrade(inst.Status, inst.GapBooks, missing, withheld)
			continue
		}

		inst := EdgeMulticastChannelInstance{
			PublisherSourceIP: s.PublisherSourceIP,
			// The recorder's feed name stands in for a capture source id, and it is a different
			// namespace — see EdgeMulticastRecorderSequenceSeries.Feed. It only ever groups this
			// leg's own instances with each other, which is what the quiet-capture-source
			// demotion should do with them: a recorder's silence is not a capture's silence.
			CaptureSource: s.Feed,
			ChannelID:     s.ChannelID,
			Node:          s.Node,
			LocationCode:  s.LocationCode,
			// Datagrams archived, which is the closest thing this plane has to a message count
			// and is the denominator's own source. Never GapMessages' denominator: there are no
			// gap-marked messages here to be a share of.
			Messages:        s.Datagrams,
			UpdatesReceived: received,
			UpdatesMissing:  missing,
			// Datagram-header sequence values, never level updates. See LossGrain.
			LossGrain:      edgeMulticastLossGrainDatagrams,
			SeqGapEvents:   s.Gaps,
			MaxGapMessages: clampUint32(s.MaxRun),
			P99GapMessages: s.P99Run,
			LastSeen:       s.LastSeen.UTC(),
			// False, and not a shortfall to paper over. GapsMeasured says whether GapBooks is a
			// reading, and on this plane there is no book-level gap marker at all: the recorder
			// reads datagram headers and never folds a book. The loss reading this leg does carry
			// travels in UpdatesMissing over UpdatesReceived, which is a different counter with a
			// different denominator, so a roll-up counting this instance in GapsUnmeasured is
			// saying the true thing — no marker here — rather than "nothing measured here".
			//
			// It also keeps this instance out of the all-paths-gapped intersection, which is
			// exactly right: that intersection is over SECONDS a book was un-anchored, a gap row
			// is a run of sequence numbers, and an empty episode list from a plane that has no
			// episodes must never be read as a path that held.
			GapsMeasured: false,
		}
		if withheld {
			// The residue is not this instance's to report, so the counters do not travel — but
			// the row says why, rather than presenting an unexplained pair of zeroes.
			inst.UpdatesReceived, inst.UpdatesMissing = 0, 0
			inst.SeqGapEvents, inst.MaxGapMessages, inst.P99GapMessages = 0, 0, 0
			inst.LossGrain = ""
			inst.AttributionWithheld = true
		}
		inst.Status = edgeMulticastSequenceStatus(0, missing, s.LastSeen, payload.GeneratedAt)
		if withheld {
			// Graded above with no magnitude, which would read as a clean window. The loss was
			// observed; only its owner is unknown.
			inst.Status = edgeMulticastSeqGapped
		}
		health.Instances = append(health.Instances, inst)
		// **Claimed as soon as it exists.** Without this a later series of the same payload can
		// match the row just appended and overwrite it, so two series differing only in `site` or
		// `env` collapse into one, last write wins — the merge edgeMulticastRecorderMatch exists
		// to prevent.
		claimed[groupPK][len(health.Instances)-1] = true
	}
}

// edgeMulticastRecorderMatch finds the instance a recorder series is another reading of, or -1.
//
// Keyed on (publisher source address, Channel ID, recording node) inside one group, which is the
// channel instance at one vantage with the destination port folded — the grain every leg of this
// column reports at. The group is already fixed by the caller, so it is not in the key.
//
// The node has to be in it. Two recorders of one instance are two observations and the whole page
// is built on not merging them; a series matching on publisher and channel alone would land on
// whichever vantage happened to be first in the slice and silently overwrite one recorder's reading
// with another's.
func edgeMulticastRecorderMatch(instances []EdgeMulticastChannelInstance, s EdgeMulticastRecorderSequenceSeries, claimed map[int]bool) int {
	for i, inst := range instances {
		if claimed[i] {
			continue
		}
		if inst.PublisherSourceIP == s.PublisherSourceIP && inst.ChannelID == s.ChannelID && inst.Node == s.Node {
			return i
		}
	}
	return -1
}

// edgeMulticastSequenceStatus grades one series.
//
// Two independent pieces of evidence make it 'gapped', and either alone is enough:
//
//   - updatesMissing, holes in the per-instrument sequence — messages that never arrived. This is
//     the one that carries a magnitude, and it is the one the column reports.
//   - gapBooks, the recorder's own gap marker. It is a RECOVERY state and not a loss count: it says
//     a book is un-anchored and its state cannot be trusted until a snapshot re-anchors it, which
//     is the same thing a venue feed handler does when it marks an instrument gapped and stops
//     publishing it. It saturates at the channel's instrument count, so it can never size a loss —
//     but a marker written is still a loss observed, so it stays as a trigger.
//
// Grading on gapBooks ALONE was a false negative, measured on mainnet over six hours: five channel
// instances lost updates with no gap marker written at all, the worst of them 958 updates at 1,551
// ppm on ligue1 ch25, and the column called every one of them 'ok'. The reverse case — a marker
// with no hole in the numbering — is loss at a reset boundary the partition key already separated,
// and is equally a finding.
//
// Staleness is measured against the coverage payload's OWN clock, not wall clock. The entry is up
// to a refresher interval old, so reading its timestamps against now() would add the refresher's
// lag to every instance and mark healthy series stalled for most of every cycle — the same
// mistake the counter columns on this page document for their own ages.
func edgeMulticastSequenceStatus(gapBooks, updatesMissing uint64, lastSeen, asOf time.Time) string {
	if gapBooks > 0 || updatesMissing > 0 {
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
		//
		// The same rule for the capture source, which is the other half of this key. An unnamed
		// one is not a bucket of its own — every series that lacks a name lands in the SAME
		// bucket, so two unrelated markets' series at one node would be read as two paths of one
		// capture source and one going quiet would excuse the other. The recorded-gap leg can
		// produce such a series, for a channel instance the capture recorded nothing for.
		if inst.PublisherSourceIP == "" || inst.CaptureSource == "" {
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
	// A vantage where every path is stalled is not observing the feed, and an intersection
	// cannot ask a silent witness whether it saw a loss. See the skip below.
	delivering := map[vantage]bool{}
	for _, inst := range instances {
		// Only the plane that measures gaps at all. A top-of-book series has no marker, so its
		// empty episode list is an absence of measurement and must not count as "held".
		//
		// And only a series whose capture source is NAMED, because this key is what the doc
		// comment above says it is: without the source in it, "two unrelated losses at two
		// different markets in the same second read as one shared outage". An unnamed source
		// does not opt out of the key, it collapses into one bucket with every other unnamed
		// one — which is that failure exactly. It used to be unreachable, since the only
		// instances without a source were top-of-book ones excluded on the line above; the
		// recorded-gap leg measures that plane now, so it is reachable and excluded here.
		if !inst.GapsMeasured || inst.PublisherSourceIP == "" || inst.CaptureSource == "" {
			continue
		}
		v := vantage{inst.CaptureSource, inst.Node}
		if byVantage[v] == nil {
			byVantage[v] = map[string]map[uint32]bool{}
		}
		if byVantage[v][inst.PublisherSourceIP] == nil {
			byVantage[v][inst.PublisherSourceIP] = map[uint32]bool{}
		}
		if inst.Status != edgeMulticastSeqStalled {
			delivering[v] = true
		}
		for _, e := range inst.GapEpisodes {
			for i := uint32(0); i < e.Seconds; i++ {
				byVantage[v][inst.PublisherSourceIP][uint32(e.Start)+i] = true
			}
		}
	}

	// **Intersected across the NODES that watch one capture source, and unioned across the
	// capture sources.** The two halves of the vantage key are not the same kind of thing, and folding
	// them into one intersection asks the wrong question. A second where one recorder lost both
	// its paths while its peers hold intact copies is that recorder's reception rather than the
	// feed's loss, so the nodes watching a market have to agree — that is what the node in the
	// key exists for. But two markets are two feeds' worth of data, and requiring them to lose
	// in the same second is a condition nothing satisfies: a sports group carries 29 capture
	// sources, so an intersection over the sources demands all 29 lose at once and the badge
	// stops being reachable at all. Each capture source answers for itself, and the answers are
	// unioned.
	//
	// **This changes nothing on the market-by-price plane**, which is where the 22-second
	// measurement above was taken: every mbp_ source is recorded at exactly one vantage
	// (aws-cmh-mn-recorder1, checked against the live store), and an intersection over one set
	// is that set. It bites where a group has several recorders — which is what the recorded-gap
	// leg just made true for top of book — and it is where a group has many markets that the
	// union matters.
	perSource := map[string]map[uint32]bool{}
	for v, publishers := range byVantage {
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
		// **A vantage that is not delivering takes no part either**, and this is the trap the
		// intersection opens: a recorder that stopped ingesting reports two paths with no gap
		// episodes at all, which intersects to nothing and vetoes every second its peers
		// agree on. One dead recorder would silence the badge for the whole group — the exact
		// inverse of the false positive the intersection was added to fix, and a worse
		// failure, because a suppressed finding leaves nothing on the page to notice.
		//
		// Stalled is the signal: it is what the sequence status already means, and a vantage
		// whose every path is stale is one whose silence is about itself.
		if !delivering[v] {
			continue
		}

		// A vantage with one path was skipped above and takes no part here: it cannot
		// demonstrate that all paths lost, so it neither confirms nor vetoes a second — and it
		// vetoes nothing at its neighbours' markets either, because the fold below is per capture
		// source.
		prev, ok := perSource[v.source]
		if !ok {
			perSource[v.source] = first
			continue
		}
		next := map[uint32]bool{}
		for sec := range prev {
			if first[sec] {
				next[sec] = true
			}
		}
		perSource[v.source] = next
	}
	shared := map[uint32]bool{}
	for _, secs := range perSource {
		for sec := range secs {
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
		// GapsUnmeasured drives a tooltip that reads "loss was never checked", so an instance
		// carrying a loss count does not belong in it even though GapsMeasured is false. The two
		// say different things: GapsMeasured is about GapBooks having been read, and the recorder
		// leg legitimately sets it false while counting loss in UpdatesMissing. Counting it here
		// put "never checked" on the row beside the measured figure.
		if !inst.GapsMeasured && inst.LossGrain == "" {
			health.GapsUnmeasured++
		} else if inst.GapsMeasured {
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
func attachEdgeMulticastSequenceHealth(lines []EdgeMulticastPublisher, health *EdgeMulticastSequenceHealth, multicastGroup string, observations edgeMulticastObservationStatsResult) {
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
		h.RecorderLoss = observations.recorderLoss[lk]
		h.RecorderLossSimultaneous = observations.recorderLossSimul[lk]
		h.RecorderLossPublisher = observations.recorderLossPublisher[lk]
		h.RecorderGapsUnavailable = observations.recorderGapsUnavailable
		// Which leg filled THIS line, which is not a property of the payload: the recorder rows
		// arrive feed by feed, so one group can be recorder-fed while the next still renders the
		// peer comparison.
		src := observations.recorderLossSource[lk]
		// Only where there is a strip to name the source of. On a line with no entry the field
		// would label an absence, and "no peer to compare" is not a property of either
		// measurement.
		if len(h.RecorderLoss) > 0 {
			h.RecorderLossSource = src
		}
		// A failed peer comparison is a finding only on the lines the peer comparison is what
		// renders. On a recorder-fed line it would print "not measured" over a strip built from
		// better rows — and with the leg chosen per line, that is now a per-line question too.
		h.RecorderLossUnavailable = observations.recorderLossUnavailable && src != edgeMulticastLossSourceRecorder
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
