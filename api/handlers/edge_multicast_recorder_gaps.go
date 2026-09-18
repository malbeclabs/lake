package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// The recorder leg of the Sequence column's loss strip: the recording nodes' own sequence-loss
// detection, read from the rows the recorder's analysis tier writes.
//
// It replaces the peer comparison in edge_multicast_observations.go where those rows exist, and the
// difference is not a refinement. The comparison measures each node against the UNION of what the
// nodes received, so a datagram nobody received is in nobody's reference and cannot be reported —
// exactly the loss an operator most wants to see. These rows measure against the publisher's own
// numbering, subtract the recorder's admitted drops instead of inferring them, and carry a verdict
// per run.
//
// Contract, the tables and what lake requires of them:
// docs/plans/2026-09-03-edge-multicast-recorder-sequence-design.md.

const (
	edgeMulticastRecorderGapTable      = "recorder_sequence_gap"
	edgeMulticastRecorderCoverageTable = "recorder_segment_coverage"
)

// edgeMulticastRecorderGapSettings bounds the read server-side, and
// edgeMulticastRecorderGapBudget bounds the whole leg — both table probes and the query — client
// side.
//
// Neither is belt-and-braces. This leg runs FIRST on StartKalshiBackgroundRefresher's serial chain,
// which gives each of its steps a three-minute deadline off ONE parent context, so a read that
// exhausts that deadline does not cost the strip it is additive to: FetchEdgeMulticastObservations
// returns the error, nothing is written, and Msg/s, Peer, Heard and recorder_coverage go absent
// with it. Both sibling reads on this payload bound themselves for that reason, and the peer leg's
// comment records what the omission cost it — failure on every ten-minute cycle at 7.20 GiB, silent
// because the failure is a WARN.
//
// The server-side cap is the tighter of the two on purpose: it fires first and returns a
// ClickHouse error naming the limit, where a client deadline yields a bare context error. The
// budget is the backstop for the case no statement timeout can reach — a proxied table whose
// remote end never answers.
const edgeMulticastRecorderGapSettings = " SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0"

const edgeMulticastRecorderGapBudget = 45 * time.Second

// The two failure modes are escalated apart: a probe that cannot answer and a read that failed are
// different causes, and a run of one must not be reset by the other succeeding.
const (
	edgeMulticastRecorderGapProbeEscKey = "edge_multicast_recorder_gaps:probe"
	edgeMulticastRecorderGapReadEscKey  = "edge_multicast_recorder_gaps:read"
)

// The five verdicts, in the upstream spec's own spelling. They are the rule set's, not this
// page's: a verdict lake invented would be one the recorder cannot produce.
const (
	edgeMulticastGapVerdictRecorder     = "recorder"
	edgeMulticastGapVerdictUpstream     = "upstream"
	edgeMulticastGapVerdictPath         = "path"
	edgeMulticastGapVerdictUnverifiable = "unverifiable"
	edgeMulticastGapVerdictPublisher    = "publisher"
)

// Which measurement produced a line's strip. Named on the payload rather than inferred from its
// shape: the two legs measure against different references, and a reader who cannot tell which one
// is on screen cannot tell what an empty strip means.
const (
	edgeMulticastLossSourceRecorder = "recorder"
	edgeMulticastLossSourcePeers    = "peers"
)

// edgeMulticastRecorderEnvFilter is the SQL literal list the rows' own `env` column is matched
// against.
//
// The folded payloads on this page are mainnet-only and their keys carry no environment, while the
// key a group resolves through is a multicast address — and both networks allocate out of the same
// 233.84.178.0/24. Testnet has had edge-solana-retrans sitting on mainnet's
// edge-kalshi-sports-tob address since 2026-07-23, so without this filter one network's loss
// renders on the other's row. `mainnet` is tolerated beside lake's own name because the recorder's
// env label is free-form configuration and both spellings are in use.
func edgeMulticastRecorderEnvFilter() string {
	return quoteSQLStrings([]string{string(EnvMainnet), "mainnet"})
}

// EdgeMulticastRecorderGapSeries is one recording node's view of one channel instance's numbering,
// as the recorder that received it measured.
//
// The channel instance is carried in full — (source address, Channel ID, destination port) — because
// that is the only key under which a sequence number means anything. The recording node is never
// folded into it: two vantages of one instance are two observations, and merging them hides a
// recorder that is missing the feed.
type EdgeMulticastRecorderGapSeries struct {
	MulticastGroup    string `json:"multicast_group,omitempty"`
	PublisherSourceIP string `json:"publisher_source_ip,omitempty"`
	ChannelID         uint8  `json:"channel_id"`
	DstPort           uint16 `json:"dst_port"`

	// Node is the recorder, LocationCode its site. Both are the recorder's own labels, which the
	// contract requires to be the ones the capture tables use — a page whose live half and
	// historical half disagree about what a node is teaches nobody anything.
	Node         string `json:"node"`
	LocationCode string `json:"location_code,omitempty"`

	// Missing is unexplained_count summed over the instance's runs: what nobody delivered here,
	// less what this recorder admits losing at a scope where that subtraction is valid. MissingRaw
	// is missing_count before that subtraction and Admitted is what came off, so the tooltip can
	// show the arithmetic rather than a number a reader has to trust.
	Missing    uint64 `json:"missing"`
	MissingRaw uint64 `json:"missing_raw,omitempty"`
	Admitted   uint64 `json:"admitted,omitempty"`

	// ReferenceSeqs is what Missing is a share of: the sequence numbers this instance should have
	// carried over the window.
	//
	// MAX-ed over the instance's gap rows and never summed. It is a per-(instance, site) figure
	// repeated on each of that instance's rows, so summing it multiplies the denominator by the
	// number of runs — a real 1% loss then reports as 0.05%, and the strip's whole point is the
	// rate.
	ReferenceSeqs uint64 `json:"reference_seqs"`

	// Runs is gap rows: one per contiguous run of missing sequence numbers. It is the count of
	// episodes, never their size — the size is Missing.
	Runs uint64 `json:"runs,omitempty"`

	// MissingByVerdict splits Missing by the rule set's attribution.
	MissingByVerdict map[string]uint64 `json:"missing_by_verdict,omitempty"`

	// Datagrams is what coverage says this node recorded on the instance, and CoverageComplete
	// whether segment_seq was dense over the window.
	//
	// Incomplete coverage makes a node with no gap rows `unverifiable` rather than clean: the
	// object that would have carried the loss is the object we do not have, so an absence of gap
	// rows there is an absence of evidence. A recorder restart inside the window renumbers
	// segment_seq and reads as a hole too, which errs toward unverifiable — the safe direction.
	Datagrams        uint64 `json:"datagrams,omitempty"`
	CoverageComplete bool   `json:"coverage_complete"`

	// Episodes places the runs on the strip's axis, at one mark per run and never a duration.
	// A run of missing sequence numbers has no length in time: at fifty datagrams a second a
	// three-second hole is a hundred and fifty missing and on a channel that only heartbeats it is
	// three, so a figure in seconds measures how busy the feed was as much as what was lost. The
	// mark says when to look and Missing says how much was lost.
	//
	// PublisherEpisodes is the subset whose verdict is `publisher`: absent from every site, with no
	// recorder overflow anywhere and coverage intact.
	Episodes          []KalshiL2GapEpisode `json:"episodes,omitempty"`
	PublisherEpisodes []KalshiL2GapEpisode `json:"publisher_episodes,omitempty"`
}

// edgeMulticastRecorderGapTablesExist reports whether both proxied recorder tables are queryable.
//
// Both, not either: the gap table alone cannot render the strip. A clean node emits no gap row, so
// coverage is what supplies the (instance x node) universe — and a node that vanishes from the
// comparison when it is healthy is worse than no strip at all, because "was lost 267" means nothing
// without "cmh lost 0" beside it.
//
// A probe failure is returned rather than folded into "absent", the same rule kalshiTableExists
// follows: absent degrades to the peer comparison silently, so swallowing a connection blip here
// would hide the recorder leg for a full refresh interval with nothing logged.
func (a *API) edgeMulticastRecorderGapTablesExist(ctx context.Context) (bool, error) {
	for _, t := range []string{edgeMulticastRecorderGapTable, edgeMulticastRecorderCoverageTable} {
		ok, err := a.kalshiTableExists(ctx, t)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// fetchEdgeMulticastRecorderGaps reads one row per (group, publisher source, channel instance,
// recording node) over the same window the rest of the folded columns use.
//
// The two tables are UNION ALL-ed into one key space rather than joined. A FULL JOIN is what the
// shape suggests — coverage has the nodes that lost nothing, the gap table has the losses — but
// ClickHouse returns the key columns of a FULL JOIN ... USING from the left side only, so a
// gap row for an instance coverage does not carry would come back keyed on empty strings. The
// union has no such edge and aggregates identically.
//
// Cheap only if the loader partitions and sorts as the contract asks. Both tables are tens of bytes
// a row, so the read is small either way, but the `WHERE before_ts >= ...` bound prunes nothing
// unless the recorder makes that column the partition key and leads its sort order with the channel
// instance — no such DDL exists yet, which is why it is requirement 5 in the design doc rather than
// an assumption stated here. Getting it wrong is a full scan of a table with no TTL, and the
// coverage table on this page is the standing example of a filter column that is not the partition
// column.
//
// It is bounded on both sides regardless: see edgeMulticastRecorderGapSettings.
func (a *API) fetchEdgeMulticastRecorderGaps(ctx context.Context) ([]EdgeMulticastRecorderGapSeries, error) {
	q := edgeMulticastRecorderGapQuery(a.FeedsDB)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("edge_multicast_recorder_gaps", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EdgeMulticastRecorderGapSeries
	for rows.Next() {
		var s EdgeMulticastRecorderGapSeries
		var byVerdict [5]uint64
		var segments, segmentSpan uint64
		var seconds, secondsPublisher []uint32
		if err := rows.Scan(&s.MulticastGroup, &s.PublisherSourceIP, &s.ChannelID, &s.DstPort,
			&s.Node, &s.LocationCode, &s.Missing, &s.MissingRaw, &s.Admitted, &s.ReferenceSeqs,
			&s.Runs, &byVerdict[0], &byVerdict[1], &byVerdict[2], &byVerdict[3], &byVerdict[4],
			&s.Datagrams, &segments, &segmentSpan, &seconds, &secondsPublisher); err != nil {
			return nil, err
		}
		s.MissingByVerdict = edgeMulticastGapVerdictMap(byVerdict)
		// Dense, and there was coverage at all: a node with no coverage row has nothing to be
		// dense over, and its clean run is unverified rather than verified.
		s.CoverageComplete = segments > 0 && segments == segmentSpan
		s.Episodes = edgeMulticastRecorderGapMarks(seconds)
		s.PublisherEpisodes = edgeMulticastRecorderGapMarks(secondsPublisher)
		out = append(out, s)
	}
	return out, rows.Err()
}

// edgeMulticastRecorderGapQuery builds the read. Separate from the scan so the SQL is a pure
// function of the database name, which is what lets a test hold the statement's text against the
// contract; what it DOES is pinned by executing it against tables the test creates
// (TestEdgeMulticastRecorderGaps_TheStatementRunsAndHoldsTheContract).
//
// No FINAL, and that is an assumption rather than an oversight: it sums unexplained_count and
// counts runs, so a re-delivered row is double-counted, and only the loader can say whether one is
// possible. It is requirement 6 of the contract for that reason — a FINAL over a plain MergeTree
// buys a merge nobody needs, and one missing over a replacing engine inflates every loss figure
// while leaving it plausible. If the recorder lands on ReplacingMergeTree, this gets FINAL.
func edgeMulticastRecorderGapQuery(feedsDB string) string {
	db := fmt.Sprintf("`%s`", feedsDB)
	return fmt.Sprintf(`
		WITH legs AS (
			SELECT
				toString(group_addr) AS multicast_group,
				toString(source_addr) AS publisher_source_ip,
				channel_id,
				dst_port,
				recorder AS node,
				min(site) AS location_code,
				-- unexplained_count, not missing_count: the recorder's own admitted loss comes
				-- off before the strip ever sees the number.
				sum(unexplained_count) AS missing,
				sum(missing_count) AS missing_raw,
				sum(admitted_recorder) AS admitted,
				-- Per (instance, site) and repeated on every run of it, so this is a max and
				-- never a sum.
				max(reference_seqs) AS reference_seqs,
				count() AS runs,
				sumIf(unexplained_count, verdict = '%[6]s') AS missing_recorder,
				sumIf(unexplained_count, verdict = '%[7]s') AS missing_upstream,
				sumIf(unexplained_count, verdict = '%[8]s') AS missing_path,
				sumIf(unexplained_count, verdict = '%[9]s') AS missing_unverifiable,
				sumIf(unexplained_count, verdict = '%[10]s') AS missing_publisher,
				toUInt64(0) AS datagrams,
				toUInt64(0) AS segments,
				toUInt64(0) AS segment_span,
				-- The publisher's own send stamp where a site that recorded the datagram
				-- supplied it, our own bracket otherwise: this site has no clock reading for
				-- something it never received, so its bracket is the weaker of the two.
				groupUniqArray(%[4]d)(toUInt32(toUnixTimestamp(ifNull(sent_from_ts, before_ts)))) AS seconds,
				groupUniqArrayIf(%[4]d)(toUInt32(toUnixTimestamp(ifNull(sent_from_ts, before_ts))), verdict = '%[10]s') AS seconds_publisher
			FROM %[1]s.%[2]s
			WHERE before_ts >= now64(9) - toIntervalMinute(%[3]d)
			  AND env IN (%[5]s)
			GROUP BY multicast_group, publisher_source_ip, channel_id, dst_port, node

			UNION ALL

			SELECT
				-- segment_coverage carries no group of its own; the group is the one joined on
				-- this instance's destination port. If the loader ever adds group_addr there,
				-- as the spec added it to the gap row and for the same reason, this goes away.
				toString(tupleElement(arrayFirst(t -> tupleElement(t, 3) = dst_port, roles_joined), 2)) AS multicast_group,
				toString(source_addr) AS publisher_source_ip,
				channel_id,
				dst_port,
				recorder AS node,
				min(site) AS location_code,
				toUInt64(0) AS missing,
				toUInt64(0) AS missing_raw,
				toUInt64(0) AS admitted,
				toUInt64(0) AS reference_seqs,
				toUInt64(0) AS runs,
				toUInt64(0) AS missing_recorder,
				toUInt64(0) AS missing_upstream,
				toUInt64(0) AS missing_path,
				toUInt64(0) AS missing_unverifiable,
				toUInt64(0) AS missing_publisher,
				sum(datagram_count) AS datagrams,
				-- Dense segment_seq is the coverage check: a hole is an object the archive does
				-- not hold, and a node's clean run means nothing over a window with one.
				uniqExact(segment_seq) AS segments,
				toUInt64(max(segment_seq) - min(segment_seq) + 1) AS segment_span,
				emptyArrayUInt32() AS seconds,
				emptyArrayUInt32() AS seconds_publisher
			FROM %[1]s.%[11]s
			-- Segments overlap the window rather than starting inside it. The lookback bounds the
			-- partition prune; a segment is minutes long, so an hour is generous.
			WHERE start_ts >= now64(9) - toIntervalMinute(60)
			  AND end_ts >= now64(9) - toIntervalMinute(%[3]d)
			  AND env IN (%[5]s)
			GROUP BY multicast_group, publisher_source_ip, channel_id, dst_port, node
		)
		-- The aggregates below are deliberately UNALIASED, and the scan is positional for that
		-- reason: "sum(missing) AS missing" is a cyclic alias in ClickHouse and the statement is
		-- refused outright. The order here is the order Scan reads them in.
		SELECT
			multicast_group,
			publisher_source_ip,
			channel_id,
			dst_port,
			node,
			min(location_code),
			sum(missing),
			sum(missing_raw),
			sum(admitted),
			-- Each leg already reduced its instance to one row, so this max picks the reference
			-- out of whichever leg carried it rather than picking between references.
			max(reference_seqs),
			sum(runs),
			sum(missing_recorder),
			sum(missing_upstream),
			sum(missing_path),
			sum(missing_unverifiable),
			sum(missing_publisher),
			sum(datagrams),
			max(segments),
			max(segment_span),
			arrayFlatten(groupArray(seconds)),
			arrayFlatten(groupArray(seconds_publisher))
		FROM legs
		WHERE multicast_group NOT IN ('', '0.0.0.0')
		GROUP BY multicast_group, publisher_source_ip, channel_id, dst_port, node%[12]s`,
		db, edgeMulticastRecorderGapTable, edgeMulticastObservationsWindowMinutes,
		edgeMulticastRecorderLossCap, edgeMulticastRecorderEnvFilter(),
		edgeMulticastGapVerdictRecorder, edgeMulticastGapVerdictUpstream,
		edgeMulticastGapVerdictPath, edgeMulticastGapVerdictUnverifiable,
		edgeMulticastGapVerdictPublisher, edgeMulticastRecorderCoverageTable,
		edgeMulticastRecorderGapSettings)
}

// edgeMulticastRecorderGapMarks places one mark per run-start second, and never joins two of them.
//
// The seconds this leg carries are RUN STARTS — groupUniqArray over each gap row's own stamp, one
// row per contiguous run of missing sequence numbers. So collapsing adjacent ones into a single
// multi-second episode, which is the whole job of collapseKalshiL2GapSeconds, contradicts the one
// thing EdgeMulticastRecorderGapSeries.Episodes promises: one mark per run, never a duration. Two
// runs beginning in consecutive seconds drew one wider mark and counted as one entry — a
// two-second hole rendered where there were two instantaneous ones, and the row underneath
// reporting half the runs.
//
// The peer leg keeps the collapse and must: its seconds are the seconds loss was OBSERVED in, so
// there a contiguous pair really is one episode two seconds long.
//
// Second resolution is the floor here, not a choice: two runs starting inside the same second share
// a mark and the query has already unique-d them away. That is why the count a tooltip prints for a
// node comes from Runs and not from the number of marks.
func edgeMulticastRecorderGapMarks(secs []uint32) []KalshiL2GapEpisode {
	if len(secs) == 0 {
		return nil
	}
	uniq := make([]uint32, 0, len(secs))
	seen := make(map[uint32]bool, len(secs))
	for _, sec := range secs {
		if seen[sec] {
			continue
		}
		seen[sec] = true
		uniq = append(uniq, sec)
	}
	// Ascending, so the marks read left to right on the axis and an unchanged payload cannot
	// shuffle between polls.
	sort.Slice(uniq, func(i, j int) bool { return uniq[i] < uniq[j] })

	out := make([]KalshiL2GapEpisode, 0, len(uniq))
	for _, sec := range uniq {
		out = append(out, KalshiL2GapEpisode{Start: int64(sec), Seconds: 1})
	}
	return out
}

// edgeMulticastGapVerdictMap keeps the five columns' order in one place, so the scan above and the
// map a tooltip reads cannot drift apart.
func edgeMulticastGapVerdictMap(counts [5]uint64) map[string]uint64 {
	verdicts := [5]string{
		edgeMulticastGapVerdictRecorder,
		edgeMulticastGapVerdictUpstream,
		edgeMulticastGapVerdictPath,
		edgeMulticastGapVerdictUnverifiable,
		edgeMulticastGapVerdictPublisher,
	}
	out := map[string]uint64{}
	for i, v := range verdicts {
		if counts[i] > 0 {
			out[v] = counts[i]
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// edgeMulticastRecorderGapFold turns the per-(instance, node) rows into the per-publisher-line
// strip, plus the row underneath it: the runs the rule set attributed to the PUBLISHER.
//
// That row is what `2+` on the peer leg was reaching for. Two recorders losing in the same second
// says the cause is not one node's branch, and it can never fire when every node lost — the message
// would be in nobody's reference. A `publisher` verdict is the claim itself: absent from every site,
// with no recorder overflow anywhere and coverage intact. They are separate fields with separate
// labels because they are separate claims.
//
// Summing across the line's instances is right and matches the peer leg: a publisher's channels each
// carry their own numbering, so their references add. What must not be summed is one instance's
// reference across its own runs — see ReferenceSeqs.
func edgeMulticastRecorderGapFold(series []EdgeMulticastRecorderGapSeries) (map[string][]EdgeMulticastRecorderLoss, map[string][]KalshiL2GapEpisode) {
	type nodeState struct {
		loss     *EdgeMulticastRecorderLoss
		seconds  map[uint32]bool
		verdicts map[string]uint64
	}

	byLine := map[string]map[string]*nodeState{}
	pubSecs := map[string]map[uint32]bool{}

	addSeconds := func(dst map[uint32]bool, eps []KalshiL2GapEpisode) {
		for _, e := range eps {
			for i := uint32(0); i < e.Seconds; i++ {
				dst[uint32(e.Start)+i] = true
			}
		}
	}

	for _, s := range series {
		if s.PublisherSourceIP == "" || s.Node == "" {
			continue
		}
		// Keyed on (destination group, publisher) exactly as the peer leg is: one publisher
		// serves several groups — the top-of-book and market-by-price halves of a feed are two
		// addresses on one tunnel — so keying on the address alone would total a publisher's
		// losses across its groups and print that total on every one of its rows.
		lk := edgeMulticastRecorderLossLineKey(s.MulticastGroup, s.PublisherSourceIP)
		if byLine[lk] == nil {
			byLine[lk] = map[string]*nodeState{}
		}
		st := byLine[lk][s.Node]
		if st == nil {
			st = &nodeState{
				loss:     &EdgeMulticastRecorderLoss{Node: s.Node, LocationCode: s.LocationCode},
				seconds:  map[uint32]bool{},
				verdicts: map[string]uint64{},
			}
			byLine[lk][s.Node] = st
		}
		st.loss.Missing += s.Missing
		st.loss.MissingRaw += s.MissingRaw
		st.loss.Admitted += s.Admitted
		st.loss.ReferenceSeqs += s.ReferenceSeqs
		st.loss.Runs += s.Runs
		// What coverage says this node recorded, carried onto the row because on a CLEAN node it
		// is the only figure there is: a node that lost nothing has no gap row, so it has no
		// reference either, and the tooltip on the line the whole comparison rests on read
		// "0 of 0 sequence numbers the publisher sent".
		st.loss.Datagrams += s.Datagrams
		// One instance short of coverage leaves the whole node's line unverified. The strip has
		// one row per node, and a row that is clean on one channel and unverified on another
		// cannot claim the clean reading for both.
		if !s.CoverageComplete {
			st.loss.Unverifiable = true
		}
		for v, n := range s.MissingByVerdict {
			st.verdicts[v] += n
		}
		addSeconds(st.seconds, s.Episodes)

		if len(s.PublisherEpisodes) > 0 {
			if pubSecs[lk] == nil {
				pubSecs[lk] = map[uint32]bool{}
			}
			addSeconds(pubSecs[lk], s.PublisherEpisodes)
		}
	}

	out := map[string][]EdgeMulticastRecorderLoss{}
	for lk, nodes := range byLine {
		lines := make([]EdgeMulticastRecorderLoss, 0, len(nodes))
		for _, st := range nodes {
			flat := make([]uint32, 0, len(st.seconds))
			for sec := range st.seconds {
				flat = append(flat, sec)
			}
			st.loss.Episodes = edgeMulticastRecorderGapMarks(flat)
			if len(st.verdicts) > 0 {
				st.loss.MissingByVerdict = st.verdicts
			}
			lines = append(lines, *st.loss)
		}
		// Worst first, then by name: a reader who looks at one line looks at the one that
		// matters, and the order cannot shuffle between polls of an unchanged payload.
		sortEdgeMulticastRecorderLoss(lines)
		out[lk] = lines
	}

	pub := map[string][]KalshiL2GapEpisode{}
	for lk, secs := range pubSecs {
		flat := make([]uint32, 0, len(secs))
		for sec := range secs {
			flat = append(flat, sec)
		}
		pub[lk] = edgeMulticastRecorderGapMarks(flat)
	}
	return out, pub
}

// appendEdgeMulticastRecorderGaps adds the recorder leg to the cached observations payload.
//
// Additive, and never fatal: a failure here costs the strip its better source and leaves the peer
// comparison — which the payload carries anyway — to render it. The Sequence column is additive to
// this page and must not be able to fail it.
//
// A failed table PROBE reads as absent rather than as unavailable, which is the opposite of what
// kalshiTableExists does with one, and for a reason that does not apply here: nothing is written
// over on this path, so a blip costs one cycle of the recorder leg with the peer strip intact. The
// alternative is worse — no environment has these tables yet, so a probe blip would put "recorder
// rows unavailable" under every publisher line on the page and claim rows exist that do not. The
// log line is the tell.
//
// Which is why both failures go through an Escalator rather than a bare slog.Warn. A WARN that can
// never escalate is exactly the shape that hid the peer leg's memory failure: a proxied table that
// loses its grant answers 497 on every ten-minute cycle forever, and the strip degrades to the peer
// comparison with nothing ever paging. Three consecutive cycles is half an hour of a column
// silently standing in for a better one; a single blip stays at WARN. The keys are separate because
// the two causes are, and the counts run per cause — a probe that starts answering must not reset a
// read that is still failing.
//
// The whole leg runs inside its own deadline, probes included, so it cannot spend the refresher's
// shared one. See edgeMulticastRecorderGapBudget.
func (a *API) appendEdgeMulticastRecorderGaps(ctx context.Context, out *EdgeMulticastObservationsResponse) {
	ctx, cancel := context.WithTimeout(ctx, edgeMulticastRecorderGapBudget)
	defer cancel()

	log := slog.Default()
	exists, err := a.edgeMulticastRecorderGapTablesExist(ctx)
	if err != nil {
		a.recorderGapEsc.Fail(log, edgeMulticastRecorderGapProbeEscKey,
			"edge multicast recorder gaps: table probe failed", "error", err)
		return
	}
	a.recorderGapEsc.Reset(edgeMulticastRecorderGapProbeEscKey)
	if !exists {
		// Absent is the steady state of every environment today, not a failure — and it says
		// nothing about a read that was failing before the tables went away, so the read's own
		// streak is left where it was.
		return
	}
	gaps, err := a.fetchEdgeMulticastRecorderGaps(ctx)
	if err != nil {
		a.recorderGapEsc.Fail(log, edgeMulticastRecorderGapReadEscKey,
			"edge multicast recorder gaps unavailable", "error", err)
		out.RecorderGapsUnavailable = true
		return
	}
	a.recorderGapEsc.Reset(edgeMulticastRecorderGapReadEscKey)
	out.RecorderGaps = gaps
}

// edgeMulticastLossLegs folds both legs and picks which one renders PER PUBLISHER LINE, returning
// the choice keyed the same way the strips are.
//
// Per line, and not once for the payload. The recorder rows arrive feed by feed — the recorder is
// deployed per capture host and the proxies are created per table, not per page — so the first feed
// it covers would otherwise switch every OTHER line to a leg that has no rows for it. Those lines
// lose the peer strip they render today and print `no peer to compare`, a statement about a
// comparison that was never run. Nothing about the choice is global: both folds key on
// (group, publisher), so making it per key costs one map lookup.
//
// Within a line the legs still never mix. They measure against different references — the
// publisher's own numbering against the union of what the nodes received — so a strip folding both
// would be neither, and the recorder rows win wherever they exist.
//
// The peer leg's own failure flag comes back as the payload-wide fact it is; which lines it may be
// reported on is settled by the source map, in attachEdgeMulticastSequenceHealth. Carried onto a
// recorder-fed line it would put "not measured" over a strip built from better rows.
func edgeMulticastLossLegs(payload *EdgeMulticastObservationsResponse) (
	loss map[string][]EdgeMulticastRecorderLoss,
	simultaneous map[string][]KalshiL2GapEpisode,
	publisher map[string][]KalshiL2GapEpisode,
	source map[string]string,
	peerUnavailable bool,
) {
	recorderLoss, recorderPub := edgeMulticastRecorderGapFold(payload.RecorderGaps)
	peerLoss, peerSimul := edgeMulticastRecorderLossFold(payload.RecorderLoss)

	loss = make(map[string][]EdgeMulticastRecorderLoss, len(recorderLoss)+len(peerLoss))
	source = make(map[string]string, len(recorderLoss)+len(peerLoss))
	simultaneous = map[string][]KalshiL2GapEpisode{}
	publisher = map[string][]KalshiL2GapEpisode{}

	for lk, rows := range recorderLoss {
		loss[lk] = rows
		source[lk] = edgeMulticastLossSourceRecorder
		if eps := recorderPub[lk]; len(eps) > 0 {
			publisher[lk] = eps
		}
	}
	// The peer comparison fills what the recorder rows do not reach, which today is every line in
	// every environment. Its bottom row travels with it and only with it: `2+` under a
	// recorder-fed strip would be the weaker claim sitting beneath the stronger one.
	for lk, rows := range peerLoss {
		if _, taken := loss[lk]; taken {
			continue
		}
		loss[lk] = rows
		source[lk] = edgeMulticastLossSourcePeers
		if eps := peerSimul[lk]; len(eps) > 0 {
			simultaneous[lk] = eps
		}
	}
	return loss, simultaneous, publisher, source, payload.RecorderLossUnavailable
}
