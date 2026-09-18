package handlers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorder leg of the loss strip: the recording nodes' own sequence-loss rows, and the
// arithmetic on them that would silently understate every rate on the page if it were wrong.

var gapVerdicts = handlers.EdgeMulticastRecorderGapVerdicts

func gapSeries(node string, channel uint8, missing, reference uint64) handlers.EdgeMulticastRecorderGapSeries {
	return handlers.EdgeMulticastRecorderGapSeries{
		MulticastGroup: "233.84.178.3", PublisherSourceIP: "148.51.121.69",
		ChannelID: channel, DstPort: 20001, Node: node, LocationCode: node[:3],
		Missing: missing, MissingRaw: missing, ReferenceSeqs: reference,
		CoverageComplete: true,
	}
}

func gapLineKey() string {
	return handlers.EdgeMulticastRecorderLossLineKeyForTest("233.84.178.3", "148.51.121.69")
}

// A publisher's channels each carry their own numbering, so their references ADD across the line —
// the same rule the peer leg follows. What must not add is one instance's reference across the runs
// it is repeated on, and that case is the next test.
func TestEdgeMulticastRecorderGaps_ReferencesAddAcrossTheLinesChannels(t *testing.T) {
	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{
		gapSeries("was-rec1", 1, 10, 1000),
		gapSeries("was-rec1", 2, 5, 500),
	})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	assert.EqualValues(t, 15, lines[0].Missing)
	assert.EqualValues(t, 1500, lines[0].ReferenceSeqs)
}

// reference_seqs is a per-(instance, site) figure the loader repeats on every gap row of that
// instance, so the query MAXes it and the fold must not undo that by summing the rows back up. The
// consequence of getting it wrong is invisible in the output: the loss still reads 10, and the rate
// it is a share of quietly halves.
func TestEdgeMulticastRecorderGaps_OneInstancesReferenceIsNotMultipliedByItsRuns(t *testing.T) {
	// What the query hands over for an instance with three runs: one row, the runs counted and
	// the reference stated once.
	s := gapSeries("was-rec1", 1, 30, 1000)
	s.Runs = 3
	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{s})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	assert.EqualValues(t, 1000, lines[0].ReferenceSeqs, "one instance, one reference")
	assert.EqualValues(t, 3, lines[0].Runs)
}

// The clean line is the whole comparison: "was lost 267" says nothing without "cmh lost 0" beside
// it. A node that lost nothing has no gap row at all, so it reaches the fold from the coverage half
// — and it has to survive to the strip.
func TestEdgeMulticastRecorderGaps_ACleanNodeKeepsItsRow(t *testing.T) {
	clean := gapSeries("cmh-rec1", 1, 0, 0)
	clean.Datagrams = 250_000

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{
		gapSeries("was-rec1", 1, 267, 300_000),
		clean,
	})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 2)
	assert.Equal(t, "was-rec1", lines[0].Node, "worst first")
	assert.EqualValues(t, 267, lines[0].Missing)
	assert.Equal(t, "cmh-rec1", lines[1].Node)
	assert.EqualValues(t, 0, lines[1].Missing)
	assert.False(t, lines[1].Unverifiable, "coverage was complete, so the clean run is a reading")
}

// With a hole in the archive a clean reading is an absence of evidence, not a clean run: the object
// that would have carried the loss is the one we do not hold. One instance short of coverage is
// enough — the strip has one row per node, and a row clean on one channel and unverified on another
// cannot claim the clean reading for both.
func TestEdgeMulticastRecorderGaps_AHoleInCoverageMakesTheNodeUnverifiable(t *testing.T) {
	short := gapSeries("cmh-rec1", 2, 0, 0)
	short.CoverageComplete = false

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{
		gapSeries("cmh-rec1", 1, 0, 0),
		short,
	})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	assert.EqualValues(t, 0, lines[0].Missing)
	assert.True(t, lines[0].Unverifiable)
}

// The recorder's own admitted drops are subtracted rather than inferred, and the arithmetic is
// shown: a strip that printed the residue alone would be a number a reader has to trust.
func TestEdgeMulticastRecorderGaps_AdmittedLossIsCarriedBesideTheResidue(t *testing.T) {
	s := gapSeries("was-rec1", 1, 2, 1000)
	s.MissingRaw = 5
	s.Admitted = 3
	s.MissingByVerdict = map[string]uint64{gapVerdicts.Recorder: 2}

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{s})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	assert.EqualValues(t, 2, lines[0].Missing, "unexplained, not missing_count")
	assert.EqualValues(t, 5, lines[0].MissingRaw)
	assert.EqualValues(t, 3, lines[0].Admitted)
	assert.Equal(t, map[string]uint64{gapVerdicts.Recorder: 2}, lines[0].MissingByVerdict)
}

// Verdicts add across the instances of one line, so the tooltip splits the line's whole loss and
// not one channel's.
func TestEdgeMulticastRecorderGaps_VerdictsAddAcrossTheLine(t *testing.T) {
	a := gapSeries("was-rec1", 1, 10, 1000)
	a.MissingByVerdict = map[string]uint64{gapVerdicts.Publisher: 7, gapVerdicts.Path: 3}
	b := gapSeries("was-rec1", 2, 4, 500)
	b.MissingByVerdict = map[string]uint64{gapVerdicts.Publisher: 4}

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{a, b})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	assert.Equal(t, map[string]uint64{gapVerdicts.Publisher: 11, gapVerdicts.Path: 3}, lines[0].MissingByVerdict)
}

// The publisher row is a union over the line's instances and its nodes, because the claim is about
// the feed: a run charged to the publisher is absent from every site, so the same run arrives from
// every node that reported it and must be drawn once.
func TestEdgeMulticastRecorderGaps_PublisherRunsAreUnionedAcrossNodes(t *testing.T) {
	was := gapSeries("was-rec1", 1, 10, 1000)
	was.PublisherEpisodes = []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}, {Start: 200, Seconds: 1}}
	cmh := gapSeries("cmh-rec1", 1, 10, 1000)
	cmh.PublisherEpisodes = []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}}

	_, pub := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{was, cmh})

	eps := pub[gapLineKey()]
	require.Len(t, eps, 2, "second 100 is one run seen twice, not two runs")
	assert.EqualValues(t, 100, eps[0].Start)
	assert.EqualValues(t, 200, eps[1].Start)
}

// Two runs beginning in consecutive seconds are two marks, never one mark two seconds wide.
//
// The seconds this leg carries are run STARTS — the gap table holds one row per contiguous run of
// missing sequence numbers, and the query groupUniqArrays each row's own stamp. So joining adjacent
// ones is the single thing Episodes promises it will not do: it draws a duration where the
// measurement has none, and the count underneath reports half the runs. collapseKalshiL2GapSeconds
// is right for the peer leg, whose seconds are the seconds loss was OBSERVED in, and wrong here.
func TestEdgeMulticastRecorderGaps_AdjacentRunsStayTwoMarks(t *testing.T) {
	s := gapSeries("was-rec1", 1, 20, 1000)
	s.Runs = 2
	s.Episodes = []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}, {Start: 101, Seconds: 1}}

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{s})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	require.Len(t, lines[0].Episodes, 2, "two runs, two marks")
	assert.Equal(t, handlers.KalshiL2GapEpisode{Start: 100, Seconds: 1}, lines[0].Episodes[0])
	assert.Equal(t, handlers.KalshiL2GapEpisode{Start: 101, Seconds: 1}, lines[0].Episodes[1],
		"a second mark, not a second appended to the first")
}

// The same rule on the publisher row underneath, where the mark count is what the tooltip prints:
// collapsed, two consecutive runs charged to the publisher read as one.
func TestEdgeMulticastRecorderGaps_AdjacentPublisherRunsStayTwoMarks(t *testing.T) {
	s := gapSeries("was-rec1", 1, 20, 1000)
	s.PublisherEpisodes = []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}, {Start: 101, Seconds: 1}}

	_, pub := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{s})

	eps := pub[gapLineKey()]
	require.Len(t, eps, 2)
	assert.EqualValues(t, 1, eps[0].Seconds)
	assert.EqualValues(t, 1, eps[1].Seconds)
}

// Marks are ascending and de-duplicated whatever order the rows arrive in, so they read left to
// right on the axis and an unchanged payload cannot shuffle between polls. The duplicate is the
// ordinary case: a run charged to the publisher is absent everywhere, so every node reports it.
func TestEdgeMulticastRecorderGaps_MarksAreSortedAndDeduplicated(t *testing.T) {
	s := gapSeries("was-rec1", 1, 20, 1000)
	s.Episodes = []handlers.KalshiL2GapEpisode{{Start: 300, Seconds: 1}, {Start: 100, Seconds: 1}, {Start: 300, Seconds: 1}}

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{s})

	lines := loss[gapLineKey()]
	require.Len(t, lines, 1)
	require.Len(t, lines[0].Episodes, 2)
	assert.EqualValues(t, 100, lines[0].Episodes[0].Start)
	assert.EqualValues(t, 300, lines[0].Episodes[1].Start)
}

// One publisher serves several groups — the two planes of a feed are two addresses on one tunnel —
// so a line is keyed on (group, publisher) and a group's losses stay on that group's row.
func TestEdgeMulticastRecorderGaps_LossStaysOnItsOwnGroupsLine(t *testing.T) {
	tob := gapSeries("was-rec1", 1, 10, 1000)
	mbp := gapSeries("was-rec1", 1, 40, 1000)
	mbp.MulticastGroup = "233.84.178.4"

	loss, _ := handlers.EdgeMulticastRecorderGapFoldForTest([]handlers.EdgeMulticastRecorderGapSeries{tob, mbp})

	require.Len(t, loss[gapLineKey()], 1)
	assert.EqualValues(t, 10, loss[gapLineKey()][0].Missing)
	other := handlers.EdgeMulticastRecorderLossLineKeyForTest("233.84.178.4", "148.51.121.69")
	require.Len(t, loss[other], 1)
	assert.EqualValues(t, 40, loss[other][0].Missing)
}

// On a line both legs have rows for, the recorder rows win and the peer comparison's bottom row
// does not come with them: `2+` beneath a recorder-fed strip would be the weaker claim sitting
// under the stronger one.
func TestEdgeMulticastLossLegs_RecorderRowsWinOverThePeerComparison(t *testing.T) {
	payload := handlers.EdgeMulticastObservationsResponse{
		RecorderLoss: []handlers.EdgeMulticastRecorderLossSeries{{
			MulticastGroup: "233.84.178.3", PublisherSourceIP: "148.51.121.69",
			ChannelID: 1, Node: "was-rec1", Missing: 999, ReferenceSeqs: 1000,
		}},
		RecorderGaps: []handlers.EdgeMulticastRecorderGapSeries{gapSeries("was-rec1", 1, 10, 1000)},
	}

	loss, simul, pub, source, _ := handlers.EdgeMulticastObservationLossLegsForTest(payload)

	assert.Equal(t, handlers.EdgeMulticastLossSources.Recorder, source[gapLineKey()])
	require.Len(t, loss[gapLineKey()], 1)
	assert.EqualValues(t, 10, loss[gapLineKey()][0].Missing, "the recorder rows, not the comparison")
	assert.Empty(t, simul, "the 2+ row belongs to the peer leg and cannot be mixed in")
	assert.Empty(t, pub, "no run was charged to the publisher")
}

// The choice is per LINE, and this is the case that made it so.
//
// The recorder rows arrive feed by feed — the recorder is deployed per capture host — so the first
// feed it covers cannot be allowed to decide the leg for the rest of the page. Chosen once per
// payload, every other publisher line switched to a leg with no rows for it: the working peer strip
// they render today disappeared and the row read `no peer to compare`, which is a claim about a
// comparison that was never run.
func TestEdgeMulticastLossLegs_TheLegIsChosenPerLineAndNotPerPayload(t *testing.T) {
	covered := gapSeries("was-rec1", 1, 10, 1000)
	uncoveredKey := handlers.EdgeMulticastRecorderLossLineKeyForTest("233.84.178.9", "148.51.120.6")
	payload := handlers.EdgeMulticastObservationsResponse{
		RecorderGaps: []handlers.EdgeMulticastRecorderGapSeries{covered},
		// Another feed entirely, which the recorder does not cover: two nodes, both losing in the
		// same second, so it has a 2+ row of its own to lose.
		RecorderLoss: []handlers.EdgeMulticastRecorderLossSeries{
			{
				MulticastGroup: "233.84.178.9", PublisherSourceIP: "148.51.120.6",
				ChannelID: 1, Node: "was-rec1", Missing: 5, ReferenceSeqs: 1000,
				Episodes: []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}},
			},
			{
				MulticastGroup: "233.84.178.9", PublisherSourceIP: "148.51.120.6",
				ChannelID: 1, Node: "cmh-rec1", Missing: 2, ReferenceSeqs: 1000,
				Episodes: []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}},
			},
		},
	}

	loss, simul, pub, source, _ := handlers.EdgeMulticastObservationLossLegsForTest(payload)

	assert.Equal(t, handlers.EdgeMulticastLossSources.Recorder, source[gapLineKey()])
	require.Len(t, pub, 0, "no run was charged to the publisher on the covered line")

	assert.Equal(t, handlers.EdgeMulticastLossSources.Peers, source[uncoveredKey],
		"a line the recorder rows do not reach keeps the comparison")
	require.Len(t, loss[uncoveredKey], 2, "and keeps its rows")
	require.Len(t, simul[uncoveredKey], 1, "and its 2+ row")
	assert.Empty(t, simul[gapLineKey()], "which stays off the recorder-fed line")
}

// A peer comparison that failed is only a finding on the lines the peer comparison renders, and
// the source map is what says which those are. Carried onto a recorder-fed line it would print
// "not measured" over a strip built from better rows.
func TestEdgeMulticastLossLegs_AFailedPeerLegIsNotReportedUnderTheRecorderLeg(t *testing.T) {
	payload := handlers.EdgeMulticastObservationsResponse{
		RecorderLossUnavailable: true,
		RecorderGaps:            []handlers.EdgeMulticastRecorderGapSeries{gapSeries("was-rec1", 1, 10, 1000)},
	}
	_, _, _, source, peerUnavailable := handlers.EdgeMulticastObservationLossLegsForTest(payload)
	assert.Equal(t, handlers.EdgeMulticastLossSources.Recorder, source[gapLineKey()])
	assert.True(t, peerUnavailable, "the failure is a fact about the payload; the line it may be reported on is not")

	// Without the recorder rows the same flag is exactly the claim it was before, and there is no
	// source to suppress it.
	payload.RecorderGaps = nil
	_, _, _, source, peerUnavailable = handlers.EdgeMulticastObservationLossLegsForTest(payload)
	assert.Empty(t, source[gapLineKey()])
	assert.True(t, peerUnavailable)
}

// No recorder rows anywhere is the ordinary state of every environment today, and it must leave the
// page exactly as it was: the peer comparison, with its own bottom row.
func TestEdgeMulticastLossLegs_NoRecorderRowsLeavesThePeerLegUntouched(t *testing.T) {
	payload := handlers.EdgeMulticastObservationsResponse{
		RecorderLoss: []handlers.EdgeMulticastRecorderLossSeries{
			{
				MulticastGroup: "233.84.178.3", PublisherSourceIP: "148.51.121.69",
				ChannelID: 1, Node: "was-rec1", Missing: 5, ReferenceSeqs: 1000,
				Episodes: []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}},
			},
			{
				MulticastGroup: "233.84.178.3", PublisherSourceIP: "148.51.121.69",
				ChannelID: 1, Node: "cmh-rec1", Missing: 2, ReferenceSeqs: 1000,
				Episodes: []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 1}},
			},
		},
	}

	loss, simul, pub, source, _ := handlers.EdgeMulticastObservationLossLegsForTest(payload)

	assert.Equal(t, handlers.EdgeMulticastLossSources.Peers, source[gapLineKey()])
	require.Len(t, loss[gapLineKey()], 2)
	require.Len(t, simul[gapLineKey()], 1, "both nodes lost in second 100")
	assert.Empty(t, pub, "the publisher row exists only on the recorder leg")
}

// seedObservationsWithRecorderGaps writes an observations payload whose recorder leg is filled,
// which is what an environment with the recorder proxies would cache.
//
// The peer comparison is marked FAILED in it on purpose. That flag renders as "not measured", and
// on a line the recorder rows fill it must not be reported at all — "not measured" over a strip
// with marks on it is a worse answer than no label.
func seedObservationsWithRecorderGaps(t *testing.T, api *handlers.API, generatedAt time.Time, gaps ...handlers.EdgeMulticastRecorderGapSeries) {
	t.Helper()
	require.NoError(t, api.WritePageCache(t.Context(), observationsKey, handlers.EdgeMulticastObservationsResponse{
		GeneratedAt:             generatedAt,
		WindowMinutes:           15,
		RecorderGaps:            gaps,
		RecorderLossUnavailable: true,
	}))
	t.Cleanup(func() {
		_, err := api.PgPool.Exec(context.Background(), `DELETE FROM page_cache WHERE key = $1`, observationsKey)
		require.NoError(t, err)
	})
}

// End to end: the recorder rows reach the publisher line, the strip says which measurement produced
// it, and the publisher row is filled instead of the peer leg's simultaneity row.
//
// The source has to travel with the strip. The two legs measure against different references — the
// publisher's own numbering against the union of what the nodes received — so an empty strip means
// different things under each, and a reader who cannot tell which is on screen cannot read it.
func TestGetEdgeMulticast_RecorderRowsFeedTheLossStrip(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")

	asOf := time.Now().UTC()
	start := asOf.Add(-2 * time.Minute).Unix()
	// The line's own series, which is what the strip hangs off today.
	seedL2Coverage(t, api, asOf, handlers.KalshiL2Lane{
		Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
		PublisherSourceIP: "10.0.0.9", LocationCode: "cmh", Messages: 2000, Seen: true,
		LastSeen: asOf.Add(-time.Second),
	})
	losing := handlers.EdgeMulticastRecorderGapSeries{
		MulticastGroup: "233.0.0.10", PublisherSourceIP: "10.0.0.9", ChannelID: 1, DstPort: 20001,
		Node: "was-rec1", LocationCode: "was", Missing: 267, MissingRaw: 267,
		ReferenceSeqs: 300_000, Runs: 2, CoverageComplete: true,
		MissingByVerdict: map[string]uint64{gapVerdicts.Publisher: 267},
		Episodes: []handlers.KalshiL2GapEpisode{
			{Start: start, Seconds: 1}, {Start: start + 30, Seconds: 1},
		},
		PublisherEpisodes: []handlers.KalshiL2GapEpisode{
			{Start: start, Seconds: 1}, {Start: start + 30, Seconds: 1},
		},
	}
	clean := handlers.EdgeMulticastRecorderGapSeries{
		MulticastGroup: "233.0.0.10", PublisherSourceIP: "10.0.0.9", ChannelID: 1, DstPort: 20001,
		Node: "cmh-rec1", LocationCode: "cmh", Datagrams: 299_733, CoverageComplete: true,
	}
	seedObservationsWithRecorderGaps(t, api, asOf, losing, clean)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.Len(t, g.PublisherLines, 1)
	seq := g.PublisherLines[0].Sequence
	require.NotNil(t, seq)

	assert.Equal(t, handlers.EdgeMulticastLossSources.Recorder, seq.RecorderLossSource)
	require.Len(t, seq.RecorderLoss, 2, "the clean node keeps its row; it is the comparison")
	assert.Equal(t, "was-rec1", seq.RecorderLoss[0].Node)
	assert.EqualValues(t, 267, seq.RecorderLoss[0].Missing)
	assert.EqualValues(t, 300_000, seq.RecorderLoss[0].ReferenceSeqs)
	assert.Equal(t, "cmh-rec1", seq.RecorderLoss[1].Node)
	assert.EqualValues(t, 0, seq.RecorderLoss[1].Missing)
	assert.EqualValues(t, 299_733, seq.RecorderLoss[1].Datagrams,
		"the clean row has no reference to be a share of, so what it recorded is the only figure on it")

	require.Len(t, seq.RecorderLossPublisher, 2, "both runs were charged to the publisher")
	assert.Empty(t, seq.RecorderLossSimultaneous, "the 2+ row is the peer leg's and must stay empty")
	assert.False(t, seq.RecorderGapsUnavailable)
	assert.False(t, seq.RecorderLossUnavailable,
		"the peer leg failed in the payload, and that is not a finding on a recorder-fed line")
}

// The statement's TEXT, which is now the lesser half: everything about what it DOES is pinned by
// executing it, below. What stays here is the contract named in one place, plus the one property
// no execution can observe — the server-side cap, which only fires on a read that overruns.
func TestEdgeMulticastRecorderGapQuery_HoldsTheContract(t *testing.T) {
	q := handlers.EdgeMulticastRecorderGapQueryForTest("feeds")

	assert.Contains(t, q, "`feeds`.recorder_sequence_gap")
	assert.Contains(t, q, "`feeds`.recorder_segment_coverage",
		"coverage is not optional: a clean node emits no gap row, and the clean line is the comparison")

	// The reference is per (instance, site), repeated on each of the instance's runs. Summed, the
	// denominator is multiplied by the number of runs and every rate on the page understates.
	assert.NotContains(t, q, "sum(reference_seqs)")

	// Bounded server-side, because this read runs first on the refresher's shared three-minute
	// deadline and exhausting it costs the whole observations payload — Msg/s, Peer, Heard and
	// recorder_coverage — rather than the strip it is additive to.
	assert.Contains(t, q, "max_execution_time = 30")
}

// createRecorderSequenceTables creates the two proxied recorder tables with the columns lake reads.
//
// The upstream tables are wider; what is here is the contract in
// docs/plans/2026-09-03-edge-multicast-recorder-sequence-design.md, which is the point — the
// statement is this PR's whole product and until now nothing had executed it. Six of its properties
// are invisible to a string match: the two legs' UNION ALL types have to agree column for column,
// the outer aggregates are unaliased because `sum(missing) AS missing` is a cyclic alias ClickHouse
// refuses outright, and `arrayFirst`/`tupleElement` over `roles_joined` has to pick the right
// element out of the tuple.
func createRecorderSequenceTables(t *testing.T, api *handlers.API) {
	t.Helper()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.recorder_sequence_gap (
			env LowCardinality(String),
			recorder LowCardinality(String),
			site LowCardinality(String),
			group_addr IPv4,
			source_addr IPv4,
			channel_id UInt8,
			dst_port UInt16,
			before_ts DateTime64(9),
			sent_from_ts Nullable(DateTime64(9)),
			missing_count UInt64,
			admitted_recorder UInt64,
			unexplained_count UInt64,
			reference_seqs UInt64,
			verdict LowCardinality(String)
		) ENGINE = MergeTree
		PARTITION BY toDate(before_ts)
		ORDER BY (env, group_addr, source_addr, channel_id, dst_port, recorder, before_ts)
	`, db)))
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.recorder_segment_coverage (
			env LowCardinality(String),
			recorder LowCardinality(String),
			site LowCardinality(String),
			source_addr IPv4,
			channel_id UInt8,
			dst_port UInt16,
			segment_seq UInt64,
			start_ts DateTime64(9),
			end_ts DateTime64(9),
			datagram_count UInt64,
			roles_joined Array(Tuple(LowCardinality(String), IPv4, UInt16))
		) ENGINE = MergeTree
		PARTITION BY toDate(start_ts)
		ORDER BY (env, source_addr, channel_id, dst_port, recorder, segment_seq)
	`, db)))
}

// insertRecorderGap writes one gap row: one contiguous run of missing sequence numbers, as the
// recorder's analysis tier reports it. agoSecs places it, and sentAgoSecs is the publisher's own
// send stamp where a site that DID record the datagram supplied one (-1 for none).
func insertRecorderGap(t *testing.T, api *handlers.API, env, node, verdict string, agoSecs, sentAgoSecs int, missing, admitted, unexplained, reference uint64) {
	t.Helper()
	sent := "NULL"
	if sentAgoSecs >= 0 {
		sent = fmt.Sprintf("now64(9) - toIntervalSecond(%d)", sentAgoSecs)
	}
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO `+"`%s`"+`.recorder_sequence_gap
		(env, recorder, site, group_addr, source_addr, channel_id, dst_port, before_ts,
		 sent_from_ts, missing_count, admitted_recorder, unexplained_count, reference_seqs, verdict)
		VALUES ('%s', '%s', '%s', '233.84.178.3', '148.51.121.69', 1, 20001,
		        now64(9) - toIntervalSecond(%d), %s, %d, %d, %d, %d, '%s')
	`, api.FeedsDB, env, node, node[:3], agoSecs, sent, missing, admitted, unexplained, reference, verdict)))
}

// insertRecorderCoverage writes one coverage segment. groupAddr and port are what roles_joined
// carries, which is the only place the coverage row names a group at all.
func insertRecorderCoverage(t *testing.T, api *handlers.API, node string, segmentSeq uint64, startAgoSecs, endAgoSecs int, datagrams uint64, groupAddr string, rolePort uint16) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO `+"`%s`"+`.recorder_segment_coverage
		(env, recorder, site, source_addr, channel_id, dst_port, segment_seq, start_ts, end_ts,
		 datagram_count, roles_joined)
		VALUES ('mainnet-beta', '%s', '%s', '148.51.121.69', 1, 20001, %d,
		        now64(9) - toIntervalSecond(%d), now64(9) - toIntervalSecond(%d), %d,
		        [('tob', '%s', %d)])
	`, api.FeedsDB, node, node[:3], segmentSeq, startAgoSecs, endAgoSecs, datagrams, groupAddr, rolePort)))
}

// The statement, executed. Every assertion here is a property no string match can reach.
//
// The fixture is one publisher line recorded at two nodes: `was` lost two runs — one charged to the
// publisher, one it admits dropping itself — and `cmh` lost nothing, so it reaches the strip from
// the coverage half alone. Beside them sit the two rows that must NOT reach it: the same loss
// labelled `testnet`, and one outside the fifteen-minute window.
func TestEdgeMulticastRecorderGaps_TheStatementRunsAndHoldsTheContract(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createRecorderSequenceTables(t, api)

	// Two runs on one instance, each repeating the instance's own reference_seqs. Summing that
	// column would double the denominator and halve every rate on the page.
	insertRecorderGap(t, api, "mainnet-beta", "was-rec1", gapVerdicts.Publisher, 120, 121, 200, 0, 200, 300_000)
	insertRecorderGap(t, api, "mainnet-beta", "was-rec1", gapVerdicts.Recorder, 60, -1, 70, 3, 67, 300_000)
	// The other network's loss, on an address mainnet also allocates out of.
	insertRecorderGap(t, api, "testnet", "was-rec1", gapVerdicts.Publisher, 60, -1, 999, 0, 999, 300_000)
	// Before the window.
	insertRecorderGap(t, api, "mainnet-beta", "was-rec1", gapVerdicts.Publisher, 3000, -1, 5000, 0, 5000, 300_000)

	insertRecorderCoverage(t, api, "was-rec1", 3, 600, 10, 299_000, "233.84.178.3", 20001)
	// Two dense segments at the clean node: 7 and 8, so span and count agree and its clean run is
	// a reading rather than an absence of evidence.
	insertRecorderCoverage(t, api, "cmh-rec1", 7, 600, 300, 150_000, "233.84.178.3", 20001)
	insertRecorderCoverage(t, api, "cmh-rec1", 8, 300, 10, 149_733, "233.84.178.3", 20001)

	// Through the production entry point, so the EXISTS TABLE gate over BOTH tables and the
	// ordering that puts this leg ahead of the capture-table gate are exercised too. The
	// observations table does not exist here, and the recorder leg must fill anyway.
	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)
	require.False(t, resp.RecorderGapsUnavailable, "the read succeeded")
	require.Len(t, resp.RecorderGaps, 2, "two nodes on one instance; the testnet and out-of-window rows are neither")

	byNode := map[string]handlers.EdgeMulticastRecorderGapSeries{}
	for _, s := range resp.RecorderGaps {
		byNode[s.Node] = s
	}

	was := byNode["was-rec1"]
	assert.Equal(t, "233.84.178.3", was.MulticastGroup, "the gap row's own group_addr, as a dotted quad")
	assert.Equal(t, "148.51.121.69", was.PublisherSourceIP, "source_addr is the ledger's dz_ip")
	assert.Equal(t, "was", was.LocationCode)
	assert.EqualValues(t, 1, was.ChannelID)
	assert.EqualValues(t, 20001, was.DstPort)
	assert.EqualValues(t, 267, was.Missing, "unexplained_count, so the admitted 3 is already off")
	assert.EqualValues(t, 270, was.MissingRaw)
	assert.EqualValues(t, 3, was.Admitted)
	assert.EqualValues(t, 300_000, was.ReferenceSeqs,
		"one instance, one reference — MAXed across its two runs and never summed")
	assert.EqualValues(t, 2, was.Runs)
	assert.Equal(t, map[string]uint64{gapVerdicts.Publisher: 200, gapVerdicts.Recorder: 67}, was.MissingByVerdict,
		"the verdict spellings the SQL matches on are the rule set's own")
	assert.True(t, was.CoverageComplete)
	require.Len(t, was.Episodes, 2, "two runs, two marks")
	assert.EqualValues(t, 1, was.Episodes[0].Seconds, "a mark is a placement, never a duration")
	// 61s apart, give or take the second boundary the two INSERTs landed on: the first mark is the
	// publisher's own send stamp 121s ago, the second the local bracket 60s ago, and each is
	// truncated to a second.
	assert.InDelta(t, 61, was.Episodes[1].Start-was.Episodes[0].Start, 1,
		"placed at sent_from_ts where a site supplied one and at before_ts where none did")
	require.Len(t, was.PublisherEpisodes, 1, "one of the two runs was charged to the publisher")
	assert.Equal(t, was.Episodes[0], was.PublisherEpisodes[0])

	cmh := byNode["cmh-rec1"]
	assert.Equal(t, "233.84.178.3", cmh.MulticastGroup,
		"a clean node has no gap row, so its group comes out of roles_joined by dst_port")
	assert.EqualValues(t, 0, cmh.Missing)
	assert.EqualValues(t, 0, cmh.ReferenceSeqs, "nothing to be a share of; datagrams is what this row has")
	assert.EqualValues(t, 299_733, cmh.Datagrams)
	assert.True(t, cmh.CoverageComplete, "segment_seq 7 and 8 are dense")
}

// A coverage row whose roles_joined carries no tuple for its OWN dst_port resolves to 0.0.0.0 and
// is dropped by the outer WHERE — silently, and the row it drops is a clean node's.
//
// That is why the design doc asks for a tuple per port with coverage rather than leaving it
// implied: the failure is invisible from the page, where the node simply is not there, and the
// clean line is what the whole strip rests on. Pinned so the requirement has a test behind it.
func TestEdgeMulticastRecorderGaps_ACoverageRowWithNoRoleForItsPortIsDropped(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createRecorderSequenceTables(t, api)

	insertRecorderGap(t, api, "mainnet-beta", "was-rec1", gapVerdicts.Publisher, 120, -1, 10, 0, 10, 1000)
	insertRecorderCoverage(t, api, "was-rec1", 3, 600, 10, 299_000, "233.84.178.3", 20001)
	// Same instance, but roles_joined names another port — so nothing matches and the group is
	// unresolvable.
	insertRecorderCoverage(t, api, "ewr-rec1", 4, 600, 10, 299_000, "233.84.178.4", 20002)

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.RecorderGaps, 1, "the unresolvable coverage row is dropped, not rendered on some other group")
	assert.Equal(t, "was-rec1", resp.RecorderGaps[0].Node)
}
