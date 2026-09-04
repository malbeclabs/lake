package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
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

// Which leg renders is decided on the payload, and the recorder rows win. Both are carried in the
// cache entry because whether the recorder tables exist is a property of the environment, so the
// selection has to be made per payload rather than at build time.
func TestEdgeMulticastLossLegs_RecorderRowsWinOverThePeerComparison(t *testing.T) {
	payload := handlers.EdgeMulticastObservationsResponse{
		RecorderLoss: []handlers.EdgeMulticastRecorderLossSeries{{
			MulticastGroup: "233.84.178.3", PublisherSourceIP: "148.51.121.69",
			ChannelID: 1, Node: "was-rec1", Missing: 999, ReferenceSeqs: 1000,
		}},
		RecorderGaps: []handlers.EdgeMulticastRecorderGapSeries{gapSeries("was-rec1", 1, 10, 1000)},
	}

	loss, simul, pub, source, peerUnavailable := handlers.EdgeMulticastObservationLossLegsForTest(payload)

	assert.Equal(t, handlers.EdgeMulticastLossSources.Recorder, source)
	require.Len(t, loss[gapLineKey()], 1)
	assert.EqualValues(t, 10, loss[gapLineKey()][0].Missing, "the recorder rows, not the comparison")
	assert.Empty(t, simul, "the 2+ row belongs to the peer leg and cannot be mixed in")
	assert.Empty(t, pub, "no run was charged to the publisher")
	assert.False(t, peerUnavailable)
}

// A peer comparison that failed is only a finding while the peer comparison is what renders.
// Carried onto the recorder leg it would print "not measured" over a strip built from better rows.
func TestEdgeMulticastLossLegs_AFailedPeerLegIsNotReportedUnderTheRecorderLeg(t *testing.T) {
	payload := handlers.EdgeMulticastObservationsResponse{
		RecorderLossUnavailable: true,
		RecorderGaps:            []handlers.EdgeMulticastRecorderGapSeries{gapSeries("was-rec1", 1, 10, 1000)},
	}
	_, _, _, source, peerUnavailable := handlers.EdgeMulticastObservationLossLegsForTest(payload)
	assert.Equal(t, handlers.EdgeMulticastLossSources.Recorder, source)
	assert.False(t, peerUnavailable)

	// Without the recorder rows the same flag is exactly the claim it was before.
	payload.RecorderGaps = nil
	_, _, _, source, peerUnavailable = handlers.EdgeMulticastObservationLossLegsForTest(payload)
	assert.Equal(t, handlers.EdgeMulticastLossSources.Peers, source)
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

	assert.Equal(t, handlers.EdgeMulticastLossSources.Peers, source)
	require.Len(t, loss[gapLineKey()], 2)
	require.Len(t, simul[gapLineKey()], 1, "both nodes lost in second 100")
	assert.Empty(t, pub, "the publisher row exists only on the recorder leg")
}

// seedObservationsWithRecorderGaps writes an observations payload whose recorder leg is filled,
// which is what an environment with the recorder proxies would cache.
func seedObservationsWithRecorderGaps(t *testing.T, api *handlers.API, generatedAt time.Time, gaps ...handlers.EdgeMulticastRecorderGapSeries) {
	t.Helper()
	require.NoError(t, api.WritePageCache(t.Context(), observationsKey, handlers.EdgeMulticastObservationsResponse{
		GeneratedAt:   generatedAt,
		WindowMinutes: 15,
		RecorderGaps:  gaps,
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

	require.Len(t, seq.RecorderLossPublisher, 2, "both runs were charged to the publisher")
	assert.Empty(t, seq.RecorderLossSimultaneous, "the 2+ row is the peer leg's and must stay empty")
	assert.False(t, seq.RecorderGapsUnavailable)
}

// The statement itself, held against the contract. Nothing can execute it — the tables exist in no
// environment yet — so these are the properties whose absence would be silent and wrong.
func TestEdgeMulticastRecorderGapQuery_HoldsTheContract(t *testing.T) {
	q := handlers.EdgeMulticastRecorderGapQueryForTest("feeds")

	assert.Contains(t, q, "`feeds`.recorder_sequence_gap")
	assert.Contains(t, q, "`feeds`.recorder_segment_coverage",
		"coverage is not optional: a clean node emits no gap row, and the clean line is the comparison")

	// Both networks allocate multicast out of the same /24 and this payload's keys carry no
	// environment, so without the filter one network's loss renders on the other's row.
	assert.Contains(t, q, "env IN ('mainnet-beta', 'mainnet')")

	// The reference is per (instance, site), repeated on each of the instance's runs. Summed, the
	// denominator is multiplied by the number of runs and every rate on the page understates.
	assert.Contains(t, q, "max(reference_seqs)")
	assert.NotContains(t, q, "sum(reference_seqs)")

	// unexplained_count is the residue the strip shows; missing_count is carried beside it, never
	// instead of it.
	assert.Contains(t, q, "sum(unexplained_count) AS missing")
	assert.Contains(t, q, "sum(missing_count) AS missing_raw")
}
