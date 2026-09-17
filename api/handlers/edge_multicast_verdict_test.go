package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The verdict's truth table, in one place. It is the page's headline and the only thing on the row
// an operator reads before deciding whether to act, so every state it can reach is pinned here
// rather than inferred from the integration tests that happen to produce one.
func TestEdgeMulticastVerdict(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		total   int
		stats   edgeMulticastPublisherStats
		lagging int
		want    string
		why     string
	}{
		{
			name: "no publishers", total: 0, want: "",
			why: "a group provisioned before anyone joined it has nothing to judge",
		},
		{
			name: "nothing measured", total: 2, stats: edgeMulticastPublisherStats{unknown: 2}, want: "unknown",
			why: "a monitoring gap is not an outage",
		},
		{
			name: "all idle", total: 2, stats: edgeMulticastPublisherStats{idle: 2}, want: "silent",
			why: "measured, and not one of them moved a byte",
		},
		{
			name: "one thin one publishing", total: 2,
			stats: edgeMulticastPublisherStats{publishing: 1, thin: 1}, want: "thin",
			why: "the feed is flowing and one of its publishers is not",
		},
		{
			name: "one idle one publishing", total: 2,
			stats: edgeMulticastPublisherStats{publishing: 1, idle: 1}, want: "thin",
			why: "zero is below the floor, so a dead publisher reads the same as a starved one",
		},
		{
			name: "all thin", total: 2, stats: edgeMulticastPublisherStats{thin: 2}, want: "thin",
			why: "not silent: they are sending, just not enough to be the product",
		},
		{
			name: "publishers fine, node behind", total: 1,
			stats: edgeMulticastPublisherStats{publishing: 1}, lagging: 1, want: "skewed",
			why: "the publisher check passed, so the recorder check gets to speak",
		},
		{
			name: "thin outranks skewed", total: 2,
			stats: edgeMulticastPublisherStats{publishing: 1, idle: 1}, lagging: 1, want: "thin",
			why: "a fault at the publisher outranks one receiver falling behind",
		},
		{
			name: "unmeasured peer does not spoil it", total: 2,
			stats: edgeMulticastPublisherStats{publishing: 1, unknown: 1}, want: "healthy",
			why: "one device's telemetry gap is not a fault in the feed",
		},
		{
			name: "all publishing", total: 3, stats: edgeMulticastPublisherStats{publishing: 3}, want: "healthy",
			why: "every publisher above the floor and no node behind",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := edgeMulticastVerdict(EdgeMulticastRoleCounts{Total: tc.total}, tc.stats, tc.lagging)
			assert.Equal(t, tc.want, got, tc.why)
		})
	}
}

// The floor is a >= test, so a publisher sitting exactly on it passes. Worth pinning: an off-by-one
// here would mark a feed amber for being precisely at the threshold it was told to clear.
func TestEdgeMulticastPublisherStatus(t *testing.T) {
	t.Parallel()

	bps := func(v float64) *float64 { return &v }
	assert.Equal(t, edgeMulticastPubUnknown, edgeMulticastPublisherStatus(nil))
	assert.Equal(t, edgeMulticastPubIdle, edgeMulticastPublisherStatus(bps(0)))
	assert.Equal(t, edgeMulticastPubThin, edgeMulticastPublisherStatus(bps(1)))
	assert.Equal(t, edgeMulticastPubThin, edgeMulticastPublisherStatus(bps(edgeMulticastPublisherFloorBps-1)))
	assert.Equal(t, edgeMulticastPubPublishing, edgeMulticastPublisherStatus(bps(edgeMulticastPublisherFloorBps)))
	assert.Equal(t, edgeMulticastPubPublishing, edgeMulticastPublisherStatus(bps(2_400_000)))
}

// Worst-first ordering is what makes the line cap safe: truncation may only ever drop publishers
// that are fine.
func TestSortEdgeMulticastPublisherLines(t *testing.T) {
	t.Parallel()

	bps := func(v float64) *float64 { return &v }
	lines := []EdgeMulticastPublisher{
		{ClientIP: "10.0.0.4", Status: edgeMulticastPubPublishing, Bps: bps(9_000_000)},
		{ClientIP: "10.0.0.3", Status: edgeMulticastPubUnknown},
		{ClientIP: "10.0.0.2", Status: edgeMulticastPubThin, Bps: bps(400)},
		{ClientIP: "10.0.0.1", Status: edgeMulticastPubIdle, Bps: bps(0)},
		{ClientIP: "10.0.0.5", Status: edgeMulticastPubPublishing, Bps: bps(1_000)},
	}
	sortEdgeMulticastPublisherLines(lines)

	got := make([]string, 0, len(lines))
	for _, l := range lines {
		got = append(got, l.ClientIP)
	}
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.5", "10.0.0.4"}, got,
		"idle, then thin, then unmeasured, then publishing by ascending rate")
}

// Parity is measured against the median and not the max, so one node running hot cannot indict
// the rest — and a lane where every node is silent produces no verdict at all, because the
// application plane cannot tell a dead recorder from a dead publisher.
func TestEdgeMulticastCaptureNodes(t *testing.T) {
	t.Parallel()

	t.Run("one node behind its peers", func(t *testing.T) {
		nodes := edgeMulticastCaptureNodes([]edgeMulticastNodeObs{
			{node: "cmh-rec1", samples: 100},
			{node: "was-rec1", samples: 98},
			{node: "dub-rec1", samples: 4},
		})
		require.Len(t, nodes, 3)
		assert.Equal(t, "dub-rec1", nodes[0].Node, "sorted by share, so the one to look at is first")
		assert.True(t, nodes[0].Lagging)
		assert.False(t, nodes[1].Lagging)
		assert.False(t, nodes[2].Lagging)
		assert.Equal(t, 1, edgeMulticastLaggingNodes(nodes))
	})

	t.Run("one node running hot indicts nobody", func(t *testing.T) {
		nodes := edgeMulticastCaptureNodes([]edgeMulticastNodeObs{
			{node: "a", samples: 100},
			{node: "b", samples: 100},
			{node: "c", samples: 10_000},
		})
		assert.Equal(t, 0, edgeMulticastLaggingNodes(nodes), "the median is 100, not 10000")
	})

	t.Run("a single node has nothing to compare against", func(t *testing.T) {
		nodes := edgeMulticastCaptureNodes([]edgeMulticastNodeObs{{node: "only", samples: 1}})
		require.Len(t, nodes, 1)
		assert.False(t, nodes[0].Lagging)
	})

	t.Run("every node silent is not one node lagging", func(t *testing.T) {
		nodes := edgeMulticastCaptureNodes([]edgeMulticastNodeObs{
			{node: "a", samples: 0},
			{node: "b", samples: 0},
		})
		assert.Equal(t, 0, edgeMulticastLaggingNodes(nodes))
		assert.Zero(t, nodes[0].ShareOfMedian, "no median, no share: nothing is claimed")
	})

	t.Run("no nodes", func(t *testing.T) {
		assert.Nil(t, edgeMulticastCaptureNodes(nil))
	})
}

func TestEdgeMulticastMedian(t *testing.T) {
	t.Parallel()

	assert.Zero(t, edgeMulticastMedian(nil))
	assert.InDelta(t, 5, edgeMulticastMedian([]float64{5}), 0.001)
	assert.InDelta(t, 7.5, edgeMulticastMedian([]float64{10, 5}), 0.001, "even count averages the middle pair")
	assert.InDelta(t, 5, edgeMulticastMedian([]float64{100, 5, 1}), 0.001)

	// The caller's slice is not reordered as a side effect of reading it.
	in := []float64{3, 1, 2}
	edgeMulticastMedian(in)
	assert.Equal(t, []float64{3, 1, 2}, in)
}

// Sequence-counter grading. Both loss checks are unconditional; staleness is measured against the
// coverage payload's own clock, which is what keeps a ten-minute-old cache from marking every
// series stalled.
func TestEdgeMulticastSequenceStatus(t *testing.T) {
	t.Parallel()

	asOf := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, edgeMulticastSeqGapped, edgeMulticastSequenceStatus(1, 0, asOf, asOf))
	assert.Equal(t, edgeMulticastSeqGapped, edgeMulticastSequenceStatus(1, 0, asOf.Add(-time.Hour), asOf),
		"a gap outranks staleness: it is the fault, the other is the symptom")
	assert.Equal(t, edgeMulticastSeqOK, edgeMulticastSequenceStatus(0, 0, asOf.Add(-30*time.Second), asOf))
	assert.Equal(t, edgeMulticastSeqOK, edgeMulticastSequenceStatus(0, 0, asOf.Add(-edgeMulticastSequenceStaleSecs*time.Second), asOf),
		"exactly at the bound is still advancing")
	assert.Equal(t, edgeMulticastSeqStalled, edgeMulticastSequenceStatus(0, 0, asOf.Add(-5*time.Minute), asOf))
	assert.Equal(t, edgeMulticastSeqStalled, edgeMulticastSequenceStatus(0, 0, time.Time{}, asOf),
		"no timestamp at all is not evidence of a healthy series")
}

// A hole in the per-instrument sequence is a fault whether or not the recorder wrote a gap marker
// for it. Measured over six hours of mainnet, five channel instances lost updates with gap_books
// at zero — ligue1 ch25 lost 958 of them, at 1,551 ppm — and grading on the marker alone called
// every one of them 'ok'.
func TestEdgeMulticastSequenceStatus_MissingUpdatesWithoutAGapMarker(t *testing.T) {
	t.Parallel()

	asOf := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	fresh := asOf.Add(-30 * time.Second)

	assert.Equal(t, edgeMulticastSeqGapped, edgeMulticastSequenceStatus(0, 958, fresh, asOf),
		"updates that never arrived are loss, marker or no marker")
	assert.Equal(t, edgeMulticastSeqGapped, edgeMulticastSequenceStatus(0, 1, fresh, asOf),
		"one missing update is still a hole in the numbering")
	assert.Equal(t, edgeMulticastSeqGapped, edgeMulticastSequenceStatus(0, 2, asOf.Add(-time.Hour), asOf),
		"loss outranks staleness the same way a marker does")

	// The top-of-book fold passes zero for both counters, and that must stay a statement about
	// staleness alone rather than a clean bill of health. GapsMeasured is what carries the
	// difference.
	assert.Equal(t, edgeMulticastSeqOK, edgeMulticastSequenceStatus(0, 0, fresh, asOf))
}

// The roll-up is worst-first and the counts are per instance, so the badge can say "1 of 4"
// instead of implying a whole group is broken.
func TestFinishEdgeMulticastSequenceHealth(t *testing.T) {
	t.Parallel()

	health := &EdgeMulticastSequenceHealth{Instances: []EdgeMulticastChannelInstance{
		{CaptureSource: "mbp_a", ChannelID: 1, Node: "cmh-rec1", Status: edgeMulticastSeqOK},
		{CaptureSource: "mbp_a", ChannelID: 2, Node: "cmh-rec1", Status: edgeMulticastSeqStalled},
		{CaptureSource: "mbp_a", ChannelID: 3, Node: "cmh-rec1", GapBooks: 2, Status: edgeMulticastSeqGapped},
		{CaptureSource: "mbp_a", ChannelID: 4, Node: "was-rec1", GapBooks: 9, Status: edgeMulticastSeqGapped},
	}}
	finishEdgeMulticastSequenceHealth(health)

	assert.Equal(t, edgeMulticastSeqGapped, health.Status)
	assert.Equal(t, 2, health.Gapped)
	assert.Equal(t, 1, health.Stalled)
	got := make([]uint8, 0, len(health.Instances))
	for _, i := range health.Instances {
		got = append(got, i.ChannelID)
	}
	assert.Equal(t, []uint8{4, 3, 2, 1}, got, "most gapped books first, then stalled, then ok")

	stalledOnly := &EdgeMulticastSequenceHealth{Instances: []EdgeMulticastChannelInstance{
		{Status: edgeMulticastSeqOK},
		{Status: edgeMulticastSeqStalled},
	}}
	finishEdgeMulticastSequenceHealth(stalledOnly)
	assert.Equal(t, edgeMulticastSeqStalled, stalledOnly.Status)

	allOK := &EdgeMulticastSequenceHealth{Instances: []EdgeMulticastChannelInstance{
		{Status: edgeMulticastSeqOK},
		{Status: edgeMulticastSeqOK},
	}}
	finishEdgeMulticastSequenceHealth(allOK)
	assert.Equal(t, edgeMulticastSeqOK, allOK.Status)
}
