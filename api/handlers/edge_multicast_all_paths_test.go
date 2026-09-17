package handlers_test

import (
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// "Every path lost at once" is the group's finding: A protects B, so one path gapping is the
// redundancy working and both gapping is the feed losing data.

func inst(pub, source, node string, measured bool, secs ...uint32) handlers.EdgeMulticastChannelInstance {
	eps := make([]handlers.KalshiL2GapEpisode, 0, len(secs))
	for _, s := range secs {
		eps = append(eps, handlers.KalshiL2GapEpisode{Start: int64(s), Seconds: 1})
	}
	return handlers.EdgeMulticastChannelInstance{
		PublisherSourceIP: pub, CaptureSource: source, Node: node,
		GapsMeasured: measured, GapEpisodes: eps,
	}
}

// The headline: the second both paths share is the only one reported.
func TestEdgeMulticastAllPathsGapped_OnlyTheSharedSecond(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_perps", "cmh-rec1", true, 100, 101, 300),
		inst("148.51.120.6", "mbp_edge_kalshi_perps", "cmh-rec1", true, 101, 500),
	})
	require.Len(t, got, 1)
	assert.EqualValues(t, 101, got[0].Start)
	assert.EqualValues(t, 1, got[0].Seconds)
}

// One path losing while its peer holds is the redundancy doing its job, and must report nothing.
func TestEdgeMulticastAllPathsGapped_OnePathAloneIsCovered(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_perps", "cmh-rec1", true, 100, 101, 102),
		inst("148.51.120.6", "mbp_edge_kalshi_perps", "cmh-rec1", true),
	})
	assert.Empty(t, got)
}

// The capture source is in the key. Two paths losing in one second at DIFFERENT markets are two
// unrelated losses, and calling that a feed outage blames the feed for a coincidence.
func TestEdgeMulticastAllPathsGapped_DifferentCaptureSourcesDoNotIntersect(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_sports_nfl", "cmh-rec1", true, 100),
		inst("148.51.120.6", "mbp_edge_kalshi_sports_nba", "cmh-rec1", true, 100),
	})
	assert.Empty(t, got)
}

// The recording node is in the key too: a recorder that stopped ingesting has every series it
// holds go stale together, which without the node would read as every path failing at once.
func TestEdgeMulticastAllPathsGapped_DifferentNodesDoNotIntersect(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_perps", "cmh-rec1", true, 100),
		inst("148.51.120.6", "mbp_edge_kalshi_perps", "was-rec1", true, 100),
	})
	assert.Empty(t, got)
}

// A single-path group has no redundancy to lose, so its ordinary gaps are not feed outages.
func TestEdgeMulticastAllPathsGapped_OnePathIsNeverAnOutage(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_perps", "cmh-rec1", true, 100, 101, 102),
	})
	assert.Empty(t, got)
}

// An unmeasured plane contributes nothing. Its empty episode list is an absence of measurement, and
// treating it as "held" would let a top-of-book series vouch for a path nothing checked.
func TestEdgeMulticastAllPathsGapped_UnmeasuredPlaneIsNotAWitness(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "tob_edge_kalshi_perps", "cmh-rec1", false, 100),
		inst("148.51.120.6", "tob_edge_kalshi_perps", "cmh-rec1", false, 100),
	})
	assert.Empty(t, got)
}

// Contiguous shared seconds collapse into one episode, so the badge reports one outage rather than
// three.
func TestEdgeMulticastAllPathsGapped_SharedRunIsOneEpisode(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_perps", "cmh-rec1", true, 100, 101, 102),
		inst("148.51.120.6", "mbp_edge_kalshi_perps", "cmh-rec1", true, 100, 101, 102),
	})
	require.Len(t, got, 1)
	assert.EqualValues(t, 3, got[0].Seconds)
}

// Section placement: a group no feed row claims is promoted out of the bottom bucket once its
// publishers are moving traffic.
func TestEdgeMulticastFamilyOf(t *testing.T) {
	// Both planes of one product share a family, which is what puts them in one section.
	assert.Equal(t, "edge-kalshi-elections-pol",
		handlers.EdgeMulticastFamilyOfForTest("edge-kalshi-elections-pol-mbp"))
	assert.Equal(t, "edge-kalshi-elections-pol",
		handlers.EdgeMulticastFamilyOfForTest("edge-kalshi-elections-pol-tob"))
	// A trailing segment that is NOT a plane is part of the name, not a suffix to strip: guessing
	// otherwise would merge unrelated groups into one section.
	assert.Equal(t, "edge-solana-shreds1",
		handlers.EdgeMulticastFamilyOfForTest("edge-solana-shreds1"))
	assert.Equal(t, "mbone", handlers.EdgeMulticastFamilyOfForTest("mbone"))
}

// The capture source is in this key so that "two unrelated losses at two different markets in the
// same second" do not read as one shared outage — which is exactly what an EMPTY source produces,
// because every unnamed series shares the one bucket rather than opting out of the key.
//
// It was unreachable until the recorded-gap leg: the only instances with no capture source were
// top-of-book ones, and GapsMeasured false already excluded them. That leg measures the top-of-book
// plane, so an unmatched series now arrives measured and unnamed.
func TestEdgeMulticastAllPathsGapped_UnnamedCaptureSourcesDoNotIntersect(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("10.0.0.9", "", "cmh-rec1", true, 100),
		inst("10.0.0.10", "", "cmh-rec1", true, 100),
	})
	assert.Empty(t, got, "one second of loss at two unnamed series is not the feed losing data")
}

// The sources are unioned, so one market losing both of its paths is a finding even while every
// other market in the group holds. Intersecting across the sources as well as the nodes reads as
// the stricter claim and is in fact an unmeetable one: a sports group carries 29 capture sources,
// and "all 29 lost in this same second" is a condition nothing produces, so the badge would never
// fire again.
func TestEdgeMulticastAllPathsGapped_OneMarketIsNotVetoedByItsNeighbours(t *testing.T) {
	got := handlers.EdgeMulticastAllPathsGappedForTest([]handlers.EdgeMulticastChannelInstance{
		inst("148.51.121.69", "mbp_edge_kalshi_sports_nfl", "cmh-rec1", true, 100),
		inst("148.51.120.6", "mbp_edge_kalshi_sports_nfl", "cmh-rec1", true, 100),
		inst("148.51.121.69", "mbp_edge_kalshi_sports_nba", "cmh-rec1", true),
		inst("148.51.120.6", "mbp_edge_kalshi_sports_nba", "cmh-rec1", true),
		inst("148.51.121.69", "mbp_edge_kalshi_sports_mlb", "cmh-rec1", true),
		inst("148.51.120.6", "mbp_edge_kalshi_sports_mlb", "cmh-rec1", true),
	})
	require.Len(t, got, 1)
	assert.EqualValues(t, 100, got[0].Start)
	assert.EqualValues(t, 1, got[0].Seconds)
}
