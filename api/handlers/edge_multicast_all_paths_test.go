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
	assert.Equal(t, "edge-kalshi-elections",
		handlers.EdgeMulticastFamilyOfForTest("edge-kalshi-elections-mbp"))
	assert.Equal(t, "edge-kalshi-elections",
		handlers.EdgeMulticastFamilyOfForTest("edge-kalshi-elections-tob"))
	// A trailing segment that is NOT a plane is part of the name, not a suffix to strip: guessing
	// otherwise would merge unrelated groups into one section.
	assert.Equal(t, "edge-solana-shreds1",
		handlers.EdgeMulticastFamilyOfForTest("edge-solana-shreds1"))
	assert.Equal(t, "mbone", handlers.EdgeMulticastFamilyOfForTest("mbone"))
}
