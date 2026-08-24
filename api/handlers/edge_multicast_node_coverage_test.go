package handlers_test

import (
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The receiver-side check, and the pair it forms with path parity. One fixes the vantage and
// compares the paths; this one fixes the path and compares the vantages. What each has to refuse
// is the case the other is meant to catch.

// The mainnet shape this was written for: three recorders of one feed, one of them holding ~91% of
// what the other two hold, on BOTH paths. Measured, sustained across every minute of the window —
// and invisible until now, because capture-node parity's floor is half the median and 0.91 sails
// past it.
func TestEdgeMulticastNodeCoverage_ReportsARecorderShortOnEveryPath(t *testing.T) {
	cov := handlers.EdgeMulticastNodeCoverageForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 125467),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 125050),
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.9", 1, 125393),
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.10", 101, 125385),
		obsSeries("tob_edge_kalshi_perps", "was-rec1", "10.0.0.9", 1, 114605),
		obsSeries("tob_edge_kalshi_perps", "was-rec1", "10.0.0.10", 101, 112914),
	})

	require.Contains(t, cov, "group-t")
	assert.Equal(t, 3, cov["group-t"].Nodes)
	require.Len(t, cov["group-t"].Lagging, 1, "only the node that is short on everything")
	lagging := cov["group-t"].Lagging[0]
	assert.Equal(t, "was-rec1", lagging.Node)
	assert.Equal(t, 2, lagging.Behind, "behind on both paths")
	assert.Equal(t, 2, lagging.Compared)
	assert.InDelta(t, 0.9006, lagging.WorstRatio, 0.001)

	// And the 0.33% deficit the other recorder carries on one path is nowhere near this floor —
	// that finding belongs to the path, and Peer is where it is reported.
	for _, l := range cov["group-t"].Lagging {
		assert.NotEqual(t, "cmh-rec1", l.Node)
	}
}

// The refusal that keeps this from duplicating path parity. A node short on ONE path of a group is
// a path fault seen from that vantage, not a bad vantage — naming the recorder for it would blame
// the box for what a publisher's branch did.
func TestEdgeMulticastNodeCoverage_ShortOnOnePathIsNotTheRecorder(t *testing.T) {
	cov := handlers.EdgeMulticastNodeCoverageForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 1000),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 400),
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.9", 1, 1000),
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.10", 101, 1000),
	})

	require.Contains(t, cov, "group-t")
	assert.Empty(t, cov["group-t"].Lagging, "one path short at one node is the path, not the node")
}

// One recorder is no comparison. Neither a pass nor a fault may be recorded for it — the same
// refusal path parity makes for a path with no peer.
func TestEdgeMulticastNodeCoverage_LoneRecorderIsNotJudged(t *testing.T) {
	cov := handlers.EdgeMulticastNodeCoverageForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 1000),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 1000),
	})

	require.Contains(t, cov, "group-t")
	assert.Equal(t, 1, cov["group-t"].Nodes)
	assert.Empty(t, cov["group-t"].Lagging)
}

// Under the message floor the ratio means nothing: four messages against five is 0.8 and would
// mark a recorder for a rounding difference. The same floor path parity applies, for the same
// reason.
func TestEdgeMulticastNodeCoverage_ThinInstanceIsNotJudged(t *testing.T) {
	cov := handlers.EdgeMulticastNodeCoverageForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 5),
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.9", 1, 4),
	})

	require.Contains(t, cov, "group-t")
	assert.Empty(t, cov["group-t"].Lagging, "below the volume floor nothing is judged")
}

// The channels of one path are summed before the vantages are compared, so a node is measured on
// the whole of what that path carries. Comparing channel by channel would report a node that
// happens to split its recording differently as permanently behind.
func TestEdgeMulticastNodeCoverage_SumsChannelsBeforeComparing(t *testing.T) {
	cov := handlers.EdgeMulticastNodeCoverageForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 600),
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 11, 400),
		obsSeries("tob_edge_kalshi_sports_nfl", "dub-rec1", "10.0.0.9", 10, 1000),
	})

	require.Contains(t, cov, "group-t")
	assert.Empty(t, cov["group-t"].Lagging, "600 + 400 is level with 1000")
}
