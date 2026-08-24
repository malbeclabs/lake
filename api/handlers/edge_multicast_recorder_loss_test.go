package handlers_test

import (
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorder-loss fold: one row per recording node, and the row that says whether a loss was one
// node's branch or something the nodes share.

func lossSeries(node string, channel uint8, missing uint64, secs ...uint32) handlers.EdgeMulticastRecorderLossSeries {
	eps := make([]handlers.KalshiL2GapEpisode, 0, len(secs))
	for _, s := range secs {
		eps = append(eps, handlers.KalshiL2GapEpisode{Start: int64(s), Seconds: 1})
	}
	return handlers.EdgeMulticastRecorderLossSeries{
		MulticastGroup: "233.84.178.3", PublisherSourceIP: "148.51.121.69",
		ChannelID: channel, Node: node, LocationCode: node[:3],
		Missing: missing, ReferenceSeqs: 1000, Episodes: eps,
	}
}

// One recorder losing while its peers do not is that recorder's branch, and the global row must
// stay empty — that emptiness is the finding, not an absence of data.
func TestEdgeMulticastRecorderLoss_OneNodeAloneIsItsOwnBranch(t *testing.T) {
	loss, simul := handlers.EdgeMulticastRecorderLossFoldForTest([]handlers.EdgeMulticastRecorderLossSeries{
		lossSeries("was-rec1", 1, 293, 100, 101, 200),
		lossSeries("cmh-rec1", 1, 0),
	})

	lines := loss["148.51.121.69"]
	require.Len(t, lines, 2)
	assert.Equal(t, "was-rec1", lines[0].Node, "worst first")
	assert.EqualValues(t, 293, lines[0].Missing)
	assert.EqualValues(t, 0, lines[1].Missing)
	// 100 and 101 are contiguous, 200 is its own.
	require.Len(t, lines[0].Episodes, 2)
	assert.EqualValues(t, 2, lines[0].Episodes[0].Seconds)

	assert.Empty(t, simul["148.51.121.69"], "nothing was lost at two recorders at once")
}

// Two recorders losing in the same second is not one branch's fault, and that is the whole point of
// the global row.
func TestEdgeMulticastRecorderLoss_TwoNodesAtOnceIsNotABranch(t *testing.T) {
	_, simul := handlers.EdgeMulticastRecorderLossFoldForTest([]handlers.EdgeMulticastRecorderLossSeries{
		lossSeries("was-rec1", 1, 10, 100, 101, 300),
		lossSeries("cmh-rec1", 1, 5, 101, 400),
	})

	eps := simul["148.51.121.69"]
	require.Len(t, eps, 1, "only second 101 had two recorders losing")
	assert.EqualValues(t, 101, eps[0].Start)
	assert.EqualValues(t, 1, eps[0].Seconds)
}

// Simultaneity is asked per PATH. Two nodes losing in one second on DIFFERENT channels are two
// unrelated losses that happen to share a clock reading, and calling that simultaneous would blame
// the feed for a coincidence.
func TestEdgeMulticastRecorderLoss_SimultaneityIsPerPath(t *testing.T) {
	_, simul := handlers.EdgeMulticastRecorderLossFoldForTest([]handlers.EdgeMulticastRecorderLossSeries{
		lossSeries("was-rec1", 1, 10, 100),
		lossSeries("cmh-rec1", 101, 10, 100),
	})
	assert.Empty(t, simul["148.51.121.69"], "different channels are different paths")
}

// A path recorded at one node has no peer to be measured against, so the global row records
// nothing rather than recording a clean run.
func TestEdgeMulticastRecorderLoss_LoneRecorderHasNoComparison(t *testing.T) {
	loss, simul := handlers.EdgeMulticastRecorderLossFoldForTest([]handlers.EdgeMulticastRecorderLossSeries{
		lossSeries("cmh-rec1", 1, 7, 100),
	})
	assert.Len(t, loss["148.51.121.69"], 1)
	assert.Empty(t, simul["148.51.121.69"])
}

// A publisher carrying several channels folds each node's seconds across them, so one row per
// recorder rather than one per recorder per channel.
func TestEdgeMulticastRecorderLoss_FoldsChannelsIntoOneRowPerRecorder(t *testing.T) {
	loss, _ := handlers.EdgeMulticastRecorderLossFoldForTest([]handlers.EdgeMulticastRecorderLossSeries{
		lossSeries("was-rec1", 1, 3, 100),
		lossSeries("was-rec1", 101, 4, 500),
		lossSeries("cmh-rec1", 1, 0),
	})
	lines := loss["148.51.121.69"]
	require.Len(t, lines, 2)
	assert.Equal(t, "was-rec1", lines[0].Node)
	assert.EqualValues(t, 7, lines[0].Missing, "summed over the channels")
	assert.Len(t, lines[0].Episodes, 2, "one per channel's second, unioned")
}

// A series with no publisher address has no line to sit on and must not become the row an empty
// address lands on.
func TestEdgeMulticastRecorderLoss_UnattributedSeriesIsDropped(t *testing.T) {
	s := lossSeries("was-rec1", 1, 5, 100)
	s.PublisherSourceIP = ""
	loss, _ := handlers.EdgeMulticastRecorderLossFoldForTest([]handlers.EdgeMulticastRecorderLossSeries{s})
	assert.Empty(t, loss)
}
