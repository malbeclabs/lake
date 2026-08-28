package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A stalled series is a reading, not a verdict about whose silence it is. These pin the one thing
// that tells the two apart: whether the other paths recording that capture source went quiet with
// it.

var quietAsOf = time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

// fresh and stale sit either side of edgeMulticastSequenceStaleSecs (120s) against quietAsOf.
func quietInstance(pubIP, source string, channel uint8, node string, lastSeen time.Time) handlers.EdgeMulticastChannelInstance {
	return handlers.EdgeMulticastChannelInstance{
		PublisherSourceIP: pubIP,
		CaptureSource:     source,
		ChannelID:         channel,
		Node:              node,
		Messages:          1000,
		LastSeen:          lastSeen,
	}
}

func fresh() time.Time { return quietAsOf.Add(-10 * time.Second) }
func stale() time.Time { return quietAsOf.Add(-10 * time.Minute) }

// The mainnet case this exists for: one sports market ends mid-window and goes quiet on BOTH paths
// at once, while every other market keeps advancing. That is the venue, not the path, and the line
// must not read 'stalled' over it.
//
// The two paths carry the market on DIFFERENT channel ids — the +100 offset every feed here runs —
// so this also pins that the channel is folded out of the comparison. Keyed on channel the paths
// would never meet and the stall would stand.
func TestEdgeMulticastSequence_QuietCaptureSourceIsNotAPathFault(t *testing.T) {
	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		quietInstance("148.51.121.209", "tob_edge_kalshi_sports_nfl", 10, "node-a", stale()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_sports_nfl", 110, "node-a", stale()),
		quietInstance("148.51.121.209", "tob_edge_kalshi_sports_nba", 11, "node-a", fresh()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_sports_nba", 111, "node-a", fresh()),
	}, quietAsOf)

	assert.Equal(t, "ok", health.Status, "the closed market is the venue's silence, not a path fault")
	assert.Equal(t, 0, health.Stalled)
	assert.Equal(t, 2, health.CaptureSourceQuiet, "both paths' instances at the quiet source")

	// The reading itself is untouched — only its attribution moved.
	for _, inst := range health.Instances {
		if inst.CaptureSource == "tob_edge_kalshi_sports_nfl" {
			assert.Equal(t, "stalled", inst.Status)
			assert.True(t, inst.CaptureSourceQuiet)
		}
	}
}

// The other half of the same call: one path stops while its peer keeps recording that source. That
// IS the path, and nothing about it may be excused.
func TestEdgeMulticastSequence_OneStalledPathIsStillAFault(t *testing.T) {
	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		quietInstance("148.51.121.209", "tob_edge_kalshi_sports_nfl", 10, "node-a", stale()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_sports_nfl", 110, "node-a", fresh()),
	}, quietAsOf)

	assert.Equal(t, "stalled", health.Status)
	assert.Equal(t, 1, health.Stalled)
	assert.Equal(t, 0, health.CaptureSourceQuiet)
}

// A path with no peer at that vantage has nothing to be compared against, and the stall stands:
// with one path there is no way to tell a dead path from a quiet source, and answering either way
// would be inventing the half of the reading that is missing.
func TestEdgeMulticastSequence_NoPeerKeepsTheStall(t *testing.T) {
	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		quietInstance("148.51.121.209", "mbp_edge_kalshi_perps", 1, "node-a", stale()),
		quietInstance("148.51.121.209", "mbp_edge_kalshi_perps", 1, "node-b", fresh()),
	}, quietAsOf)

	assert.Equal(t, "stalled", health.Status, "one path at that node is no comparison")
	assert.Equal(t, 1, health.Stalled)
	assert.Equal(t, 0, health.CaptureSourceQuiet)
}

// The guard that keeps the whole rule honest. When the feed stops everywhere, every path is quiet
// at every capture source and the pairwise test alone would excuse all of it — a dead feed reading
// as advancing. A path is only excused at one source while it is delivering at another.
func TestEdgeMulticastSequence_AFeedStoppedEverywhereStaysStalled(t *testing.T) {
	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		quietInstance("148.51.121.209", "tob_edge_kalshi_sports_nfl", 10, "node-a", stale()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_sports_nfl", 110, "node-a", stale()),
		quietInstance("148.51.121.209", "tob_edge_kalshi_sports_nba", 11, "node-a", stale()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_sports_nba", 111, "node-a", stale()),
	}, quietAsOf)

	assert.Equal(t, "stalled", health.Status)
	assert.Equal(t, 4, health.Stalled)
	assert.Equal(t, 0, health.CaptureSourceQuiet)
}

// A recording node that stops ingesting mid-window is the failure the aliveness guard has to catch
// one level below the dead feed: every series that node holds goes stale together, so every pair at
// that vantage is quiet on both paths and looks exactly like the venue going quiet. What tells them
// apart is where the paths are alive — and it has to be THAT vantage, because a path delivering at
// another recorder says nothing about this one. Keyed on the path alone this read
// stalled=0, capture_source_quiet=4, with the lines reporting 'advancing' over a dead recorder.
func TestEdgeMulticastSequence_ADeadRecorderIsNotAQuietVenue(t *testing.T) {
	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		// node-a stopped: everything it holds is stale, on both paths.
		quietInstance("148.51.121.209", "tob_edge_kalshi_perps", 1, "node-a", stale()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_perps", 101, "node-a", stale()),
		quietInstance("148.51.121.209", "tob_edge_kalshi_sports_nfl", 10, "node-a", stale()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_sports_nfl", 110, "node-a", stale()),
		// The same two paths, delivering fine at another recorder.
		quietInstance("148.51.121.209", "tob_edge_kalshi_perps", 1, "node-b", fresh()),
		quietInstance("148.51.120.152", "tob_edge_kalshi_perps", 101, "node-b", fresh()),
	}, quietAsOf)

	assert.Equal(t, "stalled", health.Status, "a dead recorder is not the venue going quiet")
	assert.Equal(t, 4, health.Stalled)
	assert.Zero(t, health.CaptureSourceQuiet)
}

// A gapped series is a fault of its own and outranks the whole question: the quiet source excuses
// silence, never loss.
func TestEdgeMulticastSequence_QuietSourceDoesNotExcuseAGap(t *testing.T) {
	gapped := quietInstance("148.51.121.209", "mbp_edge_kalshi_sports_nfl", 10, "node-a", fresh())
	gapped.GapBooks = 3
	gapped.GapsMeasured = true
	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		gapped,
		quietInstance("148.51.120.152", "mbp_edge_kalshi_sports_nfl", 110, "node-a", stale()),
	}, quietAsOf)

	assert.Equal(t, "gapped", health.Status)
	require.Equal(t, 1, health.Gapped)
	assert.Equal(t, 1, health.Stalled, "its peer is not quiet, so the stall is the path's own")
	assert.Equal(t, 0, health.CaptureSourceQuiet)
}

// GapNodes bounds what a gap finding may be attributed to. One vantage clears the recorder's HOST
// and nothing more: the branch into that recorder is upstream of a path-versus-path comparison
// there and downstream of everything else, so a loss on it reads exactly like a loss on the path.
// Observed on mainnet — 13 books gapped at the only node recording market-by-price, while the plane
// with three vantages found that same path intact at another node.
func TestEdgeMulticastSequence_GapNodesCountsTheVantages(t *testing.T) {
	gapped := func(pubIP, node string) handlers.EdgeMulticastChannelInstance {
		i := quietInstance(pubIP, "mbp_edge_kalshi_perps", 101, node, fresh())
		i.GapBooks = 13
		i.GapMessages = 7600
		i.GapsMeasured = true
		return i
	}
	clean := func(pubIP, node string) handlers.EdgeMulticastChannelInstance {
		i := quietInstance(pubIP, "mbp_edge_kalshi_perps", 1, node, fresh())
		i.GapsMeasured = true
		return i
	}

	// Both paths at one recorder: the host is cleared, the branch into it is not.
	one := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		gapped("148.51.120.6", "cmh-rec1"),
		clean("148.51.121.69", "cmh-rec1"),
	}, quietAsOf)
	assert.Equal(t, "gapped", one.Status, "data was lost either way")
	assert.Equal(t, 1, one.GapNodes)

	// A second vantage is what makes the finding attributable.
	two := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{
		gapped("148.51.120.6", "cmh-rec1"),
		clean("148.51.121.69", "cmh-rec1"),
		clean("148.51.120.6", "dub-rec1"),
		clean("148.51.121.69", "dub-rec1"),
	}, quietAsOf)
	assert.Equal(t, 2, two.GapNodes)
}

// A plane with no gap marker contributes no vantage: its instances cannot corroborate a gap they
// are structurally unable to measure.
func TestEdgeMulticastSequence_GapNodesIgnoresTheUnmeasuredPlane(t *testing.T) {
	mbp := quietInstance("148.51.120.6", "mbp_edge_kalshi_perps", 101, "cmh-rec1", fresh())
	mbp.GapBooks = 3
	mbp.GapsMeasured = true
	tob := quietInstance("148.51.120.6", "tob_edge_kalshi_perps", 101, "dub-rec1", fresh())

	health := handlers.EdgeMulticastSequenceHealthForTest([]handlers.EdgeMulticastChannelInstance{mbp, tob}, quietAsOf)
	assert.Equal(t, 1, health.GapNodes, "the top-of-book vantage counts no gaps and corroborates none")
	assert.Equal(t, 1, health.GapsUnmeasured)
}
