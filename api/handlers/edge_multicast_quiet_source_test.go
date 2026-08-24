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
