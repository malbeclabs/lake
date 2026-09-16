package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The top-of-book plane gets its gap counters from the feed-race recorder's own grain, over the
// top of the series the observations leg already folded for the same channel instance. These pin
// the rule that keeps one feed from being reported twice.

var tobGapsAsOf = time.Date(2026, 9, 16, 21, 40, 0, 0, time.UTC)

func tobGapsGroups() []handlers.EdgeMulticastGroupForTest {
	return []handlers.EdgeMulticastGroupForTest{
		{PK: "grp-perps-tob", Code: "edge-kalshi-perps-tob", MulticastIP: "233.84.178.3"},
	}
}

// What the observations leg folds: present, advancing, and unable to say whether anything was
// lost.
func stalenessOnlyInstance(pubIP string, channel uint8, node string) handlers.EdgeMulticastChannelInstance {
	return handlers.EdgeMulticastChannelInstance{
		PublisherSourceIP: pubIP,
		CaptureSource:     "tob_edge_kalshi_perps",
		ChannelID:         channel,
		Node:              node,
		LocationCode:      "cmh",
		Messages:          44_670,
		GapsMeasured:      false,
		Status:            "ok",
		LastSeen:          tobGapsAsOf.Add(-2 * time.Second),
	}
}

func tobGapSeries(pubIP string, channel uint8, node string, gapBooks, gapMessages uint64, secs []uint32) handlers.EdgeMulticastTOBGapSeries {
	return handlers.EdgeMulticastTOBGapSeries{
		MulticastGroup:    "233.84.178.3",
		PublisherSourceIP: pubIP,
		ChannelID:         channel,
		Node:              node,
		LocationCode:      "cmh",
		Messages:          44_670,
		GapMessages:       gapMessages,
		GapBooks:          gapBooks,
		GapSeconds:        secs,
		LastSeen:          tobGapsAsOf.Add(-2 * time.Second),
	}
}

// **One channel instance stays one row, and it is the measured one that survives.** Appending
// would double every top-of-book row on the page and leave half of each pair reporting
// GapsUnmeasured while the other half reported the gaps.
func TestTOBGapsReplaceTheStalenessOnlySeries(t *testing.T) {
	existing := map[string][]handlers.EdgeMulticastChannelInstance{
		"grp-perps-tob": {
			stalenessOnlyInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1"),
			stalenessOnlyInstance("148.51.120.6", 101, "aws-cmh-mn-recorder1"),
		},
	}
	series := []handlers.EdgeMulticastTOBGapSeries{
		tobGapSeries("148.51.121.69", 1, "aws-cmh-mn-recorder1", 20, 58, []uint32{100, 101, 400}),
		tobGapSeries("148.51.120.6", 101, "aws-cmh-mn-recorder1", 0, 0, nil),
	}

	got := handlers.EdgeMulticastTOBGapsMergeForTest(tobGapsGroups(), existing, series, tobGapsAsOf)
	instances := got["grp-perps-tob"]
	require.Len(t, instances, 2, "two channel instances, not four")

	byChannel := map[uint8]handlers.EdgeMulticastChannelInstance{}
	for _, inst := range instances {
		byChannel[inst.ChannelID] = inst
	}

	gapped := byChannel[1]
	assert.True(t, gapped.GapsMeasured, "the recorder's reading is a measurement, not an absence")
	assert.Equal(t, uint64(20), gapped.GapBooks)
	assert.Equal(t, uint64(58), gapped.GapMessages)
	assert.Equal(t, "gapped", gapped.Status)
	// The seconds collapse into runs: 100 and 101 are one episode, 400 another.
	require.Len(t, gapped.GapEpisodes, 2)
	assert.Equal(t, int64(100), gapped.GapEpisodes[0].Start)
	assert.Equal(t, uint32(2), gapped.GapEpisodes[0].Seconds)

	// **A clean series is still a measurement.** Zero gaps with GapsMeasured true is a clean bill
	// of health; the same zero with it false is "not checked", and the page renders them
	// differently on purpose.
	clean := byChannel[101]
	assert.True(t, clean.GapsMeasured)
	assert.Equal(t, uint64(0), clean.GapBooks)
	assert.Equal(t, "ok", clean.Status)
	assert.Empty(t, clean.GapEpisodes, "a clean series draws no timeline")
}

// The capture source name is the page's own sort key and what the quiet-source demotion reads.
// The recorder's grain names a feed and not a capture source, so the replaced series' name is
// carried over rather than invented.
func TestTOBGapsKeepTheCaptureSourceNameOfTheSeriesItReplaces(t *testing.T) {
	existing := map[string][]handlers.EdgeMulticastChannelInstance{
		"grp-perps-tob": {stalenessOnlyInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1")},
	}
	series := []handlers.EdgeMulticastTOBGapSeries{
		tobGapSeries("148.51.121.69", 1, "aws-cmh-mn-recorder1", 0, 0, nil),
	}

	got := handlers.EdgeMulticastTOBGapsMergeForTest(tobGapsGroups(), existing, series, tobGapsAsOf)
	require.Len(t, got["grp-perps-tob"], 1)
	assert.Equal(t, "tob_edge_kalshi_perps", got["grp-perps-tob"][0].CaptureSource)
}

// **The match is all three of address, Channel ID and node.** A second recorder of the same
// instance is a second observation and has to stay its own row: merging them would hide a
// recorder that is missing the feed, which is the reason the node is not folded in the first
// place.
func TestTOBGapsFromAnotherNodeIsItsOwnRow(t *testing.T) {
	existing := map[string][]handlers.EdgeMulticastChannelInstance{
		"grp-perps-tob": {stalenessOnlyInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1")},
	}
	series := []handlers.EdgeMulticastTOBGapSeries{
		tobGapSeries("148.51.121.69", 1, "aws-was-mn-recorder1", 0, 0, nil),
	}

	got := handlers.EdgeMulticastTOBGapsMergeForTest(tobGapsGroups(), existing, series, tobGapsAsOf)
	require.Len(t, got["grp-perps-tob"], 2, "two vantages of one instance are two observations")
}

// An address no group on this page carries is dropped rather than bucketed — the same rule the
// capture-source resolver documents. In practice this is what a row written before
// malbeclabs/kalshi#287 looks like if the query's filter ever stopped excluding it: 0.0.0.0
// resolves to nothing.
func TestTOBGapsForAnUnknownGroupAreDropped(t *testing.T) {
	series := []handlers.EdgeMulticastTOBGapSeries{
		tobGapSeries("148.51.121.69", 1, "aws-cmh-mn-recorder1", 0, 0, nil),
	}
	series[0].MulticastGroup = "0.0.0.0"

	got := handlers.EdgeMulticastTOBGapsMergeForTest(tobGapsGroups(), nil, series, tobGapsAsOf)
	assert.Empty(t, got, "a series with no group is not a group")
}
