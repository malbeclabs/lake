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

// **Which of several candidates is replaced cannot depend on slice order.** The payload comes from
// a GROUP BY with no ORDER BY, so the observations leg's rows arrive in whatever order the scan
// produced, and "the first match" moved the recorder's counters between rows across a poll of
// unchanged data. The lowest capture source name wins, which is a property of the data.
//
// The recorder's grain cannot resolve this case at all — it groups by (group, publisher, channel,
// node) and knows nothing about capture sources — so exactly one row is measured and the others
// keep GapsMeasured false. That understates the measurement; writing the aggregate onto each would
// multiply the group's gap books by the number of candidates, which invents one.
func TestTOBGapsPickTheSameCandidateWhateverOrderTheyArriveIn(t *testing.T) {
	alpha := stalenessOnlyInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1")
	alpha.CaptureSource = "tob_edge_kalshi_perps_alpha"
	omega := stalenessOnlyInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1")
	omega.CaptureSource = "tob_edge_kalshi_perps_omega"
	series := []handlers.EdgeMulticastTOBGapSeries{
		tobGapSeries("148.51.121.69", 1, "aws-cmh-mn-recorder1", 7, 19, []uint32{100}),
	}

	for _, order := range [][]handlers.EdgeMulticastChannelInstance{{alpha, omega}, {omega, alpha}} {
		existing := map[string][]handlers.EdgeMulticastChannelInstance{"grp-perps-tob": order}
		got := handlers.EdgeMulticastTOBGapsMergeForTest(tobGapsGroups(), existing, series, tobGapsAsOf)

		instances := got["grp-perps-tob"]
		require.Len(t, instances, 2, "the candidates stay two rows; only one is measured")
		measured := map[string]bool{}
		for _, inst := range instances {
			if inst.GapsMeasured {
				measured[inst.CaptureSource] = true
				assert.EqualValues(t, 7, inst.GapBooks)
			}
		}
		assert.Equal(t, map[string]bool{"tob_edge_kalshi_perps_alpha": true}, measured,
			"the lowest capture source name, not the first row")
	}
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

// **One recorder losing both its paths is not the feed losing data.** The group's red badge
// claims every path lost at once; measuring the top-of-book plane put three vantages behind
// that claim where the market-by-price plane has one, and a union across them let a single
// recorder's reception speak for the feed while its peers held intact copies.
func TestTOBGapsOneVantageLosingBothPathsIsNotAFeedLoss(t *testing.T) {
	lost := []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 2}}

	// cmh loses on both paths; was holds both, in the same seconds.
	instances := []handlers.EdgeMulticastChannelInstance{
		measuredInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1", lost),
		measuredInstance("148.51.120.6", 101, "aws-cmh-mn-recorder1", lost),
		measuredInstance("148.51.121.69", 1, "aws-was-mn-recorder1", nil),
		measuredInstance("148.51.120.6", 101, "aws-was-mn-recorder1", nil),
	}

	assert.Empty(t, handlers.EdgeMulticastAllPathsGappedForTest(instances),
		"a peer held an intact copy, so the feed delivered")
}

// And the claim still fires when every vantage lost both paths in the same second, which is
// the case the badge exists for.
func TestTOBGapsEveryVantageLosingTogetherIsAFeedLoss(t *testing.T) {
	lost := []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 2}}
	instances := []handlers.EdgeMulticastChannelInstance{
		measuredInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1", lost),
		measuredInstance("148.51.120.6", 101, "aws-cmh-mn-recorder1", lost),
		measuredInstance("148.51.121.69", 1, "aws-was-mn-recorder1", lost),
		measuredInstance("148.51.120.6", 101, "aws-was-mn-recorder1", lost),
	}

	got := handlers.EdgeMulticastAllPathsGappedForTest(instances)
	require.Len(t, got, 1)
	assert.Equal(t, int64(100), got[0].Start)
	assert.Equal(t, uint32(2), got[0].Seconds)
}

// **And a vantage that stopped ingesting vetoes nothing.** This is the dual of the test above and
// the worse failure of the pair, because a suppressed finding leaves nothing on the page to
// notice: a recorder whose every path is stale reports two series with no gap episodes at all,
// which intersects to nothing and would silence the badge for the whole group. Stalled is the
// signal that its silence is about itself.
func TestTOBGapsAStalledVantageDoesNotSilenceTheBadge(t *testing.T) {
	lost := []handlers.KalshiL2GapEpisode{{Start: 100, Seconds: 2}}
	instances := []handlers.EdgeMulticastChannelInstance{
		measuredInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1", lost),
		measuredInstance("148.51.120.6", 101, "aws-cmh-mn-recorder1", lost),
		measuredInstance("148.51.121.69", 1, "aws-was-mn-recorder1", lost),
		measuredInstance("148.51.120.6", 101, "aws-was-mn-recorder1", lost),
		stalledInstance("148.51.121.69", 1, "aws-dub-mn-recorder1"),
		stalledInstance("148.51.120.6", 101, "aws-dub-mn-recorder1"),
	}

	got := handlers.EdgeMulticastAllPathsGappedForTest(instances)
	require.Len(t, got, 1, "a stalled recorder is not a witness that the feed delivered")
	assert.Equal(t, int64(100), got[0].Start)
	assert.Equal(t, uint32(2), got[0].Seconds)
}

// A measured series with a capture source, which is what both rollups key on.
func measuredInstance(pubIP string, channel uint8, node string, episodes []handlers.KalshiL2GapEpisode) handlers.EdgeMulticastChannelInstance {
	return handlers.EdgeMulticastChannelInstance{
		PublisherSourceIP: pubIP,
		CaptureSource:     "tob_edge_kalshi_perps",
		ChannelID:         channel,
		Node:              node,
		GapEpisodes:       episodes,
		GapsMeasured:      true,
		Status:            "ok",
	}
}

// A measured series at a vantage that stopped ingesting: two paths, no episodes, stalled.
func stalledInstance(pubIP string, channel uint8, node string) handlers.EdgeMulticastChannelInstance {
	inst := measuredInstance(pubIP, channel, node, nil)
	inst.Status = "stalled"
	return inst
}

// **The leg refuses a group that is not top of book, and that branch guards a better
// measurement.** A recorded-gap series landing on a market-by-price group would replace that
// group's coverage instance on (source IP, Channel ID, node) and discard UpdatesReceived,
// UpdatesMissing, SeqGapEvents and the percentile pair — the page's only per-instrument loss
// source, and numbers this grain cannot reproduce. It should be unreachable; it is tested
// because the cost of it firing unnoticed is losing the better number and keeping something
// that looks complete.
func TestTOBGapsRefuseAGroupThatIsNotTopOfBook(t *testing.T) {
	mbp := []handlers.EdgeMulticastGroupForTest{
		{PK: "grp-perps-mbp", Code: "edge-kalshi-perps-mbp", MulticastIP: "233.84.178.3"},
	}
	existing := map[string][]handlers.EdgeMulticastChannelInstance{
		"grp-perps-mbp": {stalenessOnlyInstance("148.51.121.69", 1, "aws-cmh-mn-recorder1")},
	}
	series := []handlers.EdgeMulticastTOBGapSeries{
		tobGapSeries("148.51.121.69", 1, "aws-cmh-mn-recorder1", 20, 58, nil),
	}

	got := handlers.EdgeMulticastTOBGapsMergeForTest(mbp, existing, series, tobGapsAsOf)
	require.Len(t, got["grp-perps-mbp"], 1, "the coverage instance is still the only one")
	assert.False(t, got["grp-perps-mbp"][0].GapsMeasured,
		"and it is untouched: the recorder's counters did not replace it")
}
