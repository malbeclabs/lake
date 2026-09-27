package handlers_test

import (
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A gapped publisher line says data was lost. It does not say whose loss it was, and with several
// recorders on the same channel instance it can: a vantage that recorded the same series clean over
// the same window places the loss downstream of the split, on the branch into the recorders that
// did gap.
//
// Measured on mainnet 2026-09-26 and the reason this exists: both Kalshi perps paths read gapped at
// aws-cmh (23,131 ppm) and aws-was (2,428 ppm) while aws-dub recorded the same two channel
// instances with no marker at all for hours. The page rendered that identically to a path losing
// data end to end.

// The window these tests are written against: a fifteen-minute frame starting at windowStart.
const windowStart = 1_790_000_000 - 1_790_000_000%60

// minuteAt is the unix second at the start of minute n of the window.
func minuteAt(n int) int64 { return int64(windowStart + n*60) }

// minutes is the minute-boundary unix seconds for [from, to) of the window, which is the shape
// PresentMinutes carries.
func minutes(from, to int) []uint32 {
	out := make([]uint32, 0, to-from)
	for n := from; n < to; n++ {
		out = append(out, uint32(windowStart+n*60))
	}
	return out
}

func seqInst(node, source string, channel uint8, status string, measured bool) handlers.EdgeMulticastChannelInstance {
	return handlers.EdgeMulticastChannelInstance{
		PublisherSourceIP: "148.51.121.69",
		CaptureSource:     source,
		ChannelID:         channel,
		Node:              node,
		GapsMeasured:      measured,
		Status:            status,
		// Present for the whole window unless a test says otherwise, which is the ordinary
		// state: every rule other than the coverage one is about something else, and a series
		// with no minutes would fail the coverage rule before reaching it.
		PresentMinutes: minutes(0, 15),
	}
}

// The headline. Two recorders gapped, a third measured the same series clean, so the loss is theirs
// and the page may say so.
func TestEdgeMulticastGapConfined_ACleanVantageNamesTheRecorders(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-was-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true),
	})
	assert.Equal(t, []string{"aws-cmh-mn-recorder1", "aws-was-mn-recorder1"}, got,
		"the nodes that gapped, sorted, and not the one that vouched for the path")
}

// The finding this must never soften. Every vantage losing the same series is the path losing it,
// and there is no recorder to charge it to.
func TestEdgeMulticastGapConfined_EveryVantageGappedIsThePath(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
	})
	assert.Nil(t, got)
}

// One vantage is what GapNodes already bounds, and it cannot be narrowed further: the branch into
// that recorder is upstream of the comparison and downstream of everything else. Every
// market-by-price series on the page is this case today.
func TestEdgeMulticastGapConfined_OneVantageConfinesNothing(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "mbp_edge_kalshi_perps", 101, "gapped", true),
	})
	assert.Nil(t, got)
}

// A stalled peer recorded nothing over the window. Nothing is not a clean reading, and treating it
// as one would charge a recorder for a loss during a window in which the only witness was asleep.
func TestEdgeMulticastGapConfined_AStalledPeerCorroboratesNothing(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "stalled", true),
	})
	assert.Nil(t, got)
}

// A peer on a plane with no gap marker reads ok because nothing checked it. That is the
// 'advancing' reading, and it is not a witness to anything.
func TestEdgeMulticastGapConfined_AnUnmeasuredPeerIsNotAWitness(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", false),
	})
	assert.Nil(t, got)
}

// A peer whose per-instrument loss query failed counted nothing. Its zero is an absence, which is
// the same false-clean 'not counted' exists to keep off the badge.
func TestEdgeMulticastGapConfined_APeerThatCountedNothingIsNotAWitness(t *testing.T) {
	peer := seqInst("aws-dub-mn-recorder1", "mbp_edge_kalshi_perps", 101, "ok", true)
	peer.LossUnavailable = true
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "mbp_edge_kalshi_perps", 101, "gapped", true),
		peer,
	})
	assert.Nil(t, got)
}

// A clean reading on another market says nothing about this one. A sports node records dozens of
// capture sources and they fail independently.
func TestEdgeMulticastGapConfined_ADifferentCaptureSourceIsNotAWitness(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_sports_nfl", 15, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_sports_mlb", 15, "ok", true),
	})
	assert.Nil(t, got)
}

// The two paths of a feed publish under different channel ids, so dropping the channel from the key
// would let one path's clean recording exonerate the other's loss — the opposite of the finding the
// per-line verdict exists to show. Perps runs ch1 against ch101.
func TestEdgeMulticastGapConfined_TheOtherPathIsNotAWitness(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 101, "ok", true),
	})
	assert.Nil(t, got)
}

// The channel id is not what separates the two paths — collapsing them onto a single id is a
// settled upstream change — so the publisher is in the key too. On the group roll-up, where every
// publisher's instances share one slice, the peer path's clean recording must not exonerate this
// one's loss: that pair is the finding the per-line verdict exists to show.
func TestEdgeMulticastGapConfined_ThePeerPathIsNotAWitnessOnOneChannelID(t *testing.T) {
	peer := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	peer.PublisherSourceIP = "148.51.121.70"
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		peer,
	})
	assert.Nil(t, got)
}

// A clean reading is not yet a witness: it has to have been RECORDING when the loss happened. Both
// gap-counting legs aggregate one row per series over the whole window, so volume carries no time
// structure — a peer that joined at minute 6 of 15 holds most of the messages and none of the
// minutes that matter, and would exonerate the path over exactly the part it never saw.
func TestEdgeMulticastGapConfined_AWitnessMustHaveBeenWatching(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.Messages = 56_800
	gapped.PresentMinutes = minutes(0, 15)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(3) + 12, Seconds: 4}}

	latecomer := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	latecomer.Messages = 40_000 // plenty of volume, none of it in minute 3
	latecomer.PresentMinutes = minutes(6, 15)

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, latecomer}))

	// The same vantage, present for the minute the loss is in.
	latecomer.PresentMinutes = minutes(0, 15)
	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, latecomer}))
}

// A recorder that bounced mid-window is the same failure with the hole in the middle, and it is the
// one a volume bound cannot see at all: this peer carries 14 of 15 minutes.
func TestEdgeMulticastGapConfined_AWitnessThatBouncedOverTheLossIsNotOne(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.PresentMinutes = minutes(0, 15)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(8) + 30, Seconds: 2}}

	bounced := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	bounced.PresentMinutes = append(minutes(0, 8), minutes(9, 15)...)

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, bounced}))
}

// An episode that straddles a minute boundary needs the witness in BOTH minutes. Off by one here
// exonerates a path over half of the loss.
func TestEdgeMulticastGapConfined_AnEpisodeSpanningTwoMinutesNeedsBoth(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.PresentMinutes = minutes(0, 15)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(4) + 58, Seconds: 5}}

	peer := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	peer.PresentMinutes = append(minutes(0, 5), minutes(6, 15)...) // everything but minute 5

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))

	peer.PresentMinutes = minutes(0, 15)
	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))
}

// Loss counted from per-instrument holes with no marker written — the false negative this column's
// unit change exists to end — names no minute. The witness then has to cover every minute the
// gapped series was itself recording, because the loss is somewhere inside those.
func TestEdgeMulticastGapConfined_LossWithNoEpisodeNeedsTheWholeSeriesCovered(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "mbp_edge_kalshi_perps", 101, "gapped", true)
	gapped.UpdatesMissing = 958
	gapped.PresentMinutes = minutes(0, 15)

	peer := seqInst("aws-dub-mn-recorder1", "mbp_edge_kalshi_perps", 101, "ok", true)
	peer.PresentMinutes = minutes(1, 15)

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))

	peer.PresentMinutes = minutes(0, 15)
	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))
}

// A series can lose data BOTH ways at once: markers on some minutes, and UpdatesMissing holes the
// producer never marked. The markers then describe only part of the loss, so narrowing the witness
// test to the marked minutes leaves the unmarked half unwitnessed — and that half is the one
// nothing else on this page can localise.
func TestEdgeMulticastGapConfined_MarkedAndUnmarkedLossNeedsTheWholeSeriesCovered(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "mbp_edge_kalshi_perps", 101, "gapped", true)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(12), Seconds: 2}}
	gapped.UpdatesMissing = 958 // holes with no marker, somewhere else in the window
	gapped.PresentMinutes = minutes(0, 15)

	// Present for the marked minute and absent for most of the rest.
	peer := seqInst("aws-dub-mn-recorder1", "mbp_edge_kalshi_perps", 101, "ok", true)
	peer.PresentMinutes = minutes(11, 15)

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))

	peer.PresentMinutes = minutes(0, 15)
	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))
}

// The counterpart: markers and no unmarked loss, so they do account for all of it and the witness
// is checked against those minutes alone. Without this the narrow case is untested in both
// directions and the rule above reads as "always require everything".
func TestEdgeMulticastGapConfined_MarkedLossAloneChecksOnlyItsMinutes(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(12), Seconds: 2}}
	gapped.PresentMinutes = minutes(0, 15)

	peer := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	peer.PresentMinutes = minutes(11, 15)

	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))
}

// A payload written before PresentMinutes existed carries none, and an absence must cost the claim
// rather than grant it. This is the state every environment is in until the refresher next runs.
func TestEdgeMulticastGapConfined_APayloadWithNoMinutesConfinesNothing(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(3), Seconds: 1}}
	gapped.PresentMinutes = nil
	peer := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	peer.PresentMinutes = nil

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))
}

// One qualifying witness is enough, and which vantage qualifies depends on the series being
// witnessed for — so every clean peer is considered, not the best by some measure.
func TestEdgeMulticastGapConfined_OneQualifyingWitnessIsEnough(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.PresentMinutes = minutes(0, 15)
	gapped.GapEpisodes = []handlers.KalshiL2GapEpisode{{Start: minuteAt(2), Seconds: 1}}

	// The busier of the two peers is the one that was not there for minute 2.
	busyButAbsent := seqInst("aws-nrt-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	busyButAbsent.Messages = 56_871
	busyButAbsent.PresentMinutes = minutes(3, 15)

	thinButPresent := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	thinButPresent.Messages = 4_000
	thinButPresent.PresentMinutes = minutes(0, 4)

	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, busyButAbsent, thinButPresent}))
}

// An unnamed series is not a bucket of its own: every one of them lands in the same bucket, so two
// unrelated markets at one node would stand in as each other's witness. The recorded-gap leg
// produces such a series for a channel instance the capture recorded nothing for.
func TestEdgeMulticastGapConfined_AnUnnamedSeriesIsNeitherConfinedNorAWitness(t *testing.T) {
	gapped := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "", 1, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "", 1, "ok", true),
	})
	assert.Nil(t, gapped, "an unnamed gapped series has no comparable peer")

	witness := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "", 1, "ok", true),
	})
	assert.Nil(t, witness, "an unnamed clean series vouches for no named one")
}

// The claim is about ALL of the line's loss, so one gap no vantage can speak to withdraws it. A
// sports line compares dozens of capture sources and only some of them are recorded twice.
func TestEdgeMulticastGapConfined_OneUnwitnessedGapWithdrawsTheClaim(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_sports_nfl", 15, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_sports_nfl", 15, "ok", true),
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_sports_mlb", 16, "gapped", true),
	})
	assert.Nil(t, got)
}

// A clean line has nothing to confine, and must not name a recorder.
func TestEdgeMulticastGapConfined_NothingGappedNamesNobody(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true),
	})
	assert.Nil(t, got)
}

// A node that gapped on one capture source and vouched for another is named once, for the loss it
// owns. The witness role and the gapped role are per series, not per node.
func TestEdgeMulticastGapConfined_ANodeCanGapHereAndVouchThere(t *testing.T) {
	got := handlers.EdgeMulticastGapConfinedNodesForTest([]handlers.EdgeMulticastChannelInstance{
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_sports_nfl", 15, "gapped", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_sports_nfl", 15, "ok", true),
		seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_sports_mlb", 16, "ok", true),
		seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_sports_mlb", 16, "gapped", true),
	})
	require.Len(t, got, 2)
	assert.Equal(t, []string{"aws-cmh-mn-recorder1", "aws-dub-mn-recorder1"}, got)
}
