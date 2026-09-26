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

func seqInst(node, source string, channel uint8, status string, measured bool) handlers.EdgeMulticastChannelInstance {
	return handlers.EdgeMulticastChannelInstance{
		PublisherSourceIP: "148.51.121.69",
		CaptureSource:     source,
		ChannelID:         channel,
		Node:              node,
		GapsMeasured:      measured,
		Status:            status,
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

// The channel id is not what separates the two paths — collapsing the arms onto a single id is a
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

// A clean reading is not yet a witness. `ok` asks only for a fresh LastSeen, so a recorder that
// joined the group near the end of the window carries no marker for the part it missed — and would
// otherwise exonerate the path over exactly the minutes it never saw.
func TestEdgeMulticastGapConfined_AWitnessMustHaveCoveredTheWindow(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.Messages = 56_800
	latecomer := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	latecomer.Messages = 900

	assert.Nil(t, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, latecomer}))

	// The same vantage once it has the window behind it. A witness normally records MORE than the
	// recorder that lost data, so the floor is loose enough never to fire on that pair.
	latecomer.Messages = 56_871
	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, latecomer}))
}

// The floor is coverage of the window and not volume, so it must not fire on the difference between
// two healthy recorders of one feed — measured on mainnet at well under a percent.
func TestEdgeMulticastGapConfined_TheFloorDoesNotFireOnHealthySpread(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.Messages = 56_806
	peer := seqInst("aws-was-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	peer.Messages = 54_754

	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, peer}))
}

// One witness that covered the window is enough, and a second thinner one does not take that away.
func TestEdgeMulticastGapConfined_TheBusiestWitnessIsTheOneThatCounts(t *testing.T) {
	gapped := seqInst("aws-cmh-mn-recorder1", "tob_edge_kalshi_perps", 1, "gapped", true)
	gapped.Messages = 56_800
	thin := seqInst("aws-nrt-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	thin.Messages = 120
	full := seqInst("aws-dub-mn-recorder1", "tob_edge_kalshi_perps", 1, "ok", true)
	full.Messages = 56_871

	assert.Equal(t, []string{"aws-cmh-mn-recorder1"}, handlers.EdgeMulticastGapConfinedNodesForTest(
		[]handlers.EdgeMulticastChannelInstance{gapped, thin, full}))
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
