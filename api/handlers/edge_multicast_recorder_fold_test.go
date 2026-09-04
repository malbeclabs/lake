package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorder-rows leg of the Sequence column, folded onto whatever the other two legs produced.
//
// Every rule pinned here is one this plane exists for. The magnitude has to be the residue after
// the recorder's own drops are subtracted, or the leg is a third copy of a number the page already
// has. Absence of rows has to stay absence, or a recorder that is down reads as a feed that is
// fine. And the capture mode has to gate the arithmetic, or a fleet on AF_PACKET reports its own
// ring's drops as the publisher's.

const (
	recorderFoldGroupPK = "group-perps"
	recorderFoldGroupIP = "233.84.178.3"
	recorderFoldPathA   = "148.51.121.69"
	recorderFoldNode    = "aws-cmh"
)

// recorderFoldAsOf is the payload clock every case here is graded against, and recorderFoldFresh a
// last-seen inside the staleness bound so that nothing below trips the time verdict by accident.
var (
	recorderFoldAsOf  = time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	recorderFoldFresh = recorderFoldAsOf.Add(-30 * time.Second)
)

// recorderFoldSources is the one-group catalog the fold resolves a destination address against.
func recorderFoldSources() edgeMulticastCaptureSourceMap {
	return newEdgeMulticastCaptureSourceMap([]MulticastDeliveryGroup{{
		PK:          recorderFoldGroupPK,
		Code:        "edge-kalshi-perps-mbp",
		MulticastIP: recorderFoldGroupIP,
	}})
}

// recorderSeries is one recorder reading with the fields every case shares filled in. A case names
// only what it is about.
func recorderSeries(mut func(*EdgeMulticastRecorderSequenceSeries)) EdgeMulticastRecorderSequenceSeries {
	s := EdgeMulticastRecorderSequenceSeries{
		MulticastGroup:    recorderFoldGroupIP,
		PublisherSourceIP: recorderFoldPathA,
		ChannelID:         1,
		Feed:              "kalshi-perps-mbp",
		Node:              recorderFoldNode,
		LocationCode:      "cmh",
		Env:               "mainnet",
		DropScope:         edgeMulticastDropScopePortRole,
		Datagrams:         100_000,
		LastSeen:          recorderFoldFresh,
	}
	if mut != nil {
		mut(&s)
	}
	return s
}

// foldRecorder runs the fold over `existing` and hands back the group's instances.
func foldRecorder(t *testing.T, existing []EdgeMulticastChannelInstance, series ...EdgeMulticastRecorderSequenceSeries) []EdgeMulticastChannelInstance {
	t.Helper()
	out := map[string]*EdgeMulticastSequenceHealth{}
	if existing != nil {
		out[recorderFoldGroupPK] = &EdgeMulticastSequenceHealth{Instances: existing}
	}
	applyEdgeMulticastRecorderSequence(EdgeMulticastRecorderSequenceResponse{
		GeneratedAt:   recorderFoldAsOf,
		WindowMinutes: edgeMulticastRecorderSequenceWindowMinutes,
		Series:        series,
	}, recorderFoldSources(), out)
	if out[recorderFoldGroupPK] == nil {
		return nil
	}
	return out[recorderFoldGroupPK].Instances
}

// levelGrainInstance is what foldKalshiL2Coverage leaves behind for one channel instance: a loss
// count with nobody's name on it, plus the marker and the timeline only that plane has.
func levelGrainInstance(missing uint64, gapBooks uint64, status string) EdgeMulticastChannelInstance {
	return EdgeMulticastChannelInstance{
		PublisherSourceIP: recorderFoldPathA,
		CaptureSource:     "mbp_edge_kalshi_perps",
		ChannelID:         1,
		Node:              recorderFoldNode,
		LocationCode:      "cmh",
		Messages:          900_000,
		GapBooks:          gapBooks,
		GapEpisodes:       []KalshiL2GapEpisode{{Start: 1000, Seconds: 4}},
		UpdatesReceived:   500_000,
		UpdatesMissing:    missing,
		SeqGapEvents:      12,
		LastSeen:          recorderFoldFresh,
		Status:            status,
		GapsMeasured:      true,
	}
}

// The whole reason this leg is worth having: the number it reports is what was missing LESS what
// the recorder admits losing itself. Reporting missing_count would be reporting the recorder's own
// capture drops as the publisher's, which neither other leg on this page can even detect.
func TestApplyEdgeMulticastRecorderSequence_UnexplainedIsTheMagnitude(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.Missing = 500
		s.Unexplained = 120
		s.ReferenceSeqs = 100_000
		s.Gaps = 7
		s.MaxRun = 44
	}))
	require.Len(t, insts, 1)

	assert.EqualValues(t, 120, insts[0].UpdatesMissing,
		"unexplained_count, never missing_count: 380 of those datagrams were the recorder's own")
	assert.EqualValues(t, 100_000-120, insts[0].UpdatesReceived,
		"received is derived from the reference so that received + missing IS reference_seqs")
	assert.EqualValues(t, 7, insts[0].SeqGapEvents, "one discontinuity per gap row")
	assert.EqualValues(t, 44, insts[0].MaxGapMessages, "the worst single run, in sequence values")
	assert.Equal(t, edgeMulticastSeqGapped, insts[0].Status)
}

// reference_seqs is the denominator, and it is fed as (reference - missing) so that the consumer's
// own expected = received + missing lands back on reference_seqs exactly. That is what puts this
// plane's rate on the page's ONE minimum-updates threshold instead of needing a second one.
func TestApplyEdgeMulticastRecorderSequence_ReferenceSeqsIsTheDenominator(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.Missing, s.Unexplained = 90, 90
		s.ReferenceSeqs = 45_189
		s.Datagrams = 7 // deliberately not the denominator, and nowhere near it
		s.Gaps = 3
	}))
	require.Len(t, insts, 1)
	assert.EqualValues(t, 45_189, insts[0].UpdatesReceived+insts[0].UpdatesMissing,
		"expected has to reconstruct reference_seqs, or the rate is a share of the wrong thing")
}

// An instance that lost nothing has no gap row, so it has no reference_seqs either — the column
// lives on the gap row. What it received IS what it should have seen, so the archived datagram
// count is the reference, and the clean reading survives. A measured zero is the common state and
// it has to read as a reading rather than as a blank.
func TestApplyEdgeMulticastRecorderSequence_CleanInstanceCountsItsOwnDatagrams(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.Datagrams = 4_476_494
	}))
	require.Len(t, insts, 1)
	assert.EqualValues(t, 4_476_494, insts[0].UpdatesReceived,
		"no gap row means no reference column, and the datagrams archived are the reference")
	assert.EqualValues(t, 0, insts[0].UpdatesMissing)
	assert.Equal(t, edgeMulticastSeqOK, insts[0].Status)
}

// The rule this plane is most dangerous without. An instance the recorder wrote nothing for is an
// instance this leg has no opinion about — the recorder may be down, may never have been asked to
// join that group, may be loading late — and it must fall through to whatever the other legs said.
// Nothing in this system looks more like a healthy feed than a silence nobody claimed.
func TestApplyEdgeMulticastRecorderSequence_SilenceIsNotAClaimOfZeroLoss(t *testing.T) {
	t.Parallel()

	existing := []EdgeMulticastChannelInstance{levelGrainInstance(958, 0, edgeMulticastSeqGapped)}
	// A row for a DIFFERENT channel of the same publisher: the recorder is up and loading, and
	// still says nothing about channel 1.
	insts := foldRecorder(t, existing, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.ChannelID = 101
	}))
	require.Len(t, insts, 2)

	untouched := insts[0]
	require.EqualValues(t, 1, untouched.ChannelID)
	assert.EqualValues(t, 958, untouched.UpdatesMissing, "left exactly as the level-grain leg had it")
	assert.Equal(t, edgeMulticastSeqGapped, untouched.Status)
	assert.True(t, untouched.GapsMeasured)
}

// Where this leg has rows, it wins. Its magnitude is the only one with the observer's own loss
// subtracted out, so a recorder that has taken responsibility for the whole of a loss clears the
// 'gapped' the level-grain leg raised on the same window — that leg counted the same datagrams and
// had no way to know whose they were.
func TestApplyEdgeMulticastRecorderSequence_WinsOverTheLevelGrainCount(t *testing.T) {
	t.Parallel()

	existing := []EdgeMulticastChannelInstance{levelGrainInstance(958, 0, edgeMulticastSeqGapped)}
	insts := foldRecorder(t, existing, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.Missing, s.Unexplained = 958, 0
		s.ReferenceSeqs = 618_000
		s.Gaps = 4
	}))
	require.Len(t, insts, 1, "a reading of an instance already on the page is not a second instance")

	assert.EqualValues(t, 0, insts[0].UpdatesMissing, "all 958 were the recorder's own drops")
	assert.EqualValues(t, 618_000, insts[0].UpdatesReceived)
	assert.Equal(t, edgeMulticastSeqOK, insts[0].Status,
		"the loss half of the verdict is re-decided from the better number")
	assert.True(t, insts[0].GapsMeasured, "the marker reading is not this leg's to overwrite")
	assert.Len(t, insts[0].GapEpisodes, 1, "nor is the timeline: seconds un-anchored are still seconds un-anchored")
}

// A gap MARKER is a recovery state and not a loss count: a book was left un-anchored and could not
// be trusted until a snapshot re-anchored it, and that happened whoever dropped the datagram. This
// leg replaces the magnitude and has nothing to say about the marker.
func TestApplyEdgeMulticastRecorderSequence_DoesNotClearAGapMarker(t *testing.T) {
	t.Parallel()

	existing := []EdgeMulticastChannelInstance{levelGrainInstance(958, 13, edgeMulticastSeqGapped)}
	insts := foldRecorder(t, existing, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.Missing, s.Unexplained = 958, 0
		s.ReferenceSeqs = 618_000
	}))
	require.Len(t, insts, 1)
	assert.EqualValues(t, 0, insts[0].UpdatesMissing)
	assert.Equal(t, edgeMulticastSeqGapped, insts[0].Status,
		"thirteen books were left un-anchored; that is a fault observed and it stands")
	assert.EqualValues(t, 13, insts[0].GapBooks)
}

// At capture-handle scope the ring counts frames dropped BEFORE demultiplexing, so a delta caused
// by market-data frames rides on the next reference-data datagram that gets through. The number
// belongs to the handle and to NO instance, so a handle that admitted anything at all makes every
// instance under it unverifiable — and unverifiable must never be reported as the publisher's.
func TestApplyEdgeMulticastRecorderSequence_CaptureHandleScopeWithholdsTheMagnitude(t *testing.T) {
	t.Parallel()

	existing := []EdgeMulticastChannelInstance{levelGrainInstance(40, 0, edgeMulticastSeqGapped)}
	insts := foldRecorder(t, existing, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.DropScope = edgeMulticastDropScopeCaptureHandle
		s.HandleAdmitted = 40
		s.Missing, s.Unexplained = 40, 40
		s.ReferenceSeqs = 618_000
		s.Gaps = 1
	}))
	require.Len(t, insts, 1)

	assert.EqualValues(t, 0, insts[0].UpdatesMissing,
		"the handle admitted forty; naming them the publisher's is the false finding this gate exists for")
	assert.EqualValues(t, 0, insts[0].UpdatesReceived,
		"and no denominator either: a rate over a withheld numerator is still a claim")
	assert.EqualValues(t, 0, insts[0].SeqGapEvents, "the other leg's counters go with its magnitude")
	assert.Equal(t, edgeMulticastSeqGapped, insts[0].Status,
		"the loss happened; what is withheld is whose it was, not that it was")
}

// The other half of the same gate, and the interesting one: a recorder admitting NOTHING over the
// window turns every gap under it into someone else's with evidence.
func TestApplyEdgeMulticastRecorderSequence_CaptureHandleScopeExoneratesOnZero(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.DropScope = edgeMulticastDropScopeCaptureHandle
		s.HandleAdmitted = 0
		s.Missing, s.Unexplained = 267, 267
		s.ReferenceSeqs = 618_000
		s.Gaps = 5
	}))
	require.Len(t, insts, 1)
	assert.EqualValues(t, 267, insts[0].UpdatesMissing,
		"the handle admitted nothing over the whole window, so the residue is not ours")
	assert.Equal(t, edgeMulticastSeqGapped, insts[0].Status)
}

// A handle that dropped one frame must not withhold the clean bill of health from every instance it
// carries. With nothing missing there is nothing to attribute, so the scope does not decide
// anything and the measured zero stands.
func TestApplyEdgeMulticastRecorderSequence_NothingMissingNeedsNoScope(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.DropScope = edgeMulticastDropScopeCaptureHandle
		s.HandleAdmitted = 1
		s.Datagrams = 88_000
	}))
	require.Len(t, insts, 1)
	assert.EqualValues(t, 88_000, insts[0].UpdatesReceived)
	assert.Equal(t, edgeMulticastSeqOK, insts[0].Status)
}

// A capture mode this handler has never seen is not a mode it may subtract at. Failing towards
// saying less is the only safe direction: the alternative is a publisher finding minted by a string
// nobody has read yet.
func TestApplyEdgeMulticastRecorderSequence_UnknownScopeIsUnverifiable(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.DropScope = "ebpf-ring"
		s.Missing, s.Unexplained = 12, 12
		s.ReferenceSeqs = 90_000
	}))
	require.Len(t, insts, 1)
	assert.EqualValues(t, 0, insts[0].UpdatesMissing)
	assert.Equal(t, edgeMulticastSeqGapped, insts[0].Status)
}

// Two vantages of one channel instance are two observations. A series carries its recording node
// and matches only the instance recorded at that node — merging them is how a recorder that is
// missing the feed disappears behind one that is not.
func TestApplyEdgeMulticastRecorderSequence_RecordingNodeIsNeverFolded(t *testing.T) {
	t.Parallel()

	cmh := levelGrainInstance(0, 0, edgeMulticastSeqOK)
	was := levelGrainInstance(0, 0, edgeMulticastSeqOK)
	was.Node, was.LocationCode = "aws-was", "was"

	insts := foldRecorder(t, []EdgeMulticastChannelInstance{cmh, was},
		recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
			s.Missing, s.Unexplained = 267, 267
			s.ReferenceSeqs = 618_000
		}),
		recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
			s.Node, s.LocationCode = "aws-was", "was"
			s.Datagrams = 618_000
		}))
	require.Len(t, insts, 2, "one reading each, and neither became the other's")

	byNode := map[string]EdgeMulticastChannelInstance{}
	for _, inst := range insts {
		byNode[inst.Node] = inst
	}
	assert.EqualValues(t, 267, byNode["aws-cmh"].UpdatesMissing, "cmh lost 267 on its own branch")
	assert.EqualValues(t, 0, byNode["aws-was"].UpdatesMissing, "was lost nothing, and says so")
	assert.EqualValues(t, 618_000, byNode["aws-was"].UpdatesReceived)
}

// One recorder name may exist in two of the RECORDER's own environments, which the tables keep
// apart in every sort key. Two such series are two observations here too: the second must not
// overwrite the first's target.
func TestApplyEdgeMulticastRecorderSequence_ASecondSeriesDoesNotStealAClaimedInstance(t *testing.T) {
	t.Parallel()

	existing := []EdgeMulticastChannelInstance{levelGrainInstance(0, 0, edgeMulticastSeqOK)}
	insts := foldRecorder(t, existing,
		recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
			s.Missing, s.Unexplained = 11, 11
			s.ReferenceSeqs = 90_000
		}),
		recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
			s.Env = "testnet"
			s.Datagrams = 500
		}))
	require.Len(t, insts, 2, "two readings, one of them on a row of its own")
	assert.EqualValues(t, 11, insts[0].UpdatesMissing, "the first claim is not overwritten by the second")
	assert.EqualValues(t, 500, insts[1].UpdatesReceived)
}

// An instance this leg contributes on its own has no book-level gap marker behind it — the recorder
// reads datagram headers and never folds a book — so it must not lend its empty episode list to the
// all-paths intersection as a path that HELD. GapsUnmeasured saying "no marker here" is true; the
// loss reading travels in the update counters, which are a different counter entirely.
func TestApplyEdgeMulticastRecorderSequence_ContributedInstanceCarriesNoMarkerClaim(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.Missing, s.Unexplained = 3, 3
		s.ReferenceSeqs = 90_000
	}))
	require.Len(t, insts, 1)
	assert.False(t, insts[0].GapsMeasured, "no gap marker on this plane, and none claimed")
	assert.Empty(t, insts[0].GapEpisodes, "a run of sequence numbers is not a run of seconds")
	assert.Empty(t, edgeMulticastAllPathsGapped(insts), "and it never enters the seconds intersection")
}

// A destination address no group on this page carries has no row to sit on, and the recorder's feed
// name is NOT a fallback for it: that is the recorder's own spec name, a different namespace from
// the capture source ids the other legs resolve by, and matching one against the other would
// attribute a series to a group by coincidence of spelling.
func TestApplyEdgeMulticastRecorderSequence_UnknownGroupIsDropped(t *testing.T) {
	t.Parallel()

	insts := foldRecorder(t, nil, recorderSeries(func(s *EdgeMulticastRecorderSequenceSeries) {
		s.MulticastGroup = "233.99.99.99"
		s.Feed = "edge-kalshi-perps-mbp" // spelled like a group code, and still not a group key
	}))
	assert.Empty(t, insts)
}

// The time half of a verdict is not this leg's to re-decide. Staleness is measured against the
// payload's OWN clock, and the instance was graded against a different payload's — re-running it
// here would mix two refresher runs' lag into one verdict. So a series that stopped stays stopped
// even when the recorder's own count of it is clean: "0 lost" over a dead window is the false clean
// this column exists to withhold.
func TestEdgeMulticastRecorderRegrade_LeavesTheTimeHalfAlone(t *testing.T) {
	t.Parallel()

	assert.Equal(t, edgeMulticastSeqStalled,
		edgeMulticastRecorderRegrade(edgeMulticastSeqStalled, 0, 0, false),
		"a clean recorder reading does not revive a series that stopped advancing")
	assert.Equal(t, edgeMulticastSeqGapped,
		edgeMulticastRecorderRegrade(edgeMulticastSeqStalled, 0, 5, false),
		"loss outranks staleness, the same order edgeMulticastSequenceStatus puts them in")
	assert.Equal(t, edgeMulticastSeqOK,
		edgeMulticastRecorderRegrade(edgeMulticastSeqGapped, 0, 0, false),
		"a 'gapped' that came from a count this leg has replaced does not survive")
	assert.Equal(t, edgeMulticastSeqGapped,
		edgeMulticastRecorderRegrade(edgeMulticastSeqGapped, 13, 0, false),
		"but one that came from a marker does")
	assert.Equal(t, edgeMulticastSeqGapped,
		edgeMulticastRecorderRegrade(edgeMulticastSeqOK, 0, 0, true),
		"withheld attribution is loss observed, not silence")
	assert.Equal(t, edgeMulticastSeqOK,
		edgeMulticastRecorderRegrade(edgeMulticastSeqOK, 0, 0, false))
}

// A reference that does not exceed the loss is not a denominator. Using it would report a zero
// received, which every consumer on this page reads as "nothing measured this" — the false absence
// the column keeps refusing.
func TestEdgeMulticastRecorderLossFor_UnusableReferenceFallsBackToDatagrams(t *testing.T) {
	t.Parallel()

	received, missing := edgeMulticastRecorderLossFor(EdgeMulticastRecorderSequenceSeries{
		Unexplained: 50, ReferenceSeqs: 50, Datagrams: 900,
	})
	assert.EqualValues(t, 900, received, "a reference equal to the loss leaves no denominator")
	assert.EqualValues(t, 50, missing)

	received, missing = edgeMulticastRecorderLossFor(EdgeMulticastRecorderSequenceSeries{
		Unexplained: 50, ReferenceSeqs: 1_000, Datagrams: 900,
	})
	assert.EqualValues(t, 950, received, "a usable reference is the denominator, not the datagram count")
	assert.EqualValues(t, 50, missing)
}
