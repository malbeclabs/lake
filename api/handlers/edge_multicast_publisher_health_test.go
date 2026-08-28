package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
)

// The per-publisher verdict and its ranking. Nothing tested this function, and the ordering is the
// page's whole contract: the badge moved off the group precisely so that one dead path among live
// ones could be named, which only works if the worst thing about a line is what the line says.

func pubLine(status string, seq *handlers.EdgeMulticastSequenceHealth, parity *handlers.EdgeMulticastPathParity) handlers.EdgeMulticastPublisher {
	return handlers.EdgeMulticastPublisher{Status: status, Sequence: seq, PathParity: parity}
}

func seqHealth(status string, gapped, stalled int, instances int) *handlers.EdgeMulticastSequenceHealth {
	h := &handlers.EdgeMulticastSequenceHealth{Status: status, Gapped: gapped, Stalled: stalled}
	for range instances {
		h.Instances = append(h.Instances, handlers.EdgeMulticastChannelInstance{
			Messages: 10, LastSeen: time.Now().UTC(), Status: status,
		})
	}
	return h
}

// The counter plane's two faults outrank everything: a publisher moving no bytes, and one moving
// too few, are worse than anything a recorder can report about what did arrive.
func TestEdgeMulticastPublisherHealth_CounterFaultsWin(t *testing.T) {
	gapped := seqHealth("gapped", 1, 0, 1)

	assert.Equal(t, "silent",
		handlers.EdgeMulticastPublisherHealthForTest(pubLine(handlers.EdgeMulticastPubIdleForTest, gapped, nil), true))
	assert.Equal(t, "thin",
		handlers.EdgeMulticastPublisherHealthForTest(pubLine(handlers.EdgeMulticastPubThinForTest, gapped, nil), true))
}

// The one this exists for: a publisher nothing measured on the counter plane, whose series is
// gapping. 'unknown' used to win by position, and Faulted() excludes it, so the collapsed group's
// dot went grey over a feed that was losing data.
func TestEdgeMulticastPublisherHealth_SeriesOutranksAnUnmeasuredCounter(t *testing.T) {
	assert.Equal(t, "gapped", handlers.EdgeMulticastPublisherHealthForTest(
		pubLine(handlers.EdgeMulticastPubUnknownForTest, seqHealth("gapped", 1, 0, 1), nil), true))
	assert.Equal(t, "stalled", handlers.EdgeMulticastPublisherHealthForTest(
		pubLine(handlers.EdgeMulticastPubUnknownForTest, seqHealth("stalled", 0, 1, 1), nil), true))
	assert.Equal(t, "behind", handlers.EdgeMulticastPublisherHealthForTest(
		pubLine(handlers.EdgeMulticastPubUnknownForTest, nil,
			&handlers.EdgeMulticastPathParity{Compared: 2, Behind: 1, WorstRatio: 0.4}), true))
}

// With nothing to say on either plane, 'unknown' is still the answer — a monitoring gap, and it
// must not be dressed up as either a fault or a clean bill of health.
func TestEdgeMulticastPublisherHealth_UnknownWhenNothingMeasured(t *testing.T) {
	assert.Equal(t, "unknown",
		handlers.EdgeMulticastPublisherHealthForTest(pubLine(handlers.EdgeMulticastPubUnknownForTest, nil, nil), true))

	// And an unmeasured publisher is not 'unrecorded': that word is about a publisher that IS
	// clearing the floor and has no series behind it.
	assert.Equal(t, "unknown", handlers.EdgeMulticastPublisherHealthForTest(
		pubLine(handlers.EdgeMulticastPubUnknownForTest, seqHealth("ok", 0, 0, 0), nil), true))
}

// The share gate, from the verdict's side. The ratio is reported either way; what the count decides
// is whether it is a finding about the path.
func TestEdgeMulticastPublisherHealth_OneFailingPairIsNotBehind(t *testing.T) {
	line := func(behind, compared int) handlers.EdgeMulticastPublisher {
		return pubLine(handlers.EdgeMulticastPubPublishingForTest, seqHealth("ok", 0, 0, 1),
			&handlers.EdgeMulticastPathParity{Compared: compared, Behind: behind, WorstRatio: 0.96})
	}
	assert.Equal(t, "healthy", handlers.EdgeMulticastPublisherHealthForTest(line(1, 29), true),
		"one market out of twenty-nine is an outlier, not a path finding")
	assert.Equal(t, "behind", handlers.EdgeMulticastPublisherHealthForTest(line(12, 29), true),
		"a deficit across the feed is")
	assert.Equal(t, "behind", handlers.EdgeMulticastPublisherHealthForTest(line(1, 1), true),
		"and a feed with one comparison still fires on it")
}

// A path with no peer has no parity verdict. Zero of zero compared must not read as passing.
func TestEdgeMulticastPublisherHealth_ParityNeedsAPeer(t *testing.T) {
	assert.Equal(t, "healthy", handlers.EdgeMulticastPublisherHealthForTest(
		pubLine(handlers.EdgeMulticastPubPublishingForTest, seqHealth("ok", 0, 0, 1),
			&handlers.EdgeMulticastPathParity{Compared: 0, Behind: 0, WorstRatio: 1}), true))
}

// 'unrecorded' only where the group has series to be missing from. On a group with none — the
// shreds groups run Turbine and record no wire protocol at all — clearing the floor is the whole
// truth.
func TestEdgeMulticastPublisherHealth_Unrecorded(t *testing.T) {
	line := pubLine(handlers.EdgeMulticastPubPublishingForTest, nil, nil)
	assert.Equal(t, "unrecorded", handlers.EdgeMulticastPublisherHealthForTest(line, true))
	assert.Equal(t, "healthy", handlers.EdgeMulticastPublisherHealthForTest(line, false))
}

// A shared tunnel counter cannot attest that THIS group is being fed, and a group with no
// subscriber has no application plane that ever could. Both together are 'unknown' — nothing
// measured this publisher here — and not 'healthy'.
//
// Measured on mainnet: edge-kalshi-elections-tob read 2/2 publishing and both lines healthy while
// its publishers sent that plane nothing, the whole ~18.6 Mbps belonging to the mbp group on the
// same two tunnels.
func TestEdgeMulticastPublisherHealth_SharedCounterOnAnUnsubscribedGroupIsNotHealthy(t *testing.T) {
	line := handlers.EdgeMulticastPublisher{
		Status:     handlers.EdgeMulticastPubPublishingForTest,
		MultiGroup: true,
	}
	assert.Equal(t, "unknown",
		handlers.EdgeMulticastPublisherHealthUnsubscribedForTest(line, false),
		"the counter measured a tunnel this group shares, and nothing can measure the group")

	// Both conditions are required, and each on its own leaves 'healthy' standing.
	solo := line
	solo.MultiGroup = false
	assert.Equal(t, "healthy", handlers.EdgeMulticastPublisherHealthUnsubscribedForTest(solo, false),
		"a publisher serving only this group has attributable bytes")
	assert.Equal(t, "healthy", handlers.EdgeMulticastPublisherHealthForTest(line, false),
		"a group with subscribers keeps its counter-only healthy — what the shreds groups rely on")
}

// Evidence beats the guard: a recorded message rate is a per-group measurement, so a line carrying
// one is measured no matter what the tunnel counter can or cannot attribute.
func TestEdgeMulticastPublisherHealth_RecordedRateOutweighsTheSharedCounter(t *testing.T) {
	rate := 42.0
	line := handlers.EdgeMulticastPublisher{
		Status:     handlers.EdgeMulticastPubPublishingForTest,
		MultiGroup: true,
		MsgPerSec:  &rate,
	}
	assert.Equal(t, "healthy",
		handlers.EdgeMulticastPublisherHealthUnsubscribedForTest(line, false))
}
