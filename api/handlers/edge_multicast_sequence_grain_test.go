package handlers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The channel-instance grain, from both ends: the coverage query has to keep two publishers of one
// channel apart, and the page has to put each publisher's verdict on its own line.

// Two arms of one channel, told apart only by their source address. This is the shape the perps
// arms take once their channel ids collapse onto a single id, and it is the case the old key
// (capture source, channel, node) folded into one row whose gap count belonged to neither
// publisher.
func TestKalshiL2Coverage_SeparatesArmsOnOneChannel(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// Same source, same channel, same instrument, two addresses — and only one of them gapped.
	insertLevelFrom(t, api, "148.51.120.6", "mbp_edge_kalshi_perps", 1, 7, "level_update", 5, "gap", 10)
	insertLevelFrom(t, api, "148.51.121.69", "mbp_edge_kalshi_perps", 1, 7, "level_update", 5, "ready", 10)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)

	arms := []handlers.KalshiL2Lane{}
	for _, l := range resp.Lanes {
		if l.Source == "mbp_edge_kalshi_perps" && l.Seen {
			arms = append(arms, l)
		}
	}
	require.Len(t, arms, 2, "one channel, two source addresses: two channel instances")

	// Sorted by channel id then source address, so the arms of one lane sit together.
	assert.Equal(t, "148.51.120.6", arms[0].PublisherSourceIP)
	assert.Equal(t, "148.51.121.69", arms[1].PublisherSourceIP)
	for _, l := range arms {
		assert.EqualValues(t, 1, l.ChannelID)
	}
	assert.EqualValues(t, 1, arms[0].GapBooks, "the gap belongs to the arm that recorded it")
	assert.EqualValues(t, 0, arms[1].GapBooks, "and not to its peer, which was intact")
}

// The verdict follows the source address, in both directions. One direction alone would pass on an
// implementation that always blamed the same line — the second case is what makes the first
// non-vacuous.
func TestGetEdgeMulticast_SequenceGapFollowsTheSourceAddress(t *testing.T) {
	// The two publishers of group-k, by tunnel address: insertEdgeMulticastCapturePublisher and
	// its sibling. Both are above the traffic floor, so nothing else on the row is faulted.
	for _, tc := range []struct{ gapped, intact string }{
		{gapped: "10.0.0.9", intact: "10.0.0.10"},
		{gapped: "10.0.0.10", intact: "10.0.0.9"},
	} {
		t.Run(fmt.Sprintf("gapped=%s", tc.gapped), func(t *testing.T) {
			api := newEdgeMulticastTestAPI(t)
			insertMulticastTestData(t, api)
			insertEdgeMulticastCaptureGroups(t, api)
			insertEdgeMulticastCapturePublisher(t, api, "group-k")
			insertEdgeMulticastCapturePublisher2(t, api, "group-k")

			asOf := time.Now().UTC()
			seedL2Coverage(t, api, asOf,
				handlers.KalshiL2Lane{
					Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
					PublisherSourceIP: tc.gapped, Messages: 1200, GapBooks: 5, Resets: 5,
					SnapshotCycles: 4, Seen: true, LastSeen: asOf.Add(-time.Second),
				},
				handlers.KalshiL2Lane{
					Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
					PublisherSourceIP: tc.intact, Messages: 1300, Seen: true,
					LastSeen: asOf.Add(-time.Second),
				},
			)

			g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
			require.Len(t, g.PublisherLines, 2)
			byIP := map[string]handlers.EdgeMulticastPublisher{}
			for _, l := range g.PublisherLines {
				byIP[l.DZIP] = l
			}

			broken := byIP[tc.gapped]
			require.NotNil(t, broken.Sequence, "the series recorded from %s must land on that publisher's line", tc.gapped)
			assert.Equal(t, "gapped", broken.Sequence.Status)
			require.Len(t, broken.Sequence.Instances, 1, "one series per publisher here, not the group's two folded together")
			assert.EqualValues(t, 5, broken.Sequence.Instances[0].GapBooks)

			intact := byIP[tc.intact]
			require.NotNil(t, intact.Sequence, "the healthy publisher has a series too, and it is not the gapped one")
			assert.Equal(t, "ok", intact.Sequence.Status,
				"the gap belongs to %s: folding the two series onto one key reports it on both", tc.gapped)
			assert.Zero(t, intact.Sequence.Gapped)

			require.NotNil(t, g.Sequence)
			assert.Equal(t, 2, g.Sequence.Publishers)
			assert.Equal(t, 1, g.Sequence.PublishersGapped, "one of the two publishers, not the group")
			assert.Zero(t, g.Sequence.Unattributed)
		})
	}
}
