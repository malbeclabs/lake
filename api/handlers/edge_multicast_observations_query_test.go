package handlers_test

import (
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The observations query itself, against a real table.
//
// Every other test on this leg seeds the cached payload directly, which exercises the folding and
// none of the read. That left the column names, both raw_meta keys and the reset arithmetic
// unverified — and a break there is invisible from the page: the refresher WARNs, showObservations
// drops Msg/s and Peer, and the row reads exactly like a feed with no recorder behind it while
// `behind` and `unrecorded` quietly stop being computed. The market-by-price leg has a live-query
// fixture for the same reason.

// insertBBOObservation records one top-of-book change as `node` saw it, `agoSecs` ago, with the
// addressing the query reads out of raw_meta.
func insertBBOObservation(t *testing.T, api *handlers.API, node, source, multicastGroup, publisherSourceIP string, channelID uint8, sequence uint64, resetCount uint8, agoSecs int) {
	t.Helper()
	insertBBOObservationAtReset(t, api, node, "cmh", source, multicastGroup, publisherSourceIP, channelID, sequence, resetCount, agoSecs)
}

// insertBBOObservationAt is insertBBOObservation with the recording node's own metro spelled out.
// The metro is a property of the node, and a test that cannot vary it cannot catch a query that
// labels every node with one arbitrary one.
func insertBBOObservationAt(t *testing.T, api *handlers.API, node, location, source, multicastGroup, publisherSourceIP string, channelID uint8, sequence uint64, agoSecs int) {
	t.Helper()
	insertBBOObservationAtReset(t, api, node, location, source, multicastGroup, publisherSourceIP, channelID, sequence, 0, agoSecs)
}

func insertBBOObservationAtReset(t *testing.T, api *handlers.API, node, location, source, multicastGroup, publisherSourceIP string, channelID uint8, sequence uint64, resetCount uint8, agoSecs int) {
	t.Helper()
	db := "`" + api.FeedsDB + "`"
	rawMeta := fmt.Sprintf(
		`{"publisher_source_ip":"%s","multicast_group":"%s","port":%d}`,
		publisherSourceIP, multicastGroup, 20000+int(channelID))
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %s.kalshi_bbo_observations
		(measurement_node_id, location_code, source, symbol, source_ts_ms, recv_ts_ns, source_id,
		 channel_id, sequence, reset_count, raw_meta)
		VALUES ('%s', '%s', '%s', 'KXNFLGAME', 0,
		        toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalSecond(%d))), 3,
		        %d, %d, %d, '%s')
	`, db, node, location, source, agoSecs, channelID, sequence, resetCount, rawMeta)))
}

func seriesByKey(series []handlers.EdgeMulticastObservationSeries) map[string]handlers.EdgeMulticastObservationSeries {
	out := map[string]handlers.EdgeMulticastObservationSeries{}
	for _, s := range series {
		out[fmt.Sprintf("%s|%s|%d|%s", s.Source, s.PublisherSourceIP, s.ChannelID, s.Node)] = s
	}
	return out
}

// An absent table is the local-dev and never-proxied state. It costs the columns, never the refresh.
func TestFetchEdgeMulticastObservations_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	// Deliberately no table.

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.Series)
	assert.Equal(t, 15, resp.WindowMinutes)
}

// The grain: one series per (source, multicast group, publisher address, channel, recording node).
// Two paths of one feed at one node are two rows, which is what path parity compares; two nodes
// seeing one path are two rows, which is what keeps a recorder missing the feed visible.
func TestFetchEdgeMulticastObservations_Grain(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiObservationsTable(t, api)

	const group = "233.84.178.3"
	const pathA = "148.51.121.69"
	const pathB = "148.51.120.6"

	// Path A at cmh: three messages, reset_count walking 4 -> 6.
	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", group, pathA, 1, 100, 4, 30)
	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", group, pathA, 1, 101, 5, 20)
	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", group, pathA, 1, 102, 6, 10)
	// Its redundant peer at the same node, on the +100 channel: two messages, no reset.
	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", group, pathB, 101, 700, 2, 25)
	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", group, pathB, 101, 701, 2, 15)
	// Path A again, at a second recorder.
	insertBBOObservation(t, api, "dub-rec1", "tob_edge_kalshi_perps", group, pathA, 1, 100, 4, 22)

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 3, "two paths at one node plus one of them at a second node")

	by := seriesByKey(resp.Series)

	a := by["tob_edge_kalshi_perps|"+pathA+"|1|cmh-rec1"]
	assert.EqualValues(t, 3, a.Messages)
	assert.EqualValues(t, 2, a.Resets, "resets are how far reset_count advanced, not its value")
	assert.Equal(t, group, a.MulticastGroup, "the destination address comes out of raw_meta")
	assert.Equal(t, "cmh", a.LocationCode)
	assert.False(t, a.LastSeen.IsZero(), "the newest recv_ts_ns is what the staleness grade reads")

	b := by["tob_edge_kalshi_perps|"+pathB+"|101|cmh-rec1"]
	assert.EqualValues(t, 2, b.Messages)
	assert.EqualValues(t, 0, b.Resets)

	second := by["tob_edge_kalshi_perps|"+pathA+"|1|dub-rec1"]
	assert.EqualValues(t, 1, second.Messages)
}

// Both planes are read — top-of-book becomes Sequence series and market-by-price feeds parity only
// — and nothing else is. A capture source outside the two prefixes is not an Edge feed.
func TestFetchEdgeMulticastObservations_PlanePrefixes(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiObservationsTable(t, api)

	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", "233.84.178.3", "148.51.121.69", 1, 1, 0, 10)
	insertBBOObservation(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", "233.84.178.4", "148.51.121.69", 1, 1, 0, 10)
	insertBBOObservation(t, api, "cmh-rec1", "kalshi_public_api", "", "", 0, 1, 0, 10)

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)

	sources := map[string]bool{}
	for _, s := range resp.Series {
		sources[s.Source] = true
	}
	assert.True(t, sources["tob_edge_kalshi_perps"])
	assert.True(t, sources["mbp_edge_kalshi_perps"])
	assert.False(t, sources["kalshi_public_api"], "the venue's own feed is not an Edge publisher")
}

// Outside the window there is no series at all, rather than a zeroed one: an empty result is the
// absence of a reading, and a zero would be a claim that nothing arrived.
func TestFetchEdgeMulticastObservations_WindowBounded(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiObservationsTable(t, api)

	insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", "233.84.178.3", "148.51.121.69", 1, 1, 0, 16*60)

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)
	assert.Empty(t, resp.Series)
}

// Every recorder of a path gets a row, including one that recorded everything its peers did.
//
// This is the case the obvious query drops. Emitting only nodes that lost something leaves the
// clean line out, and the clean line IS the comparison: "was lost 267" means nothing without
// "cmh lost 0" beside it, and a window where one node alone lost anything would render no
// comparison at all.
func TestEdgeMulticastRecorderLossQuery_CleanRecorderStillGetsARow(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiObservationsTable(t, api)

	const group, pub = "233.84.178.3", "148.51.121.69"
	// cmh records all three. was misses the middle one.
	for _, seq := range []uint64{100, 101, 102} {
		insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_perps", group, pub, 1, seq, 0, 30)
	}
	insertBBOObservation(t, api, "was-rec1", "tob_edge_kalshi_perps", group, pub, 1, 100, 0, 30)
	insertBBOObservation(t, api, "was-rec1", "tob_edge_kalshi_perps", group, pub, 1, 102, 0, 30)

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)

	byNode := map[string]handlers.EdgeMulticastRecorderLossSeries{}
	for _, s := range resp.RecorderLoss {
		byNode[s.Node] = s
	}
	require.Len(t, byNode, 2, "both recorders, not only the one that lost")

	assert.EqualValues(t, 1, byNode["was-rec1"].Missing, "sequence 101")
	assert.EqualValues(t, 3, byNode["was-rec1"].ReferenceMessages, "the union of what the two recorded")
	assert.Len(t, byNode["was-rec1"].Episodes, 1)

	assert.EqualValues(t, 0, byNode["cmh-rec1"].Missing, "it recorded everything")
	assert.Empty(t, byNode["cmh-rec1"].Episodes)
	assert.EqualValues(t, 3, byNode["cmh-rec1"].ReferenceMessages)
}

// Each row carries its OWN node's metro. Resolving it inside the per-sequence group instead makes
// it an any() over a group that spans every node, which labels all three lines with one arbitrary
// metro — cmh/cmh/cmh — and the comparison the strip exists for cannot be read at all.
func TestEdgeMulticastRecorderLossQuery_EachRowCarriesItsOwnMetro(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiObservationsTable(t, api)

	const group, pub = "233.84.178.3", "148.51.121.69"
	insertBBOObservationAt(t, api, "cmh-rec1", "cmh", "tob_edge_kalshi_perps", group, pub, 1, 100, 30)
	insertBBOObservationAt(t, api, "cmh-rec1", "cmh", "tob_edge_kalshi_perps", group, pub, 1, 101, 30)
	insertBBOObservationAt(t, api, "dub-rec1", "dub", "tob_edge_kalshi_perps", group, pub, 1, 100, 30)

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)

	byNode := map[string]string{}
	for _, s := range resp.RecorderLoss {
		byNode[s.Node] = s.LocationCode
	}
	require.Len(t, byNode, 2)
	assert.Equal(t, "cmh", byNode["cmh-rec1"])
	assert.Equal(t, "dub", byNode["dub-rec1"], "not the other node's metro")
}

// A path with one recorder has no peer to be measured against, so it produces nothing at all —
// every hole in its numbering is the top-of-book plane's own legitimate hole, not a loss.
func TestEdgeMulticastRecorderLossQuery_LoneRecorderProducesNothing(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiObservationsTable(t, api)

	for _, seq := range []uint64{100, 105, 110} {
		insertBBOObservation(t, api, "cmh-rec1", "tob_edge_kalshi_sports_nfl", "233.84.178.17", "148.51.120.152", 2, seq, 0, 30)
	}

	resp, err := api.FetchEdgeMulticastObservations(t.Context())
	require.NoError(t, err)
	assert.Empty(t, resp.RecorderLoss, "no peer, no reference, no claim")
}
