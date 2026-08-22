package handlers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The top-of-book leg of the Sequence column. Its whole reason to exist is that TOP rows used to
// read as an em dash, and its whole constraint is that it cannot count gaps — so these tests are
// about both: the series reaching the right publisher line, and the verdict staying honest about
// what was never measured.

const tobSequenceKey = "edge_multicast_tob_sequence:v1"

// seedTOBSequence writes the cached payload the page folds, standing in for the refresher.
func seedTOBSequence(t *testing.T, api *handlers.API, generatedAt time.Time, series ...handlers.EdgeMulticastTOBSeries) {
	t.Helper()
	require.NoError(t, api.WritePageCache(t.Context(), tobSequenceKey, handlers.EdgeMulticastTOBSequenceResponse{
		GeneratedAt:   generatedAt,
		WindowMinutes: 15,
		Series:        series,
	}))
	t.Cleanup(func() {
		_, err := api.PgPool.Exec(context.Background(), `DELETE FROM page_cache WHERE key = $1`, tobSequenceKey)
		require.NoError(t, err)
	})
}

// insertEdgeMulticastTOBGroup adds a top-of-book group, which the shared capture fixture has no
// equivalent of — the plane the column could not previously say anything about.
func insertEdgeMulticastTOBGroup(t *testing.T, api *handlers.API) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-t', now(), now(), generateUUIDv4(), 0, 3, 'group-t', '', 'edge-kalshi-sports-tob', '233.0.0.12', 50000000, 'activated', 0, 0)`))
}

func tobTestAPI(t *testing.T) *handlers.API {
	t.Helper()
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastTOBGroup(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-t")
	insertEdgeMulticastCapturePublisher2(t, api, "group-t")
	return api
}

// A top-of-book series lands on the publisher that emitted it — the same grain the market-by-price
// leg uses — and reports itself as unmeasured for gaps rather than borrowing the other plane's
// stronger claim.
func TestGetEdgeMulticast_TOBSeriesLandsOnItsPublisherAndSaysGapsAreUnmeasured(t *testing.T) {
	api := tobTestAPI(t)
	asOf := time.Now().UTC()
	seedTOBSequence(t, api, asOf,
		handlers.EdgeMulticastTOBSeries{
			Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
			PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
			Messages: 1200, Resets: 2, LastSeen: asOf.Add(-time.Second),
		},
		handlers.EdgeMulticastTOBSeries{
			Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
			PublisherSourceIP: "10.0.0.10", ChannelID: 110, Node: "cmh-rec1",
			Messages: 1300, Resets: 0, LastSeen: asOf.Add(-time.Second),
		},
	)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-tob")
	require.Len(t, g.PublisherLines, 2)
	byIP := map[string]handlers.EdgeMulticastPublisher{}
	for _, l := range g.PublisherLines {
		byIP[l.DZIP] = l
	}

	for _, ip := range []string{"10.0.0.9", "10.0.0.10"} {
		line := byIP[ip]
		require.NotNil(t, line.Sequence, "the series recorded from %s must reach that line", ip)
		require.Len(t, line.Sequence.Instances, 1)
		assert.Equal(t, "ok", line.Sequence.Status, "advancing and not stalled")
		assert.False(t, line.Sequence.Instances[0].GapsMeasured,
			"top-of-book records no gap marker, so this must not claim the count was taken")
		assert.Equal(t, 1, line.Sequence.GapsUnmeasured)
		assert.Zero(t, line.Sequence.Instances[0].GapBooks,
			"zero here is the absence of a reading, which GapsMeasured is what distinguishes")
	}
	assert.EqualValues(t, 2, byIP["10.0.0.9"].Sequence.Instances[0].Resets,
		"resets are a real reading on this plane and survive the fold")

	require.NotNil(t, g.Sequence)
	assert.Equal(t, 2, g.Sequence.Publishers)
	assert.Equal(t, 2, g.Sequence.GapsUnmeasured)
	assert.Zero(t, g.Sequence.Gapped)
	assert.Zero(t, g.Sequence.Unattributed)
}

// The destination address is the primary group key, and it has to work when the capture source
// name resolves to nothing — a renamed source must not silently drop the series.
func TestGetEdgeMulticast_TOBGroupResolvedByMulticastAddress(t *testing.T) {
	api := tobTestAPI(t)
	asOf := time.Now().UTC()
	seedTOBSequence(t, api, asOf, handlers.EdgeMulticastTOBSeries{
		Source: "tob_some_name_nothing_on_this_page_claims", MulticastGroup: "233.0.0.12",
		PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
		Messages: 1200, LastSeen: asOf.Add(-time.Second),
	})

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-tob")
	require.NotNil(t, g.Sequence, "the address claims the group even though the name does not")
	require.Len(t, g.Sequence.Instances, 1)
}

// A series whose recorder stopped writing is stalled, graded against the payload's own clock and
// never against wall clock — the entry is up to a refresher interval old.
func TestGetEdgeMulticast_TOBStalledSeries(t *testing.T) {
	api := tobTestAPI(t)
	asOf := time.Now().UTC()
	seedTOBSequence(t, api, asOf, handlers.EdgeMulticastTOBSeries{
		Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
		PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
		Messages: 1200, LastSeen: asOf.Add(-10 * time.Minute),
	})

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-tob")
	require.NotNil(t, g.Sequence)
	assert.Equal(t, "stalled", g.Sequence.Status)
	assert.Equal(t, 1, g.Sequence.PublishersStalled)
}

// A source address no publisher of the group carries has no line to sit on. It is counted on the
// roll-up rather than dropped: silently discarding a recorded series is the one outcome this
// column must not have.
func TestGetEdgeMulticast_TOBUnattributedSeries(t *testing.T) {
	api := tobTestAPI(t)
	asOf := time.Now().UTC()
	seedTOBSequence(t, api, asOf, handlers.EdgeMulticastTOBSeries{
		Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
		PublisherSourceIP: "10.99.99.99", ChannelID: 110, Node: "cmh-rec1",
		Messages: 1200, LastSeen: asOf.Add(-time.Second),
	})

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-tob")
	require.NotNil(t, g.Sequence)
	assert.Equal(t, 1, g.Sequence.Unattributed)
	assert.Zero(t, g.Sequence.Publishers)
	for _, l := range g.PublisherLines {
		assert.Nil(t, l.Sequence, "no line may claim a series recorded from an address it does not carry")
	}
}

// The two legs are independent: a market-by-price series and a top-of-book series can reach the
// page in the same payload, and one leg missing costs only its own plane. This is the case that
// would break if the fold overwrote the map instead of adding to it.
func TestGetEdgeMulticast_BothSequenceLegsFoldTogether(t *testing.T) {
	api := tobTestAPI(t)
	insertEdgeMulticastCapturePublisher3(t, api, "group-k")
	asOf := time.Now().UTC()

	seedL2Coverage(t, api, asOf, handlers.KalshiL2Lane{
		Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
		PublisherSourceIP: "10.0.0.11", Messages: 900, GapBooks: 3, Resets: 1,
		SnapshotCycles: 1, Seen: true, LastSeen: asOf.Add(-time.Second),
	})
	seedTOBSequence(t, api, asOf, handlers.EdgeMulticastTOBSeries{
		Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
		PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
		Messages: 1200, LastSeen: asOf.Add(-time.Second),
	})

	resp := getEdgeMulticast(t, api)

	mbp := findEdgeMulticastGroup(t, resp, "edge-kalshi-sports-mbp")
	require.NotNil(t, mbp.Sequence)
	assert.Equal(t, "gapped", mbp.Sequence.Status, "the market-by-price leg still counts gaps")
	assert.Zero(t, mbp.Sequence.GapsUnmeasured)

	tob := findEdgeMulticastGroup(t, resp, "edge-kalshi-sports-tob")
	require.NotNil(t, tob.Sequence)
	assert.Equal(t, "ok", tob.Sequence.Status)
	assert.Equal(t, 1, tob.Sequence.GapsUnmeasured)

	require.NotNil(t, resp.SequenceAsOf, "one column, one as-of, taken from the older leg")
}

// insertEdgeMulticastCapturePublisher3 adds a publisher to a third group, so a test can exercise
// both sequence legs at once without the two planes sharing a publisher line.
func insertEdgeMulticastCapturePublisher3(t *testing.T, api *handlers.API, groupPK string) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-kpub3', now(), now(), generateUUIDv4(), 0, 13, 'user-kpub3', 'pubkey-kpub3', 'activated',
		        'multicast', '10.0.0.11', '10.0.0.11', 'dev-ams1', 511, ?, '[]')`,
		fmt.Sprintf(`["%s"]`, groupPK)))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES (now() - INTERVAL 1 MINUTE, 'dev-ams1', 'Tunnel511', 511, 'user-kpub3', 3000000, 0, now())`))
}

// The verdict is per publisher, and the group row carries none. Two publishers of one group, one
// gapped and one intact, is the case a group badge cannot describe: it reads the same as a feed
// where both are fine but one path is merely quiet.
func TestGetEdgeMulticast_HealthIsPerPublisher(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	insertEdgeMulticastCapturePublisher2(t, api, "group-k")

	asOf := time.Now().UTC()
	seedL2Coverage(t, api, asOf,
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", Messages: 1200, GapBooks: 5, Resets: 5,
			SnapshotCycles: 4, Seen: true, LastSeen: asOf.Add(-time.Second),
		},
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.10", Messages: 1300, Seen: true,
			LastSeen: asOf.Add(-time.Second),
		},
	)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.Len(t, g.PublisherLines, 2)
	byIP := map[string]handlers.EdgeMulticastPublisher{}
	for _, l := range g.PublisherLines {
		byIP[l.DZIP] = l
	}

	assert.Equal(t, "gapped", byIP["10.0.0.9"].Health,
		"the publisher whose series lost data carries the verdict")
	assert.Equal(t, "healthy", byIP["10.0.0.10"].Health,
		"and its peer is not painted by it — that folding is what moving the badge off the group fixes")
}

// A series whose gaps were never counted cannot produce a clean bill of health. Top-of-book
// arrives with a zero gap count that is an absence, not a reading, and a verdict that treated it
// as a reading would be the page asserting something nothing measured.
func TestGetEdgeMulticast_UnmeasuredSeriesDoesNotClaimHealthy(t *testing.T) {
	api := tobTestAPI(t)
	asOf := time.Now().UTC()
	seedTOBSequence(t, api, asOf, handlers.EdgeMulticastTOBSeries{
		Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
		PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
		Messages: 1200, LastSeen: asOf.Add(-10 * time.Minute),
	})

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-tob")
	byIP := map[string]handlers.EdgeMulticastPublisher{}
	for _, l := range g.PublisherLines {
		byIP[l.DZIP] = l
	}
	assert.Equal(t, "stalled", byIP["10.0.0.9"].Health,
		"staleness is gradable without a gap marker, and it is the fault this plane can still name")
}
