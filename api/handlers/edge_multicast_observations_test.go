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

const observationsKey = "edge_multicast_observations:v1"

// seedObservations writes the cached payload the page folds, standing in for the refresher.
func seedObservations(t *testing.T, api *handlers.API, generatedAt time.Time, series ...handlers.EdgeMulticastObservationSeries) {
	t.Helper()
	require.NoError(t, api.WritePageCache(t.Context(), observationsKey, handlers.EdgeMulticastObservationsResponse{
		GeneratedAt:   generatedAt,
		WindowMinutes: 15,
		Series:        series,
	}))
	t.Cleanup(func() {
		_, err := api.PgPool.Exec(context.Background(), `DELETE FROM page_cache WHERE key = $1`, observationsKey)
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
	seedObservations(t, api, asOf,
		handlers.EdgeMulticastObservationSeries{
			Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
			PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
			Messages: 1200, Resets: 2, LastSeen: asOf.Add(-time.Second),
		},
		handlers.EdgeMulticastObservationSeries{
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
	seedObservations(t, api, asOf, handlers.EdgeMulticastObservationSeries{
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
	seedObservations(t, api, asOf, handlers.EdgeMulticastObservationSeries{
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
	seedObservations(t, api, asOf, handlers.EdgeMulticastObservationSeries{
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
	seedObservations(t, api, asOf, handlers.EdgeMulticastObservationSeries{
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

// The two folded payloads are separate cache entries with separate clocks, and the columns they
// fill have to age against their own. SequenceAsOf is the OLDER of the two sequence legs, so if
// Msg/s and Peer read it they would dim over the market-by-price leg's staleness — a payload they
// do not come from and cannot be made stale by.
func TestGetEdgeMulticast_ObservationsCarryTheirOwnAsOf(t *testing.T) {
	api := tobTestAPI(t)
	insertEdgeMulticastCapturePublisher3(t, api, "group-k")
	fresh := time.Now().UTC()
	lagging := fresh.Add(-30 * time.Minute)

	seedL2Coverage(t, api, lagging, handlers.KalshiL2Lane{
		Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
		PublisherSourceIP: "10.0.0.11", Messages: 900, Seen: true,
		LastSeen: lagging.Add(-time.Second),
	})
	seedObservations(t, api, fresh, handlers.EdgeMulticastObservationSeries{
		Source: "tob_edge_kalshi_sports_nfl", MulticastGroup: "233.0.0.12",
		PublisherSourceIP: "10.0.0.9", ChannelID: 110, Node: "cmh-rec1",
		Messages: 1200, LastSeen: fresh.Add(-time.Second),
	})

	resp := getEdgeMulticast(t, api)

	require.NotNil(t, resp.SequenceAsOf)
	assert.WithinDuration(t, lagging, *resp.SequenceAsOf, time.Second, "the older of the two sequence legs")
	require.NotNil(t, resp.ObservationsAsOf)
	assert.WithinDuration(t, fresh, *resp.ObservationsAsOf, time.Second, "its own clock, not the sequence roll-up's")
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
	seedObservations(t, api, asOf, handlers.EdgeMulticastObservationSeries{
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

// What a COLLAPSED group has instead of a badge. Both publishers clear the floor, so the counter
// plane alone reads clean — and one of them is gapping. The tally has to show it, or a group
// nobody expanded goes green over a broken feed.
func TestGetEdgeMulticast_GroupTallyReflectsSequenceFaults(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	insertEdgeMulticastCapturePublisher2(t, api, "group-k")

	asOf := time.Now().UTC()
	seedL2Coverage(t, api, asOf,
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", Messages: 1200, GapBooks: 5, Seen: true,
			LastSeen: asOf.Add(-time.Second),
		},
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.10", Messages: 1300, Seen: true,
			LastSeen: asOf.Add(-time.Second),
		},
	)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	assert.Zero(t, g.PublishersBelowFloor, "the counter plane on its own has nothing to report here")
	assert.Equal(t, 1, g.PublisherVerdicts.Gapped, "and the tally still surfaces the gapping publisher")
	assert.Equal(t, 1, g.PublisherVerdicts.Healthy)
	assert.Equal(t, 1, g.PublisherVerdicts.Faulted())
	assert.Equal(t, g.Publishers.Total,
		g.PublisherVerdicts.Healthy+g.PublisherVerdicts.Faulted()+g.PublisherVerdicts.Unknown,
		"the tally must account for every publisher")
}

// Path parity: the check that reaches the feeds capture-node parity cannot. These are unit tests
// on the measurement itself — a database adds nothing to a ratio.

// The two groups the parity tests resolve against, by destination address.
var parityGroups = []handlers.EdgeMulticastGroupForTest{
	{PK: "group-t", Code: "edge-kalshi-sports-tob", MulticastIP: "233.0.0.12"},
	{PK: "group-k", Code: "edge-kalshi-sports-mbp", MulticastIP: "233.0.0.10"},
}

// The destination address is what resolves a series to its group, and every parity test uses the
// one group EdgeMulticastPathParityForTest sets up — parity is measured per (group, publisher),
// so a series that resolves to nothing is not compared at all.
func obsSeries(source, node, pubIP string, channel uint8, messages uint64) handlers.EdgeMulticastObservationSeries {
	return handlers.EdgeMulticastObservationSeries{
		Source: source, Node: node, PublisherSourceIP: pubIP, MulticastGroup: "233.0.0.12",
		ChannelID: channel, Messages: messages,
	}
}

// The two paths of a feed publish it on DIFFERENT channel ids — mainnet runs a +100 offset — so a
// key that included the channel would put each path in a group of one and compare nothing. This is
// the test that fails if the channel creeps back into the key.
func TestEdgeMulticastPathParity_ComparesAcrossChannelOffset(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 1000),
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.10", 110, 500),
	})

	require.Contains(t, parity, "group-t|10.0.0.9")
	require.Contains(t, parity, "group-t|10.0.0.10")
	assert.Equal(t, 1, parity["group-t|10.0.0.9"].Compared)
	assert.Zero(t, parity["group-t|10.0.0.9"].Behind, "the leading path is not behind itself")
	assert.Equal(t, 1, parity["group-t|10.0.0.10"].Behind, "half its peer's volume is behind by any floor")
	assert.InDelta(t, 0.5, parity["group-t|10.0.0.10"].WorstRatio, 0.0001)
	assert.Equal(t, "tob_edge_kalshi_sports_nfl", parity["group-t|10.0.0.10"].WorstSource)
}

// sportsPairs builds one capture source per market, both paths at one node, with the market at
// index `odd` carrying a small volume and a twenty-message shortfall on the second path — the shape
// a market that opened a few seconds out of step with its peer leaves just above the volume floor.
func sportsPairs(markets, odd int) []handlers.EdgeMulticastObservationSeries {
	series := []handlers.EdgeMulticastObservationSeries{}
	for i := range markets {
		src := fmt.Sprintf("tob_edge_kalshi_sports_%d", i)
		peer, mine := uint64(20000), uint64(20000)
		if i == odd {
			peer, mine = 600, 580
		}
		series = append(series,
			obsSeries(src, "cmh-rec1", "10.0.0.9", 10, peer),
			obsSeries(src, "cmh-rec1", "10.0.0.10", 110, mine))
	}
	return series
}

// A sports node compares 29-33 capture sources, and the verdict used to be decided by the flakiest
// one of them. The reading stands — the pair really is under the floor — but one market out of
// twenty-nine is an outlier, and calling the path behind over it is the same one-instance
// sensitivity the stalled verdict had.
func TestEdgeMulticastPathParity_OneFailingPairIsNotAFinding(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, sportsPairs(29, 7))

	p := parity["group-t|10.0.0.10"]
	require.NotNil(t, p)
	assert.Equal(t, 29, p.Compared)
	assert.Equal(t, 1, p.Behind, "the pair is under the floor and stays counted")
	assert.False(t, p.Faulted, "one market of twenty-nine does not make the path behind")
	assert.InDelta(t, 0.9667, p.WorstRatio, 0.001, "and the ratio is still reported")
}

// The other end of the same rule: a path with ONE comparison — every perps group — still fires on
// it. The gate must not quietly retire the verdict on the feeds that only ever had one pair.
func TestEdgeMulticastPathParity_LonePairStillFires(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 20000),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 19000),
	})

	p := parity["group-t|10.0.0.10"]
	require.NotNil(t, p)
	assert.Equal(t, 1, p.Compared)
	assert.True(t, p.Faulted, "one of one is the whole feed, not an outlier")
}

// And a real branch deficit is not shy: loss is indiscriminate, so it clears the floor nearly
// everywhere rather than at one market. That is the case the gate has to let through.
func TestEdgeMulticastPathParity_ABroadDeficitStillFires(t *testing.T) {
	series := []handlers.EdgeMulticastObservationSeries{}
	for i := range 29 {
		src := fmt.Sprintf("tob_edge_kalshi_sports_%d", i)
		series = append(series,
			obsSeries(src, "cmh-rec1", "10.0.0.9", 10, 20000),
			obsSeries(src, "cmh-rec1", "10.0.0.10", 110, 19000))
	}
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, series)

	p := parity["group-t|10.0.0.10"]
	require.NotNil(t, p)
	assert.Equal(t, 29, p.Behind)
	assert.True(t, p.Faulted)
}

// A path with no peer at that node has nothing to be measured against, and neither a pass nor a
// fail may be recorded for it.
func TestEdgeMulticastPathParity_LonePathIsNotJudged(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 1000),
	})
	assert.Empty(t, parity, "one path is not a comparison")
}

// A recorder that is behind on everything must cancel out of the ratio instead of reading as a
// fault in both paths. That is what keying the comparison on the node buys.
func TestEdgeMulticastPathParity_SlowRecorderDoesNotFaultBothPaths(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 1000),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 1000),
		// Same feed, a recorder holding half as much of everything.
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.9", 1, 500),
		obsSeries("tob_edge_kalshi_perps", "dub-rec1", "10.0.0.10", 101, 500),
	})
	for _, ip := range []string{"group-t|10.0.0.9", "group-t|10.0.0.10"} {
		assert.Equal(t, 2, parity[ip].Compared, "both vantages are comparisons")
		assert.Zero(t, parity[ip].Behind, "%s is level with its peer at both, whatever the recorder held", ip)
	}
}

// Summed across channels, because one path serves a feed on several of them. Comparing a single
// channel of a multi-channel path against the whole of its peer would report a permanent deficit.
func TestEdgeMulticastPathParity_SumsChannelsPerPath(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 600),
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 11, 400),
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.10", 110, 1000),
	})
	assert.Zero(t, parity["group-t|10.0.0.9"].Behind, "600+400 is level with 1000")
	assert.Zero(t, parity["group-t|10.0.0.10"].Behind)
}

// Every path silent at a vantage is the counter plane's finding, not this check's. A 0/0 ratio
// would report it as perfect parity, which is the one answer it must not give.
func TestEdgeMulticastPathParity_AllSilentIsNotPerfectParity(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 0),
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.10", 110, 0),
	})
	assert.Empty(t, parity)
}

// The floor is tight because redundant paths carry the same feed: mainnet runs 0.9985-1.0000. A
// path just inside it passes, one just outside does not.
func TestEdgeMulticastPathParity_FloorBoundary(t *testing.T) {
	pass := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 10000),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 9850),
	})
	assert.Zero(t, pass["group-t|10.0.0.10"].Behind, "98.5%% of its peer clears the floor")

	fail := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.9", 1, 10000),
		obsSeries("tob_edge_kalshi_perps", "cmh-rec1", "10.0.0.10", 101, 9700),
	})
	assert.Equal(t, 1, fail["group-t|10.0.0.10"].Behind, "97%% does not")
}

// Below a few hundred messages the ratio is noise: one message is a percent or more, where the
// floor leaves two. An off-hours league, or a path that came up inside the window, would otherwise
// read 'behind' — and one failed pair is enough to mark the whole line.
func TestEdgeMulticastPathParity_TrickleIsNotJudged(t *testing.T) {
	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_ncaamb", "cmh-rec1", "10.0.0.9", 15, 5),
		obsSeries("tob_edge_kalshi_sports_ncaamb", "cmh-rec1", "10.0.0.10", 115, 4),
	})
	assert.Empty(t, parity, "4 messages against 5 is not a 20%% deficit, it is no measurement")

	// And the floor is on the BEST path of the pair, so a real deficit at volume still lands.
	judged := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_ncaamb", "cmh-rec1", "10.0.0.9", 15, 600),
		obsSeries("tob_edge_kalshi_sports_ncaamb", "cmh-rec1", "10.0.0.10", 115, 480),
	})
	assert.Equal(t, 1, judged["group-t|10.0.0.10"].Behind)
}

// A box publishes into several groups, and a path that is behind on one of them must not mark that
// box's line on another. Parity is keyed on (group, publisher) for exactly this.
func TestEdgeMulticastPathParity_IsScopedToTheGroup(t *testing.T) {
	sportsTOB := func(pubIP string, channel uint8, messages uint64) handlers.EdgeMulticastObservationSeries {
		s := obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", pubIP, channel, messages)
		return s
	}
	sportsMBP := func(pubIP string, channel uint8, messages uint64) handlers.EdgeMulticastObservationSeries {
		s := obsSeries("mbp_edge_kalshi_sports_nfl", "cmh-rec1", pubIP, channel, messages)
		s.MulticastGroup = "233.0.0.10"
		return s
	}

	parity := handlers.EdgeMulticastPathParityForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		// Behind on top-of-book.
		sportsTOB("10.0.0.9", 10, 400),
		sportsTOB("10.0.0.10", 110, 1000),
		// Level with its peer on market-by-price, from the same box.
		sportsMBP("10.0.0.9", 10, 1000),
		sportsMBP("10.0.0.10", 110, 1000),
	})

	assert.Equal(t, 1, parity["group-t|10.0.0.9"].Behind, "the fault is on the top-of-book path")
	assert.Zero(t, parity["group-k|10.0.0.9"].Behind,
		"and must not follow the box onto its market-by-price line")
	assert.InDelta(t, 1.0, parity["group-k|10.0.0.9"].WorstRatio, 0.0001)
}

// The recorded message rate is per group and taken as the max over recording nodes. Both halves of
// that matter: summing the nodes would multiply the feed by the number of recorders watching it,
// and mixing the groups would report a box's whole output against each of its lines.
func TestEdgeMulticastPathRates_MaxOverNodesAndScopedToTheGroup(t *testing.T) {
	series := []handlers.EdgeMulticastObservationSeries{
		// One path, one group, seen by two recorders — the same 900 messages, twice.
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 600),
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 11, 300),
		obsSeries("tob_edge_kalshi_sports_nfl", "dub-rec1", "10.0.0.9", 10, 590),
		obsSeries("tob_edge_kalshi_sports_nfl", "dub-rec1", "10.0.0.9", 11, 295),
	}
	mbp := obsSeries("mbp_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 9000)
	mbp.MulticastGroup = "233.0.0.10"
	series = append(series, mbp)

	rates := handlers.EdgeMulticastPathRatesForTest(parityGroups, series, 15)

	// 900 messages over 15 minutes, from the recorder that saw the most.
	assert.InDelta(t, 1.0, rates["group-t|10.0.0.9"], 0.0001,
		"the two vantages are one feed, not two")
	assert.InDelta(t, 10.0, rates["group-k|10.0.0.9"], 0.0001,
		"and the box's other group is counted on its own line")
}

// A window of zero would divide by zero. An unset WindowMinutes on an old cached payload is the
// realistic way that happens, and it must cost the column rather than the page.
func TestEdgeMulticastPathRates_ZeroWindowYieldsNothing(t *testing.T) {
	rates := handlers.EdgeMulticastPathRatesForTest(parityGroups, []handlers.EdgeMulticastObservationSeries{
		obsSeries("tob_edge_kalshi_sports_nfl", "cmh-rec1", "10.0.0.9", 10, 600),
	}, 0)
	assert.Empty(t, rates)
}

// A publisher with no recorded series cannot read 'healthy' while its peers on the same group have
// one. That is missing coverage, and calling it healthy put a measured, gapping feed beside an
// unmeasured one and made the unmeasured one look the better of the two.
func TestGetEdgeMulticast_PublisherWithNoSeriesIsUnrecorded(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")  // 10.0.0.9
	insertEdgeMulticastCapturePublisher2(t, api, "group-k") // 10.0.0.10

	asOf := time.Now().UTC()
	// Only one of the two publishers has a series, and it is intact.
	seedL2Coverage(t, api, asOf, handlers.KalshiL2Lane{
		Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
		PublisherSourceIP: "10.0.0.9", Messages: 1200, Seen: true, LastSeen: asOf.Add(-time.Second),
	})

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	byIP := map[string]handlers.EdgeMulticastPublisher{}
	for _, l := range g.PublisherLines {
		byIP[l.DZIP] = l
	}
	assert.Equal(t, "healthy", byIP["10.0.0.9"].Health, "measured and intact")
	assert.Equal(t, "unrecorded", byIP["10.0.0.10"].Health,
		"its peer has a series and it has none: coverage, not health")
	assert.Equal(t, 1, g.PublisherVerdicts.Unrecorded)
	assert.Zero(t, g.PublisherVerdicts.Faulted(), "missing coverage is not a fault")
}

// And the case that must NOT change: a group nothing records at all. The shreds groups run Turbine
// and have no wire protocol behind them, so there is nothing to be uncovered by.
func TestGetEdgeMulticast_NoSeriesAnywhereStaysHealthy(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.Len(t, g.PublisherLines, 1)
	assert.Equal(t, "healthy", g.PublisherLines[0].Health,
		"no series on the group at all: clearing the floor is the whole truth")
	assert.Zero(t, g.PublisherVerdicts.Unrecorded)
}
