package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newEdgeMulticastTestAPI gives ClickHouse with full migrations (the health views the fetch
// reads are built by them) plus Postgres for the operator override table. The Postgres container
// is shared across the package, so the override table is truncated per test — same reason
// newKalshiTestAPI does it for kalshi_scoreboard_entry.
//
// That truncate is also why none of these tests call t.Parallel(): ClickHouse is isolated per
// test but Postgres is not, so parallel tests would clear each other's overrides mid-assertion.
func newEdgeMulticastTestAPI(t *testing.T) *handlers.API {
	t.Helper()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	_, err := api.PgPool.Exec(t.Context(), "TRUNCATE multicast_member_class")
	require.NoError(t, err)
	return api
}

// assertEdgeMulticastRoleInvariant checks the two decompositions of a role's Total that the API
// promises: measured state and classification must each account for every member.
func assertEdgeMulticastRoleInvariant(t *testing.T, role handlers.EdgeMulticastRoleCounts, what string) {
	t.Helper()
	assert.Equal(t, role.Total, role.Active+role.Idle+role.Unknown, "%s: active+idle+unknown must equal total", what)
	assert.Equal(t, role.Total, role.Recorders+role.InternalProbes+role.Customers, "%s: classes must equal total", what)
}

// classifyMember asserts an operator classification for one member IP.
func classifyMember(t *testing.T, api *handlers.API, clientIP, class string) {
	t.Helper()
	_, err := api.PgPool.Exec(t.Context(), `
		INSERT INTO multicast_member_class (client_ip, class, label)
		VALUES ($1, $2, 'test')
		ON CONFLICT (client_ip) DO UPDATE SET class = EXCLUDED.class, enabled = TRUE`, clientIP, class)
	require.NoError(t, err)
}

// promoteTestGroupToEdge renames the shared fixture's group into the Edge product's namespace.
// The page is scoped by code prefix, and dz_multicast_groups_current takes the newest row per
// entity_id, so a second history row is a rename — every user still references the pk.
func promoteTestGroupToEdge(t *testing.T, api *handlers.API, entityID, code string) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		SELECT entity_id, now() + INTERVAL 1 SECOND, now() + INTERVAL 1 SECOND, generateUUIDv4(), 0, attrs_hash + 1,
		       pk, owner_pubkey, ?, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count
		FROM dz_multicast_groups_current WHERE entity_id = ?`, code, entityID))
}

// insertEdgeMulticastFeed claims group-1 (inserted by insertMulticastTestData) for a feed sold
// in two metros. dz_feeds_current holds one row per (feed, metro), which is why the metro count
// has to come from a DISTINCT over rows rather than from a column.
func insertEdgeMulticastFeed(t *testing.T, api *handlers.API) {
	t.Helper()
	promoteTestGroupToEdge(t, api, "group-1", "edge-test-lane")
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
			('feed-ams', now(), now(), generateUUIDv4(), 0, 1, 'feed-ams', 'owner-dz', 'test-feed', 'test-feed-ams', 'metro-ams', '["group-1"]'),
			('feed-nyc', now(), now(), generateUUIDv4(), 0, 2, 'feed-nyc', 'owner-dz', 'test-feed', 'test-feed-nyc', 'metro-nyc', '["group-1"]')`))
}

// insertEdgeMulticastUnclaimedGroup adds a group no feed row claims, plus a publisher whose
// tunnel carries zero traffic — the silent-lane case.
func insertEdgeMulticastUnclaimedGroup(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-2', now(), now(), generateUUIDv4(), 0, 1, 'group-2', '', 'edge-lab-lane', '233.0.0.2', 100000000, 'activated', 0, 0)`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-lab-pub', now(), now(), generateUUIDv4(), 0, 1, 'user-lab-pub', 'pubkey-lab', 'activated', 'multicast', '10.0.0.3', '10.0.0.3', 'dev-ams1', 503, '["group-2"]', '[]')`))
	// A counter row that exists and reads zero. Without the row the publisher would be
	// Unknown ("no telemetry"), which is deliberately NOT silent.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES (now() - INTERVAL 1 MINUTE, 'dev-ams1', 'Tunnel503', 503, 'user-lab-pub', 0, 0, now())`))
}

func getEdgeMulticast(t *testing.T, api *handlers.API) handlers.EdgeMulticastResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/multicast", nil)
	rr := httptest.NewRecorder()
	api.GetEdgeMulticast(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.EdgeMulticastResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return resp
}

func findEdgeMulticastService(t *testing.T, resp handlers.EdgeMulticastResponse, code string) handlers.EdgeMulticastService {
	t.Helper()
	for _, s := range resp.Services {
		if s.Code == code {
			return s
		}
	}
	t.Fatalf("service %q not found in %+v", code, resp.Services)
	return handlers.EdgeMulticastService{}
}

func TestGetEdgeMulticast_Empty(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)

	resp := getEdgeMulticast(t, api)
	assert.Empty(t, resp.Services)
	assert.Equal(t, 5, resp.RateGrainMinutes)
}

func TestGetEdgeMulticast_GroupsByFeed(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)

	resp := getEdgeMulticast(t, api)

	svc := findEdgeMulticastService(t, resp, "test-feed")
	assert.True(t, svc.Managed)
	assert.Equal(t, 2, svc.MetroCount, "one feed row per metro, counted distinct")
	require.Len(t, svc.Groups, 1)

	g := svc.Groups[0]
	assert.Equal(t, "edge-test-lane", g.Code)
	assert.Equal(t, "233.0.0.1", g.MulticastIP)
	assert.Equal(t, 1, g.Publishers.Total)
	assert.Equal(t, 1, g.Publishers.Active)
	assert.Equal(t, 1, g.Subscribers.Total)
	assert.Equal(t, 1, g.Subscribers.Active)
	assert.False(t, g.Silent, "a publisher moving traffic is not silent")
	assert.InDelta(t, 10_000_000, g.IngressBps, 1, "publisher RX is the group's ingress")
	assert.InDelta(t, 10_000_000, g.EgressBps, 1, "subscriber TX is the group's egress")
	assert.False(t, g.TrafficAmbiguous, "single-group publisher attributes cleanly")
	assert.Equal(t, 0, g.PublishersMultiGroup)
	require.NotNil(t, g.ObservedAt)
	require.NotNil(t, g.ObservedAgeSeconds)
	assert.Equal(t, uint64(2), g.HealthCounts.Total, "one row per member in the rate view")
	assertEdgeMulticastRoleInvariant(t, g.Publishers, "publishers")
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "subscribers")
	assert.NotEmpty(t, g.Health)
}

func TestGetEdgeMulticast_UnmanagedGroupIsListedAndSilent(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)
	insertEdgeMulticastUnclaimedGroup(t, api)

	resp := getEdgeMulticast(t, api)

	// The unclaimed bucket sorts last, so a group with no feed behind it never displaces a sold one.
	require.NotEmpty(t, resp.Services)
	last := resp.Services[len(resp.Services)-1]
	assert.Equal(t, "edge-unclaimed", last.Code)
	assert.False(t, last.Managed)
	require.Len(t, last.Groups, 1)

	g := last.Groups[0]
	assert.Equal(t, "edge-lab-lane", g.Code)
	assert.Equal(t, 1, g.Publishers.Total)
	assert.Equal(t, 0, g.Publishers.Active)
	assert.Equal(t, 1, g.Publishers.Idle)
	assert.True(t, g.Silent, "publisher present, counters read zero")
	assert.Zero(t, g.IngressBps)
}

func TestGetEdgeMulticast_NoTelemetryIsNotSilent(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)
	// No rollup rows at all: the publisher exists in the ledger and nothing measures it.

	resp := getEdgeMulticast(t, api)
	svc := findEdgeMulticastService(t, resp, "test-feed")
	require.Len(t, svc.Groups, 1)

	g := svc.Groups[0]
	assert.Equal(t, 1, g.Publishers.Total)
	assert.Equal(t, 0, g.Publishers.Active)
	assert.Equal(t, 0, g.Publishers.Idle)
	assert.Equal(t, 1, g.Publishers.Unknown, "unmeasured members fall into Unknown, keeping the parts summing to Total")
	assert.False(t, g.Silent, "no data is not the same claim as no traffic")
}

func TestGetEdgeMulticast_SubscriberClassification(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)
	// A second subscriber, owned by the same wallet that publishes into the group. Shared
	// ownership used to be what marked a member internal; it no longer classifies anything,
	// because on mainnet that rule matched 515 wallets — every validator publishing shreds.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-rec', now(), now(), generateUUIDv4(), 0, 1, 'user-rec', 'pubkey-pub', 'activated', 'multicast', '10.0.0.4', '10.0.0.4', 'dev-nyc1', 504, '[]', '["group-1"]')`))

	resp := getEdgeMulticast(t, api)
	g := findEdgeMulticastService(t, resp, "test-feed").Groups[0]

	// Nothing classifies these members: no operator row, and 10.0.0.x is not a capture host.
	// They default to customer, and the payload says so by counting neither as known.
	assert.Equal(t, 2, g.Subscribers.Total)
	assert.Equal(t, 2, g.Subscribers.Customers)
	assert.Zero(t, g.Subscribers.Recorders)
	assert.Zero(t, g.Subscribers.ClassAsserted)
	assert.Zero(t, g.Subscribers.ClassDerived)
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "subscribers")

	// An operator row is the only way a market-data recorder gets classified at all.
	classifyMember(t, api, "10.0.0.4", "recorder")
	g = findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Equal(t, 1, g.Subscribers.Recorders)
	assert.Equal(t, 1, g.Subscribers.Customers)
	assert.Equal(t, 1, g.Subscribers.ClassAsserted)
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "subscribers after assertion")

	// A probe is counted apart from a recorder: it receives the feed but records nothing.
	classifyMember(t, api, "10.0.0.4", "internal_probe")
	g = findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Zero(t, g.Subscribers.Recorders)
	assert.Equal(t, 1, g.Subscribers.InternalProbes)
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "subscribers as probe")
}

// The derived tier is the edge scoreboard's capture-host map. A member on one of those IPs is
// classified with no operator row at all — and an operator row still overrides it, including
// when it demotes the box to a customer.
func TestGetEdgeMulticast_DerivedRecorderAndOverride(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	const captureHostIP = "64.130.37.175" // nyc-mn-bm1, from edgeNodeIPs
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-cap', now(), now(), generateUUIDv4(), 0, 1, 'user-cap', 'pubkey-cap', 'activated', 'multicast', '`+captureHostIP+`', '10.0.0.9', 'dev-nyc1', 505, '[]', '["group-1"]')`))

	g := findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Equal(t, 1, g.Subscribers.Recorders, "capture host classified with no operator row")
	assert.Equal(t, 1, g.Subscribers.ClassDerived)
	assert.Zero(t, g.Subscribers.ClassAsserted)
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "derived recorder")

	// Asserted beats derived, even downwards: a decommissioned capture box handed to a
	// customer must stop being counted as a recorder without waiting for a deploy.
	classifyMember(t, api, captureHostIP, "customer")
	g = findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Zero(t, g.Subscribers.Recorders, "operator row overrides the derived list")
	assert.Equal(t, 1, g.Subscribers.ClassAsserted)
	assert.Zero(t, g.Subscribers.ClassDerived, "the derived row is superseded, not double-counted")
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "derived recorder overridden")
}

// A malformed row would be inlined into a ClickHouse IN list. It is dropped, and the rest of the
// configuration still serves.
func TestGetEdgeMulticast_MalformedClassRowIsSkipped(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	_, err := api.PgPool.Exec(t.Context(), `
		INSERT INTO multicast_member_class (client_ip, class) VALUES ('not an ip', 'recorder')`)
	require.NoError(t, err)
	classifyMember(t, api, "10.0.0.2", "recorder")

	g := findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Equal(t, 1, g.Subscribers.Recorders, "the well-formed row still applies")
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "subscribers")
}

// insertEdgeMulticastCaptureGroups adds two groups whose codes the capture sources name: the
// Kalshi sports market-by-price lane and the Solana shreds group.
func insertEdgeMulticastCaptureGroups(t *testing.T, api *handlers.API) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('group-k', now(), now(), generateUUIDv4(), 0, 1, 'group-k', '', 'edge-kalshi-sports-mbp', '233.0.0.10', 50000000, 'activated', 0, 0),
			('group-s', now(), now(), generateUUIDv4(), 0, 2, 'group-s', '', 'edge-solana-shreds', '233.0.0.11', 100000000, 'activated', 0, 0)`))
}

func findEdgeMulticastGroup(t *testing.T, resp handlers.EdgeMulticastResponse, code string) handlers.EdgeMulticastGroup {
	t.Helper()
	for _, s := range resp.Services {
		for _, g := range s.Groups {
			if g.Code == code {
				return g
			}
		}
	}
	t.Fatalf("group %q not found", code)
	return handlers.EdgeMulticastGroup{}
}

// Local dev and most tests have no proxied capture tables at all. That must cost the column and
// nothing else — the page still renders every group off the counters.
func TestGetEdgeMulticast_LastHeardAbsentWithoutCaptureTables(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	resp := getEdgeMulticast(t, api)
	assert.False(t, resp.LastHeardAvailable, "no capture table is queryable")
	g := findEdgeMulticastService(t, resp, "test-feed").Groups[0]
	assert.Nil(t, g.LastHeard)
	assert.Empty(t, g.LastHeardSource)
}

func TestGetEdgeMulticast_LastHeardFromCaptureTables(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	promoteTestGroupToEdge(t, api, "group-1", "edge-test-lane")
	insertEdgeMulticastCaptureGroups(t, api)
	createKalshiObservationsTable(t, api)
	createShredderTables(t, api)

	// Two league lanes of the same Kalshi group, so the fold factor is observable.
	nowMs := uint64(time.Now().UnixMilli())
	insertObservation(t, api, "mbp_edge_kalshi_sports_nfl", 3, 2, "KXNFLGAME", nowMs, 1)
	insertObservation(t, api, "mbp_edge_kalshi_sports_nba", 3, 2, "KXNBAGAME", nowMs, 1)
	// A competitor feed shares the table and must not be attributed to any group.
	insertObservation(t, api, "kalshi_public_api", 9, 0, "KXNFLGAME", nowMs, 1)

	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO `+"`%s`"+`.slot_feed_race_summary_v2
			(event_ts, host, feed_type, epoch, slot, feed, loser_feed, total_shreds, shreds_won)
		VALUES
			(now(), 'slc-qa-bm1', 'shred', 700, 1000, 'dz',      '', 100, 70),
			(now(), 'slc-qa-bm1', 'shred', 700, 1000, 'turbine', '', 100, 30)`, api.ShredderDB)))

	resp := getEdgeMulticast(t, api)
	assert.True(t, resp.LastHeardAvailable)

	k := findEdgeMulticastGroup(t, resp, "edge-kalshi-sports-mbp")
	require.NotNil(t, k.LastHeard, "the sports lanes map to their group by naming convention")
	assert.Equal(t, "kalshi_bbo_observations", k.LastHeardSource)
	assert.Equal(t, 2, k.LastHeardLanes, "two league lanes folded into one group timestamp")
	require.NotNil(t, k.LastHeardAgeSecs)
	assert.Less(t, *k.LastHeardAgeSecs, 300.0)

	s := findEdgeMulticastGroup(t, resp, "edge-solana-shreds")
	require.NotNil(t, s.LastHeard, "the 'dz' feed alias resolves to the shreds group")
	assert.Equal(t, "slot_feed_race_summary_v2", s.LastHeardSource)
	assert.Equal(t, 1, s.LastHeardLanes)

	// The competitor observation was dropped, not bucketed into some group.
	other := findEdgeMulticastGroup(t, resp, "edge-test-lane")
	assert.Nil(t, other.LastHeard)

	// App-plane silence never sets Silent: it is receive-side, so it cannot tell a dead
	// recorder from a dead publisher.
	assert.False(t, other.Silent)
}

// A group outside the product's naming convention is not on this page at all, even when a feed
// row claims it — that is the qa-payments case on mainnet, where a non-product feed carries the
// mg0* groups.
func TestGetEdgeMulticast_NonEdgeGroupsExcluded(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-qa', now(), now(), generateUUIDv4(), 0, 1, 'group-qa', '', 'mg02', '233.0.0.90', 1000000000, 'activated', 0, 0)`))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES ('feed-qa', now(), now(), generateUUIDv4(), 0, 1, 'feed-qa', 'owner-dz', 'qa-payments', 'qa-payments-ams', 'metro-ams', '["group-qa"]')`))

	resp := getEdgeMulticast(t, api)
	for _, s := range resp.Services {
		assert.NotEqual(t, "qa-payments", s.Code, "a non-product feed must not open a section")
		for _, g := range s.Groups {
			assert.NotEqual(t, "mg02", g.Code, "a group outside the edge- namespace is not on this page")
		}
	}
}

// The two planes of one product are one section with a row each, not two sections. Metros are
// counted across both, deduplicated — the planes are sold in overlapping metro sets.
func TestGetEdgeMulticast_PlanesShareOneSection(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)

	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('group-mbp', now(), now(), generateUUIDv4(), 0, 1, 'group-mbp', '', 'edge-kalshi-perps-mbp', '233.0.0.20', 50000000, 'activated', 0, 0),
			('group-tob', now(), now(), generateUUIDv4(), 0, 2, 'group-tob', '', 'edge-kalshi-perps-tob', '233.0.0.21', 50000000, 'activated', 0, 0)`))
	// ams appears under both planes; nyc only under mbp. Three catalog rows, two metros.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
			('f-mbp-ams', now(), now(), generateUUIDv4(), 0, 1, 'f-mbp-ams', 'o', 'kalshi-perps-mbp', 'kalshi-perps-mbp-ams', 'metro-ams', '["group-mbp"]'),
			('f-mbp-nyc', now(), now(), generateUUIDv4(), 0, 2, 'f-mbp-nyc', 'o', 'kalshi-perps-mbp', 'kalshi-perps-mbp-nyc', 'metro-nyc', '["group-mbp"]'),
			('f-tob-ams', now(), now(), generateUUIDv4(), 0, 3, 'f-tob-ams', 'o', 'kalshi-perps-tob', 'kalshi-perps-tob-ams', 'metro-ams', '["group-tob"]')`))

	resp := getEdgeMulticast(t, api)

	svc := findEdgeMulticastService(t, resp, "kalshi-perps")
	require.Len(t, svc.Groups, 2, "one section, one row per plane")
	assert.Equal(t, 2, svc.MetroCount, "ams counted once across the two planes")
	assert.Equal(t, "MBP", svc.Groups[0].Plane)
	assert.Equal(t, "TOP", svc.Groups[1].Plane)

	for _, s := range resp.Services {
		assert.NotContains(t, s.Code, "-mbp", "the plane suffix must not survive as a section")
		assert.NotContains(t, s.Code, "-tob")
	}
}

// A group that carries no plane reports none, rather than borrowing the last segment of its code.
func TestEdgeMulticastPlaneAbsentOnSolanaGroups(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-solana-shreds")
	assert.Empty(t, g.Plane, "edge-solana-shreds is not plane-split")
}
