package handlers_test

import (
	"context"
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
	// The sequence column is folded from the L2 coverage refresher's cache entry, which lives in
	// the same shared Postgres. Cleared here and after every write below so one test's coverage
	// payload cannot put a Sequence on another test's groups. The key is spelled out rather than
	// exported: same call the publisher-check tests make for theirs.
	_, err = api.PgPool.Exec(t.Context(), `DELETE FROM page_cache WHERE key = $1`, kalshiL2CoverageKey)
	require.NoError(t, err)
	return api
}

// kalshiL2CoverageKey mirrors the unexported page-cache key in kalshi_l2_coverage.go. If that
// constant's version is bumped without this one, the sequence tests stop exercising the fold and
// start asserting the absent case, which still passes — so bump both.
const kalshiL2CoverageKey = "kalshi_l2_coverage:v5"

// seedL2Coverage writes a coverage payload for the sequence column to fold, and removes it again
// afterwards.
func seedL2Coverage(t *testing.T, api *handlers.API, generatedAt time.Time, lanes ...handlers.KalshiL2Lane) {
	t.Helper()
	require.NoError(t, api.WritePageCache(t.Context(), kalshiL2CoverageKey, handlers.KalshiL2CoverageResponse{
		GeneratedAt:   generatedAt,
		WindowMinutes: 15,
		Lanes:         lanes,
	}))
	t.Cleanup(func() {
		_, err := api.PgPool.Exec(context.Background(), `DELETE FROM page_cache WHERE key = $1`, kalshiL2CoverageKey)
		require.NoError(t, err)
	})
}

// assertEdgeMulticastRoleInvariant checks the two decompositions of a role's Total that the API
// promises: measured state and classification must each account for every member.
func assertEdgeMulticastRoleInvariant(t *testing.T, role handlers.EdgeMulticastRoleCounts, what string) {
	t.Helper()
	assert.Equal(t, role.Total, role.Active+role.Idle+role.Unknown, "%s: active+idle+unknown must equal total", what)
	assert.Equal(t, role.Total, role.Recorders+role.InternalProbes+role.DoubleZero+role.Customers, "%s: classes must equal total", what)
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
	promoteTestGroupToEdge(t, api, "group-1", "edge-test-feed")
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
			('feed-ams', now(), now(), generateUUIDv4(), 0, 1, 'feed-ams', 'owner-dz', 'test-feed', 'test-feed-ams', 'metro-ams', '["group-1"]'),
			('feed-nyc', now(), now(), generateUUIDv4(), 0, 2, 'feed-nyc', 'owner-dz', 'test-feed', 'test-feed-nyc', 'metro-nyc', '["group-1"]')`))
}

// insertEdgeMulticastUnclaimedGroup adds a group no feed row claims, plus a publisher whose
// tunnel carries zero traffic — the silent-feed case.
func insertEdgeMulticastUnclaimedGroup(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-2', now(), now(), generateUUIDv4(), 0, 1, 'group-2', '', 'edge-lab-feed', '233.0.0.2', 100000000, 'activated', 0, 0)`))
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
	assert.Equal(t, "edge-test-feed", g.Code)
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
	require.NotNil(t, g.ObservedAt, "the newest bucket's timestamp; the page computes the age from it")
	assert.Equal(t, uint64(2), g.HealthCounts.Total, "one row per member in the rate view")
	assertEdgeMulticastRoleInvariant(t, g.Publishers, "publishers")
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "subscribers")
	assert.Equal(t, "healthy", g.Health, "a publisher moving data is the whole verdict")
}

// The verdict is the counter plane's publisher signal and not the control-plane roll-up. A group
// whose members are all flagged unhealthy by reconciliation still reads healthy while its
// publishers are sending: on mainnet a publisher drops out of one device's mroute snapshot for a
// cycle constantly, and a worst-of over hundreds of members is red permanently.
func TestGetEdgeMulticast_VerdictIgnoresControlPlaneRollup(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)
	// Take the publisher's BGP down: reconciliation reports 'disconnected' for it, which the
	// old worst-of ranked above healthy. The counters still show it sending.
	// A newer snapshot of the same publisher; dz_users_current takes the latest per entity_id,
	// so this is the same member with its BGP session reported down.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES ('user-pub', now() + INTERVAL 2 SECOND, now() + INTERVAL 2 SECOND, generateUUIDv4(), 0, 9,
		        'user-pub', 'pubkey-pub', 'activated', 'multicast', '10.0.0.1', '10.0.0.1', 'dev-ams1', 501, '["group-1"]', '[]', 'down')`))

	resp := getEdgeMulticast(t, api)
	g := findEdgeMulticastService(t, resp, "test-feed").Groups[0]

	assert.Equal(t, 1, g.Publishers.Active, "the counters are unchanged")
	assert.Equal(t, "healthy", g.Health)
	assert.Positive(t, g.HealthCounts.Disconnected, "the control-plane detail is still reported, just not as the verdict")
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
	assert.Equal(t, "edge-lab-feed", g.Code)
	assert.Equal(t, 1, g.Publishers.Total)
	assert.Equal(t, 0, g.Publishers.Active)
	assert.Equal(t, 1, g.Publishers.Idle)
	assert.True(t, g.Silent, "publisher present, counters read zero")
	assert.Equal(t, "silent", g.Health, "the one state worth acting on")
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
	assert.Equal(t, "unknown", g.Health, "nothing measured the publisher: a monitoring gap, not a verdict")
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

// The second derived tier is the operator-wallet allow-list. It is what classifies the market-data
// receivers, which the capture-host map structurally cannot reach — and it stops at "ours", because
// one wallet holds recorders, probes and lab boxes at once.
func TestGetEdgeMulticast_OperatorWalletDerivedClass(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	// From doubleZeroOperatorWallets. Spelled out rather than read from the package so a wallet
	// removed from that list fails this test instead of silently taking its coverage with it.
	const operatorWallet = "DZfHfcCXTLwgZeCRKQ1FL1UuwAwFAZM93g86NMYpfYan"
	// Not a capture host: the point is that the wallet classifies it and the IP map cannot.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-dz', now(), now(), generateUUIDv4(), 0, 1, 'user-dz', '`+operatorWallet+`', 'activated', 'multicast', '10.0.0.5', '10.0.0.5', 'dev-nyc1', 506, '[]', '["group-1"]')`))

	g := findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Equal(t, 1, g.Subscribers.DoubleZero, "the wallet says whose box it is")
	assert.Zero(t, g.Subscribers.Recorders, "and does not say it records: the wallet cannot tell")
	assert.Equal(t, 1, g.Subscribers.ClassDerived, "a wallet match is a known classification")
	assert.Zero(t, g.Subscribers.ClassAsserted)
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "wallet-derived member")

	// Asserted still beats derived downwards. Without this the escape hatch would not reach a
	// box we handed back, since the wallet keeps matching until the ledger account is torn down.
	classifyMember(t, api, "10.0.0.5", "customer")
	g = findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Zero(t, g.Subscribers.DoubleZero, "an operator row saying customer wins over the wallet")
	assert.Equal(t, 1, g.Subscribers.ClassAsserted)
	assert.Zero(t, g.Subscribers.ClassDerived, "superseded, not double-counted")
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "wallet-derived member overridden")

	// And upwards: naming the kind is the whole reason the asserted tier exists.
	classifyMember(t, api, "10.0.0.5", "recorder")
	g = findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	assert.Equal(t, 1, g.Subscribers.Recorders)
	assert.Zero(t, g.Subscribers.DoubleZero, "a member is counted in exactly one class")
	assertEdgeMulticastRoleInvariant(t, g.Subscribers, "wallet-derived member promoted")
}

// A publisher line carries the same tiers as the subscriber split, so a DoubleZero-run publisher
// is labelled on its own line rather than reading as a customer of its own feed.
func TestGetEdgeMulticast_PublisherLineCarriesWalletClass(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	const operatorWallet = "DZfHfcCXTLwgZeCRKQ1FL1UuwAwFAZM93g86NMYpfYan"
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-dzp', now(), now(), generateUUIDv4(), 0, 1, 'user-dzp', '`+operatorWallet+`', 'activated', 'multicast', '10.0.0.6', '10.0.0.6', 'dev-nyc1', 507, '["group-1"]', '[]')`))

	g := findEdgeMulticastService(t, getEdgeMulticast(t, api), "test-feed").Groups[0]
	var found bool
	for _, line := range g.PublisherLines {
		if line.ClientIP == "10.0.0.6" {
			found = true
			assert.Equal(t, "doublezero", line.Class)
		}
	}
	assert.True(t, found, "the wallet-owned publisher has a line")
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
// Kalshi sports market-by-price plane and the Solana shreds group.
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
	assert.Empty(t, g.LastHeardTable)
}

func TestGetEdgeMulticast_LastHeardFromCaptureTables(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	promoteTestGroupToEdge(t, api, "group-1", "edge-test-feed")
	insertEdgeMulticastCaptureGroups(t, api)
	createKalshiObservationsTable(t, api)
	createShredderTables(t, api)

	// Two league capture sources of the same Kalshi group, so the fold factor is observable.
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
	require.NotNil(t, k.LastHeard, "the sports capture sources map to their group by naming convention")
	assert.Equal(t, "kalshi_bbo_observations", k.LastHeardTable)
	assert.Equal(t, 2, k.LastHeardCaptureSources, "two league capture sources folded into one group timestamp")
	assert.Less(t, time.Since(*k.LastHeard), 5*time.Minute, "the page ages this against its own clock")

	s := findEdgeMulticastGroup(t, resp, "edge-solana-shreds")
	require.NotNil(t, s.LastHeard, "the 'dz' feed alias resolves to the shreds group")
	assert.Equal(t, "slot_feed_race_summary_v2", s.LastHeardTable)
	assert.Equal(t, 1, s.LastHeardCaptureSources)

	// The competitor observation was dropped, not bucketed into some group.
	other := findEdgeMulticastGroup(t, resp, "edge-test-feed")
	assert.Nil(t, other.LastHeard)

	// App-plane silence never sets Silent: it is receive-side, so it cannot tell a dead
	// recorder from a dead publisher.
	assert.False(t, other.Silent)
}

// A group outside the product's naming convention is not on this page at all, even when a feed
// row claims it — that is the qa-payments case on mainnet, where a non-product feed carries the
// mg0* groups.
// A member that both publishes into a group and subscribes to it gets mode 'P+S', and the rate
// view hands that row the EGRESS rate — ur_max_out_bps — because it picks the direction by mode.
// So a P+S row says nothing about whether the member is publishing. Counting it on the publisher
// side inverted the page in both directions: RPF means the member does not receive its own group
// back, so max_out reads 0 and the row rendered Idle, hence Silent and a red feed while the
// publisher was sending.
func TestGetEdgeMulticast_PublisherSubscriberIsNotCountedAsPublisher(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)
	ctx := t.Context()

	// Its own group, so the assertions below are about this one member and nothing else.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-ps', now(), now(), generateUUIDv4(), 0, 1, 'group-ps', '', 'edge-ps-feed', '233.0.0.3', 100000000, 'activated', 0, 0)`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-ps', now(), now(), generateUUIDv4(), 0, 1, 'user-ps', 'pubkey-ps', 'activated', 'multicast', '10.0.0.5', '10.0.0.5', 'dev-ams1', 505, '["group-ps"]', '["group-ps"]')`))
	// Sending 10 Mbps, receiving nothing back — the RPF case, and exactly the shape that used to
	// render as a silent feed.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES (now() - INTERVAL 1 MINUTE, 'dev-ams1', 'Tunnel505', 505, 'user-ps', 10000000, 0, now())`))

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-ps-feed")

	assert.Equal(t, 1, g.Publishers.Total)
	assert.Equal(t, 0, g.Publishers.Active)
	assert.Equal(t, 0, g.Publishers.Idle, "a P+S row's rate is egress and cannot answer for the send side")
	assert.Equal(t, 1, g.Publishers.Unknown, "unmeasured, which is the true statement")
	assert.False(t, g.Silent, "the publisher was never measured, so nothing here says it went quiet")
	assert.Equal(t, "unknown", g.Health)
	assert.Zero(t, g.IngressBps, "ingress comes from mode P alone; 0 here is visibly unmeasured")
	assert.Equal(t, 1, g.Subscribers.Total, "the member still counts on the side its rate speaks for")
	assertEdgeMulticastRoleInvariant(t, g.Publishers, "publishers")
}

// A feed row whose `groups` is '[]' — sold in a metro where the group is not provisioned yet — is
// still a metro the feed is sold in. arrayJoin over an empty array drops the row, so counting
// metros downstream of one lost it and the section header claimed fewer metros than the catalog.
func TestGetEdgeMulticast_MetroCountKeepsGrouplessCatalogRows(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)
	// A third metro for the same feed family, with no group listed yet.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES ('feed-lax', now(), now(), generateUUIDv4(), 0, 3, 'feed-lax', 'owner-dz', 'test-feed', 'test-feed-lax', 'metro-lax', '[]')`))

	resp := getEdgeMulticast(t, api)
	svc := findEdgeMulticastService(t, resp, "test-feed")

	assert.Equal(t, 3, svc.MetroCount, "every metro row counts, group or no group")
	require.Len(t, svc.Groups, 1, "and the group union is unchanged")
	assert.Equal(t, "edge-test-feed", svc.Groups[0].Code)
}

func TestGetEdgeMulticast_NonEdgeGroupsExcluded(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastFeed(t, api)

	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-qa', now(), now(), generateUUIDv4(), 0, 1, 'group-qa', '', 'mg02', '233.0.0.90', 1000000000, 'activated', 0, 0)`))
	// A code that starts with the letters but not with the prefix. Without the hyphen in
	// edgeMulticastGroupCodePrefix this lands in the unclaimed section, flagged silent, with
	// nothing on the page to explain why it is there.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('group-edgecase', now(), now(), generateUUIDv4(), 0, 1, 'group-edgecase', '', 'edgecase-lab', '233.0.0.91', 1000000000, 'activated', 0, 0)`))
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
			assert.NotEqual(t, "edgecase-lab", g.Code, "the prefix is 'edge-', not 'edge'")
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

// insertEdgeMulticastPublisher adds one more publisher to group-1 with the rate given, so the
// per-publisher checks can be exercised against a group that already has a healthy one.
func insertEdgeMulticastPublisher(t *testing.T, api *handlers.API, id, clientIP string, tunnel int, bps int64) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES (?, now(), now(), generateUUIDv4(), 0, 7, ?, 'pubkey-pub2', 'activated', 'multicast', ?, ?, 'dev-ams1', ?, '["group-1"]', '[]')`,
		id, id, clientIP, clientIP, tunnel))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES (now() - INTERVAL 1 MINUTE, 'dev-ams1', ?, ?, ?, ?, 0, now())`,
		fmt.Sprintf("Tunnel%d", tunnel), tunnel, id, bps))
}

// The lines are the grain the verdict is taken at, so the payload has to carry them: identity,
// rate, bucket and per-member status for each publisher, not just a count.
func TestGetEdgeMulticast_PublisherLines(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-test-feed")

	require.Len(t, g.PublisherLines, 1)
	assert.Equal(t, 1, g.PublisherLinesTotal)
	line := g.PublisherLines[0]
	assert.Equal(t, "10.0.0.1", line.ClientIP)
	assert.Equal(t, "ams001-dz001", line.DeviceCode, "the device code comes from the ledger, not the rate view")
	assert.Equal(t, int32(501), line.TunnelID)
	assert.Equal(t, "publishing", line.Status)
	assert.Equal(t, "customer", line.Class, "nothing has classified this box: the default, and a weak claim")
	require.NotNil(t, line.Bps)
	assert.InDelta(t, 10_000_000, *line.Bps, 1)
	assert.False(t, line.MultiGroup, "one group, so the counter attributes cleanly")
	require.NotNil(t, line.ObservedAt, "the bucket is per line: one stale publisher among fresh ones is a fact")

	assert.Equal(t, 1, g.PublishersPublishing)
	assert.Equal(t, 0, g.PublishersBelowFloor)
	assert.Equal(t, "healthy", g.Health)
}

// A publisher whose tunnel carries a trickle is NOT a publishing publisher. This is the case the
// old verdict scored as healthy: it only asked whether the counter was non-zero, so 200 bps of
// protocol overhead and 2.4 Mbps of product were the same answer.
func TestGetEdgeMulticast_ThinPublisherIsNotHealthy(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)
	insertEdgeMulticastPublisher(t, api, "user-pub2", "10.0.0.7", 507, 200)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-test-feed")

	assert.Equal(t, "thin", g.Health, "one publisher below the floor is a fault in the feed")
	assert.Equal(t, 1, g.PublishersPublishing)
	assert.Equal(t, 1, g.PublishersBelowFloor)
	assert.False(t, g.Silent, "the other publisher is sending, so the feed did not go quiet")

	require.Len(t, g.PublisherLines, 2)
	// Found by address, not by position: lines are READ in address order and only CHOSEN
	// worst-first, and what this test is about is the verdict on the line, not where it sits.
	// TestGetEdgeMulticast_PublisherLinesReadByClientIP owns the ordering.
	byIP := edgeMulticastLinesByClientIP(g.PublisherLines)
	require.Contains(t, byIP, "10.0.0.7")
	assert.Equal(t, "thin", byIP["10.0.0.7"].Status)
	for ip, line := range byIP {
		if ip != "10.0.0.7" {
			assert.Equal(t, "publishing", line.Status)
		}
	}
	assert.Equal(t, 2, g.Publishers.Active, "the group counts still report both counters as non-zero")
}

// A dead publisher next to a live one is the same verdict as a thin one — zero is below the floor
// — and specifically NOT 'silent', which is reserved for a feed where nothing is sending at all.
func TestGetEdgeMulticast_IdlePublisherBesideALiveOneIsThin(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)
	insertEdgeMulticastPublisher(t, api, "user-pub2", "10.0.0.8", 508, 0)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-test-feed")

	assert.Equal(t, "thin", g.Health)
	assert.Equal(t, 1, g.PublishersBelowFloor)
	assert.False(t, g.Silent)
	require.Len(t, g.PublisherLines, 2)
	byIP := edgeMulticastLinesByClientIP(g.PublisherLines)
	require.Contains(t, byIP, "10.0.0.8")
	assert.Equal(t, "idle", byIP["10.0.0.8"].Status)
}

// edgeMulticastLinesByClientIP indexes publisher lines by the box's address. Tests that care about
// one line's verdict look it up rather than indexing into the slice: display order is by address
// and selection order is worst-first, so a position is not a stable way to name a publisher.
func edgeMulticastLinesByClientIP(lines []handlers.EdgeMulticastPublisher) map[string]handlers.EdgeMulticastPublisher {
	out := make(map[string]handlers.EdgeMulticastPublisher, len(lines))
	for _, l := range lines {
		out[l.ClientIP] = l
	}
	return out
}

// insertEdgeMulticastCapturePublisher gives a capture group a publisher above the floor, so the
// parity half of the verdict is not masked by the publisher half.
func insertEdgeMulticastCapturePublisher(t *testing.T, api *handlers.API, groupPK string) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-kpub', now(), now(), generateUUIDv4(), 0, 11, 'user-kpub', 'pubkey-kpub', 'activated',
		        'multicast', '10.0.0.9', '10.0.0.9', 'dev-ams1', 509, ?, '[]')`,
		fmt.Sprintf(`["%s"]`, groupPK)))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES (now() - INTERVAL 1 MINUTE, 'dev-ams1', 'Tunnel509', 509, 'user-kpub', 5000000, 0, now())`))
}

// insertEdgeMulticastCapturePublisher2 adds a second publisher to a capture group, so the
// per-publisher sequence attribution has two lines to tell apart.
func insertEdgeMulticastCapturePublisher2(t *testing.T, api *handlers.API, groupPK string) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES ('user-kpub2', now(), now(), generateUUIDv4(), 0, 12, 'user-kpub2', 'pubkey-kpub2', 'activated',
		        'multicast', '10.0.0.10', '10.0.0.10', 'dev-ams1', 510, ?, '[]')`,
		fmt.Sprintf(`["%s"]`, groupPK)))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES (now() - INTERVAL 1 MINUTE, 'dev-ams1', 'Tunnel510', 510, 'user-kpub2', 4000000, 0, now())`))
}

// A sequence series belongs to ONE publisher: each path keeps its own counters, so one can gap
// while its peer is intact. The old roll-up could only say "this group gapped", which on a
// two-publisher feed names neither the healthy path nor the broken one.
func TestGetEdgeMulticast_SequenceIsPerPublisher(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	insertEdgeMulticastCapturePublisher2(t, api, "group-k")

	asOf := time.Now().UTC()
	seedL2Coverage(t, api, asOf,
		// 10.0.0.9's series is intact; 10.0.0.10's gapped. Same group, same capture source,
		// same recording node: the address is the only thing that separates them.
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", Messages: 2000, Seen: true, LastSeen: asOf.Add(-time.Second),
		},
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.10", Messages: 1900, GapBooks: 4, Resets: 3, Seen: true,
			LastSeen: asOf.Add(-time.Second),
		},
	)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")

	require.Len(t, g.PublisherLines, 2)
	byIP := map[string]handlers.EdgeMulticastPublisher{}
	for _, l := range g.PublisherLines {
		byIP[l.DZIP] = l
	}

	intact := byIP["10.0.0.9"]
	require.NotNil(t, intact.Sequence, "the series is reported on the line that emitted it")
	assert.Equal(t, "ok", intact.Sequence.Status)
	assert.Zero(t, intact.Sequence.Gapped)

	broken := byIP["10.0.0.10"]
	require.NotNil(t, broken.Sequence)
	assert.Equal(t, "gapped", broken.Sequence.Status)
	assert.Equal(t, 1, broken.Sequence.Gapped)
	require.Len(t, broken.Sequence.Instances, 1)
	assert.Equal(t, uint64(4), broken.Sequence.Instances[0].GapBooks)
	assert.Equal(t, "10.0.0.10", broken.Sequence.Instances[0].PublisherSourceIP)

	// The roll-up still answers the group-level question, and now counts publishers as well as
	// series: one of two publishers is broken, not "the group is broken".
	require.NotNil(t, g.Sequence)
	assert.Equal(t, "gapped", g.Sequence.Status)
	assert.Equal(t, 2, g.Sequence.Publishers)
	assert.Equal(t, 1, g.Sequence.PublishersGapped)
	assert.Equal(t, 0, g.Sequence.Unattributed)
}

// A recorded series whose source address matches no publisher in the ledger has no line to sit on.
// It is counted on the roll-up rather than dropped: silently discarding a recorded gap is the one
// outcome this column must not have.
func TestGetEdgeMulticast_SequenceUnattributedSeries(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")

	asOf := time.Now().UTC()
	seedL2Coverage(t, api, asOf,
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.9.9.9", Messages: 900, GapBooks: 2, Seen: true,
			LastSeen: asOf.Add(-time.Second),
		},
	)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")

	require.Len(t, g.PublisherLines, 1)
	assert.Nil(t, g.PublisherLines[0].Sequence, "nothing recorded from this publisher's address")
	require.NotNil(t, g.Sequence)
	assert.Equal(t, 1, g.Sequence.Unattributed)
	assert.Equal(t, 0, g.Sequence.Publishers)
	assert.Equal(t, "gapped", g.Sequence.Status, "the gap is still reported, just not on a line")
}

// A publisher with no BGP session cannot be sending the feed it is registered to send, and the
// line says so. It deliberately does NOT move the group's verdict: the ledger snapshot and the
// counter bucket are minutes apart, so a publisher can read 'down' while its tunnel still moved
// bytes, and the reader is given both rather than one overruling the other.
func TestGetEdgeMulticast_PublisherBGPDown(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)

	// A newer snapshot of the same publisher with its session down. dz_users_current takes the
	// latest row per entity_id, so this is the same member, not a second one.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES ('user-pub', now() + INTERVAL 2 SECOND, now() + INTERVAL 2 SECOND, generateUUIDv4(), 0, 21,
		        'user-pub', 'pubkey-pub', 'activated', 'multicast', '10.0.0.1', '10.0.0.1', 'dev-ams1', 501, '["group-1"]', '[]', 'down')`))

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-test-feed")

	require.Len(t, g.PublisherLines, 1)
	assert.Equal(t, "down", g.PublisherLines[0].BGPStatus, "the line carries the session state")
	assert.Equal(t, "publishing", g.PublisherLines[0].Status, "the counters are unchanged")
	assert.Equal(t, "healthy", g.Health, "a ledger snapshot minutes from the bucket does not overrule it")
}

// The common case, so the field cannot quietly start reporting 'down' for everyone.
func TestGetEdgeMulticast_PublisherBGPUp(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)
	insertEdgeMulticastFeed(t, api)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-test-feed")
	require.Len(t, g.PublisherLines, 1)
	assert.NotEqual(t, "down", g.PublisherLines[0].BGPStatus)
}

// The recorder half of the verdict. Every recording node on a group receives the same feed, so a
// node writing down a fraction of what its peers do is not hearing it — a fault the publisher
// counters cannot see, because they are per tunnel and sum every group the tunnel carries.
func TestGetEdgeMulticast_CaptureNodeParity(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	createKalshiObservationsTable(t, api)

	nowMs := uint64(time.Now().UnixMilli())
	// cmh hears the feed; was hears a twentieth of it.
	for i := range 20 {
		insertObservationAt(t, api, "cmh", "mbp_edge_kalshi_sports_nfl", 3, 2, "KXNFLGAME", nowMs+uint64(i), 1)
	}
	insertObservationAt(t, api, "was", "mbp_edge_kalshi_sports_nfl", 3, 2, "KXNFLGAME", nowMs, 1)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")

	assert.Equal(t, 1, g.PublishersPublishing, "the publisher side is clean, so the verdict is about the recorders")
	require.Len(t, g.CaptureNodes, 2)
	assert.Equal(t, "was-rec1", g.CaptureNodes[0].Node, "the node behind its peers sorts first")
	assert.True(t, g.CaptureNodes[0].Lagging)
	assert.Less(t, g.CaptureNodes[0].ShareOfMedian, 0.5)
	assert.Equal(t, "cmh-rec1", g.CaptureNodes[1].Node)
	assert.False(t, g.CaptureNodes[1].Lagging)
	assert.Equal(t, 1, g.CaptureNodesLagging)
	assert.Equal(t, "skewed", g.Health)
}

// One node has nothing to be compared against. Calling that feed skewed on the strength of a
// single sample would be a claim the data does not carry, so parity stays silent and the
// publisher check decides.
func TestGetEdgeMulticast_SingleCaptureNodeIsNeverSkewed(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	createKalshiObservationsTable(t, api)

	insertObservationAt(t, api, "cmh", "mbp_edge_kalshi_sports_nfl", 3, 2, "KXNFLGAME", uint64(time.Now().UnixMilli()), 1)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")

	require.Len(t, g.CaptureNodes, 1)
	assert.False(t, g.CaptureNodes[0].Lagging)
	assert.Equal(t, 0, g.CaptureNodesLagging)
	assert.Equal(t, "healthy", g.Health)
}

// The sequence column: one recorded series per channel instance, folded from the L2 coverage
// refresher's cache so this page never runs that scan itself and the two can never disagree.
// The loss timeline reaches the publisher's series, and the axis it is drawn on reaches the payload.
//
// Both halves are asserted because either one alone is useless: episodes carry an absolute start, so
// without the window's width the consumer has a right edge and no span and every mark lands at an
// arbitrary offset. The width travels with the data rather than being read from a second copy of the
// constant on the far side, which is what would let the axis and the episodes disagree.
func TestGetEdgeMulticast_GapEpisodesAndWindowReachThePayload(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)

	asOf := time.Now().UTC()
	start := asOf.Add(-5 * time.Minute).Unix()
	seedL2Coverage(t, api, asOf,
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nba", ChannelID: 2, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", LocationCode: "cmh", Messages: 500, GapBooks: 3,
			Seen: true, LastSeen: asOf.Add(-1 * time.Second),
			GapEpisodes: []handlers.KalshiL2GapEpisode{{Start: start, Seconds: 4}},
		},
	)

	resp := getEdgeMulticast(t, api)
	assert.Equal(t, 900, resp.GapWindowSeconds, "the fifteen-minute coverage window, in seconds")

	k := findEdgeMulticastGroup(t, resp, "edge-kalshi-sports-mbp")
	require.NotNil(t, k.Sequence)
	require.Len(t, k.Sequence.Instances, 1)
	require.Len(t, k.Sequence.Instances[0].GapEpisodes, 1)
	assert.Equal(t, start, k.Sequence.Instances[0].GapEpisodes[0].Start)
	assert.Equal(t, uint32(4), k.Sequence.Instances[0].GapEpisodes[0].Seconds)
}

// A top-of-book series has no gap marker, so it must carry no timeline. An empty one drawn under a
// true GapsMeasured would be the clean bill of health this column refuses to give that plane.
func TestGetEdgeMulticast_TopOfBookCarriesNoGapTimeline(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)

	// Attached by destination address, which is how the top-of-book leg resolves a series to a
	// group. The fixture's group happens to be the market-by-price one; what this test is about is
	// the PLANE the series came from, and that is carried by the tob_ prefix on the capture source.
	asOf := time.Now().UTC()
	seedObservations(t, api, asOf, handlers.EdgeMulticastObservationSeries{
		Source: "tob_edge_kalshi_sports_nfl", ChannelID: 2, Node: "cmh-rec1", LocationCode: "cmh",
		PublisherSourceIP: "10.0.0.9", MulticastGroup: "233.0.0.10", Messages: 900,
		LastSeen: asOf.Add(-1 * time.Second),
	})

	k := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.NotNil(t, k.Sequence)
	require.Len(t, k.Sequence.Instances, 1)
	assert.False(t, k.Sequence.Instances[0].GapsMeasured)
	assert.Empty(t, k.Sequence.Instances[0].GapEpisodes, "no marker to count means no timeline")
}

func TestGetEdgeMulticast_SequenceHealthFromL2Cache(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)

	asOf := time.Now().UTC()
	seedL2Coverage(t, api, asOf,
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", LocationCode: "cmh", Messages: 1000, Seen: true,
			LastSeen: asOf.Add(-2 * time.Second),
		},
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nba", ChannelID: 2, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", LocationCode: "cmh", Messages: 500, GapBooks: 3,
			GapMessages: 158912, Resets: 2, SnapshotCycles: 1, Seen: true,
			LastSeen: asOf.Add(-1 * time.Second),
		},
		// A configured capture source that produced nothing in the window. It says nothing
		// about a sequence series and must not become an 'ok' instance.
		handlers.KalshiL2Lane{Source: "mbp_edge_kalshi_sports_wnba", ChannelID: 3, Seen: false},
		// A source no group on this page claims is dropped, not bucketed.
		handlers.KalshiL2Lane{Source: "kalshi_public_api", Messages: 9, Seen: true, LastSeen: asOf},
	)

	resp := getEdgeMulticast(t, api)
	require.NotNil(t, resp.SequenceAsOf, "the column ages against the refresher's clock, not this payload's")
	assert.WithinDuration(t, asOf, *resp.SequenceAsOf, time.Second)

	k := findEdgeMulticastGroup(t, resp, "edge-kalshi-sports-mbp")
	require.NotNil(t, k.Sequence)
	assert.Equal(t, "gapped", k.Sequence.Status, "worst of the instances")
	assert.Equal(t, 1, k.Sequence.Gapped)
	assert.Equal(t, 0, k.Sequence.Stalled)
	require.Len(t, k.Sequence.Instances, 2, "the unseen source and the unclaimed one are both out")
	assert.Equal(t, uint64(3), k.Sequence.Instances[0].GapBooks, "worst first")
	assert.Equal(t, uint8(2), k.Sequence.Instances[0].ChannelID)
	assert.Equal(t, "cmh-rec1", k.Sequence.Instances[0].Node)
	assert.Equal(t, "ok", k.Sequence.Instances[1].Status)

	// A group with no recorder running the wire protocol carries no column at all.
	s := findEdgeMulticastGroup(t, resp, "edge-solana-shreds")
	assert.Nil(t, s.Sequence)
}

// A series with no gaps that stopped advancing is not 'ok'. Staleness is measured against the
// coverage payload's own clock: read against wall clock it would inherit the refresher's lag and
// mark healthy series stalled for most of every cycle.
func TestGetEdgeMulticast_SequenceStalledSeries(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)

	asOf := time.Now().UTC()
	seedL2Coverage(t, api, asOf,
		handlers.KalshiL2Lane{
			Source: "mbp_edge_kalshi_sports_nfl", ChannelID: 1, MeasurementNodeID: "cmh-rec1",
			PublisherSourceIP: "10.0.0.9", Messages: 1000, Seen: true,
			LastSeen: asOf.Add(-10 * time.Minute),
		},
	)

	k := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.NotNil(t, k.Sequence)
	assert.Equal(t, "stalled", k.Sequence.Status)
	assert.Equal(t, 1, k.Sequence.Stalled)
	assert.Zero(t, k.Sequence.Gapped)
}

// No cache entry is the normal state in local dev and before the refresher's first run. It costs
// the column and nothing else — the page must not fail, and must not claim 'ok'.
func TestGetEdgeMulticast_SequenceAbsentWithoutL2Cache(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)

	resp := getEdgeMulticast(t, api)
	assert.Nil(t, resp.SequenceAsOf)
	assert.Nil(t, findEdgeMulticastGroup(t, resp, "edge-kalshi-sports-mbp").Sequence)
}
