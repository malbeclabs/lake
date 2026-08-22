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

// The DZD column reads the device's own view of a publisher's BGP session. What is worth testing
// here is the join, because it does not go through a pk: the device names the peer
// `USER-<tunnel_id>` in a free-text description, and that string is the whole key.

// createBGPNeighborsTable creates the telemetry mirror table the DZD column reads. Tests default to
// the mainnet environment, so the database name is fixed.
func createBGPNeighborsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, "CREATE DATABASE IF NOT EXISTS `telemetry_mainnet_beta`"))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+"`telemetry_mainnet_beta`"+`.bgp_neighbors_latest (
			timestamp DateTime64(9),
			device_pubkey LowCardinality(String),
			network_instance LowCardinality(String),
			neighbor_address String,
			description String,
			peer_as UInt32,
			local_as UInt32,
			peer_type LowCardinality(String),
			session_state LowCardinality(String),
			established_transitions UInt64,
			last_established Int64,
			messages_received_update UInt64,
			messages_sent_update UInt64
		) ENGINE = MergeTree ORDER BY (device_pubkey, neighbor_address)`))
	// context.Background(), not t.Context(): the test context is already cancelled by the time
	// cleanups run, so a DROP on it silently does nothing and the table leaks into the next test
	// — which is exactly what the absent-telemetry case must not inherit. The error is checked
	// for the same reason.
	t.Cleanup(func() {
		require.NoError(t, api.DB.Exec(context.Background(),
			"DROP TABLE IF EXISTS `telemetry_mainnet_beta`.bgp_neighbors_latest"))
	})
}

func insertBGPNeighbor(t *testing.T, api *handlers.API, devicePK, vrf, peerType, description, state string, flaps uint64, establishedAgo time.Duration) {
	t.Helper()
	established := int64(0)
	if establishedAgo > 0 {
		established = time.Now().Add(-establishedAgo).UnixNano()
	}
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO `+"`telemetry_mainnet_beta`"+`.bgp_neighbors_latest
			(timestamp, device_pubkey, network_instance, neighbor_address, description,
			 peer_as, local_as, peer_type, session_state, established_transitions, last_established,
			 messages_received_update, messages_sent_update)
		VALUES (now64(9), '%s', '%s', '169.254.0.1', '%s', 65000, 65342, '%s', '%s', %d, %d, 0, 0)`,
		devicePK, vrf, description, peerType, state, flaps, established)))
}

// The happy path and the two ways the join must not fire: a peer in another network instance, and
// a description that is not a user session at all.
func TestGetEdgeMulticast_DZDSessionJoinsOnTheDeviceDescription(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")  // dev-ams1, tunnel 509
	insertEdgeMulticastCapturePublisher2(t, api, "group-k") // dev-ams1, tunnel 510
	createBGPNeighborsTable(t, api)

	insertBGPNeighbor(t, api, "dev-ams1", "vrf1", "EXTERNAL", "USER-509", "ESTABLISHED", 17, 3*time.Hour)
	// Same device and tunnel number, wrong network instance: the fabric's own peers must never
	// be read as a customer's session.
	insertBGPNeighbor(t, api, "dev-ams1", "default", "INTERNAL", "USER-510", "ESTABLISHED", 2, time.Hour)
	// A description that is not a user session. toInt32OrNull drops it rather than matching a
	// tunnel by accident.
	insertBGPNeighbor(t, api, "dev-ams1", "vrf1", "EXTERNAL", "peering-partner", "ESTABLISHED", 1, time.Hour)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	byTunnel := map[int32]handlers.EdgeMulticastPublisher{}
	for _, l := range g.PublisherLines {
		byTunnel[l.TunnelID] = l
	}

	joined := byTunnel[509]
	require.NotNil(t, joined.BGPSession, "USER-509 on dev-ams1 is this line's session")
	assert.Equal(t, "ESTABLISHED", joined.BGPSession.State)
	assert.EqualValues(t, 17, joined.BGPSession.Flaps)
	require.NotNil(t, joined.BGPSession.EstablishedAt)
	assert.WithinDuration(t, time.Now().Add(-3*time.Hour), *joined.BGPSession.EstablishedAt, time.Minute)

	assert.Nil(t, byTunnel[510].BGPSession,
		"the row for tunnel 510 is an INTERNAL fabric peer in another network instance")
}

// A session the device has never brought up must not render as one established at the Unix epoch.
func TestGetEdgeMulticast_DZDNeverEstablishedHasNoTimestamp(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	createBGPNeighborsTable(t, api)

	insertBGPNeighbor(t, api, "dev-ams1", "vrf1", "EXTERNAL", "USER-509", "ACTIVE", 0, 0)

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.Len(t, g.PublisherLines, 1)
	session := g.PublisherLines[0].BGPSession
	require.NotNil(t, session)
	assert.Equal(t, "ACTIVE", session.State)
	assert.Nil(t, session.EstablishedAt, "zero is 'never', not 1970")
}

// The telemetry mirror is absent in local dev and in most tests. That must cost the column and
// nothing else — every other signal on the page still renders.
func TestGetEdgeMulticast_DZDAbsentTelemetryCostsOnlyTheColumn(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	insertEdgeMulticastCapturePublisher(t, api, "group-k")

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.Len(t, g.PublisherLines, 1)
	assert.Nil(t, g.PublisherLines[0].BGPSession)
	assert.NotEmpty(t, g.PublisherLines[0].Health, "the rest of the line is unaffected")
	assert.NotEmpty(t, g.PublisherLines[0].DeviceCode)
}

// Selection and display are two different orderings: worst-first chooses which lines survive the
// cap, client IP is how the survivors are read. Both have to hold at once.
func TestGetEdgeMulticast_PublisherLinesReadByClientIP(t *testing.T) {
	api := newEdgeMulticastTestAPI(t)
	insertMulticastTestData(t, api)
	insertEdgeMulticastCaptureGroups(t, api)
	// 10.0.0.9 is the healthy one and 10.0.0.10 is idle, so worst-first would put .10 on top.
	// Address order is the opposite, and .10 sorting before .9 is also what a string compare
	// would get wrong.
	insertEdgeMulticastCapturePublisher(t, api, "group-k")
	insertEdgeMulticastCapturePublisher2(t, api, "group-k")
	require.NoError(t, api.DB.Exec(t.Context(), `
		ALTER TABLE device_interface_rollup_5m UPDATE max_out_bps = 0
		WHERE user_pk = 'user-kpub2' SETTINGS mutations_sync = 2`))

	g := findEdgeMulticastGroup(t, getEdgeMulticast(t, api), "edge-kalshi-sports-mbp")
	require.Len(t, g.PublisherLines, 2)
	assert.Equal(t, []string{"10.0.0.9", "10.0.0.10"},
		[]string{g.PublisherLines[0].ClientIP, g.PublisherLines[1].ClientIP},
		"read in address order: .9 before .10, which a string compare reverses")
}
