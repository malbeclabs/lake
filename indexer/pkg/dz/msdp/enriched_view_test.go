package msdp

import (
	"fmt"
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnrichedView_MSDPPeers verifies that an MSDP peer row joins to the
// local device, and that the mesh-space peer_address resolves to a remote
// device via the dz_device_interface_ips lookup.
func TestEnrichedView_MSDPPeers(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, longitude, latitude)
		VALUES
			('metro-sea', now(), now(), generateUUIDv4(), 0, 1, 'metro-sea', 'sea', 'Seattle', 0, 0),
			('metro-nyc', now(), now(), generateUUIDv4(), 0, 2, 'metro-nyc', 'nyc', 'New York', 0, 0)
	`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
			('contrib-jump', now(), now(), generateUUIDv4(), 0, 1, 'contrib-jump', 'jump_', 'Jump Trading')
	`))

	// Two devices: local (sea) and remote (nyc). Remote has the peer_address on a Loopback.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('dev-sea', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'activated', 'edge', 'sea001-dz001', '63.243.225.62',
			 'contrib-jump', 'metro-sea', 0, '[]'),
			('dev-nyc', now(), now(), generateUUIDv4(), 0, 2,
			 'dev-nyc', 'activated', 'edge', 'nyc001-dz001', '169.150.226.117',
			 'contrib-jump', 'metro-nyc', 0,
			 '[{"name":"Loopback255","ip":"172.16.0.28/32","status":"activated"}]')
	`))

	// Force a sync refresh of the dz_device_interface_ips MV so the peer
	// address resolution finds dev-nyc's loopback.
	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_msdp_peers_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, peer_address, state, session_start_time, sa_count, reset_count)
		VALUES
			('peer-1', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', '172.16.0.28', 'established', now(), 5, 0)
	`))

	rows, err := conn.Query(ctx, `
		SELECT device_pk, device_code, metro_code, contributor_code,
			peer_address, peer_device_pk, peer_device_code, peer_interface_name,
			state, sa_count
		FROM enriched_ip_msdp_peers
		WHERE msdp_peer_entity_id = 'peer-1'
	`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "expected one enriched peer row")
	var devicePK, deviceCode, metroCode, contributorCode string
	var peerAddress, peerDevicePK, peerDeviceCode, peerInterfaceName string
	var state string
	var saCount int64
	require.NoError(t, rows.Scan(
		&devicePK, &deviceCode, &metroCode, &contributorCode,
		&peerAddress, &peerDevicePK, &peerDeviceCode, &peerInterfaceName,
		&state, &saCount,
	))
	assert.Equal(t, "dev-sea", devicePK)
	assert.Equal(t, "sea001-dz001", deviceCode)
	assert.Equal(t, "sea", metroCode)
	assert.Equal(t, "172.16.0.28", peerAddress)
	assert.Equal(t, "dev-nyc", peerDevicePK)
	assert.Equal(t, "nyc001-dz001", peerDeviceCode)
	assert.Equal(t, "Loopback255", peerInterfaceName)
	assert.Equal(t, "established", state)
	assert.EqualValues(t, 5, saCount)
}

// TestEnrichedView_MSDPSACache verifies the SA cache enrichment: local
// device join, remote device lookup via mesh address, group + publisher
// enrichment via source_address.
func TestEnrichedView_MSDPSACache(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, longitude, latitude)
		VALUES
			('metro-sea', now(), now(), generateUUIDv4(), 0, 1, 'metro-sea', 'sea', 'Seattle', 0, 0),
			('metro-nyc', now(), now(), generateUUIDv4(), 0, 2, 'metro-nyc', 'nyc', 'New York', 0, 0)
	`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
			('contrib-jump', now(), now(), generateUUIDv4(), 0, 1, 'contrib-jump', 'jump_', 'Jump Trading')
	`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('dev-sea', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'activated', 'edge', 'sea001-dz001', '', 'contrib-jump', 'metro-sea', 0, '[]'),
			('dev-nyc', now(), now(), generateUUIDv4(), 0, 2,
			 'dev-nyc', 'activated', 'edge', 'nyc001-dz001', '', 'contrib-jump', 'metro-nyc', 0,
			 '[{"name":"Loopback255","ip":"172.16.1.195/32","status":"activated"}]')
	`))

	// Force a sync refresh of the dz_device_interface_ips MV so the SA
	// remote_address resolution finds dev-nyc's loopback.
	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('grp-shreds', now(), now(), generateUUIDv4(), 0, 1,
			 'grp-shreds', 'owner-pub', 'edge-solana-shreds', '233.84.178.1', 100000000, 'activated', 1, 0)
	`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('user-pub', now(), now(), generateUUIDv4(), 0, 1,
			 'user-pub', 'pub-owner', 'activated', 'multicast', '203.0.113.10', '148.51.122.190', 'dev-sea', 'tenant-1', 0, '["grp-shreds"]', '[]')
	`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_msdp_sa_cache_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, group_address, source_address, remote_address, status, rp_address)
		VALUES
			('sa-1', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', '233.84.178.1', '148.51.122.190', '172.16.1.195', 'accepted', '10.0.0.0')
	`))

	rows, err := conn.Query(ctx, `
		SELECT device_pk, device_code,
			remote_address, remote_device_pk, remote_device_code,
			multicast_group_code, publisher_user_pk, publisher_device_code,
			accept_status, source_match_status
		FROM enriched_ip_msdp_sa_cache
		WHERE msdp_sa_entity_id = 'sa-1'
	`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "expected one enriched SA row")
	var devicePK, deviceCode, remoteAddress, remoteDevicePK, remoteDeviceCode string
	var groupCode, publisherUserPK, publisherDeviceCode, acceptStatus, sourceMatch string
	require.NoError(t, rows.Scan(
		&devicePK, &deviceCode,
		&remoteAddress, &remoteDevicePK, &remoteDeviceCode,
		&groupCode, &publisherUserPK, &publisherDeviceCode,
		&acceptStatus, &sourceMatch,
	))
	assert.Equal(t, "dev-sea", devicePK)
	assert.Equal(t, "172.16.1.195", remoteAddress)
	assert.Equal(t, "dev-nyc", remoteDevicePK)
	assert.Equal(t, "nyc001-dz001", remoteDeviceCode)
	assert.Equal(t, "edge-solana-shreds", groupCode)
	assert.Equal(t, "user-pub", publisherUserPK)
	assert.Equal(t, "sea001-dz001", publisherDeviceCode)
	assert.Equal(t, "accepted", acceptStatus)
	assert.Equal(t, "publisher_matched", sourceMatch)
}

// TestEnrichedView_MSDPJoinDoesNotExplode is the regression guard for
// the dz_device_interface_ips → materialized view conversion. With the
// previous plain VIEW definition, ClickHouse's optimizer allocated
// max-int hash tables when JOINing the IP-to-device lookup against MSDP
// peer/SA cache rows; selecting peer_device_pk / remote_device_pk would
// OOM with a 128 TiB allocation. Running the same JOIN under a tight
// max_memory_usage cap surfaces a regression to the plain-VIEW shape:
// the buggy plan trips the cap immediately, the MV-backed plan stays
// well under it.
func TestEnrichedView_MSDPJoinDoesNotExplode(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	// A handful of devices, one peer per device, all addresses resolvable.
	for i := 0; i < 10; i++ {
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO dim_dz_devices_history
				(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
				 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
			VALUES
				(?, now(), now(), generateUUIDv4(), 0, ?,
				 ?, 'activated', 'edge', ?, '', '', '', 0,
				 ?)
		`,
			fmt.Sprintf("dev-%d", i), uint64(i+1),
			fmt.Sprintf("dev-%d", i), fmt.Sprintf("d%d-dz001", i),
			fmt.Sprintf(`[{"name":"Loopback255","ip":"172.16.0.%d/32","status":"activated"}]`, i+1),
		))
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO dim_dz_ip_msdp_peers_history
				(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
				 device_pubkey, peer_address, state, session_start_time, sa_count, reset_count)
			VALUES
				(?, now(), now(), generateUUIDv4(), 0, ?,
				 ?, ?, 'established', now(), 0, 0)
		`,
			fmt.Sprintf("peer-%d", i), uint64(i+1),
			fmt.Sprintf("dev-%d", i),
			fmt.Sprintf("172.16.0.%d", ((i+1)%10)+1), // each peer points at the next device's loopback
		))
	}

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	// 256 MB is generous for 10 devices × 10 peers. The plain-VIEW shape
	// would attempt a 128 TiB allocation and trip this cap immediately;
	// the MV-backed shape stays well under it.
	rows, err := conn.Query(ctx, `
		SELECT count(), sum(if(peer_device_pk != '', 1, 0)) AS resolved
		FROM enriched_ip_msdp_peers
		SETTINGS max_memory_usage = 268435456
	`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var total, resolved uint64
	require.NoError(t, rows.Scan(&total, &resolved))
	assert.EqualValues(t, 10, total)
	assert.EqualValues(t, 10, resolved, "expected every peer_address to resolve to a peer_device_pk")
}
