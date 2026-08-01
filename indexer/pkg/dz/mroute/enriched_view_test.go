package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnrichedView_DeviceInterfaceIPs verifies that dz_device_interface_ips
// parses dz_devices_current.interfaces JSON into one row per (device, IP)
// with the CIDR mask stripped. This is the lookup view used by enriched
// MSDP/mroute joins for mesh-space → device mapping.
func TestEnrichedView_DeviceInterfaceIPs(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	err = conn.Exec(t.Context(), `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('dev-sea', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'activated', 'edge', 'sea001-dz001', '63.243.225.62', 'contrib-jump', 'metro-sea', 0,
			 '[{"name":"Loopback255","ip":"172.16.0.3/32","status":"activated"},{"name":"Port-Channel2000","ip":"172.16.0.10/31","status":"activated"}]')
	`)
	require.NoError(t, err)

	// dz_device_interface_ips is a refreshable MV (60s cadence in prod).
	// Force a sync refresh so the just-inserted device row is visible.
	require.NoError(t, conn.Exec(t.Context(), `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(t.Context(), `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(t.Context(), `
		SELECT device_pk, device_code, interface_name, ip_address
		FROM dz_device_interface_ips
		WHERE device_pk = 'dev-sea'
		ORDER BY interface_name
	`)
	require.NoError(t, err)
	defer rows.Close()

	type record struct {
		DevicePK, DeviceCode, InterfaceName, IPAddress string
	}
	var got []record
	for rows.Next() {
		var r record
		require.NoError(t, rows.Scan(&r.DevicePK, &r.DeviceCode, &r.InterfaceName, &r.IPAddress))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, got, 2)
	assert.Equal(t, "sea001-dz001", got[0].DeviceCode)
	assert.Equal(t, "Loopback255", got[0].InterfaceName)
	assert.Equal(t, "172.16.0.3", got[0].IPAddress) // CIDR stripped
	assert.Equal(t, "Port-Channel2000", got[1].InterfaceName)
	assert.Equal(t, "172.16.0.10", got[1].IPAddress)
}

// TestEnrichedView_IPMroute verifies that enriched_ip_mroute correctly joins
// an mroute entry to device/metro/contributor/group/publisher with every
// pubkey paired with its human-readable code.
func TestEnrichedView_IPMroute(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	ctx := t.Context()

	// Metro
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, longitude, latitude)
		VALUES
			('metro-sea', now(), now(), generateUUIDv4(), 0, 1, 'metro-sea', 'sea', 'Seattle', 0, 0)
	`))

	// Contributor
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
			('contrib-jump', now(), now(), generateUUIDv4(), 0, 1, 'contrib-jump', 'jump_', 'Jump Trading')
	`))

	// Device — RP that owns the source address as one of its interface IPs
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('dev-sea', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'activated', 'edge', 'sea001-dz001', '63.243.225.62', 'contrib-jump', 'metro-sea', 0,
			 '[]')
	`))

	// Multicast group
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('grp-shreds', now(), now(), generateUUIDv4(), 0, 1,
			 'grp-shreds', 'owner-pub', 'edge-solana-shreds', '233.84.178.1', 100000000, 'activated', 1, 0)
	`))

	// Publisher user attached to the device, whose dz_ip is the mroute source
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('user-pub', now(), now(), generateUUIDv4(), 0, 1,
			 'user-pub', 'pub-owner', 'activated', 'multicast', '203.0.113.10', '148.51.122.190', 'dev-sea', 'tenant-1', 0, '["grp-shreds"]', '[]')
	`))

	// Mroute entry
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			('mroute-1', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'default', 'sparse', '233.84.178.1', '148.51.122.190',
			 'SBNP', 0, 'Tunnel501', '', '', 0, 0, '', 0, 0,
			 '["Switch1/11/1","Port-Channel1000.2035"]', 2, now())
	`))

	rows, err := conn.Query(ctx, `
		SELECT
			device_pk, device_code, metro_code, contributor_code,
			multicast_group_pk, multicast_group_code,
			publisher_user_pk, publisher_device_code, publisher_metro_code, publisher_contributor_code,
			source_match_status
		FROM enriched_ip_mroute
		WHERE mroute_entity_id = 'mroute-1'
	`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "expected one enriched row")
	var devicePK, deviceCode, metroCode, contributorCode string
	var groupPK, groupCode string
	var pubUserPK, pubDeviceCode, pubMetroCode, pubContributorCode string
	var sourceMatch string
	require.NoError(t, rows.Scan(
		&devicePK, &deviceCode, &metroCode, &contributorCode,
		&groupPK, &groupCode,
		&pubUserPK, &pubDeviceCode, &pubMetroCode, &pubContributorCode,
		&sourceMatch,
	))

	assert.Equal(t, "dev-sea", devicePK)
	assert.Equal(t, "sea001-dz001", deviceCode)
	assert.Equal(t, "sea", metroCode)
	assert.Equal(t, "jump_", contributorCode)
	assert.Equal(t, "grp-shreds", groupPK)
	assert.Equal(t, "edge-solana-shreds", groupCode)
	assert.Equal(t, "user-pub", pubUserPK)
	assert.Equal(t, "sea001-dz001", pubDeviceCode)
	assert.Equal(t, "sea", pubMetroCode)
	assert.Equal(t, "jump_", pubContributorCode)
	assert.Equal(t, "publisher_matched", sourceMatch)
}
