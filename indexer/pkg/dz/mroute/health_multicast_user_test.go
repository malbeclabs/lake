package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthMulticastUser_Reconciliation exercises the per-user
// onchain ↔ dataplane reconciliation. Three users:
//   - u-pub-healthy: publisher whose Tunnel<N> appears as RPF interface on
//     an mroute for the group → healthy
//   - u-sub-healthy: subscriber whose Tunnel<N> appears in OIF list of an
//     mroute for the group → healthy
//   - u-pub-silent: publisher whose Tunnel<N> is NOT present anywhere on
//     the device → unhealthy with a specific mismatch_reason
func TestHealthMulticastUser_Reconciliation(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES ('d-sea', now(), now(), generateUUIDv4(), 0, 1,
			'd-sea', 'activated', 'edge', 'sea001-dz001', '', '', '', 0, '[]'),
			('d-nyc', now(), now(), generateUUIDv4(), 0, 2,
			'd-nyc', 'activated', 'edge', 'nyc001-dz001', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-r', now(), now(), generateUUIDv4(), 0, 1,
			'grp-r', 'owner', 'test-recon', '233.99.99.2', 100000000, 'activated', 2, 1)`))

	// Three multicast users for grp-r
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('u-pub-healthy', now(), now(), generateUUIDv4(), 0, 1, 'u-pub-healthy', 'o', 'activated', 'multicast', '203.0.113.10', '10.99.0.10', 'd-sea', 't1', 503, '["grp-r"]', '[]'),
			('u-pub-silent',  now(), now(), generateUUIDv4(), 0, 2, 'u-pub-silent',  'o', 'activated', 'multicast', '203.0.113.11', '10.99.0.11', 'd-sea', 't1', 504, '["grp-r"]', '[]'),
			('u-sub-healthy', now(), now(), generateUUIDv4(), 0, 3, 'u-sub-healthy', 'o', 'activated', 'multicast', '203.0.113.12', '10.99.0.12', 'd-nyc', 't1', 602, '[]', '["grp-r"]')`))

	// FHR mroute for u-pub-healthy — its tunnel appears as RPF interface
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-fhr-r', now(), now(), generateUUIDv4(), 0, 1,
			'd-sea', 'default', 'sparse', '233.99.99.2', '10.99.0.10',
			'SBNP', 0, 'Tunnel503', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// LHR mroute on d-nyc — u-sub-healthy's tunnel in OIF list
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-lhr-r', now(), now(), generateUUIDv4(), 0, 1,
			'd-nyc', 'default', 'sparse', '233.99.99.2', '10.99.0.10',
			'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel602"]', 1, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT user_pk, mode, publisher_iif_observed, subscriber_oif_observed, reconciled, health_status, mismatch_reason
		FROM health_multicast_user
		WHERE multicast_group_pk = 'grp-r'
		ORDER BY user_pk`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		userPK, mode, mismatchReason, healthStatus string
		pubIIF, subOIF, reconciled                 bool
	}
	got := []row{}
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.userPK, &r.mode, &r.pubIIF, &r.subOIF, &r.reconciled, &r.healthStatus, &r.mismatchReason))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)

	// u-pub-healthy
	assert.Equal(t, "u-pub-healthy", got[0].userPK)
	assert.Equal(t, "P", got[0].mode)
	assert.True(t, got[0].pubIIF)
	assert.True(t, got[0].reconciled)
	assert.Equal(t, "healthy", got[0].healthStatus)

	// u-pub-silent
	assert.Equal(t, "u-pub-silent", got[1].userPK)
	assert.Equal(t, "P", got[1].mode)
	assert.False(t, got[1].pubIIF, "Tunnel504 isn't on any mroute → IIF not observed")
	assert.False(t, got[1].reconciled)
	assert.Equal(t, "unhealthy", got[1].healthStatus)
	assert.Contains(t, got[1].mismatchReason, "Tunnel504")
	assert.Contains(t, got[1].mismatchReason, "RPF interface")

	// u-sub-healthy
	assert.Equal(t, "u-sub-healthy", got[2].userPK)
	assert.Equal(t, "S", got[2].mode)
	assert.True(t, got[2].subOIF)
	assert.True(t, got[2].reconciled)
	assert.Equal(t, "healthy", got[2].healthStatus)
}

// TestHealthMulticastUser_BgpDownIsDisconnected verifies that a publisher whose
// onchain BGP session is down is classified 'disconnected' (not 'unhealthy'):
// with no session there is no (S,G)/RPF entry, so the absence is expected, not a
// forwarding fault. A BGP-up publisher with the same missing RPF entry stays
// 'unhealthy'.
func TestHealthMulticastUser_BgpDownIsDisconnected(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES ('d-bgp', now(), now(), generateUUIDv4(), 0, 1,
			'd-bgp', 'activated', 'edge', 'bgp001-dz001', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-bgp', now(), now(), generateUUIDv4(), 0, 1,
			'grp-bgp', 'owner', 'test-bgp', '233.99.99.9', 100000000, 'activated', 2, 0)`))

	// Two publishers, neither with any mroute. One BGP-down, one BGP-up.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES
			('u-bgp-down', now(), now(), generateUUIDv4(), 0, 1, 'u-bgp-down', 'o', 'activated', 'multicast', '203.0.113.20', '10.99.9.20', 'd-bgp', 't1', 701, '["grp-bgp"]', '[]', 'down'),
			('u-bgp-up',   now(), now(), generateUUIDv4(), 0, 2, 'u-bgp-up',   'o', 'activated', 'multicast', '203.0.113.21', '10.99.9.21', 'd-bgp', 't1', 702, '["grp-bgp"]', '[]', 'up')`))

	rows, err := conn.Query(ctx, `
		SELECT user_pk, health_status, mismatch_reason
		FROM health_multicast_user
		WHERE multicast_group_pk = 'grp-bgp'
		ORDER BY user_pk`)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][2]string{}
	for rows.Next() {
		var pk, hs, mr string
		require.NoError(t, rows.Scan(&pk, &hs, &mr))
		got[pk] = [2]string{hs, mr}
	}
	require.NoError(t, rows.Err())

	require.Contains(t, got, "u-bgp-down")
	assert.Equal(t, "disconnected", got["u-bgp-down"][0])
	assert.Contains(t, got["u-bgp-down"][1], "BGP session down")

	require.Contains(t, got, "u-bgp-up")
	assert.Equal(t, "unhealthy", got["u-bgp-up"][0], "BGP up but no RPF entry is still a real fault")
}

// TestHealthMulticastUser_PartialDelivery covers the multi-publisher case:
// a subscriber whose Tunnel<N> is in the OIF list of one publisher's (S, G)
// mroute but missing from another publisher's (S, G) mroute. The aggregated
// view would have called this "reconciled"; the per-source view degrades it.
//
// Setup at d-lhr:
//   - (S=10.99.5.10, G=233.99.99.5) → OIF includes Tunnel610  ← subscriber present
//   - (S=10.99.5.20, G=233.99.99.5) → OIF does NOT include Tunnel610  ← gap
//
// Expected result for u-sub-partial:
//
//	subscriber_total_sources       = 2
//	subscriber_oif_present_sources = 1
//	health_status                  = degraded
//	mismatch_reason contains "1 of 2"
func TestHealthMulticastUser_PartialDelivery(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES ('d-lhr-pd', now(), now(), generateUUIDv4(), 0, 1,
			'd-lhr-pd', 'activated', 'edge', 'lhr-pd-dz1', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-pd', now(), now(), generateUUIDv4(), 0, 1,
			'grp-pd', 'o', 'partial-delivery', '233.99.99.5', 100000000, 'activated', 2, 1)`))

	// Two publishers and a subscriber on the LHR. Publishers don't need to
	// be onchain users at d-lhr — what matters is their source_address
	// appearing in the mroute table on d-lhr's perspective.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES ('u-sub-partial', now(), now(), generateUUIDv4(), 0, 1,
			'u-sub-partial', 'o', 'activated', 'multicast', '203.0.113.50', '10.99.5.50', 'd-lhr-pd', 't1', 610, '[]', '["grp-pd"]')`))

	// (S=10.99.5.10, G=233.99.99.5) — subscriber's tunnel IS in OIF
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-pd-1', now(), now(), generateUUIDv4(), 0, 1,
			'd-lhr-pd', 'default', 'sparse', '233.99.99.5', '10.99.5.10',
			'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel610"]', 1, now())`))

	// (S=10.99.5.20, G=233.99.99.5) — subscriber's tunnel is NOT in OIF
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-pd-2', now(), now(), generateUUIDv4(), 0, 2,
			'd-lhr-pd', 'default', 'sparse', '233.99.99.5', '10.99.5.20',
			'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '[]', 0, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT
			subscriber_total_sources,
			subscriber_oif_present_sources,
			subscriber_oif_observed,
			reconciled,
			health_status,
			mismatch_reason
		FROM health_multicast_user
		WHERE user_pk = 'u-sub-partial' AND multicast_group_pk = 'grp-pd'`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "expected one row for u-sub-partial")

	var totalSources uint64
	var oifPresent uint32
	var oifObserved, reconciled bool
	var healthStatus, mismatchReason string
	require.NoError(t, rows.Scan(&totalSources, &oifPresent, &oifObserved, &reconciled, &healthStatus, &mismatchReason))

	assert.EqualValues(t, 2, totalSources, "two (S, G) mroutes on device")
	assert.EqualValues(t, 1, oifPresent, "tunnel present for one of the two sources")
	assert.False(t, oifObserved, "partial coverage is NOT 'oif observed'")
	assert.False(t, reconciled, "partial coverage is NOT reconciled")
	assert.Equal(t, "degraded", healthStatus, "partial coverage → degraded, not unhealthy or healthy")
	assert.Contains(t, mismatchReason, "1 of 2", "reason should call out the partial coverage count")
	assert.Contains(t, mismatchReason, "Tunnel610")
}
