package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthMulticastUserRate exercises the rate-reconciliation view across
// the three publisher rate states (active / idle / no_data) and the four
// subscriber reasons (reconciled / mismatch / monitoring_gap / group_idle).
// Three groups are set up:
//
//   grp-rate-clean — full counter data; one active publisher, one matched
//     subscriber, one mismatched subscriber.
//   grp-rate-gap — publisher has no counter row in the freshness window;
//     subscriber's rate goes to unknown via monitoring_gap.
//   grp-rate-idle — publisher transmits zero; subscriber's rate goes to
//     unknown via group_idle.
//
// The combined health_status column is verified against the rollup matrix.
func TestHealthMulticastUserRate(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	// Devices
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('d-fhr', now(), now(), generateUUIDv4(), 0, 1, 'd-fhr', 'activated', 'edge', 'fhr-dz1', '', '', '', 0, '[]'),
			('d-lhr', now(), now(), generateUUIDv4(), 0, 2, 'd-lhr', 'activated', 'edge', 'lhr-dz1', '', '', '', 0, '[]')`))

	// Groups
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('grp-rate-clean', now(), now(), generateUUIDv4(), 0, 1, 'grp-rate-clean', 'o', 'rate-clean', '233.99.1.1', 100000000, 'activated', 1, 2),
			('grp-rate-gap',   now(), now(), generateUUIDv4(), 0, 2, 'grp-rate-gap',   'o', 'rate-gap',   '233.99.1.2', 100000000, 'activated', 1, 1),
			('grp-rate-idle',  now(), now(), generateUUIDv4(), 0, 3, 'grp-rate-idle',  'o', 'rate-idle',  '233.99.1.3', 100000000, 'activated', 1, 1)`))

	// Users
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			-- grp-rate-clean: one active publisher at 10 Mbps, two subscribers (one matched, one mismatched).
			('u-pub-active', now(), now(), generateUUIDv4(), 0, 1, 'u-pub-active', 'o', 'activated', 'multicast', '203.0.113.10', '10.99.1.10', 'd-fhr', 't1', 701, '["grp-rate-clean"]', '[]'),
			('u-sub-recon',  now(), now(), generateUUIDv4(), 0, 2, 'u-sub-recon',  'o', 'activated', 'multicast', '203.0.113.11', '10.99.1.11', 'd-lhr', 't1', 801, '[]', '["grp-rate-clean"]'),
			('u-sub-mis',    now(), now(), generateUUIDv4(), 0, 3, 'u-sub-mis',    'o', 'activated', 'multicast', '203.0.113.12', '10.99.1.12', 'd-lhr', 't1', 802, '[]', '["grp-rate-clean"]'),
			-- grp-rate-gap: publisher will have no counter row.
			('u-pub-nodata', now(), now(), generateUUIDv4(), 0, 4, 'u-pub-nodata', 'o', 'activated', 'multicast', '203.0.113.13', '10.99.1.13', 'd-fhr', 't1', 702, '["grp-rate-gap"]', '[]'),
			('u-sub-gap',    now(), now(), generateUUIDv4(), 0, 5, 'u-sub-gap',    'o', 'activated', 'multicast', '203.0.113.14', '10.99.1.14', 'd-lhr', 't1', 803, '[]', '["grp-rate-gap"]'),
			-- grp-rate-idle: publisher has counter row with 0 bps.
			('u-pub-idle',   now(), now(), generateUUIDv4(), 0, 6, 'u-pub-idle',   'o', 'activated', 'multicast', '203.0.113.15', '10.99.1.15', 'd-fhr', 't1', 703, '["grp-rate-idle"]', '[]'),
			('u-sub-idle',   now(), now(), generateUUIDv4(), 0, 7, 'u-sub-idle',   'o', 'activated', 'multicast', '203.0.113.16', '10.99.1.16', 'd-lhr', 't1', 804, '[]', '["grp-rate-idle"]')`))

	// Mroute entries — control plane is healthy for every (user, group) pair below.
	// FHR mroutes — publisher tunnel as RPF interface.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			('mr-fhr-clean', now(), now(), generateUUIDv4(), 0, 1, 'd-fhr', 'default', 'sparse', '233.99.1.1', '10.99.1.10', 'SBNP', 0, 'Tunnel701', '', '', 0, 0, '', 0, 0, '', 0, now()),
			('mr-fhr-gap',   now(), now(), generateUUIDv4(), 0, 2, 'd-fhr', 'default', 'sparse', '233.99.1.2', '10.99.1.13', 'SBNP', 0, 'Tunnel702', '', '', 0, 0, '', 0, 0, '', 0, now()),
			('mr-fhr-idle',  now(), now(), generateUUIDv4(), 0, 3, 'd-fhr', 'default', 'sparse', '233.99.1.3', '10.99.1.15', 'SBNP', 0, 'Tunnel703', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// LHR mroutes — subscriber tunnel in OIF list.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			('mr-lhr-recon', now(), now(), generateUUIDv4(), 0, 1, 'd-lhr', 'default', 'sparse', '233.99.1.1', '10.99.1.10', 'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel801"]', 1, now()),
			('mr-lhr-mis',   now(), now(), generateUUIDv4(), 0, 2, 'd-lhr', 'default', 'sparse', '233.99.1.1', '10.99.1.10', 'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel802"]', 1, now()),
			('mr-lhr-gap',   now(), now(), generateUUIDv4(), 0, 3, 'd-lhr', 'default', 'sparse', '233.99.1.2', '10.99.1.13', 'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel803"]', 1, now()),
			('mr-lhr-idle',  now(), now(), generateUUIDv4(), 0, 4, 'd-lhr', 'default', 'sparse', '233.99.1.3', '10.99.1.15', 'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel804"]', 1, now())`))

	// Counter rollup rows. NOTE: u-pub-nodata / Tunnel702 deliberately omitted.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES
			-- grp-rate-clean: publisher RX = 10 Mbps; matched subscriber TX = 10 Mbps; mismatched subscriber TX = 50 Mbps.
			(now() - INTERVAL 1 MINUTE, 'd-fhr', 'Tunnel701', 701, 'u-pub-active', 10000000, 0, now()),
			(now() - INTERVAL 1 MINUTE, 'd-lhr', 'Tunnel801', 801, 'u-sub-recon',  0, 10000000, now()),
			(now() - INTERVAL 1 MINUTE, 'd-lhr', 'Tunnel802', 802, 'u-sub-mis',    0, 50000000, now()),
			-- grp-rate-gap: subscriber has data; publisher does not.
			(now() - INTERVAL 1 MINUTE, 'd-lhr', 'Tunnel803', 803, 'u-sub-gap',    0, 5000000,  now()),
			-- grp-rate-idle: publisher and subscriber both report 0 bps.
			(now() - INTERVAL 1 MINUTE, 'd-fhr', 'Tunnel703', 703, 'u-pub-idle',   0, 0, now()),
			(now() - INTERVAL 1 MINUTE, 'd-lhr', 'Tunnel804', 804, 'u-sub-idle',   0, 0, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT
			user_pk, mode, multicast_group_pk,
			control_plane_status,
			rate_status, rate_status_reason,
			observed_bps_5m, expected_bps_5m,
			health_status
		FROM health_multicast_user_rate
		WHERE multicast_group_pk LIKE 'grp-rate-%'
		ORDER BY multicast_group_pk, user_pk`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		userPK, mode, group              string
		cp, rate, reason, combined       string
		observedBps, expectedBps         *float64
	}
	got := []row{}
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(
			&r.userPK, &r.mode, &r.group,
			&r.cp, &r.rate, &r.reason,
			&r.observedBps, &r.expectedBps,
			&r.combined,
		))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	byKey := map[string]row{}
	for _, r := range got {
		byKey[r.userPK] = r
	}

	// grp-rate-clean: u-pub-active is transmitting → active → reconciled.
	pub := byKey["u-pub-active"]
	assert.Equal(t, "healthy", pub.cp, "publisher CP healthy")
	assert.Equal(t, "active", pub.reason)
	assert.Equal(t, "reconciled", pub.rate)
	require.NotNil(t, pub.observedBps)
	assert.InDelta(t, 10000000, *pub.observedBps, 1)
	assert.Equal(t, "healthy", pub.combined)

	// u-sub-recon: 10 Mbps matches sum-of-publishers = 10 Mbps → reconciled.
	subR := byKey["u-sub-recon"]
	assert.Equal(t, "reconciled", subR.rate)
	assert.Equal(t, "reconciled", subR.reason)
	require.NotNil(t, subR.observedBps)
	require.NotNil(t, subR.expectedBps)
	assert.InDelta(t, 10000000, *subR.observedBps, 1)
	assert.InDelta(t, 10000000, *subR.expectedBps, 1)
	assert.Equal(t, "healthy", subR.combined)

	// u-sub-mis: 50 Mbps vs expected 10 Mbps → mismatch.
	subM := byKey["u-sub-mis"]
	assert.Equal(t, "mismatch", subM.rate)
	assert.Equal(t, "mismatch", subM.reason)
	require.NotNil(t, subM.observedBps)
	assert.InDelta(t, 50000000, *subM.observedBps, 1)
	// CP healthy + rate mismatch → combined degraded.
	assert.Equal(t, "degraded", subM.combined)

	// grp-rate-gap: publisher has no rate row.
	pubNoData := byKey["u-pub-nodata"]
	assert.Equal(t, "no_data", pubNoData.reason)
	assert.Equal(t, "unknown", pubNoData.rate)
	assert.Nil(t, pubNoData.observedBps, "no observed rate when no counter row")
	assert.Equal(t, "unknown", pubNoData.combined)

	// u-sub-gap: subscriber has data but expected is NULL (one+ publisher no_data) → monitoring_gap.
	subGap := byKey["u-sub-gap"]
	assert.Equal(t, "monitoring_gap", subGap.reason)
	assert.Equal(t, "unknown", subGap.rate)
	assert.Nil(t, subGap.expectedBps, "expected NULL when group has no_data publishers")
	assert.Equal(t, "unknown", subGap.combined)

	// grp-rate-idle: publisher RX = 0 → idle.
	pubIdle := byKey["u-pub-idle"]
	assert.Equal(t, "idle", pubIdle.reason)
	assert.Equal(t, "unknown", pubIdle.rate)
	require.NotNil(t, pubIdle.observedBps)
	assert.InDelta(t, 0, *pubIdle.observedBps, 1)
	assert.Equal(t, "unknown", pubIdle.combined)

	// u-sub-idle: expected = 0 (publisher present but transmitting 0) → group_idle.
	subIdle := byKey["u-sub-idle"]
	assert.Equal(t, "group_idle", subIdle.reason)
	assert.Equal(t, "unknown", subIdle.rate)
	require.NotNil(t, subIdle.expectedBps)
	assert.InDelta(t, 0, *subIdle.expectedBps, 1)
	assert.Equal(t, "unknown", subIdle.combined)
}
