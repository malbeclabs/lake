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
//	grp-rate-clean — full counter data; one active publisher, one matched
//	  subscriber, one mismatched subscriber.
//	grp-rate-gap — publisher has no counter row in the freshness window;
//	  subscriber's rate goes to unknown via monitoring_gap.
//	grp-rate-idle — publisher transmits zero; subscriber's rate goes to
//	  unknown via group_idle.
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
		userPK, mode, group        string
		cp, rate, reason, combined string
		observedBps, expectedBps   *float64
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

// TestHealthMulticastUserRate_PlusSubscriber covers a dual-role (P+S) user.
// The P+S user publishes at 5 Mbps and is also a subscriber to the same
// group; the group has one other publisher transmitting at 5 Mbps too.
// Group total = 10 Mbps. The P+S user's expected subscriber rate excludes
// its own contribution, so expected = 10M - 5M = 5M. Their subscriber TX
// is 5 Mbps → reconciled.
func TestHealthMulticastUserRate_PlusSubscriber(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('d-ps-fhr', now(), now(), generateUUIDv4(), 0, 1, 'd-ps-fhr', 'activated', 'edge', 'ps-fhr-dz1', '', '', '', 0, '[]'),
			('d-ps-mid', now(), now(), generateUUIDv4(), 0, 2, 'd-ps-mid', 'activated', 'edge', 'ps-mid-dz1', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-ps', now(), now(), generateUUIDv4(), 0, 1, 'grp-ps', 'o', 'rate-ps', '233.99.2.1', 100000000, 'activated', 2, 1)`))

	// u-ps is publisher + subscriber; u-pub-other contributes the other half.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('u-ps',        now(), now(), generateUUIDv4(), 0, 1, 'u-ps',        'o', 'activated', 'multicast', '203.0.114.1', '10.99.2.1', 'd-ps-mid', 't1', 901, '["grp-ps"]', '["grp-ps"]'),
			('u-pub-other', now(), now(), generateUUIDv4(), 0, 2, 'u-pub-other', 'o', 'activated', 'multicast', '203.0.114.2', '10.99.2.2', 'd-ps-fhr', 't1', 902, '["grp-ps"]', '[]')`))

	// CP healthy: mroute on u-ps's device shows their tunnel both as RPF (publisher)
	// and in OIF (subscriber); u-pub-other's device shows their RPF.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			-- u-ps is both publisher and subscriber on d-ps-mid: RPF=Tunnel901 (pub),
			-- and OIF contains Tunnel901 reflecting OTHER publisher's traffic to it.
			('mr-ps-pub',   now(), now(), generateUUIDv4(), 0, 1, 'd-ps-mid', 'default', 'sparse', '233.99.2.1', '10.99.2.1', 'SBNP', 0, 'Tunnel901',     '', '', 0, 0, '', 0, 0, '', 0, now()),
			('mr-ps-sub',   now(), now(), generateUUIDv4(), 0, 2, 'd-ps-mid', 'default', 'sparse', '233.99.2.1', '10.99.2.2', 'SMP',  0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel901"]', 1, now()),
			('mr-other-pub',now(), now(), generateUUIDv4(), 0, 3, 'd-ps-fhr', 'default', 'sparse', '233.99.2.1', '10.99.2.2', 'SBNP', 0, 'Tunnel902',     '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// Counter rows: u-ps publishing at 5 Mbps (max_in) AND receiving 5 Mbps (max_out, == other publisher's RX).
	// u-pub-other publishing at 5 Mbps.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES
			(now() - INTERVAL 1 MINUTE, 'd-ps-mid', 'Tunnel901', 901, 'u-ps',        5000000, 5000000, now()),
			(now() - INTERVAL 1 MINUTE, 'd-ps-fhr', 'Tunnel902', 902, 'u-pub-other', 5000000, 0,       now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT user_pk, mode, rate_status, rate_status_reason, observed_bps_5m, expected_bps_5m
		FROM health_multicast_user_rate
		WHERE multicast_group_pk = 'grp-ps' AND user_pk = 'u-ps'`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "expected one row for u-ps")
	var userPK, mode, rate, reason string
	var observed, expected *float64
	require.NoError(t, rows.Scan(&userPK, &mode, &rate, &reason, &observed, &expected))
	require.NoError(t, rows.Err())

	assert.Equal(t, "P+S", mode)
	assert.Equal(t, "reconciled", reason, "P+S subscriber side reconciles against group_total minus self")
	assert.Equal(t, "reconciled", rate)
	require.NotNil(t, observed)
	require.NotNil(t, expected)
	// Observed = max_out_bps = 5 Mbps; expected = group_total (10M) - self.max_in_bps (5M) = 5M.
	assert.InDelta(t, 5000000, *observed, 1)
	assert.InDelta(t, 5000000, *expected, 1)
}

// TestHealthMulticastUserRate_TunnelReuse verifies the user_pk component
// of the join prevents misattribution when the same tunnel ID is bound to
// two different user_pks on the same device within the freshness window
// (e.g. a tunnel reassigned mid-bucket).
func TestHealthMulticastUserRate_TunnelReuse(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES ('d-reuse', now(), now(), generateUUIDv4(), 0, 1, 'd-reuse', 'activated', 'edge', 'reuse-dz1', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-reuse', now(), now(), generateUUIDv4(), 0, 1, 'grp-reuse', 'o', 'reuse', '233.99.3.1', 100000000, 'activated', 2, 0)`))

	// Two publisher users on the SAME device with the SAME tunnel id (777)
	// but different user_pks.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('u-A', now(), now(), generateUUIDv4(), 0, 1, 'u-A', 'o', 'activated', 'multicast', '203.0.115.1', '10.99.3.1', 'd-reuse', 't1', 777, '["grp-reuse"]', '[]'),
			('u-B', now(), now(), generateUUIDv4(), 0, 2, 'u-B', 'o', 'activated', 'multicast', '203.0.115.2', '10.99.3.2', 'd-reuse', 't1', 777, '["grp-reuse"]', '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			('mr-reuse-A', now(), now(), generateUUIDv4(), 0, 1, 'd-reuse', 'default', 'sparse', '233.99.3.1', '10.99.3.1', 'SBNP', 0, 'Tunnel777', '', '', 0, 0, '', 0, 0, '', 0, now()),
			('mr-reuse-B', now(), now(), generateUUIDv4(), 0, 2, 'd-reuse', 'default', 'sparse', '233.99.3.1', '10.99.3.2', 'SBNP', 0, 'Tunnel777', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// Two rollup rows in the same window: tunnel 777 bound to u-A at 1 Mbps,
	// then to u-B at 99 Mbps. Without user_pk in the join, the same row
	// would be picked for both users.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES
			(now() - INTERVAL 2 MINUTE, 'd-reuse', 'Tunnel777', 777, 'u-A',  1000000, 0, now()),
			(now() - INTERVAL 1 MINUTE, 'd-reuse', 'Tunnel777', 777, 'u-B', 99000000, 0, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT user_pk, observed_bps_5m
		FROM health_multicast_user_rate
		WHERE multicast_group_pk = 'grp-reuse'
		ORDER BY user_pk`)
	require.NoError(t, err)
	defer rows.Close()

	observed := map[string]float64{}
	for rows.Next() {
		var pk string
		var bps *float64
		require.NoError(t, rows.Scan(&pk, &bps))
		require.NotNil(t, bps, "user %s should have a rate row", pk)
		observed[pk] = *bps
	}
	require.NoError(t, rows.Err())

	// Each user attributed only to their own rollup row, not to each other's.
	assert.InDelta(t, 1000000, observed["u-A"], 1, "u-A keeps its own 1 Mbps")
	assert.InDelta(t, 99000000, observed["u-B"], 1, "u-B keeps its own 99 Mbps")
}

// TestHealthMulticastUserRate_MultiGroupAmbiguity verifies that when one
// (device, tunnel, user) tuple subscribes to multiple multicast groups —
// so its per-tunnel rate is a cross-group aggregate that cannot be
// attributed per-group — every rate row for that user is marked
// rate_status='unknown' with reason 'multi_group_ambiguity', not
// silently reconciled or mismatched.
func TestHealthMulticastUserRate_MultiGroupAmbiguity(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('d-mga-fhr', now(), now(), generateUUIDv4(), 0, 1, 'd-mga-fhr', 'activated', 'edge', 'mga-fhr-dz1', '', '', '', 0, '[]'),
			('d-mga-lhr', now(), now(), generateUUIDv4(), 0, 2, 'd-mga-lhr', 'activated', 'edge', 'mga-lhr-dz1', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('grp-mga-A', now(), now(), generateUUIDv4(), 0, 1, 'grp-mga-A', 'o', 'mga-A', '233.99.4.1', 100000000, 'activated', 1, 1),
			('grp-mga-B', now(), now(), generateUUIDv4(), 0, 2, 'grp-mga-B', 'o', 'mga-B', '233.99.4.2', 100000000, 'activated', 1, 1)`))

	// u-mga-sub subscribes to BOTH grp-mga-A and grp-mga-B via the same tunnel.
	// Each group has its own publisher contributing 5 Mbps.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('u-mga-pub-A', now(), now(), generateUUIDv4(), 0, 1, 'u-mga-pub-A', 'o', 'activated', 'multicast', '203.0.116.1', '10.99.4.1', 'd-mga-fhr', 't1', 950, '["grp-mga-A"]', '[]'),
			('u-mga-pub-B', now(), now(), generateUUIDv4(), 0, 2, 'u-mga-pub-B', 'o', 'activated', 'multicast', '203.0.116.2', '10.99.4.2', 'd-mga-fhr', 't1', 951, '["grp-mga-B"]', '[]'),
			('u-mga-sub',   now(), now(), generateUUIDv4(), 0, 3, 'u-mga-sub',   'o', 'activated', 'multicast', '203.0.116.3', '10.99.4.3', 'd-mga-lhr', 't1', 952, '[]', '["grp-mga-A","grp-mga-B"]')`))

	// CP healthy for both groups for the shared subscriber.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			('mr-mga-fhr-A', now(), now(), generateUUIDv4(), 0, 1, 'd-mga-fhr', 'default', 'sparse', '233.99.4.1', '10.99.4.1', 'SBNP', 0, 'Tunnel950', '', '', 0, 0, '', 0, 0, '', 0, now()),
			('mr-mga-fhr-B', now(), now(), generateUUIDv4(), 0, 2, 'd-mga-fhr', 'default', 'sparse', '233.99.4.2', '10.99.4.2', 'SBNP', 0, 'Tunnel951', '', '', 0, 0, '', 0, 0, '', 0, now()),
			('mr-mga-lhr-A', now(), now(), generateUUIDv4(), 0, 3, 'd-mga-lhr', 'default', 'sparse', '233.99.4.1', '10.99.4.1', 'SMP',  0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel952"]', 1, now()),
			('mr-mga-lhr-B', now(), now(), generateUUIDv4(), 0, 4, 'd-mga-lhr', 'default', 'sparse', '233.99.4.2', '10.99.4.2', 'SMP',  0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel952"]', 1, now())`))

	// Subscriber's tunnel reports 10 Mbps total (aggregate of both groups).
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES
			(now() - INTERVAL 1 MINUTE, 'd-mga-fhr', 'Tunnel950', 950, 'u-mga-pub-A', 5000000, 0,        now()),
			(now() - INTERVAL 1 MINUTE, 'd-mga-fhr', 'Tunnel951', 951, 'u-mga-pub-B', 5000000, 0,        now()),
			(now() - INTERVAL 1 MINUTE, 'd-mga-lhr', 'Tunnel952', 952, 'u-mga-sub',   0,       10000000, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT multicast_group_pk, rate_status, rate_status_reason, expected_bps_5m
		FROM health_multicast_user_rate
		WHERE user_pk = 'u-mga-sub'
		ORDER BY multicast_group_pk`)
	require.NoError(t, err)
	defer rows.Close()

	type r struct {
		group, rate, reason string
		expected            *float64
	}
	got := []r{}
	for rows.Next() {
		var x r
		require.NoError(t, rows.Scan(&x.group, &x.rate, &x.reason, &x.expected))
		got = append(got, x)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 2, "subscriber should produce one row per group")

	for _, row := range got {
		assert.Equal(t, "unknown", row.rate, "multi-group subscriber rate must not reconcile (group=%s)", row.group)
		assert.Equal(t, "multi_group_ambiguity", row.reason, "group=%s", row.group)
		assert.Nil(t, row.expected, "no per-group expected when ambiguous (group=%s)", row.group)
	}
}
