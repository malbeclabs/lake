package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthPublisherSubscriberPath_Endpoints exercises the endpoint-only
// verification. One publisher + two subscribers in a group:
//   - sub-good: subscriber whose Tunnel<N> appears in the OIF list of the
//     publisher's mroute on the subscriber's device → endpoints_reconciled
//   - sub-bad:  subscriber whose Tunnel<N> isn't in any OIF list →
//     unhealthy, missing_endpoint_reasons contains the explanation
func TestHealthPublisherSubscriberPath_Endpoints(t *testing.T) {
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
			('d-sea', now(), now(), generateUUIDv4(), 0, 1, 'd-sea', 'activated', 'edge', 'sea001-dz001', '', '', '', 0, '[]'),
			('d-nyc', now(), now(), generateUUIDv4(), 0, 2, 'd-nyc', 'activated', 'edge', 'nyc001-dz001', '', '', '', 0, '[]'),
			('d-fra', now(), now(), generateUUIDv4(), 0, 3, 'd-fra', 'activated', 'edge', 'fra001-dz001', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-p', now(), now(), generateUUIDv4(), 0, 1,
			'grp-p', 'owner', 'test-path', '233.99.99.3', 100000000, 'activated', 1, 2)`))

	// One publisher, two subscribers
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('u-pub',  now(), now(), generateUUIDv4(), 0, 1, 'u-pub',  'o', 'activated', 'multicast', '203.0.113.20', '10.99.0.20', 'd-sea', 't1', 505, '["grp-p"]', '[]'),
			('u-good', now(), now(), generateUUIDv4(), 0, 2, 'u-good', 'o', 'activated', 'multicast', '203.0.113.21', '10.99.0.21', 'd-nyc', 't1', 603, '[]', '["grp-p"]'),
			('u-bad',  now(), now(), generateUUIDv4(), 0, 3, 'u-bad',  'o', 'activated', 'multicast', '203.0.113.22', '10.99.0.22', 'd-fra', 't1', 604, '[]', '["grp-p"]')`))

	// Publisher FHR on d-sea with Tunnel505 as RPF interface (publisher endpoint OK)
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-fhr-p', now(), now(), generateUUIDv4(), 0, 1,
			'd-sea', 'default', 'sparse', '233.99.99.3', '10.99.0.20',
			'SBNP', 0, 'Tunnel505', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// LHR on d-nyc with u-good's Tunnel603 in OIF list (sub-good endpoint OK)
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-lhr-good', now(), now(), generateUUIDv4(), 0, 1,
			'd-nyc', 'default', 'sparse', '233.99.99.3', '10.99.0.20',
			'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel603"]', 1, now())`))

	// NO LHR mroute on d-fra → u-bad's endpoint will be missing

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT subscriber_user_pk,
			publisher_endpoint_observed, subscriber_endpoint_observed,
			endpoints_reconciled, health_status,
			length(missing_endpoint_reasons), arrayStringConcat(missing_endpoint_reasons, ' | ') AS reasons,
			verification_method
		FROM health_publisher_subscriber_path
		WHERE multicast_group_pk = 'grp-p'
		ORDER BY subscriber_user_pk`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		subPK, healthStatus, reasons, verification string
		pubObs, subObs, reconciled                 bool
		missingCount                               uint64
	}
	got := []row{}
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.subPK, &r.pubObs, &r.subObs, &r.reconciled, &r.healthStatus, &r.missingCount, &r.reasons, &r.verification))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 2)

	// u-bad first alphabetically
	assert.Equal(t, "u-bad", got[0].subPK)
	assert.True(t, got[0].pubObs, "publisher endpoint observed on d-sea")
	assert.False(t, got[0].subObs, "subscriber endpoint NOT observed (no mroute on d-fra)")
	assert.False(t, got[0].reconciled)
	assert.Equal(t, "unhealthy", got[0].healthStatus)
	assert.EqualValues(t, 1, got[0].missingCount)
	assert.Contains(t, got[0].reasons, "Tunnel604")
	assert.Contains(t, got[0].reasons, "fra001-dz001")
	assert.Equal(t, "endpoints_only", got[0].verification)

	// u-good
	assert.Equal(t, "u-good", got[1].subPK)
	assert.True(t, got[1].pubObs)
	assert.True(t, got[1].subObs)
	assert.True(t, got[1].reconciled)
	assert.Equal(t, "healthy", got[1].healthStatus)
	assert.EqualValues(t, 0, got[1].missingCount)
}

// TestHealthPublisherSubscriberPath_BgpDownIsDisconnected verifies a pair is
// 'disconnected' when an endpoint's BGP session is down, versus 'unhealthy' for
// the same missing endpoint when BGP is up.
func TestHealthPublisherSubscriberPath_BgpDownIsDisconnected(t *testing.T) {
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
			('dp-sea', now(), now(), generateUUIDv4(), 0, 1, 'dp-sea', 'activated', 'edge', 'psea-dz001', '', '', '', 0, '[]'),
			('dp-nyc', now(), now(), generateUUIDv4(), 0, 2, 'dp-nyc', 'activated', 'edge', 'pnyc-dz001', '', '', '', 0, '[]'),
			('dp-fra', now(), now(), generateUUIDv4(), 0, 3, 'dp-fra', 'activated', 'edge', 'pfra-dz001', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-pd', now(), now(), generateUUIDv4(), 0, 1,
			'grp-pd', 'owner', 'test-path-bgp', '233.99.99.4', 100000000, 'activated', 1, 2)`))

	// Publisher (BGP up) + two subscribers with no LHR mroute: one BGP-down, one up.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES
			('up-pub',      now(), now(), generateUUIDv4(), 0, 1, 'up-pub',      'o', 'activated', 'multicast', '203.0.113.30', '10.99.4.30', 'dp-sea', 't1', 505, '["grp-pd"]', '[]', 'up'),
			('up-sub-down', now(), now(), generateUUIDv4(), 0, 2, 'up-sub-down', 'o', 'activated', 'multicast', '203.0.113.31', '10.99.4.31', 'dp-fra', 't1', 604, '[]', '["grp-pd"]', 'down'),
			('up-sub-up',   now(), now(), generateUUIDv4(), 0, 3, 'up-sub-up',   'o', 'activated', 'multicast', '203.0.113.32', '10.99.4.32', 'dp-nyc', 't1', 605, '[]', '["grp-pd"]', 'up')`))

	// Publisher FHR so the publisher endpoint is observed; no LHR for either sub.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-fhr-pd', now(), now(), generateUUIDv4(), 0, 1,
			'dp-sea', 'default', 'sparse', '233.99.99.4', '10.99.4.30',
			'SBNP', 0, 'Tunnel505', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	rows, err := conn.Query(ctx, `
		SELECT subscriber_user_pk, health_status
		FROM health_publisher_subscriber_path
		WHERE multicast_group_pk = 'grp-pd'
		ORDER BY subscriber_user_pk`)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]string{}
	for rows.Next() {
		var sub, hs string
		require.NoError(t, rows.Scan(&sub, &hs))
		got[sub] = hs
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, "disconnected", got["up-sub-down"], "BGP-down subscriber → disconnected")
	assert.Equal(t, "unhealthy", got["up-sub-up"], "BGP-up subscriber with missing endpoint → unhealthy")
}
