package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthMroute_Classifier exercises the health_mroute role + status
// classifier against a synthetic group with two publishers — one transmitting
// (full SPT delivery), one registered but silent. The view should:
//   - Classify the FHR row as 'fhr / healthy' (SBNP flags).
//   - Classify the LHR row as 'lhr / healthy' (SMP flags + subscriber OIF).
//   - Classify the learned-passive ME row as 'learned_passive / healthy'.
//   - Classify the (*,G) row as 'star_g / unhealthy' because the silent
//     publisher's (S,G) is missing on the LHR.
//   - Surface the missing publisher dz_ip in `missing_publisher_ips` on the (*,G) row.
func TestHealthMroute_Classifier(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, longitude, latitude)
		VALUES ('m-sea', now(), now(), generateUUIDv4(), 0, 1, 'm-sea', 'sea', 'Seattle', 0, 0),
			('m-nyc', now(), now(), generateUUIDv4(), 0, 2, 'm-nyc', 'nyc', 'New York', 0, 0),
			('m-fra', now(), now(), generateUUIDv4(), 0, 3, 'm-fra', 'fra', 'Frankfurt', 0, 0)`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES ('c-jump', now(), now(), generateUUIDv4(), 0, 1, 'c-jump', 'jump_', 'Jump Trading')`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('d-sea', now(), now(), generateUUIDv4(), 0, 1, 'd-sea', 'activated', 'edge', 'sea001-dz001', '', 'c-jump', 'm-sea', 0, '[]'),
			('d-nyc', now(), now(), generateUUIDv4(), 0, 2, 'd-nyc', 'activated', 'edge', 'nyc001-dz001', '', 'c-jump', 'm-nyc', 0, '[]'),
			('d-fra', now(), now(), generateUUIDv4(), 0, 3, 'd-fra', 'activated', 'edge', 'fra001-dz001', '', 'c-jump', 'm-fra', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-test', now(), now(), generateUUIDv4(), 0, 1,
			'grp-test', 'owner', 'test-mcast', '233.99.99.1', 100000000, 'activated', 2, 1)`))

	// Two publishers on d-sea: pub-active (transmits, has (S,G) everywhere)
	// and pub-silent (registered onchain, no (S,G) anywhere → triggers strict-mode anomaly)
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('u-pub-active', now(), now(), generateUUIDv4(), 0, 1, 'u-pub-active', 'o', 'activated', 'multicast', '203.0.113.1', '10.99.0.1', 'd-sea', 't1', 501, '["grp-test"]', '[]'),
			('u-pub-silent', now(), now(), generateUUIDv4(), 0, 2, 'u-pub-silent', 'o', 'activated', 'multicast', '203.0.113.2', '10.99.0.2', 'd-sea', 't1', 502, '["grp-test"]', '[]'),
			('u-sub',        now(), now(), generateUUIDv4(), 0, 3, 'u-sub',        'o', 'activated', 'multicast', '203.0.113.3', '10.99.0.3', 'd-nyc', 't1', 601, '[]', '["grp-test"]')`))

	// SBNP on FHR (d-sea) for pub-active — healthy publisher state
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			device_pubkey, vrf, mode, group_address, source_address,
			route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix, rpf_preference, rpf_metric,
			rpf_neighbor, rpf_attached, rpf_has_block, oif_list, oif_count, creation_time)
		VALUES ('mr-fhr-active', now(), now(), generateUUIDv4(), 0, 1,
			'd-sea', 'default', 'sparse', '233.99.99.1', '10.99.0.1',
			'SBNP', 0, 'Tunnel501', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// SMP on LHR (d-nyc) for pub-active forwarding to u-sub
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			device_pubkey, vrf, mode, group_address, source_address,
			route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix, rpf_preference, rpf_metric,
			rpf_neighbor, rpf_attached, rpf_has_block, oif_list, oif_count, creation_time)
		VALUES ('mr-lhr-active', now(), now(), generateUUIDv4(), 0, 1,
			'd-nyc', 'default', 'sparse', '233.99.99.1', '10.99.0.1',
			'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel601"]', 1, now())`))

	// Passive ME on d-fra for pub-active (learned via MSDP, not forwarding)
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			device_pubkey, vrf, mode, group_address, source_address,
			route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix, rpf_preference, rpf_metric,
			rpf_neighbor, rpf_attached, rpf_has_block, oif_list, oif_count, creation_time)
		VALUES ('mr-pass-active', now(), now(), generateUUIDv4(), 0, 1,
			'd-fra', 'default', 'sparse', '233.99.99.1', '10.99.0.1',
			'ME', 0, 'Null0', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// (*,G) on the LHR — the unhealthy entry: receiver pulling on shared tree
	// AND pub-silent has no (S,G) anywhere → triggers strict-mode unhealthy.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			device_pubkey, vrf, mode, group_address, source_address,
			route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix, rpf_preference, rpf_metric,
			rpf_neighbor, rpf_attached, rpf_has_block, oif_list, oif_count, creation_time)
		VALUES ('mr-star-g', now(), now(), generateUUIDv4(), 0, 1,
			'd-nyc', 'default', 'sparse', '233.99.99.1', '0.0.0.0',
			'W', 0, 'Register0', '', '', 0, 0, '', 0, 0, '["Tunnel601"]', 1, now())`))

	// Subscriber OIF expansion (so health_mroute's lhr role classifier finds the tunnel OIF)
	// is materialised by enriched_ip_mroute_oifs automatically — no separate insert needed.

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	// Verify per-row classification
	type row struct {
		device, source, role, status string
		missingCount                 uint64
	}
	queryRows := func() []row {
		t.Helper()
		got := []row{}
		rs, err := conn.Query(ctx, `
			SELECT device_code, source_address, role, health_status, length(missing_publisher_ips) AS missing_count
			FROM health_mroute
			WHERE group_address = '233.99.99.1'
			ORDER BY source_address, device_code`)
		require.NoError(t, err)
		defer rs.Close()
		for rs.Next() {
			var r row
			require.NoError(t, rs.Scan(&r.device, &r.source, &r.role, &r.status, &r.missingCount))
			got = append(got, r)
		}
		return got
	}

	rows := queryRows()
	require.Len(t, rows, 4)

	// (*,G) first (source_address = "0.0.0.0" sorts first lexically)
	assert.Equal(t, "0.0.0.0", rows[0].source)
	assert.Equal(t, "nyc001-dz001", rows[0].device)
	assert.Equal(t, "star_g", rows[0].role)
	assert.Equal(t, "unhealthy", rows[0].status)
	assert.EqualValues(t, 1, rows[0].missingCount, "missing_publisher_ips should contain pub-silent's dz_ip")

	// (S,G) on d-fra (passive) — sorted next by device_code
	assert.Equal(t, "10.99.0.1", rows[1].source)
	assert.Equal(t, "fra001-dz001", rows[1].device)
	assert.Equal(t, "learned_passive", rows[1].role)
	assert.Equal(t, "healthy", rows[1].status)

	// (S,G) on d-nyc (LHR)
	assert.Equal(t, "10.99.0.1", rows[2].source)
	assert.Equal(t, "nyc001-dz001", rows[2].device)
	assert.Equal(t, "lhr", rows[2].role)
	assert.Equal(t, "healthy", rows[2].status)

	// (S,G) on d-sea (FHR)
	assert.Equal(t, "10.99.0.1", rows[3].source)
	assert.Equal(t, "sea001-dz001", rows[3].device)
	assert.Equal(t, "fhr", rows[3].role)
	assert.Equal(t, "healthy", rows[3].status)

	// health_missing_sg should surface pub-silent on every device with mroute data
	missingRows, err := conn.Query(ctx, `
		SELECT device_code, publisher_dz_ip, severity
		FROM health_missing_sg
		WHERE multicast_group_pk = 'grp-test'
		ORDER BY device_code`)
	require.NoError(t, err)
	defer missingRows.Close()
	type missing struct{ device, dzIP, severity string }
	var missingGot []missing
	for missingRows.Next() {
		var m missing
		require.NoError(t, missingRows.Scan(&m.device, &m.dzIP, &m.severity))
		missingGot = append(missingGot, m)
	}

	// Three devices have mroute data (d-sea, d-nyc, d-fra). pub-silent (10.99.0.2)
	// has no (S,G) on any of them. d-sea is the silent publisher's FHR so it's
	// the most severe; the other two are downstream.
	require.Len(t, missingGot, 3, "expected pub-silent to be missing on all 3 devices")
	for _, m := range missingGot {
		assert.Equal(t, "10.99.0.2", m.dzIP)
	}
	severities := map[string]bool{}
	for _, m := range missingGot {
		severities[m.severity] = true
	}
	assert.True(t, severities["fhr_missing"], "d-sea should report fhr_missing severity")
	assert.True(t, severities["downstream_missing"], "d-nyc / d-fra should report downstream_missing")
}

// TestHealthMroute_BgpDownPublisherExcluded verifies a BGP-down publisher is
// excluded from the active-publisher set, so a (*,G) is not flagged unhealthy
// for that publisher's legitimately-absent (S,G). A BGP-up publisher with the
// same missing (S,G) still counts.
func TestHealthMroute_BgpDownPublisherExcluded(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx := t.Context()

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES ('dm-lhr', now(), now(), generateUUIDv4(), 0, 1, 'dm-lhr', 'activated', 'edge', 'mlhr-dz001', '', '', '', 0, '[]')`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES ('grp-mx', now(), now(), generateUUIDv4(), 0, 1,
			'grp-mx', 'owner', 'test-mroute-bgp', '233.99.99.7', 100000000, 'activated', 2, 0)`))

	// Two publishers, neither with any (S,G): one BGP-down (must be excluded),
	// one BGP-up (must still count as expected-but-missing).
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES
			('um-pub-down', now(), now(), generateUUIDv4(), 0, 1, 'um-pub-down', 'o', 'activated', 'multicast', '203.0.113.41', '10.99.7.1', 'dm-lhr', 't1', 511, '["grp-mx"]', '[]', 'down'),
			('um-pub-up',   now(), now(), generateUUIDv4(), 0, 2, 'um-pub-up',   'o', 'activated', 'multicast', '203.0.113.42', '10.99.7.2', 'dm-lhr', 't1', 512, '["grp-mx"]', '[]', 'up')`))

	// (*,G) on dm-lhr — its health depends on which active publishers' (S,G) are missing.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			device_pubkey, vrf, mode, group_address, source_address,
			route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix, rpf_preference, rpf_metric,
			rpf_neighbor, rpf_attached, rpf_has_block, oif_list, oif_count, creation_time)
		VALUES ('mr-star-mx', now(), now(), generateUUIDv4(), 0, 1,
			'dm-lhr', 'default', 'sparse', '233.99.99.7', '0.0.0.0',
			'W', 0, 'Register0', '', '', 0, 0, '', 0, 0, '["Tunnel601"]', 1, now())`))

	require.NoError(t, conn.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, conn.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))

	var (
		status      string
		activeCount uint64
		missing     []string
	)
	rows, err := conn.Query(ctx, `
		SELECT health_status, active_publisher_count, missing_publisher_ips
		FROM health_mroute
		WHERE multicast_group_pk = 'grp-mx' AND source_address = '0.0.0.0'`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&status, &activeCount, &missing))

	// Only the BGP-up publisher counts: active_publisher_count = 1, and the
	// BGP-down publisher's IP is not in missing_publisher_ips.
	assert.EqualValues(t, 1, activeCount, "BGP-down publisher excluded from active set")
	assert.NotContains(t, missing, "10.99.7.1", "BGP-down publisher must not be flagged missing")
	assert.Contains(t, missing, "10.99.7.2", "BGP-up publisher with no (S,G) is still missing")
	assert.Equal(t, "unhealthy", status)
}
