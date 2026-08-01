package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnrichedView_IPMrouteOIFs verifies that enriched_ip_mroute_oifs
// expands an mroute's OIF list and classifies each OIF correctly:
//
//   - An OIF matching a link side resolves to a link + peer device
//     (oif_kind = 'underlay_link', toward_network).
//   - A Tunnel<N> OIF where N matches a user.tunnel_id on the same device,
//     and that user subscribes to the multicast group, resolves to a
//     subscriber (oif_kind = 'subscriber_tunnel', toward_subscriber).
//   - An OIF that matches none of the above falls through to 'unknown'.
func TestEnrichedView_IPMrouteOIFs(t *testing.T) {
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
	// Two devices: dev-sea (the local mroute device) and dev-nyc (peer on the link).
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users, interfaces)
		VALUES
			('dev-sea', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'activated', 'edge', 'sea001-dz001', '', 'contrib-jump', 'metro-sea', 0, '[]'),
			('dev-nyc', now(), now(), generateUUIDv4(), 0, 2,
			 'dev-nyc', 'activated', 'edge', 'nyc001-dz001', '', 'contrib-jump', 'metro-nyc', 0, '[]')
	`))
	// Link between sea (side_a) and nyc (side_z), side_a interface is Switch1/11/1.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_links_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, code, tunnel_net, contributor_pk,
			 side_a_pk, side_z_pk, side_a_iface_name, side_z_iface_name,
			 side_a_ip, side_z_ip, link_type, committed_rtt_ns, committed_jitter_ns,
			 bandwidth_bps, isis_delay_override_ns, link_topologies, unicast_drained)
		VALUES
			('link-sea-nyc', now(), now(), generateUUIDv4(), 0, 1,
			 'link-sea-nyc', 'activated', 'sea001-dz001:nyc001-dz001', '', 'contrib-jump',
			 'dev-sea', 'dev-nyc', 'Switch1/11/1', 'Ethernet1/1',
			 '', '', 'DZX', 1000000, 0,
			 10000000000, 0, '[]', 0)
	`))
	// Multicast group
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('grp-shreds', now(), now(), generateUUIDv4(), 0, 1,
			 'grp-shreds', 'owner-pub', 'edge-solana-shreds', '233.84.178.1', 100000000, 'activated', 1, 1)
	`))
	// Subscriber on dev-sea with tunnel_id 504, subscribed to the group.
	// Publisher elsewhere; not needed for the OIF resolution.
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('user-sub', now(), now(), generateUUIDv4(), 0, 1,
			 'user-sub', 'sub-owner', 'activated', 'multicast', '203.0.113.10', '148.51.122.55', 'dev-sea', 'tenant-1', 504, '[]', '["grp-shreds"]')
	`))
	// Mroute on dev-sea with three OIFs:
	//   - Switch1/11/1     → matches link side_a → underlay_link
	//   - Tunnel504        → matches user-sub on dev-sea → subscriber_tunnel
	//   - Mystery0         → matches nothing → unknown
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES
			('mroute-multi-oif', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-sea', 'default', 'sparse', '233.84.178.1', '148.51.122.190',
			 'SMP', 0, 'Tunnel501', '', '', 0, 0, '', 0, 0,
			 '["Switch1/11/1","Tunnel504","Mystery0"]', 3, now())
	`))

	rows, err := conn.Query(ctx, `
		SELECT oif_name, oif_kind, observed_delivery_role,
			link_pk, link_code, peer_device_pk, peer_device_code,
			subscriber_user_pk, subscriber_device_pk, subscriber_device_code, subscriber_tunnel_id
		FROM enriched_ip_mroute_oifs
		WHERE mroute_entity_id = 'mroute-multi-oif'
		ORDER BY oif_name
	`)
	require.NoError(t, err)
	defer rows.Close()

	type rec struct {
		oifName, oifKind, role string
		linkPK, linkCode       string
		peerDevicePK, peerCode string
		subUserPK, subDevicePK string
		subDeviceCode          string
		subTunnelID            int32
	}
	var got []rec
	for rows.Next() {
		var r rec
		require.NoError(t, rows.Scan(
			&r.oifName, &r.oifKind, &r.role,
			&r.linkPK, &r.linkCode, &r.peerDevicePK, &r.peerCode,
			&r.subUserPK, &r.subDevicePK, &r.subDeviceCode, &r.subTunnelID,
		))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)

	// Mystery0 — no link, no user, no interface dim row → unknown
	mystery := got[0]
	assert.Equal(t, "Mystery0", mystery.oifName)
	assert.Equal(t, "unknown", mystery.oifKind)
	assert.Equal(t, "unclassified", mystery.role)

	// Switch1/11/1 — link side_a match
	link := got[1]
	assert.Equal(t, "Switch1/11/1", link.oifName)
	assert.Equal(t, "underlay_link", link.oifKind)
	assert.Equal(t, "toward_network", link.role)
	assert.Equal(t, "link-sea-nyc", link.linkPK)
	assert.Equal(t, "sea001-dz001:nyc001-dz001", link.linkCode)
	assert.Equal(t, "dev-nyc", link.peerDevicePK)
	assert.Equal(t, "nyc001-dz001", link.peerCode)

	// Tunnel504 — subscriber tunnel match on dev-sea (user-sub, tunnel_id 504,
	// subscribed to grp-shreds)
	tunnel := got[2]
	assert.Equal(t, "Tunnel504", tunnel.oifName)
	assert.Equal(t, "subscriber_tunnel", tunnel.oifKind)
	assert.Equal(t, "toward_subscriber", tunnel.role)
	assert.Equal(t, "user-sub", tunnel.subUserPK)
	assert.Equal(t, "dev-sea", tunnel.subDevicePK)
	assert.Equal(t, "sea001-dz001", tunnel.subDeviceCode)
	assert.EqualValues(t, 504, tunnel.subTunnelID)
}
