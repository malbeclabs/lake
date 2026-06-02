-- +goose Up

-- +goose StatementBegin
-- enriched_ip_mroute_oifs
-- Expands each mroute entry's `oif_list` (a JSON string) into one row per
-- outgoing interface, then classifies each OIF and joins to whichever
-- entity it represents:
--
--   - oif_kind = 'underlay_link'       → joined to dz_links_current
--                                         (side_a or side_z matched on
--                                         (device_pubkey, interface_name))
--   - oif_kind = 'subscriber_tunnel'   → joined to a multicast user whose
--                                         tunnel_id matches Tunnel<N> on
--                                         the same device AND who subscribes
--                                         to the multicast group
--   - oif_kind = 'local_interface'     → joined to dz_device_interfaces_current
--                                         (interface metadata only; no link)
--   - oif_kind = 'register' | 'null'   → recognised by name prefix; no join
--   - oif_kind = 'unknown'             → fell through every match
--
-- observed_delivery_role gives a coarser semantic label:
--   toward_network, toward_subscriber, local_register, unclassified.
--
-- Every entity pubkey is paired with its human-readable code.
CREATE OR REPLACE VIEW enriched_ip_mroute_oifs
AS
WITH expanded AS (
    SELECT
        r.entity_id AS mroute_entity_id,
        r.snapshot_ts,
        r.ingested_at,
        r.device_pubkey,
        r.vrf,
        r.mode,
        r.group_address,
        r.source_address,
        oif_name
    FROM dz_ip_mroute_entries_current r
    ARRAY JOIN JSONExtract(r.oif_list, 'Array(String)') AS oif_name
    WHERE r.oif_list != ''
)
SELECT
    -- mroute identity (matches the composite id from enriched_ip_mroute)
    concat(o.device_pubkey, '|', o.vrf, '|', o.mode, '|', o.group_address, '|', o.source_address) AS mroute_id,
    o.mroute_entity_id,
    o.snapshot_ts,
    o.ingested_at,

    -- local device (same enrichment shape as enriched_ip_mroute)
    o.device_pubkey AS device_pk,
    d.code AS device_code,
    d.metro_pk,
    m.code AS metro_code,
    d.contributor_pk,
    c.code AS contributor_code,

    -- (S,G) keys
    o.vrf,
    o.mode,
    o.group_address,
    o.source_address,

    -- group enrichment
    g.pk AS multicast_group_pk,
    g.code AS multicast_group_code,
    g.owner_pubkey AS multicast_group_owner_pubkey,
    g.status AS multicast_group_status,

    -- publisher enrichment (publisher = user whose dz_ip is the source)
    pub.pk AS publisher_user_pk,
    pub.device_pk AS publisher_device_pk,
    pub_dev.code AS publisher_device_code,

    -- OIF identity
    o.oif_name,

    -- Underlay link enrichment (matched on either side of the link)
    if(la.pk != '', la.pk, lz.pk) AS link_pk,
    if(la.pk != '', la.code, lz.code) AS link_code,
    if(la.pk != '', 'a', if(lz.pk != '', 'z', '')) AS link_side,
    if(la.pk != '', la.side_z_pk, if(lz.pk != '', lz.side_a_pk, '')) AS peer_device_pk,
    if(la.pk != '', peer_a.code, if(lz.pk != '', peer_z.code, '')) AS peer_device_code,
    if(la.pk != '', la.side_z_iface_name, if(lz.pk != '', lz.side_a_iface_name, '')) AS peer_interface_name,
    if(la.pk != '', la.link_type, lz.link_type) AS link_type,
    if(la.pk != '', la.bandwidth_bps, lz.bandwidth_bps) AS bandwidth_bps,
    if(la.pk != '', la.link_topologies, lz.link_topologies) AS link_topologies,
    if(la.pk != '', la.unicast_drained, lz.unicast_drained) AS unicast_drained,

    -- Local interface enrichment (for OIFs that aren't link sides)
    iface.interface_type,
    iface.routing_mode,
    iface.bandwidth AS interface_bandwidth,
    iface.mtu AS interface_mtu,
    iface.user_tunnel_endpoint,

    -- Subscriber tunnel enrichment (Tunnel<N> on this device matching a
    -- multicast user that subscribes to this group)
    sub.pk AS subscriber_user_pk,
    sub.device_pk AS subscriber_device_pk,
    sub_dev.code AS subscriber_device_code,
    sub.tunnel_id AS subscriber_tunnel_id,
    sub.owner_pubkey AS subscriber_owner_pubkey,
    sub.dz_ip AS subscriber_dz_ip,
    sub.client_ip AS subscriber_client_ip,

    -- Classification
    multiIf(
        la.pk != '' OR lz.pk != '', 'underlay_link',
        sub.pk != '',                'subscriber_tunnel',
        iface.intf != '',            'local_interface',
        startsWith(lower(o.oif_name), 'register'), 'register',
        startsWith(lower(o.oif_name), 'null'),     'null',
        'unknown'
    ) AS oif_kind,
    multiIf(
        la.pk != '' OR lz.pk != '', 'toward_network',
        sub.pk != '',                'toward_subscriber',
        startsWith(lower(o.oif_name), 'register'), 'local_register',
        'unclassified'
    ) AS observed_delivery_role

FROM expanded o
LEFT ANY JOIN dz_devices_current d ON o.device_pubkey = d.pk
LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
LEFT ANY JOIN dz_multicast_groups_current g ON o.group_address = g.multicast_ip
LEFT ANY JOIN (
    SELECT pk, dz_ip, device_pk, publishers FROM dz_users_current
    WHERE status = 'activated' AND kind = 'multicast'
) pub
    ON o.source_address = pub.dz_ip
    AND has(JSONExtract(pub.publishers, 'Array(String)'), assumeNotNull(g.pk))
LEFT ANY JOIN dz_devices_current pub_dev ON pub.device_pk = pub_dev.pk
LEFT ANY JOIN dz_links_current la
    ON o.device_pubkey = la.side_a_pk AND o.oif_name = la.side_a_iface_name
LEFT ANY JOIN dz_links_current lz
    ON o.device_pubkey = lz.side_z_pk AND o.oif_name = lz.side_z_iface_name
LEFT ANY JOIN dz_devices_current peer_a ON la.side_z_pk = peer_a.pk
LEFT ANY JOIN dz_devices_current peer_z ON lz.side_a_pk = peer_z.pk
LEFT ANY JOIN dz_device_interfaces_current iface
    ON o.device_pubkey = iface.device_pk AND o.oif_name = iface.intf
LEFT ANY JOIN (
    SELECT pk, dz_ip, owner_pubkey, client_ip, device_pk, tunnel_id, subscribers
    FROM dz_users_current
    WHERE status = 'activated' AND kind = 'multicast' AND tunnel_id > 0
) sub
    ON o.device_pubkey = sub.device_pk
    AND sub.tunnel_id = toInt32OrZero(extract(o.oif_name, '^Tunnel(\\d+)$'))
    AND has(JSONExtract(sub.subscribers, 'Array(String)'), assumeNotNull(g.pk))
LEFT ANY JOIN dz_devices_current sub_dev ON sub.device_pk = sub_dev.pk;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS enriched_ip_mroute_oifs;
-- +goose StatementEnd
