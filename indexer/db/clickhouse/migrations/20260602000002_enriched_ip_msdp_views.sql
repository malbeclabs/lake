-- +goose Up

-- +goose StatementBegin
-- enriched_ip_msdp_peers
-- Joins dz_ip_msdp_peers_current with the local device and (via
-- dz_device_interface_ips) the peer device. Mesh-space peer addresses
-- (172.16.x.x) map back to whichever device has that IP on one of its
-- interfaces. Every entity pubkey is paired with its human-readable code.
CREATE OR REPLACE VIEW enriched_ip_msdp_peers
AS
SELECT
    p.entity_id AS msdp_peer_entity_id,
    p.snapshot_ts,
    p.ingested_at,

    -- local device
    p.device_pubkey AS device_pk,
    d.code AS device_code,
    d.status AS device_status,
    d.metro_pk,
    m.code AS metro_code,
    m.name AS metro_name,
    d.contributor_pk,
    c.code AS contributor_code,
    c.name AS contributor_name,

    -- session payload
    p.peer_address,
    p.state,
    p.session_start_time,
    p.sa_count,
    p.reset_count,

    -- peer device (resolved from peer_address via interface IPs)
    peer_iface.device_pk AS peer_device_pk,
    peer_iface.device_code AS peer_device_code,
    peer_iface.interface_name AS peer_interface_name
FROM dz_ip_msdp_peers_current p
LEFT ANY JOIN dz_devices_current d ON p.device_pubkey = d.pk
LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
LEFT ANY JOIN dz_device_interface_ips peer_iface ON p.peer_address = peer_iface.ip_address;
-- +goose StatementEnd

-- +goose StatementBegin
-- enriched_ip_msdp_pim_sa_cache
-- Joins dz_ip_msdp_pim_sa_cache_current with local device, multicast group,
-- and publisher-user metadata. rp_address is left raw — current production
-- uses anycast `10.0.0.0` which doesn't map to a specific device.
CREATE OR REPLACE VIEW enriched_ip_msdp_pim_sa_cache
AS
SELECT
    sa.entity_id AS msdp_pim_sa_entity_id,
    sa.snapshot_ts,
    sa.ingested_at,

    -- local device
    sa.device_pubkey AS device_pk,
    d.code AS device_code,
    d.metro_pk,
    m.code AS metro_code,
    d.contributor_pk,
    c.code AS contributor_code,

    -- SA payload
    sa.group_address,
    sa.source_address,
    sa.rp_address,

    -- group enrichment
    g.pk AS multicast_group_pk,
    g.code AS multicast_group_code,
    g.owner_pubkey AS multicast_group_owner_pubkey,
    g.status AS multicast_group_status,

    -- publisher enrichment
    pub.pk AS publisher_user_pk,
    pub.owner_pubkey AS publisher_owner_pubkey,
    pub.device_pk AS publisher_device_pk,
    pub_dev.code AS publisher_device_code,
    pub.dz_ip AS publisher_dz_ip,
    pub.tunnel_id AS publisher_tunnel_id,

    CASE
        WHEN pub.pk != '' THEN 'publisher_matched'
        WHEN sa.source_address = '' OR sa.source_address = '*' THEN 'group_only'
        ELSE 'unknown_source'
    END AS source_match_status
FROM dz_ip_msdp_pim_sa_cache_current sa
LEFT ANY JOIN dz_devices_current d ON sa.device_pubkey = d.pk
LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
LEFT ANY JOIN dz_multicast_groups_current g ON sa.group_address = g.multicast_ip
LEFT ANY JOIN (
    SELECT pk, owner_pubkey, dz_ip, tunnel_id, device_pk, publishers
    FROM dz_users_current
    WHERE status = 'activated' AND kind = 'multicast'
) pub
    ON sa.source_address = pub.dz_ip
    AND has(JSONExtract(pub.publishers, 'Array(String)'), assumeNotNull(g.pk))
LEFT ANY JOIN dz_devices_current pub_dev ON pub.device_pk = pub_dev.pk;
-- +goose StatementEnd

-- +goose StatementBegin
-- enriched_ip_msdp_sa_cache
-- Joins dz_ip_msdp_sa_cache_current with local device, remote device (via
-- dz_device_interface_ips), multicast group, and publisher user. `status`
-- column on the source row reports accepted vs rejected.
CREATE OR REPLACE VIEW enriched_ip_msdp_sa_cache
AS
SELECT
    sa.entity_id AS msdp_sa_entity_id,
    sa.snapshot_ts,
    sa.ingested_at,

    -- local device
    sa.device_pubkey AS device_pk,
    d.code AS device_code,
    d.metro_pk,
    m.code AS metro_code,
    d.contributor_pk,
    c.code AS contributor_code,

    -- SA payload
    sa.group_address,
    sa.source_address,
    sa.remote_address,
    sa.status AS accept_status,
    sa.rp_address,

    -- remote device (resolved from remote_address via interface IPs)
    remote_iface.device_pk AS remote_device_pk,
    remote_iface.device_code AS remote_device_code,
    remote_iface.interface_name AS remote_interface_name,

    -- group enrichment
    g.pk AS multicast_group_pk,
    g.code AS multicast_group_code,
    g.owner_pubkey AS multicast_group_owner_pubkey,
    g.status AS multicast_group_status,

    -- publisher enrichment
    pub.pk AS publisher_user_pk,
    pub.owner_pubkey AS publisher_owner_pubkey,
    pub.device_pk AS publisher_device_pk,
    pub_dev.code AS publisher_device_code,
    pub.dz_ip AS publisher_dz_ip,
    pub.tunnel_id AS publisher_tunnel_id,

    CASE
        WHEN pub.pk != '' THEN 'publisher_matched'
        WHEN sa.source_address = '' OR sa.source_address = '*' THEN 'group_only'
        ELSE 'unknown_source'
    END AS source_match_status
FROM dz_ip_msdp_sa_cache_current sa
LEFT ANY JOIN dz_devices_current d ON sa.device_pubkey = d.pk
LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
LEFT ANY JOIN dz_device_interface_ips remote_iface ON sa.remote_address = remote_iface.ip_address
LEFT ANY JOIN dz_multicast_groups_current g ON sa.group_address = g.multicast_ip
LEFT ANY JOIN (
    SELECT pk, owner_pubkey, dz_ip, tunnel_id, device_pk, publishers
    FROM dz_users_current
    WHERE status = 'activated' AND kind = 'multicast'
) pub
    ON sa.source_address = pub.dz_ip
    AND has(JSONExtract(pub.publishers, 'Array(String)'), assumeNotNull(g.pk))
LEFT ANY JOIN dz_devices_current pub_dev ON pub.device_pk = pub_dev.pk;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS enriched_ip_msdp_sa_cache;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS enriched_ip_msdp_pim_sa_cache;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS enriched_ip_msdp_peers;
-- +goose StatementEnd
