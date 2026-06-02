-- +goose Up

-- +goose StatementBegin
-- dz_device_interface_ips
-- Parses dz_devices_current.interfaces JSON into one row per (device, interface IP).
-- Used by enriched views to map mesh-space addresses (172.16.x.x for MSDP, etc.)
-- back to the device pubkey that owns them. The CIDR mask is stripped to give a
-- plain IP suitable for equality joins.
CREATE OR REPLACE VIEW dz_device_interface_ips
AS
SELECT
    d.pk AS device_pk,
    d.code AS device_code,
    d.status AS device_status,
    JSONExtractString(iface, 'name') AS interface_name,
    JSONExtractString(iface, 'status') AS interface_status,
    splitByChar('/', JSONExtractString(iface, 'ip'))[1] AS ip_address
FROM dz_devices_current d
ARRAY JOIN JSONExtractArrayRaw(d.interfaces) AS iface
WHERE d.status = 'activated'
  AND JSONExtractString(iface, 'ip') != '';
-- +goose StatementEnd

-- +goose StatementBegin
-- enriched_ip_mroute
-- Joins dz_ip_mroute_entries_current with device, metro, contributor, multicast
-- group, and publisher-user metadata in one row per mroute entry. The PK columns
-- (device_pk, group_pk, publisher_user_pk, etc.) are paired with their
-- human-readable codes wherever the underlying dim provides one.
--
-- Source attribution: source_address matches the publisher user's dz_ip
-- (when an activated multicast user has the group in its publishers list).
-- source_match_status reports whether the match landed: 'publisher_matched'
-- when both the dz_ip and the publisher membership line up, 'group_only' when
-- the source is wildcard/empty, otherwise 'unknown_source'.
CREATE OR REPLACE VIEW enriched_ip_mroute
AS
SELECT
    -- mroute identity
    concat(r.device_pubkey, '|', r.vrf, '|', r.mode, '|', r.group_address, '|', r.source_address) AS mroute_id,
    r.entity_id AS mroute_entity_id,
    r.snapshot_ts,
    r.ingested_at,
    r.device_pubkey AS device_pk,
    d.code AS device_code,
    d.status AS device_status,
    d.device_type,
    d.metro_pk,
    m.code AS metro_code,
    m.name AS metro_name,
    d.contributor_pk,
    c.code AS contributor_code,
    c.name AS contributor_name,

    -- mroute payload
    r.vrf,
    r.mode,
    r.group_address,
    r.source_address,
    r.route_flags,
    r.register_in_oif_list,
    r.rpf_interface,
    r.rpf_rib,
    r.rpf_prefix,
    r.rpf_preference,
    r.rpf_metric,
    r.rpf_neighbor,
    r.rpf_attached,
    r.rpf_has_block,
    r.oif_list,
    r.oif_count,
    r.creation_time,

    -- group enrichment
    g.pk AS multicast_group_pk,
    g.code AS multicast_group_code,
    g.owner_pubkey AS multicast_group_owner_pubkey,
    g.max_bandwidth AS multicast_group_max_bandwidth,
    g.status AS multicast_group_status,

    -- publisher enrichment (publisher = the user owning the source_address)
    pub.pk AS publisher_user_pk,
    pub.owner_pubkey AS publisher_owner_pubkey,
    pub.tenant_pk AS publisher_tenant_pk,
    pub.client_ip AS publisher_client_ip,
    pub.dz_ip AS publisher_dz_ip,
    pub.tunnel_id AS publisher_tunnel_id,
    pub.device_pk AS publisher_device_pk,
    pub_dev.code AS publisher_device_code,
    pub_dev.metro_pk AS publisher_metro_pk,
    pub_m.code AS publisher_metro_code,
    pub_dev.contributor_pk AS publisher_contributor_pk,
    pub_c.code AS publisher_contributor_code,

    -- source-match diagnostic
    CASE
        WHEN pub.pk != '' THEN 'publisher_matched'
        WHEN r.source_address = '' OR r.source_address = '*' THEN 'group_only'
        ELSE 'unknown_source'
    END AS source_match_status
FROM dz_ip_mroute_entries_current r
LEFT ANY JOIN dz_devices_current d ON r.device_pubkey = d.pk
LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
LEFT ANY JOIN dz_multicast_groups_current g ON r.group_address = g.multicast_ip
LEFT ANY JOIN (
    SELECT pk, owner_pubkey, tenant_pk, client_ip, dz_ip, tunnel_id, device_pk, publishers
    FROM dz_users_current
    WHERE status = 'activated' AND kind = 'multicast'
) pub
    ON r.source_address = pub.dz_ip
    AND has(JSONExtract(pub.publishers, 'Array(String)'), assumeNotNull(g.pk))
LEFT ANY JOIN dz_devices_current pub_dev ON pub.device_pk = pub_dev.pk
LEFT ANY JOIN dz_metros_current pub_m ON pub_dev.metro_pk = pub_m.pk
LEFT ANY JOIN dz_contributors_current pub_c ON pub_dev.contributor_pk = pub_c.pk;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS enriched_ip_mroute;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_device_interface_ips;
-- +goose StatementEnd
