-- +goose Up

-- +goose StatementBegin
-- health_publisher_subscriber_path
--
-- One row per (multicast group, publisher user, subscriber user). Verifies
-- that the endpoints of the delivery path are correctly wired into the
-- dataplane:
--   publisher_endpoint_observed: pub.device has an (S,G) for pub.dz_ip
--                                with rpf_interface = Tunnel<pub.tunnel_id>
--   subscriber_endpoint_observed: sub.device has an (S,G) for pub.dz_ip
--                                 with Tunnel<sub.tunnel_id> in oif_list
--
-- Third view of Track 1 (malbeclabs/infra#1501). Current implementation is
-- ENDPOINTS-ONLY: it does not verify that intermediate transit devices on
-- the path have (S,G) entries. A follow-up phase will add recursive OIF
-- chain walking (CH 25.12 supports WITH RECURSIVE; verified) and populate
-- additional columns (visited_devices, hop_count, missing_segments).
-- Consumers should check verification_method to know which checks ran.
CREATE OR REPLACE VIEW health_publisher_subscriber_path
AS
WITH
multicast_users AS (
    SELECT
        u.pk AS mu_user_pk,
        u.owner_pubkey AS mu_owner_pubkey,
        u.dz_ip AS mu_dz_ip,
        u.tunnel_id AS mu_tunnel_id,
        u.device_pk AS mu_device_pk,
        d.code AS mu_device_code,
        JSONExtract(u.publishers, 'Array(String)') AS mu_publisher_groups,
        JSONExtract(u.subscribers, 'Array(String)') AS mu_subscriber_groups
    FROM dz_users_current u
    LEFT ANY JOIN dz_devices_current d ON u.device_pk = d.pk
    WHERE u.status = 'activated' AND u.kind = 'multicast'
),
publishers AS (
    SELECT
        mu_user_pk AS p_user_pk,
        mu_owner_pubkey AS p_owner_pubkey,
        mu_dz_ip AS p_dz_ip,
        mu_tunnel_id AS p_tunnel_id,
        mu_device_pk AS p_device_pk,
        mu_device_code AS p_device_code,
        gpk AS p_group_pk
    FROM multicast_users
    ARRAY JOIN mu_publisher_groups AS gpk
),
subscribers AS (
    SELECT
        mu_user_pk AS s_user_pk,
        mu_owner_pubkey AS s_owner_pubkey,
        mu_dz_ip AS s_dz_ip,
        mu_tunnel_id AS s_tunnel_id,
        mu_device_pk AS s_device_pk,
        mu_device_code AS s_device_code,
        gpk AS s_group_pk
    FROM multicast_users
    ARRAY JOIN mu_subscriber_groups AS gpk
),
-- For each (device, group, source): does Tunnel<N> appear as rpf_interface
-- on an (S,G) row on this device for this source?
publisher_iif_per_device AS (
    SELECT
        device_pk AS pi_device_pk,
        group_address AS pi_group_address,
        source_address AS pi_source_address,
        groupUniqArrayIf(
            toInt32OrZero(extract(rpf_interface, '^Tunnel(\\d+)$')),
            match(rpf_interface, '^Tunnel\\d+$')
        ) AS pi_iif_tunnel_ids
    FROM enriched_ip_mroute
    WHERE source_address != '0.0.0.0'
    GROUP BY device_pk, group_address, source_address
),
-- For each (device, group, source): which subscriber Tunnel<N> values appear
-- in oif_list?
subscriber_oif_per_device AS (
    SELECT
        device_pk AS so_device_pk,
        group_address AS so_group_address,
        source_address AS so_source_address,
        groupUniqArray(toInt32OrZero(extract(oif_name, '^Tunnel(\\d+)$'))) AS so_oif_tunnel_ids
    FROM enriched_ip_mroute_oifs
    WHERE source_address != '0.0.0.0' AND match(oif_name, '^Tunnel\\d+$')
    GROUP BY device_pk, group_address, source_address
)
SELECT
    p.p_group_pk AS multicast_group_pk,
    g.code AS multicast_group_code,
    g.multicast_ip AS group_address,

    p.p_user_pk AS publisher_user_pk,
    p.p_owner_pubkey AS publisher_owner_pubkey,
    p.p_dz_ip AS publisher_dz_ip,
    p.p_tunnel_id AS publisher_tunnel_id,
    p.p_device_pk AS publisher_device_pk,
    p.p_device_code AS publisher_device_code,

    s.s_user_pk AS subscriber_user_pk,
    s.s_owner_pubkey AS subscriber_owner_pubkey,
    s.s_dz_ip AS subscriber_dz_ip,
    s.s_tunnel_id AS subscriber_tunnel_id,
    s.s_device_pk AS subscriber_device_pk,
    s.s_device_code AS subscriber_device_code,

    has(coalesce(pi.pi_iif_tunnel_ids, []), p.p_tunnel_id) AS publisher_endpoint_observed,
    has(coalesce(so.so_oif_tunnel_ids, []), s.s_tunnel_id) AS subscriber_endpoint_observed,

    has(coalesce(pi.pi_iif_tunnel_ids, []), p.p_tunnel_id)
        AND has(coalesce(so.so_oif_tunnel_ids, []), s.s_tunnel_id) AS endpoints_reconciled,

    multiIf(
        has(coalesce(pi.pi_iif_tunnel_ids, []), p.p_tunnel_id)
            AND has(coalesce(so.so_oif_tunnel_ids, []), s.s_tunnel_id), 'healthy',
        'unhealthy'
    ) AS health_status,

    'endpoints_only' AS verification_method,

    arrayFilter(x -> x != '', [
        if(NOT has(coalesce(pi.pi_iif_tunnel_ids, []), p.p_tunnel_id),
            concat('publisher Tunnel', toString(p.p_tunnel_id),
                   ' not seen as RPF interface on ', p.p_device_code,
                   ' for source ', p.p_dz_ip), ''),
        if(NOT has(coalesce(so.so_oif_tunnel_ids, []), s.s_tunnel_id),
            concat('subscriber Tunnel', toString(s.s_tunnel_id),
                   ' not in any OIF list on ', s.s_device_code,
                   ' for source ', p.p_dz_ip), '')
    ]) AS missing_endpoint_reasons

FROM publishers p
INNER JOIN subscribers s ON p.p_group_pk = s.s_group_pk
LEFT JOIN dz_multicast_groups_current g ON p.p_group_pk = g.pk
LEFT JOIN publisher_iif_per_device pi
    ON pi.pi_device_pk = p.p_device_pk
    AND pi.pi_group_address = g.multicast_ip
    AND pi.pi_source_address = p.p_dz_ip
LEFT JOIN subscriber_oif_per_device so
    ON so.so_device_pk = s.s_device_pk
    AND so.so_group_address = g.multicast_ip
    AND so.so_source_address = p.p_dz_ip
WHERE p.p_user_pk != s.s_user_pk;  -- exclude self-loops (a user that is both pub and sub of the same group, against itself)
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS health_publisher_subscriber_path;
-- +goose StatementEnd
