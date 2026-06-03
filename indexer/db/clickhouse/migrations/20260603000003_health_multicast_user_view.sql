-- +goose Up

-- +goose StatementBegin
-- health_multicast_user
--
-- One row per (multicast user, group) reconciling onchain membership
-- (dz_users_current.publishers / .subscribers) against the observed
-- dataplane mroute state on the user's device.
--
-- A user is healthy for a group iff their Tunnel<tunnel_id> appears in
-- the expected position(s) on the device's mroute entries for that group:
--   - mode 'P' (publisher):   Tunnel<N> appears as rpf_interface (IIF)
--   - mode 'S' (subscriber):  Tunnel<N> appears in oif_list (OIF)
--   - mode 'P+S' (both):      both of the above
--
-- Second track of malbeclabs/infra#1501. The third view —
-- health_publisher_subscriber_path — lands in a follow-up.
CREATE OR REPLACE VIEW health_multicast_user
AS
WITH
multicast_users AS (
    SELECT
        u.pk AS mu_user_pk,
        u.dz_ip AS mu_user_dz_ip,
        u.tunnel_id AS mu_user_tunnel_id,
        u.device_pk AS mu_user_device_pk,
        u.owner_pubkey AS mu_user_owner_pubkey,
        d.code AS mu_user_device_code,
        JSONExtract(u.publishers, 'Array(String)') AS mu_publisher_group_pks,
        JSONExtract(u.subscribers, 'Array(String)') AS mu_subscriber_group_pks
    FROM dz_users_current u
    LEFT ANY JOIN dz_devices_current d ON u.device_pk = d.pk
    WHERE u.status = 'activated' AND u.kind = 'multicast'
),
publisher_memberships AS (
    SELECT
        mu_user_pk, mu_user_dz_ip, mu_user_tunnel_id, mu_user_device_pk,
        mu_user_owner_pubkey, mu_user_device_code,
        gpk AS mem_group_pk,
        'P' AS mem_role
    FROM multicast_users
    ARRAY JOIN mu_publisher_group_pks AS gpk
),
subscriber_memberships AS (
    SELECT
        mu_user_pk, mu_user_dz_ip, mu_user_tunnel_id, mu_user_device_pk,
        mu_user_owner_pubkey, mu_user_device_code,
        gpk AS mem_group_pk,
        'S' AS mem_role
    FROM multicast_users
    ARRAY JOIN mu_subscriber_group_pks AS gpk
),
all_memberships AS (
    SELECT * FROM publisher_memberships
    UNION ALL
    SELECT * FROM subscriber_memberships
),
user_group_modes AS (
    SELECT
        mu_user_pk AS ug_user_pk,
        mu_user_dz_ip AS ug_user_dz_ip,
        mu_user_tunnel_id AS ug_user_tunnel_id,
        mu_user_device_pk AS ug_user_device_pk,
        mu_user_owner_pubkey AS ug_user_owner_pubkey,
        any(mu_user_device_code) AS ug_user_device_code,
        mem_group_pk AS ug_multicast_group_pk,
        arrayStringConcat(arraySort(groupUniqArray(mem_role)), '+') AS ug_mode
    FROM all_memberships
    GROUP BY mu_user_pk, mu_user_dz_ip, mu_user_tunnel_id, mu_user_device_pk,
             mu_user_owner_pubkey, mem_group_pk
),
-- For each (device, group), collect the set of tunnel_ids seen as RPF interface
iif_tunnels_per_device_group AS (
    SELECT
        device_pk AS iif_device_pk,
        group_address AS iif_group_address,
        groupUniqArrayIf(
            toInt32OrZero(extract(rpf_interface, '^Tunnel(\\d+)$')),
            match(rpf_interface, '^Tunnel\\d+$')
        ) AS iif_tunnel_ids
    FROM enriched_ip_mroute
    GROUP BY device_pk, group_address
),
-- For each (device, group), collect the set of tunnel_ids seen in OIF lists
oif_tunnels_per_device_group AS (
    SELECT
        device_pk AS oif_device_pk,
        group_address AS oif_group_address,
        groupUniqArray(toInt32OrZero(extract(oif_name, '^Tunnel(\\d+)$'))) AS oif_tunnel_ids
    FROM enriched_ip_mroute_oifs
    WHERE match(oif_name, '^Tunnel\\d+$')
    GROUP BY device_pk, group_address
)
SELECT
    ug.ug_user_pk AS user_pk,
    ug.ug_user_owner_pubkey AS user_owner_pubkey,
    ug.ug_user_dz_ip AS user_dz_ip,
    ug.ug_user_tunnel_id AS user_tunnel_id,
    ug.ug_user_device_pk AS user_device_pk,
    ug.ug_user_device_code AS user_device_code,
    ug.ug_multicast_group_pk AS multicast_group_pk,
    g.code AS multicast_group_code,
    g.multicast_ip AS group_address,
    ug.ug_mode AS mode,
    -- expected_tunnel_position (computed from mode)
    multiIf(
        ug.ug_mode = 'P', 'iif',
        ug.ug_mode = 'S', 'oif',
        ug.ug_mode = 'P+S', 'iif|oif',
        ''
    ) AS expected_tunnel_position,
    -- observed_positions
    has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id) AS publisher_iif_observed,
    has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id) AS subscriber_oif_observed,
    -- reconciled — every expected position is observed
    multiIf(
        ug.ug_mode = 'P',
            has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id),
        ug.ug_mode = 'S',
            has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id),
        ug.ug_mode = 'P+S',
            has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id)
            AND has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id),
        false
    ) AS reconciled,
    -- health_status
    multiIf(
        -- both expected positions observed → healthy
        ug.ug_mode = 'P' AND has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id), 'healthy',
        ug.ug_mode = 'S' AND has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id), 'healthy',
        ug.ug_mode = 'P+S'
            AND has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id)
            AND has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id), 'healthy',
        -- P+S with only one half observed → degraded
        ug.ug_mode = 'P+S'
            AND (has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id)
                 OR has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id)), 'degraded',
        'unhealthy'
    ) AS health_status,
    -- mismatch_reason
    multiIf(
        ug.ug_mode = 'P' AND NOT has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id),
            concat('publisher Tunnel', toString(ug.ug_user_tunnel_id), ' not seen as RPF interface for any mroute on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND NOT has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id),
            concat('subscriber Tunnel', toString(ug.ug_user_tunnel_id), ' not seen in any OIF list for the group on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND NOT has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id) AND NOT has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id),
            concat('Tunnel', toString(ug.ug_user_tunnel_id), ' not seen in either IIF or OIF position on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND NOT has(coalesce(iif.iif_tunnel_ids, []), ug.ug_user_tunnel_id),
            concat('publisher half: Tunnel', toString(ug.ug_user_tunnel_id), ' not seen as RPF interface on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND NOT has(coalesce(oif.oif_tunnel_ids, []), ug.ug_user_tunnel_id),
            concat('subscriber half: Tunnel', toString(ug.ug_user_tunnel_id), ' not seen in any OIF list on ', ug.ug_user_device_code),
        ''
    ) AS mismatch_reason
FROM user_group_modes ug
LEFT JOIN dz_multicast_groups_current g ON ug.ug_multicast_group_pk = g.pk
LEFT JOIN iif_tunnels_per_device_group iif
    ON iif.iif_device_pk = ug.ug_user_device_pk
    AND iif.iif_group_address = g.multicast_ip
LEFT JOIN oif_tunnels_per_device_group oif
    ON oif.oif_device_pk = ug.ug_user_device_pk
    AND oif.oif_group_address = g.multicast_ip;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS health_multicast_user;
-- +goose StatementEnd
