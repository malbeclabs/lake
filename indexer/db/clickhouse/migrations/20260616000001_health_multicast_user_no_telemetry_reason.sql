-- +goose Up

-- +goose StatementBegin
-- health_multicast_user: distinguish "device exports no mroute telemetry"
-- from a genuine forwarding fault.
--
-- When a device's state-collect agent isn't reporting (the device has zero
-- rows in enriched_ip_mroute), every (S, G) RPF/OIF lookup in this view is
-- silently empty. The original verdict then read as a confirmed fault
-- ("Tunnel<N> not seen as RPF interface for ...", health_status='unhealthy'),
-- implying a dataplane problem when the real cause is simply missing
-- telemetry. We can't determine health without data, so the device-reports-
-- nothing case now resolves to health_status='unknown' with a reason that
-- says so. Both the health_status and mismatch_reason no-telemetry branches
-- are evaluated first, so they supersede the per-mode logic whenever the
-- device reports nothing.
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
oif_per_dgs AS (
    SELECT
        device_pk AS oif_device_pk,
        group_address AS oif_group_address,
        source_address AS oif_source_address,
        groupUniqArrayIf(
            toInt32OrZero(extract(oif_name, '^Tunnel(\\d+)$')),
            match(oif_name, '^Tunnel\\d+$')
        ) AS oif_tunnel_ids
    FROM enriched_ip_mroute_oifs
    WHERE source_address != '' AND source_address != '0.0.0.0'
    GROUP BY device_pk, group_address, source_address
),
sources_per_dg AS (
    SELECT
        device_pk AS tg_device_pk,
        group_address AS tg_group_address,
        countDistinct(source_address) AS tg_total_sources
    FROM enriched_ip_mroute
    WHERE source_address != '' AND source_address != '0.0.0.0'
    GROUP BY device_pk, group_address
),
oif_sources_per_dg AS (
    SELECT
        oif_device_pk AS s_device_pk,
        oif_group_address AS s_group_address,
        groupArray(oif_source_address) AS s_sources,
        groupArray(oif_tunnel_ids) AS s_oifs_per_source
    FROM oif_per_dgs
    GROUP BY oif_device_pk, oif_group_address
),
iif_per_dgs AS (
    SELECT
        device_pk AS iif_device_pk,
        group_address AS iif_group_address,
        source_address AS iif_source_address,
        toInt32OrZero(extract(rpf_interface, '^Tunnel(\\d+)$')) AS iif_tunnel_id
    FROM enriched_ip_mroute
    WHERE match(rpf_interface, '^Tunnel\\d+$')
      AND source_address != '' AND source_address != '0.0.0.0'
),
-- Per-device flag: does the device export ANY mroute telemetry at all?
-- A device whose state-collect agent isn't reporting has zero rows in
-- enriched_ip_mroute (no source filter — wildcard/Null0 rows count), so
-- dm_present = 0 marks "no telemetry" as distinct from "reports mroutes
-- but not the expected (S, G)".
devices_with_mroutes AS (
    SELECT DISTINCT device_pk AS dm_device_pk, toUInt8(1) AS dm_present
    FROM enriched_ip_mroute
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
    multiIf(
        ug.ug_mode = 'P', 'iif',
        ug.ug_mode = 'S', 'oif',
        ug.ug_mode = 'P+S', 'iif|oif',
        ''
    ) AS expected_tunnel_position,
    (iif.iif_tunnel_id = ug.ug_user_tunnel_id) AS publisher_iif_observed,
    coalesce(tg.tg_total_sources, 0) AS subscriber_total_sources,
    arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) AS subscriber_oif_present_sources,
    (subscriber_total_sources > 0
        AND subscriber_oif_present_sources = subscriber_total_sources) AS subscriber_oif_observed,
    multiIf(
        ug.ug_mode = 'P', (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
        ug.ug_mode = 'S', (coalesce(tg.tg_total_sources, 0) > 0
                           AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0)),
        ug.ug_mode = 'P+S', (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
                            AND (coalesce(tg.tg_total_sources, 0) > 0
                                 AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0)),
        false
    ) AS reconciled,
    -- health_status. The no-telemetry branch is first: if the device reports
    -- no mroutes at all we can't determine health, so it's 'unknown' (not a
    -- confirmed 'unhealthy'). The rest is the per-mode control-plane verdict.
    multiIf(
        dm.dm_present = 0, 'unknown',
        ug.ug_mode = 'P' AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id), 'healthy',
        ug.ug_mode = 'P', 'unhealthy',
        ug.ug_mode = 'S' AND coalesce(tg.tg_total_sources, 0) = 0, 'unhealthy',
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'healthy',
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) > 0, 'degraded',
        ug.ug_mode = 'S', 'unhealthy',
        ug.ug_mode = 'P+S'
            AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
            AND coalesce(tg.tg_total_sources, 0) > 0
            AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'healthy',
        ug.ug_mode = 'P+S'
            AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
            AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) > 0, 'degraded',
        ug.ug_mode = 'P+S'
            AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id), 'degraded',
        ug.ug_mode = 'P+S'
            AND coalesce(tg.tg_total_sources, 0) > 0
            AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'degraded',
        'unhealthy'
    ) AS health_status,
    -- mismatch_reason
    -- The no-telemetry branch comes first: when the device reports no
    -- mroutes at all, none of the per-mode checks are meaningful.
    multiIf(
        dm.dm_present = 0,
            concat('no mroute telemetry observed from ', ug.ug_user_device_code,
                ' — cannot verify Tunnel', toString(ug.ug_user_tunnel_id),
                ' for group ', g.multicast_ip),
        ug.ug_mode = 'P' AND NOT (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
            concat('publisher Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen as RPF interface for (', ug.ug_user_dz_ip, ', ', g.multicast_ip,
                ') on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND coalesce(tg.tg_total_sources, 0) = 0,
            concat('no (S, ', g.multicast_ip, ') mroutes on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = 0,
            concat('subscriber Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen in any OIF list for the group on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) < coalesce(tg.tg_total_sources, 0),
            concat('partial OIF coverage on ', ug.ug_user_device_code, ': Tunnel',
                toString(ug.ug_user_tunnel_id), ' present for ',
                toString(arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, []))), ' of ',
                toString(coalesce(tg.tg_total_sources, 0)), ' publisher sources'),
        ug.ug_mode = 'P+S' AND NOT (iif.iif_tunnel_id = ug.ug_user_tunnel_id) AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = 0,
            concat('Tunnel', toString(ug.ug_user_tunnel_id),
                ' missing on both halves: publisher RPF for (', ug.ug_user_dz_ip,
                ', ', g.multicast_ip, ') and subscriber OIF on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND NOT (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
            concat('publisher half: Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen as RPF interface for (', ug.ug_user_dz_ip, ', ', g.multicast_ip,
                ') on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = 0,
            concat('subscriber half: Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen in any OIF list on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) < coalesce(tg.tg_total_sources, 0),
            concat('subscriber half: partial OIF coverage on ',
                ug.ug_user_device_code, ': Tunnel',
                toString(ug.ug_user_tunnel_id), ' present for ',
                toString(arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, []))), ' of ',
                toString(coalesce(tg.tg_total_sources, 0)), ' publisher sources'),
        ''
    ) AS mismatch_reason
FROM user_group_modes ug
LEFT JOIN dz_multicast_groups_current g ON ug.ug_multicast_group_pk = g.pk
LEFT JOIN iif_per_dgs iif
    ON iif.iif_device_pk = ug.ug_user_device_pk
    AND iif.iif_group_address = g.multicast_ip
    AND iif.iif_source_address = ug.ug_user_dz_ip
LEFT JOIN oif_sources_per_dg o
    ON o.s_device_pk = ug.ug_user_device_pk
    AND o.s_group_address = g.multicast_ip
LEFT JOIN sources_per_dg tg
    ON tg.tg_device_pk = ug.ug_user_device_pk
    AND tg.tg_group_address = g.multicast_ip
LEFT JOIN devices_with_mroutes dm
    ON dm.dm_device_pk = ug.ug_user_device_pk;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
-- Restore the pre-no-telemetry definition (migration 20260603000003).
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
oif_per_dgs AS (
    SELECT
        device_pk AS oif_device_pk,
        group_address AS oif_group_address,
        source_address AS oif_source_address,
        groupUniqArrayIf(
            toInt32OrZero(extract(oif_name, '^Tunnel(\\d+)$')),
            match(oif_name, '^Tunnel\\d+$')
        ) AS oif_tunnel_ids
    FROM enriched_ip_mroute_oifs
    WHERE source_address != '' AND source_address != '0.0.0.0'
    GROUP BY device_pk, group_address, source_address
),
sources_per_dg AS (
    SELECT
        device_pk AS tg_device_pk,
        group_address AS tg_group_address,
        countDistinct(source_address) AS tg_total_sources
    FROM enriched_ip_mroute
    WHERE source_address != '' AND source_address != '0.0.0.0'
    GROUP BY device_pk, group_address
),
oif_sources_per_dg AS (
    SELECT
        oif_device_pk AS s_device_pk,
        oif_group_address AS s_group_address,
        groupArray(oif_source_address) AS s_sources,
        groupArray(oif_tunnel_ids) AS s_oifs_per_source
    FROM oif_per_dgs
    GROUP BY oif_device_pk, oif_group_address
),
iif_per_dgs AS (
    SELECT
        device_pk AS iif_device_pk,
        group_address AS iif_group_address,
        source_address AS iif_source_address,
        toInt32OrZero(extract(rpf_interface, '^Tunnel(\\d+)$')) AS iif_tunnel_id
    FROM enriched_ip_mroute
    WHERE match(rpf_interface, '^Tunnel\\d+$')
      AND source_address != '' AND source_address != '0.0.0.0'
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
    multiIf(
        ug.ug_mode = 'P', 'iif',
        ug.ug_mode = 'S', 'oif',
        ug.ug_mode = 'P+S', 'iif|oif',
        ''
    ) AS expected_tunnel_position,
    (iif.iif_tunnel_id = ug.ug_user_tunnel_id) AS publisher_iif_observed,
    coalesce(tg.tg_total_sources, 0) AS subscriber_total_sources,
    arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) AS subscriber_oif_present_sources,
    (subscriber_total_sources > 0
        AND subscriber_oif_present_sources = subscriber_total_sources) AS subscriber_oif_observed,
    multiIf(
        ug.ug_mode = 'P', (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
        ug.ug_mode = 'S', (coalesce(tg.tg_total_sources, 0) > 0
                           AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0)),
        ug.ug_mode = 'P+S', (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
                            AND (coalesce(tg.tg_total_sources, 0) > 0
                                 AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0)),
        false
    ) AS reconciled,
    multiIf(
        ug.ug_mode = 'P' AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id), 'healthy',
        ug.ug_mode = 'P', 'unhealthy',
        ug.ug_mode = 'S' AND coalesce(tg.tg_total_sources, 0) = 0, 'unhealthy',
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'healthy',
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) > 0, 'degraded',
        ug.ug_mode = 'S', 'unhealthy',
        ug.ug_mode = 'P+S'
            AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
            AND coalesce(tg.tg_total_sources, 0) > 0
            AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'healthy',
        ug.ug_mode = 'P+S'
            AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
            AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) > 0, 'degraded',
        ug.ug_mode = 'P+S'
            AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id), 'degraded',
        ug.ug_mode = 'P+S'
            AND coalesce(tg.tg_total_sources, 0) > 0
            AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'degraded',
        'unhealthy'
    ) AS health_status,
    multiIf(
        ug.ug_mode = 'P' AND NOT (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
            concat('publisher Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen as RPF interface for (', ug.ug_user_dz_ip, ', ', g.multicast_ip,
                ') on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND coalesce(tg.tg_total_sources, 0) = 0,
            concat('no (S, ', g.multicast_ip, ') mroutes on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = 0,
            concat('subscriber Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen in any OIF list for the group on ', ug.ug_user_device_code),
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) < coalesce(tg.tg_total_sources, 0),
            concat('partial OIF coverage on ', ug.ug_user_device_code, ': Tunnel',
                toString(ug.ug_user_tunnel_id), ' present for ',
                toString(arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, []))), ' of ',
                toString(coalesce(tg.tg_total_sources, 0)), ' publisher sources'),
        ug.ug_mode = 'P+S' AND NOT (iif.iif_tunnel_id = ug.ug_user_tunnel_id) AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = 0,
            concat('Tunnel', toString(ug.ug_user_tunnel_id),
                ' missing on both halves: publisher RPF for (', ug.ug_user_dz_ip,
                ', ', g.multicast_ip, ') and subscriber OIF on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND NOT (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
            concat('publisher half: Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen as RPF interface for (', ug.ug_user_dz_ip, ', ', g.multicast_ip,
                ') on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = 0,
            concat('subscriber half: Tunnel', toString(ug.ug_user_tunnel_id),
                ' not seen in any OIF list on ', ug.ug_user_device_code),
        ug.ug_mode = 'P+S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) < coalesce(tg.tg_total_sources, 0),
            concat('subscriber half: partial OIF coverage on ',
                ug.ug_user_device_code, ': Tunnel',
                toString(ug.ug_user_tunnel_id), ' present for ',
                toString(arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, []))), ' of ',
                toString(coalesce(tg.tg_total_sources, 0)), ' publisher sources'),
        ''
    ) AS mismatch_reason
FROM user_group_modes ug
LEFT JOIN dz_multicast_groups_current g ON ug.ug_multicast_group_pk = g.pk
LEFT JOIN iif_per_dgs iif
    ON iif.iif_device_pk = ug.ug_user_device_pk
    AND iif.iif_group_address = g.multicast_ip
    AND iif.iif_source_address = ug.ug_user_dz_ip
LEFT JOIN oif_sources_per_dg o
    ON o.s_device_pk = ug.ug_user_device_pk
    AND o.s_group_address = g.multicast_ip
LEFT JOIN sources_per_dg tg
    ON tg.tg_device_pk = ug.ug_user_device_pk
    AND tg.tg_group_address = g.multicast_ip;
-- +goose StatementEnd
