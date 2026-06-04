-- +goose Up

-- +goose StatementBegin
-- health_multicast_user
--
-- One row per (multicast user, group) reconciling onchain membership
-- (dz_users_current.publishers / .subscribers) against the observed
-- dataplane mroute state on the user's device.
--
-- Per-source granularity: this view tracks IIF/OIF tunnel membership at
-- (device, group, source_address), not just (device, group). A subscriber
-- whose Tunnel<N> is in the OIF list of (S1, G) but missing from (S2, G)
-- on its LHR device is "degraded", not "healthy". The verdict reflects
-- whether every (S, G) mroute carries the expected tunnel.
--
-- (*,G) wildcard mroutes are excluded from the source set — DZ forwards
-- via SPT, so a (*,G) without any (S,G) is unhealthy by definition.
--
-- Verdicts:
--   - mode 'P' (publisher):   Tunnel<N> is the RPF interface of the
--                             (S=user.dz_ip, G) mroute at the user's
--                             device. There's exactly one such mroute.
--   - mode 'S' (subscriber):  Tunnel<N> is in the OIF list of every
--                             (S, G) mroute at the user's device.
--                             Partial coverage → degraded.
--   - mode 'P+S' (both):      both of the above. Either half failing
--                             alone → degraded; both → unhealthy.
--
-- Second track of malbeclabs/infra#1501.
CREATE OR REPLACE VIEW health_multicast_user
AS
WITH
-- The full SELECT below uses inlined per-source expressions repeatedly
-- because ClickHouse aliasing inside multiIf does not consistently
-- reference SELECT-list aliases. Keep the CTEs as the single source of
-- truth and inline the count/coverage expressions where needed.
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
-- Per-(device, group, source): the OIF tunnels carried by the (S, G) mroute.
-- (*,G) entries are excluded; partial OIF coverage manifests by comparing
-- per-source rows to the device's full set of (S, G) sources for the group.
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
-- Per-(device, group): count of distinct (S, G) mroutes at the device.
-- Sourced from enriched_ip_mroute (NOT enriched_ip_mroute_oifs) so that
-- mroutes with an empty OIF list still count toward total_sources — those
-- represent a real publisher whose source the subscriber should be
-- reachable for, and an empty OIF is itself a coverage failure.
sources_per_dg AS (
    SELECT
        device_pk AS tg_device_pk,
        group_address AS tg_group_address,
        countDistinct(source_address) AS tg_total_sources
    FROM enriched_ip_mroute
    WHERE source_address != '' AND source_address != '0.0.0.0'
    GROUP BY device_pk, group_address
),
-- Per-(device, group): array of OIF tunnel sets for sources that have any
-- OIF rows. Sources with empty OIF lists are absent here; the gap is
-- captured by the difference between this set's size and tg_total_sources.
oif_sources_per_dg AS (
    SELECT
        oif_device_pk AS s_device_pk,
        oif_group_address AS s_group_address,
        groupArray(oif_source_address) AS s_sources,
        groupArray(oif_tunnel_ids) AS s_oifs_per_source
    FROM oif_per_dgs
    GROUP BY oif_device_pk, oif_group_address
),
-- Per-(device, group, source): RPF interface tunnel id. There's at most
-- one RPF interface per mroute, so this is a single-valued lookup.
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
    -- expected_tunnel_position (computed from mode)
    multiIf(
        ug.ug_mode = 'P', 'iif',
        ug.ug_mode = 'S', 'oif',
        ug.ug_mode = 'P+S', 'iif|oif',
        ''
    ) AS expected_tunnel_position,
    -- Publisher IIF check — the (S=user.dz_ip, G) mroute's RPF interface
    -- must match the user's tunnel. iif.iif_tunnel_id IS NULL when no such
    -- (S, G) mroute exists at the device.
    (iif.iif_tunnel_id = ug.ug_user_tunnel_id) AS publisher_iif_observed,
    -- Subscriber per-source counts. total_sources is the number of (S, G)
    -- mroutes for the group at the device; oif_present_sources is how many
    -- of those carry the user's tunnel in their OIF list.
    coalesce(tg.tg_total_sources, 0) AS subscriber_total_sources,
    arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) AS subscriber_oif_present_sources,
    -- subscriber_oif_observed is TRUE only when fully covered.
    (subscriber_total_sources > 0
        AND subscriber_oif_present_sources = subscriber_total_sources) AS subscriber_oif_observed,
    -- reconciled
    multiIf(
        ug.ug_mode = 'P', (iif.iif_tunnel_id = ug.ug_user_tunnel_id),
        ug.ug_mode = 'S', (coalesce(tg.tg_total_sources, 0) > 0
                           AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0)),
        ug.ug_mode = 'P+S', (iif.iif_tunnel_id = ug.ug_user_tunnel_id)
                            AND (coalesce(tg.tg_total_sources, 0) > 0
                                 AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0)),
        false
    ) AS reconciled,
    -- health_status
    --   publisher: IIF matches → healthy; else unhealthy.
    --   subscriber: full coverage → healthy; partial → degraded;
    --               none observed or no (S,G) at all → unhealthy.
    --   P+S: both halves healthy → healthy; one half degraded/unhealthy
    --        and the other healthy → degraded; both failing → unhealthy.
    multiIf(
        -- P
        ug.ug_mode = 'P' AND (iif.iif_tunnel_id = ug.ug_user_tunnel_id), 'healthy',
        ug.ug_mode = 'P', 'unhealthy',
        -- S
        ug.ug_mode = 'S' AND coalesce(tg.tg_total_sources, 0) = 0, 'unhealthy',
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) = coalesce(tg.tg_total_sources, 0), 'healthy',
        ug.ug_mode = 'S' AND arrayCount(x -> has(x, ug.ug_user_tunnel_id), coalesce(o.s_oifs_per_source, [])) > 0, 'degraded',
        ug.ug_mode = 'S', 'unhealthy',
        -- P+S
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

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS health_multicast_user;
-- +goose StatementEnd
