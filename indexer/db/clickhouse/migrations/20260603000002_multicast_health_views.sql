-- +goose Up

-- ============================================================================
-- Multicast end-to-end health views.
--
-- Two complementary views answering different questions about multicast
-- delivery state per malbeclabs/infra#1501:
--
--   * health_mroute      — classifies each row in enriched_ip_mroute as
--                          healthy / degraded / unhealthy, with a device
--                          role and a flag-correctness check.
--   * health_missing_sg  — surfaces (active_publisher, group, device) tuples
--                          where an (S,G) entry is expected but absent.
--
-- Health definition reflects the DZ multicast forwarding model:
--   - SPT (source-specific tree) is the desired steady state. Every device
--     should have an (S,G) row for every active publisher of every group.
--   - (*,G) entries exist as part of normal PIM-SM but are unhealthy ALONE:
--     an (*,G) without matching (S,G) for every active publisher means
--     receivers are stuck on the shared tree.
--   - Flag interpretation (Arista PIM-SM):
--       S = SPT bit set, P = Programmed in hardware,
--       M = Learned via MSDP, E = Forwarding on RPT,
--       B = Learned via Border Router, N = May notify MSDP,
--       W = Wildcard entry.
-- ============================================================================

-- +goose StatementBegin
-- health_mroute
CREATE OR REPLACE VIEW health_mroute
AS
WITH
oif_role_hints AS (
    SELECT
        mroute_entity_id AS hint_mroute_entity_id,
        max(if(oif_kind = 'subscriber_tunnel', 1, 0)) AS hint_has_subscriber_tunnel,
        max(if(oif_kind = 'underlay_link', 1, 0)) AS hint_has_underlay_link
    FROM enriched_ip_mroute_oifs
    GROUP BY mroute_entity_id
),
sg_sources_on_device AS (
    SELECT
        device_pk AS sg_device_pk,
        group_address AS sg_group_address,
        groupUniqArray(source_address) AS sg_sources_with_sg
    FROM enriched_ip_mroute
    WHERE source_address != '0.0.0.0'
    GROUP BY device_pk, group_address
),
active_publishers_for_group AS (
    SELECT
        g.multicast_ip AS ap_group_address,
        groupUniqArray(u.dz_ip) AS ap_active_publisher_ips
    FROM dz_multicast_groups_current g
    LEFT ARRAY JOIN [g.pk] AS gpk
    INNER JOIN dz_users_current u
        ON has(JSONExtract(u.publishers, 'Array(String)'), gpk)
    WHERE u.status = 'activated' AND u.kind = 'multicast'
    GROUP BY g.multicast_ip
)
SELECT
    r.mroute_id AS mroute_id,
    r.mroute_entity_id AS mroute_entity_id,
    r.device_pk AS device_pk,
    r.device_code AS device_code,
    r.metro_code AS metro_code,
    r.contributor_code AS contributor_code,
    r.multicast_group_pk AS multicast_group_pk,
    r.multicast_group_code AS multicast_group_code,
    r.group_address AS group_address,
    r.source_address AS source_address,
    r.route_flags AS route_flags,
    r.rpf_interface AS rpf_interface,
    r.oif_count AS oif_count,
    r.publisher_user_pk AS publisher_user_pk,
    r.publisher_device_pk AS publisher_device_pk,
    r.publisher_device_code AS publisher_device_code,

    -- Role classification
    multiIf(
        r.source_address = '0.0.0.0', 'star_g',
        r.publisher_device_pk != '' AND r.publisher_device_pk = r.device_pk, 'fhr',
        coalesce(h.hint_has_subscriber_tunnel, 0) = 1, 'lhr',
        coalesce(h.hint_has_underlay_link, 0) = 1, 'transit',
        r.oif_count = 0 AND position(r.route_flags, 'M') > 0 AND position(r.route_flags, 'E') > 0, 'learned_passive',
        'unknown'
    ) AS role,

    -- For (*,G): which active publishers' (S,G) are missing on this device
    if(
        r.source_address = '0.0.0.0',
        arrayFilter(x -> NOT has(coalesce(sg.sg_sources_with_sg, []), x), coalesce(ap.ap_active_publisher_ips, [])),
        []
    ) AS missing_publisher_ips,

    -- Active publishers expected for this group
    length(coalesce(ap.ap_active_publisher_ips, [])) AS active_publisher_count,

    -- Health status
    multiIf(
        -- (*,G) unhealthy if any active publisher's (S,G) is missing on this device
        r.source_address = '0.0.0.0'
            AND length(coalesce(ap.ap_active_publisher_ips, [])) > 0
            AND length(arrayFilter(x -> NOT has(coalesce(sg.sg_sources_with_sg, []), x), coalesce(ap.ap_active_publisher_ips, []))) > 0,
            'unhealthy',
        r.source_address = '0.0.0.0', 'healthy',

        -- FHR: needs S + P
        r.publisher_device_pk != '' AND r.publisher_device_pk = r.device_pk
            AND position(r.route_flags, 'S') > 0 AND position(r.route_flags, 'P') > 0, 'healthy',
        r.publisher_device_pk != '' AND r.publisher_device_pk = r.device_pk, 'degraded',

        -- LHR: needs S + P
        coalesce(h.hint_has_subscriber_tunnel, 0) = 1
            AND position(r.route_flags, 'S') > 0 AND position(r.route_flags, 'P') > 0, 'healthy',
        coalesce(h.hint_has_subscriber_tunnel, 0) = 1, 'degraded',

        -- Transit: needs S + P
        coalesce(h.hint_has_underlay_link, 0) = 1
            AND position(r.route_flags, 'S') > 0 AND position(r.route_flags, 'P') > 0, 'healthy',
        coalesce(h.hint_has_underlay_link, 0) = 1, 'degraded',

        -- Learned passive: M + E expected, no OIFs
        r.oif_count = 0 AND position(r.route_flags, 'M') > 0 AND position(r.route_flags, 'E') > 0, 'healthy',

        'degraded'
    ) AS health_status,

    -- Free-text reasons
    arrayFilter(x -> x != '', [
        if(r.source_address = '0.0.0.0'
            AND length(coalesce(ap.ap_active_publisher_ips, [])) > 0
            AND length(arrayFilter(x -> NOT has(coalesce(sg.sg_sources_with_sg, []), x), coalesce(ap.ap_active_publisher_ips, []))) > 0,
            '(*,G) missing (S,G) for active publisher(s)', ''),
        if((r.publisher_device_pk != '' AND r.publisher_device_pk = r.device_pk
             OR coalesce(h.hint_has_subscriber_tunnel, 0) = 1
             OR coalesce(h.hint_has_underlay_link, 0) = 1)
            AND position(r.route_flags, 'S') > 0
            AND position(r.route_flags, 'P') = 0,
            'SPT established but not HW-programmed (slow path)', ''),
        if((r.publisher_device_pk != '' AND r.publisher_device_pk = r.device_pk
             OR coalesce(h.hint_has_subscriber_tunnel, 0) = 1
             OR coalesce(h.hint_has_underlay_link, 0) = 1)
            AND position(r.route_flags, 'S') = 0,
            'forwarding role but SPT bit not set', '')
    ]) AS degraded_reasons
FROM enriched_ip_mroute r
LEFT JOIN oif_role_hints h ON r.mroute_entity_id = h.hint_mroute_entity_id
LEFT JOIN sg_sources_on_device sg ON r.device_pk = sg.sg_device_pk AND r.group_address = sg.sg_group_address
LEFT JOIN active_publishers_for_group ap ON r.group_address = ap.ap_group_address;
-- +goose StatementEnd

-- +goose StatementBegin
-- health_missing_sg
CREATE OR REPLACE VIEW health_missing_sg
AS
WITH
devices_with_mroute AS (
    SELECT device_pk AS d_device_pk, any(device_code) AS d_device_code
    FROM enriched_ip_mroute
    GROUP BY device_pk
),
active_pubs AS (
    SELECT
        g.pk AS p_multicast_group_pk,
        g.code AS p_multicast_group_code,
        g.multicast_ip AS p_group_address,
        u.pk AS p_publisher_user_pk,
        u.dz_ip AS p_publisher_dz_ip,
        u.device_pk AS p_publisher_device_pk
    FROM dz_multicast_groups_current g
    LEFT ARRAY JOIN [g.pk] AS gpk
    INNER JOIN dz_users_current u
        ON has(JSONExtract(u.publishers, 'Array(String)'), gpk)
    WHERE u.status = 'activated' AND u.kind = 'multicast'
),
sg_present AS (
    SELECT device_pk AS s_device_pk, group_address AS s_group_address, source_address AS s_source_address
    FROM enriched_ip_mroute
    WHERE source_address != '0.0.0.0'
)
SELECT
    d.d_device_pk AS device_pk,
    d.d_device_code AS device_code,
    p.p_multicast_group_pk AS multicast_group_pk,
    p.p_multicast_group_code AS multicast_group_code,
    p.p_group_address AS group_address,
    p.p_publisher_user_pk AS publisher_user_pk,
    p.p_publisher_dz_ip AS publisher_dz_ip,
    p.p_publisher_device_pk AS publisher_device_pk,
    if(p.p_publisher_device_pk = d.d_device_pk, 'fhr_missing', 'downstream_missing') AS severity
FROM devices_with_mroute d
CROSS JOIN active_pubs p
LEFT JOIN sg_present sg
    ON sg.s_device_pk = d.d_device_pk
    AND sg.s_group_address = p.p_group_address
    AND sg.s_source_address = p.p_publisher_dz_ip
WHERE sg.s_source_address = '';
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS health_missing_sg;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS health_mroute;
-- +goose StatementEnd
