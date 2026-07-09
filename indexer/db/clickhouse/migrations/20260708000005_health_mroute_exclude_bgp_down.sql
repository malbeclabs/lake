-- +goose Up
--
-- Exclude BGP-down publishers from the active-publisher set: their (S,G) is
-- legitimately absent (no session, no route to source), so a (*,G) must not
-- be flagged unhealthy for missing it. Requires 20260708000001.

-- +goose StatementBegin
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
    WHERE u.status = 'activated' AND u.kind = 'multicast' AND u.bgp_status != 'down'
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

-- +goose Down
-- +goose StatementBegin
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
