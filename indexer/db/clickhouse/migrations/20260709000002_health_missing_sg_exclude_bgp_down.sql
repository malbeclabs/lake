-- +goose Up
--
-- Exclude BGP-down publishers from health_missing_sg's active-publisher set,
-- matching health_mroute (20260708000005). Their (S,G) is legitimately absent
-- (no session, no route to source), so they must not be reported as
-- fhr_missing/downstream_missing. The view has no API consumer today; this
-- keeps it consistent with health_mroute if it is ever surfaced. Requires
-- dz_users_current.bgp_status (20260708000001).

-- +goose StatementBegin
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
    WHERE u.status = 'activated' AND u.kind = 'multicast' AND u.bgp_status != 'down'
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
