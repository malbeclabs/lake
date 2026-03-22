-- +goose Up

-- entity_changes_v: normalized feed of all entity state changes across
-- devices, links, metros, contributors, and users history tables.
-- Used by the timeline API to replace 5 separate goroutines with one view query.
CREATE OR REPLACE VIEW entity_changes_v AS

-- Devices
SELECT * FROM (
    WITH min_ts AS (
        SELECT min(snapshot_ts) AS ts FROM dim_dz_devices_history
    ),
    all_history AS (
        SELECT
            entity_id,
            snapshot_ts,
            pk,
            code,
            status,
            is_deleted,
            attrs_hash,
            row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS row_num,
            lag(attrs_hash) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_attrs_hash,
            lag(is_deleted) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_is_deleted,
            lag(status) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_status,
            lag(device_type) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_device_type,
            lag(public_ip) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_public_ip,
            lag(contributor_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_contributor_pk,
            lag(metro_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_metro_pk,
            lag(max_users) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_max_users,
            device_type,
            public_ip,
            contributor_pk,
            metro_pk,
            max_users
        FROM dim_dz_devices_history
    )
    SELECT
        'device' AS entity_type,
        h.entity_id AS entity_id,
        h.pk AS entity_pk,
        h.code AS entity_code,
        h.snapshot_ts AS snapshot_ts,
        CASE
            WHEN h.row_num = 1 THEN 'created'
            WHEN h.is_deleted = 1 AND h.prev_is_deleted = 0 THEN 'deleted'
            ELSE 'updated'
        END AS change_type,
        arrayFilter(x -> x != '', [
            if(h.row_num > 1 AND h.prev_status != h.status, 'status', ''),
            if(h.row_num > 1 AND h.prev_device_type != h.device_type, 'device_type', ''),
            if(h.row_num > 1 AND h.prev_public_ip != h.public_ip, 'public_ip', ''),
            if(h.row_num > 1 AND h.prev_contributor_pk != h.contributor_pk, 'contributor', ''),
            if(h.row_num > 1 AND h.prev_metro_pk != h.metro_pk, 'metro', ''),
            if(h.row_num > 1 AND toString(h.prev_max_users) != toString(h.max_users), 'max_users', '')
        ]) AS changed_fields,
        h.status AS new_status,
        COALESCE(c.code, '') AS contributor_code,
        COALESCE(m.code, '') AS metro_code
    FROM all_history h
    CROSS JOIN min_ts
    LEFT JOIN dz_contributors_current c ON h.contributor_pk = c.pk
    LEFT JOIN dz_metros_current m ON h.metro_pk = m.pk
    WHERE (h.attrs_hash != h.prev_attrs_hash OR h.row_num = 1)
      AND NOT (h.row_num = 1 AND h.snapshot_ts = min_ts.ts)
)

UNION ALL

-- Links
SELECT * FROM (
    WITH min_ts AS (
        SELECT min(snapshot_ts) AS ts FROM dim_dz_links_history
    ),
    all_history AS (
        SELECT
            entity_id,
            snapshot_ts,
            pk,
            code,
            status,
            is_deleted,
            attrs_hash,
            row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS row_num,
            lag(attrs_hash) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_attrs_hash,
            lag(is_deleted) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_is_deleted,
            lag(status) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_status,
            lag(link_type) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_link_type,
            lag(tunnel_net) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_tunnel_net,
            lag(contributor_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_contributor_pk,
            lag(side_a_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_side_a_pk,
            lag(side_z_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_side_z_pk,
            lag(committed_rtt_ns) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_committed_rtt_ns,
            lag(committed_jitter_ns) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_committed_jitter_ns,
            lag(bandwidth_bps) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_bandwidth_bps,
            lag(isis_delay_override_ns) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_isis_delay_override_ns,
            link_type,
            tunnel_net,
            contributor_pk,
            side_a_pk,
            side_z_pk,
            committed_rtt_ns,
            committed_jitter_ns,
            bandwidth_bps,
            isis_delay_override_ns
        FROM dim_dz_links_history
    )
    SELECT
        'link' AS entity_type,
        h.entity_id AS entity_id,
        h.pk AS entity_pk,
        h.code AS entity_code,
        h.snapshot_ts AS snapshot_ts,
        CASE
            WHEN h.row_num = 1 THEN 'created'
            WHEN h.is_deleted = 1 AND h.prev_is_deleted = 0 THEN 'deleted'
            ELSE 'updated'
        END AS change_type,
        arrayFilter(x -> x != '', [
            if(h.row_num > 1 AND h.prev_status != h.status, 'status', ''),
            if(h.row_num > 1 AND h.prev_link_type != h.link_type, 'link_type', ''),
            if(h.row_num > 1 AND h.prev_tunnel_net != h.tunnel_net, 'tunnel_net', ''),
            if(h.row_num > 1 AND h.prev_contributor_pk != h.contributor_pk, 'contributor', ''),
            if(h.row_num > 1 AND h.prev_side_a_pk != h.side_a_pk, 'side_a', ''),
            if(h.row_num > 1 AND h.prev_side_z_pk != h.side_z_pk, 'side_z', ''),
            if(h.row_num > 1 AND h.prev_committed_rtt_ns != h.committed_rtt_ns, 'committed_rtt', ''),
            if(h.row_num > 1 AND h.prev_committed_jitter_ns != h.committed_jitter_ns, 'committed_jitter', ''),
            if(h.row_num > 1 AND h.prev_bandwidth_bps != h.bandwidth_bps, 'bandwidth', ''),
            if(h.row_num > 1 AND h.prev_isis_delay_override_ns != h.isis_delay_override_ns, 'isis_delay_override', '')
        ]) AS changed_fields,
        h.status AS new_status,
        COALESCE(c.code, '') AS contributor_code,
        '' AS metro_code
    FROM all_history h
    CROSS JOIN min_ts
    LEFT JOIN dz_contributors_current c ON h.contributor_pk = c.pk
    WHERE (h.attrs_hash != h.prev_attrs_hash OR h.row_num = 1)
      AND NOT (h.row_num = 1 AND h.snapshot_ts = min_ts.ts)
)

UNION ALL

-- Metros
SELECT * FROM (
    WITH min_ts AS (
        SELECT min(snapshot_ts) AS ts FROM dim_dz_metros_history
    ),
    all_history AS (
        SELECT
            entity_id,
            snapshot_ts,
            pk,
            code,
            name,
            is_deleted,
            attrs_hash,
            row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS row_num,
            lag(attrs_hash) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_attrs_hash,
            lag(is_deleted) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_is_deleted,
            lag(name) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_name,
            longitude,
            latitude,
            lag(longitude) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_longitude,
            lag(latitude) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_latitude
        FROM dim_dz_metros_history
    )
    SELECT
        'metro' AS entity_type,
        h.entity_id AS entity_id,
        h.pk AS entity_pk,
        h.code AS entity_code,
        h.snapshot_ts AS snapshot_ts,
        CASE
            WHEN h.row_num = 1 THEN 'created'
            WHEN h.is_deleted = 1 AND h.prev_is_deleted = 0 THEN 'deleted'
            ELSE 'updated'
        END AS change_type,
        arrayFilter(x -> x != '', [
            if(h.row_num > 1 AND h.prev_name != h.name, 'name', ''),
            if(h.row_num > 1 AND h.prev_longitude != h.longitude, 'longitude', ''),
            if(h.row_num > 1 AND h.prev_latitude != h.latitude, 'latitude', '')
        ]) AS changed_fields,
        '' AS new_status,
        '' AS contributor_code,
        '' AS metro_code
    FROM all_history h
    CROSS JOIN min_ts
    WHERE (h.attrs_hash != h.prev_attrs_hash OR h.row_num = 1)
      AND NOT (h.row_num = 1 AND h.snapshot_ts = min_ts.ts)
)

UNION ALL

-- Contributors
SELECT * FROM (
    WITH min_ts AS (
        SELECT min(snapshot_ts) AS ts FROM dim_dz_contributors_history
    ),
    all_history AS (
        SELECT
            entity_id,
            snapshot_ts,
            pk,
            code,
            name,
            is_deleted,
            attrs_hash,
            row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS row_num,
            lag(attrs_hash) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_attrs_hash,
            lag(is_deleted) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_is_deleted,
            lag(code) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_code,
            lag(name) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_name
        FROM dim_dz_contributors_history
    )
    SELECT
        'contributor' AS entity_type,
        h.entity_id AS entity_id,
        h.pk AS entity_pk,
        h.code AS entity_code,
        h.snapshot_ts AS snapshot_ts,
        CASE
            WHEN h.row_num = 1 THEN 'created'
            WHEN h.is_deleted = 1 AND h.prev_is_deleted = 0 THEN 'deleted'
            ELSE 'updated'
        END AS change_type,
        arrayFilter(x -> x != '', [
            if(h.row_num > 1 AND h.prev_code != h.code, 'code', ''),
            if(h.row_num > 1 AND h.prev_name != h.name, 'name', '')
        ]) AS changed_fields,
        '' AS new_status,
        '' AS contributor_code,
        '' AS metro_code
    FROM all_history h
    CROSS JOIN min_ts
    WHERE (h.attrs_hash != h.prev_attrs_hash OR h.row_num = 1)
      AND NOT (h.row_num = 1 AND h.snapshot_ts = min_ts.ts)
)

UNION ALL

-- Users (excluding validators and gossip_only)
SELECT * FROM (
    WITH min_ts AS (
        SELECT min(snapshot_ts) AS ts FROM dim_dz_users_history
    ),
    all_history AS (
        SELECT
            entity_id,
            snapshot_ts,
            pk,
            owner_pubkey,
            kind,
            status,
            device_pk,
            is_deleted,
            attrs_hash,
            row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS row_num,
            lag(attrs_hash) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_attrs_hash,
            lag(is_deleted) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_is_deleted,
            lag(status) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_status,
            lag(kind) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_kind,
            lag(client_ip) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_client_ip,
            lag(dz_ip) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_dz_ip,
            lag(device_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_device_pk,
            lag(tunnel_id) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_tunnel_id,
            client_ip,
            dz_ip,
            tunnel_id
        FROM dim_dz_users_history
        WHERE kind NOT IN ('validator', 'gossip_only')
    )
    SELECT
        'user' AS entity_type,
        h.entity_id AS entity_id,
        h.pk AS entity_pk,
        h.owner_pubkey AS entity_code,
        h.snapshot_ts AS snapshot_ts,
        CASE
            WHEN h.row_num = 1 THEN 'created'
            WHEN h.is_deleted = 1 AND h.prev_is_deleted = 0 THEN 'deleted'
            ELSE 'updated'
        END AS change_type,
        arrayFilter(x -> x != '', [
            if(h.row_num > 1 AND h.prev_status != h.status, 'status', ''),
            if(h.row_num > 1 AND h.prev_kind != h.kind, 'kind', ''),
            if(h.row_num > 1 AND h.prev_client_ip != h.client_ip, 'client_ip', ''),
            if(h.row_num > 1 AND h.prev_dz_ip != h.dz_ip, 'dz_ip', ''),
            if(h.row_num > 1 AND h.prev_device_pk != h.device_pk, 'device', ''),
            if(h.row_num > 1 AND toString(h.prev_tunnel_id) != toString(h.tunnel_id), 'tunnel_id', '')
        ]) AS changed_fields,
        h.status AS new_status,
        '' AS contributor_code,
        COALESCE(m.code, '') AS metro_code
    FROM all_history h
    CROSS JOIN min_ts
    LEFT JOIN dz_devices_current d ON h.device_pk = d.pk
    LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
    WHERE (h.attrs_hash != h.prev_attrs_hash OR h.row_num = 1)
      AND NOT (h.row_num = 1 AND h.snapshot_ts = min_ts.ts)
);


-- +goose Down

DROP VIEW IF EXISTS entity_changes_v;
