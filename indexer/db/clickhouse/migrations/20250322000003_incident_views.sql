-- +goose Up

-- link_incidents_v: detects all link incident types from rollup tables.
-- Pipeline: above-threshold UNION ALL → gap-and-island → coalesce → metadata JOINs.
-- Hardcoded defaults: loss ≥ 10%, counters ≥ 1, coalesce gap 180 min, bucket 5 min.
CREATE OR REPLACE VIEW link_incidents_v AS
WITH
above AS (
    -- Packet loss from link_rollup_5m
    SELECT link_pk AS entity_pk, bucket_ts,
        greatest(a_loss_pct, z_loss_pct) AS metric_value,
        'packet_loss' AS incident_type
    FROM link_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND provisioning = false
      AND greatest(a_loss_pct, z_loss_pct) >= 10

    UNION ALL

    -- ISIS down from link_rollup_5m
    SELECT link_pk AS entity_pk, bucket_ts,
        toFloat64(1) AS metric_value,
        'isis_down' AS incident_type
    FROM link_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND isis_down = true
      AND provisioning = false

    UNION ALL

    -- No data: missing rollup rows (3+ consecutive missing buckets = 15min gap)
    SELECT al.link_pk AS entity_pk, e.bucket_ts AS bucket_ts,
        toFloat64(1) AS metric_value,
        'no_data' AS incident_type
    FROM (
        SELECT DISTINCT link_pk FROM link_rollup_5m FINAL
        WHERE bucket_ts >= now() - INTERVAL 8 DAY - INTERVAL 1 HOUR
          AND provisioning = false
    ) al
    CROSS JOIN (
        SELECT toDateTime(toStartOfFiveMinutes(now() - INTERVAL 8 DAY)) + number * 300 AS bucket_ts
        FROM numbers(toUInt64(dateDiff('second', now() - INTERVAL 8 DAY, now()) / 300))
    ) e
    LEFT JOIN (
        SELECT link_pk, bucket_ts FROM link_rollup_5m FINAL
        WHERE bucket_ts >= now() - INTERVAL 8 DAY
    ) a ON al.link_pk = a.link_pk AND e.bucket_ts = a.bucket_ts
    WHERE a.bucket_ts IS NULL

    UNION ALL

    -- Errors from device_interface_rollup_5m grouped by link_pk
    SELECT link_pk AS entity_pk, bucket_ts,
        toFloat64(sum(in_errors + out_errors)) AS metric_value,
        'errors' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk != ''
    GROUP BY link_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- FCS errors from device_interface_rollup_5m grouped by link_pk
    SELECT link_pk AS entity_pk, bucket_ts,
        toFloat64(sum(in_fcs_errors)) AS metric_value,
        'fcs' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk != ''
    GROUP BY link_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- Discards from device_interface_rollup_5m grouped by link_pk
    SELECT link_pk AS entity_pk, bucket_ts,
        toFloat64(sum(in_discards + out_discards)) AS metric_value,
        'discards' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk != ''
    GROUP BY link_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- Carrier transitions from device_interface_rollup_5m grouped by link_pk
    SELECT link_pk AS entity_pk, bucket_ts,
        toFloat64(sum(carrier_transitions)) AS metric_value,
        'carrier' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk != ''
    GROUP BY link_pk, bucket_ts
    HAVING metric_value >= 1
),

-- Gap-and-island: group consecutive above-threshold buckets
islands AS (
    SELECT entity_pk, incident_type, bucket_ts, metric_value,
        bucket_ts - toIntervalSecond(row_number() OVER (
            PARTITION BY entity_pk, incident_type ORDER BY bucket_ts
        ) * 5 * 60) AS island_grp
    FROM above
),

-- Aggregate each island
raw_incidents AS (
    SELECT entity_pk, incident_type, island_grp,
        min(bucket_ts) AS started_at,
        max(bucket_ts) + toIntervalSecond(5 * 60) AS ended_at,
        max(metric_value) AS peak_value,
        count() AS bucket_count
    FROM islands
    GROUP BY entity_pk, incident_type, island_grp
),

-- Coalesce nearby incidents (gap < 180 min)
numbered AS (
    SELECT *,
        lagInFrame(ended_at) OVER (
            PARTITION BY entity_pk, incident_type ORDER BY started_at
        ) AS prev_ended_at
    FROM raw_incidents
),
coalesce_groups AS (
    SELECT *,
        sum(if(prev_ended_at IS NULL
            OR dateDiff('minute', prev_ended_at, started_at) >= 180, 1, 0))
            OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS coalesce_grp
    FROM numbered
),
coalesced AS (
    SELECT entity_pk, incident_type,
        min(started_at) AS started_at,
        max(ended_at) AS ended_at,
        max(peak_value) AS peak_value,
        sum(bucket_count) AS total_buckets
    FROM coalesce_groups
    GROUP BY entity_pk, incident_type, coalesce_grp
)

SELECT
    c.entity_pk,
    c.incident_type,
    c.started_at,
    if(c.ended_at >= now() - toIntervalMinute(15), toDateTime('1970-01-01 00:00:00'), c.ended_at) AS ended_at,
    c.ended_at >= now() - toIntervalMinute(15) AS is_ongoing,
    c.peak_value,
    c.total_buckets,
    dateDiff('second', c.started_at,
        if(c.ended_at >= now() - toIntervalMinute(15), now(), c.ended_at)) AS duration_seconds,
    COALESCE(l.code, '') AS link_code,
    COALESCE(l.link_type, '') AS link_type,
    COALESCE(l.status, '') AS status,
    COALESCE(ma.code, '') AS side_a_metro,
    COALESCE(mz.code, '') AS side_z_metro,
    COALESCE(cc.code, '') AS contributor_code
FROM coalesced c
LEFT JOIN dz_links_current l ON c.entity_pk = l.pk
LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
LEFT JOIN dz_contributors_current cc ON l.contributor_pk = cc.pk
ORDER BY c.started_at DESC;


-- device_incidents_v: detects all device incident types from rollup tables.
-- Same pipeline as link_incidents_v but for devices. Excludes link-associated interfaces by default.
CREATE OR REPLACE VIEW device_incidents_v AS
WITH
above AS (
    -- Errors from device_interface_rollup_5m grouped by device_pk (exclude link interfaces)
    SELECT device_pk AS entity_pk, bucket_ts,
        toFloat64(sum(in_errors + out_errors)) AS metric_value,
        'errors' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk = ''
    GROUP BY device_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- FCS errors
    SELECT device_pk AS entity_pk, bucket_ts,
        toFloat64(sum(in_fcs_errors)) AS metric_value,
        'fcs' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk = ''
    GROUP BY device_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- Discards
    SELECT device_pk AS entity_pk, bucket_ts,
        toFloat64(sum(in_discards + out_discards)) AS metric_value,
        'discards' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk = ''
    GROUP BY device_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- Carrier transitions
    SELECT device_pk AS entity_pk, bucket_ts,
        toFloat64(sum(carrier_transitions)) AS metric_value,
        'carrier' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND link_pk = ''
    GROUP BY device_pk, bucket_ts
    HAVING metric_value >= 1

    UNION ALL

    -- No data: missing rollup rows
    SELECT ad.device_pk AS entity_pk, e.bucket_ts AS bucket_ts,
        toFloat64(1) AS metric_value,
        'no_data' AS incident_type
    FROM (
        SELECT DISTINCT device_pk FROM device_interface_rollup_5m FINAL
        WHERE bucket_ts >= now() - INTERVAL 8 DAY - INTERVAL 1 HOUR
          AND link_pk = ''
    ) ad
    CROSS JOIN (
        SELECT toDateTime(toStartOfFiveMinutes(now() - INTERVAL 8 DAY)) + number * 300 AS bucket_ts
        FROM numbers(toUInt64(dateDiff('second', now() - INTERVAL 8 DAY, now()) / 300))
    ) e
    LEFT JOIN (
        SELECT device_pk, bucket_ts FROM device_interface_rollup_5m FINAL
        WHERE bucket_ts >= now() - INTERVAL 8 DAY
          AND link_pk = ''
        GROUP BY device_pk, bucket_ts
    ) a ON ad.device_pk = a.device_pk AND e.bucket_ts = a.bucket_ts
    WHERE a.bucket_ts IS NULL

    UNION ALL

    -- ISIS overload
    SELECT device_pk AS entity_pk, bucket_ts,
        toFloat64(1) AS metric_value,
        'isis_overload' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND isis_overload = true
      AND link_pk = ''
    GROUP BY device_pk, bucket_ts

    UNION ALL

    -- ISIS unreachable
    SELECT device_pk AS entity_pk, bucket_ts,
        toFloat64(1) AS metric_value,
        'isis_unreachable' AS incident_type
    FROM device_interface_rollup_5m FINAL
    WHERE bucket_ts >= now() - INTERVAL 8 DAY
      AND isis_unreachable = true
      AND link_pk = ''
    GROUP BY device_pk, bucket_ts
),

islands AS (
    SELECT entity_pk, incident_type, bucket_ts, metric_value,
        bucket_ts - toIntervalSecond(row_number() OVER (
            PARTITION BY entity_pk, incident_type ORDER BY bucket_ts
        ) * 5 * 60) AS island_grp
    FROM above
),

raw_incidents AS (
    SELECT entity_pk, incident_type, island_grp,
        min(bucket_ts) AS started_at,
        max(bucket_ts) + toIntervalSecond(5 * 60) AS ended_at,
        max(metric_value) AS peak_value,
        count() AS bucket_count
    FROM islands
    GROUP BY entity_pk, incident_type, island_grp
),

numbered AS (
    SELECT *,
        lagInFrame(ended_at) OVER (
            PARTITION BY entity_pk, incident_type ORDER BY started_at
        ) AS prev_ended_at
    FROM raw_incidents
),
coalesce_groups AS (
    SELECT *,
        sum(if(prev_ended_at IS NULL
            OR dateDiff('minute', prev_ended_at, started_at) >= 180, 1, 0))
            OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS coalesce_grp
    FROM numbered
),
coalesced AS (
    SELECT entity_pk, incident_type,
        min(started_at) AS started_at,
        max(ended_at) AS ended_at,
        max(peak_value) AS peak_value,
        sum(bucket_count) AS total_buckets
    FROM coalesce_groups
    GROUP BY entity_pk, incident_type, coalesce_grp
)

SELECT
    c.entity_pk,
    c.incident_type,
    c.started_at,
    if(c.ended_at >= now() - toIntervalMinute(15), toDateTime('1970-01-01 00:00:00'), c.ended_at) AS ended_at,
    c.ended_at >= now() - toIntervalMinute(15) AS is_ongoing,
    c.peak_value,
    c.total_buckets,
    dateDiff('second', c.started_at,
        if(c.ended_at >= now() - toIntervalMinute(15), now(), c.ended_at)) AS duration_seconds,
    COALESCE(d.code, '') AS device_code,
    COALESCE(d.device_type, '') AS device_type,
    COALESCE(d.status, '') AS status,
    COALESCE(m.code, '') AS metro,
    COALESCE(cc.code, '') AS contributor_code
FROM coalesced c
LEFT JOIN dz_devices_current d ON c.entity_pk = d.pk
LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT JOIN dz_contributors_current cc ON d.contributor_pk = cc.pk
ORDER BY c.started_at DESC;

-- +goose Down

DROP VIEW IF EXISTS link_incidents_v;
DROP VIEW IF EXISTS device_incidents_v;
