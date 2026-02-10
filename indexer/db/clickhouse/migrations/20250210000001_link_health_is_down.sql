-- +goose Up

-- +goose StatementBegin
-- Add is_down column to dz_links_health_current
-- True when ALL samples in the most recent 5 minutes are losses (link currently down)
CREATE OR REPLACE VIEW dz_links_health_current
AS
WITH recent_latency AS (
    SELECT
        link_pk,
        COUNT(*) AS sample_count,
        countIf(loss = true) * 100.0 / COUNT(*) AS loss_pct,
        avgIf(rtt_us, loss = false AND rtt_us > 0) AS avg_rtt_us,
        quantileIf(0.95)(rtt_us, loss = false AND rtt_us > 0) AS p95_rtt_us,
        max(event_ts) AS last_sample_ts
    FROM fact_dz_device_link_latency
    WHERE event_ts >= now() - INTERVAL 1 HOUR
      AND link_pk != ''
    GROUP BY link_pk
),
recent_5min_loss AS (
    SELECT
        link_pk,
        COUNT(*) AS sample_count_5min,
        countIf(loss = true) AS loss_count_5min
    FROM fact_dz_device_link_latency
    WHERE event_ts >= now() - INTERVAL 5 MINUTE
      AND link_pk != ''
    GROUP BY link_pk
)
SELECT
    l.pk AS pk,
    l.code AS code,
    l.status AS status,
    l.isis_delay_override_ns AS isis_delay_override_ns,
    l.committed_rtt_ns AS committed_rtt_ns,
    l.bandwidth_bps AS bandwidth_bps,
    ma.code AS side_a_metro,
    mz.code AS side_z_metro,
    l.status = 'soft-drained' AS is_soft_drained,
    l.status = 'hard-drained' AS is_hard_drained,
    l.isis_delay_override_ns = 1000000000 AS is_isis_soft_drained,
    COALESCE(rl.loss_pct, 0) AS loss_pct,
    COALESCE(rl.loss_pct, 0) >= 1 AS has_packet_loss,
    COALESCE(rl.avg_rtt_us, 0) AS avg_rtt_us,
    COALESCE(rl.p95_rtt_us, 0) AS p95_rtt_us,
    CASE
        WHEN l.committed_rtt_ns > 0 AND COALESCE(rl.avg_rtt_us, 0) > (l.committed_rtt_ns / 1000.0)
        THEN true ELSE false
    END AS exceeds_committed_rtt,
    rl.last_sample_ts AS last_sample_ts,
    CASE
        WHEN rl.last_sample_ts IS NULL THEN true
        WHEN rl.last_sample_ts < now() - INTERVAL 2 HOUR THEN true
        ELSE false
    END AS is_dark,
    CASE
        WHEN COALESCE(r5.sample_count_5min, 0) > 0 AND r5.loss_count_5min = r5.sample_count_5min
        THEN true ELSE false
    END AS is_down
FROM dz_links_current l
LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
LEFT JOIN recent_latency rl ON l.pk = rl.link_pk
LEFT JOIN recent_5min_loss r5 ON l.pk = r5.link_pk;
-- +goose StatementEnd

-- +goose Down
-- Restore original view without is_down
-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_links_health_current
AS
WITH recent_latency AS (
    SELECT
        link_pk,
        COUNT(*) AS sample_count,
        countIf(loss = true) * 100.0 / COUNT(*) AS loss_pct,
        avgIf(rtt_us, loss = false AND rtt_us > 0) AS avg_rtt_us,
        quantileIf(0.95)(rtt_us, loss = false AND rtt_us > 0) AS p95_rtt_us,
        max(event_ts) AS last_sample_ts
    FROM fact_dz_device_link_latency
    WHERE event_ts >= now() - INTERVAL 1 HOUR
      AND link_pk != ''
    GROUP BY link_pk
)
SELECT
    l.pk AS pk,
    l.code AS code,
    l.status AS status,
    l.isis_delay_override_ns AS isis_delay_override_ns,
    l.committed_rtt_ns AS committed_rtt_ns,
    l.bandwidth_bps AS bandwidth_bps,
    ma.code AS side_a_metro,
    mz.code AS side_z_metro,
    l.status = 'soft-drained' AS is_soft_drained,
    l.status = 'hard-drained' AS is_hard_drained,
    l.isis_delay_override_ns = 1000000000 AS is_isis_soft_drained,
    COALESCE(rl.loss_pct, 0) AS loss_pct,
    COALESCE(rl.loss_pct, 0) >= 1 AS has_packet_loss,
    COALESCE(rl.avg_rtt_us, 0) AS avg_rtt_us,
    COALESCE(rl.p95_rtt_us, 0) AS p95_rtt_us,
    CASE
        WHEN l.committed_rtt_ns > 0 AND COALESCE(rl.avg_rtt_us, 0) > (l.committed_rtt_ns / 1000.0)
        THEN true ELSE false
    END AS exceeds_committed_rtt,
    rl.last_sample_ts AS last_sample_ts,
    CASE
        WHEN rl.last_sample_ts IS NULL THEN true
        WHEN rl.last_sample_ts < now() - INTERVAL 2 HOUR THEN true
        ELSE false
    END AS is_dark
FROM dz_links_current l
LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
LEFT JOIN recent_latency rl ON l.pk = rl.link_pk;
-- +goose StatementEnd
