-- +goose Up

-- +goose StatementBegin
-- health_multicast_user_rate: rewrite to fix two bugs in the original definition.
--
-- 1. ReplacingMergeTree fan-out
--    device_interface_rollup_5m is SharedReplacingMergeTree. Between merges,
--    the same logical (bucket_ts, device_pk, intf) row can sit on disk as
--    multiple part copies. The original view's user_rates CTE joined the
--    raw table back to itself via recent_bucket, so every part copy became
--    its own row in the view output. Symptoms:
--      a. Duplicate rows per (user_pk, multicast_group_pk, mode).
--      b. gpt_total_publisher_rx_bps inflated by the duplicate factor,
--         which made subscriber reconciliation flag false 'mismatch'.
--    The rewrite collapses recent_bucket + user_rates into one aggregating
--    CTE that picks the latest bucket via max(bucket_ts) and dedupes within
--    that bucket via argMax. Part copies share identical values by RMT
--    contract, so argMax's tie-break is safe.
--
-- 2. multi_group_ambiguity is gone
--    The original view bailed to rate_status='unknown'/'multi_group_ambiguity'
--    when a user's tunnel was shared across multicast groups (per-row) or
--    when a publisher in the group was multi-group (per-group). Per product
--    direction: when the subscriber TX doesn't match expected within tolerance,
--    flag it as a mismatch (combined verdict: degraded) even when we cannot
--    cleanly attribute per-group. Operators read this as "user tunnel
--    deviates, investigate." The standard tolerance check runs over whatever
--    observed/expected we have; cross-group contamination shows up as a
--    real-looking mismatch because the math doesn't add up. We accept the
--    occasional false-positive in exchange for not silently suppressing real
--    divergence.
--
-- Behavior carried over unchanged from the original view:
--   - Join key still includes user_pk so tunnel reuse is handled correctly.
--   - rate_bucket_ts / observed_bps_5m / expected_bps_5m semantics by mode.
--   - Idle / monitoring_gap / group_idle reasons still distinguish "publisher
--     transmitting 0" from "publisher missing counters" from "all idle".
--   - control_plane_status preserved alongside combined health_status.
CREATE OR REPLACE VIEW health_multicast_user_rate
AS
WITH
-- Aggregating + argMax collapses two sources of duplication into one CTE:
--   1. Multiple buckets per (device, tunnel, user) → keep max(bucket_ts).
--   2. ReplacingMergeTree part-row duplicates within that bucket → argMax
--      picks one (values are identical by RMT contract, so tie-break safe).
-- ur_present = 1 lets the outer LEFT JOIN distinguish "no row" (default
-- 0) from "row exists with rate 0" (sentinel 1).
user_rates AS (
    SELECT
        device_pk AS ur_device_pk,
        user_tunnel_id AS ur_user_tunnel_id,
        user_pk AS ur_user_pk,
        max(bucket_ts) AS ur_bucket_ts,
        argMax(max_in_bps, bucket_ts) AS ur_max_in_bps,
        argMax(max_out_bps, bucket_ts) AS ur_max_out_bps,
        1 AS ur_present
    FROM device_interface_rollup_5m
    WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
      AND user_tunnel_id IS NOT NULL
      AND user_pk != ''
    GROUP BY ur_device_pk, ur_user_tunnel_id, ur_user_pk
),
-- Per-group publisher RX total + missing-publisher count so subscribers
-- (and P+S users on the subscriber side) can reconcile their TX against
-- expected sum-of-publishers. gpt_missing_publisher_count > 0 means at
-- least one publisher in the group has no counter row, which forces
-- subscribers to 'monitoring_gap' (we refuse to reconcile against a
-- deflated expected).
group_publisher_total AS (
    SELECT
        h.multicast_group_pk AS gpt_multicast_group_pk,
        sumIf(ur.ur_max_in_bps, ur.ur_present = 1) AS gpt_total_publisher_rx_bps,
        countIf(ur.ur_present = 0) AS gpt_missing_publisher_count,
        count() AS gpt_publisher_count
    FROM health_multicast_user h
    LEFT JOIN user_rates ur
        ON ur.ur_device_pk = h.user_device_pk
       AND ur.ur_user_tunnel_id = h.user_tunnel_id
       AND ur.ur_user_pk = h.user_pk
    WHERE h.mode IN ('P', 'P+S')
    GROUP BY gpt_multicast_group_pk
),
-- Inner layer: compute rate_status_reason / observed_bps_5m / expected_bps_5m
-- / control_plane_status. The outer SELECT rolls these into the three-value
-- rate_status and the combined health_status.
with_rate AS (
    SELECT
        h.* EXCEPT (health_status),
        h.health_status AS control_plane_status,
        if(ur.ur_present = 1, ur.ur_bucket_ts, NULL) AS rate_bucket_ts,
        multiIf(
            ur.ur_present = 0, NULL,
            h.mode = 'P',   toNullable(ur.ur_max_in_bps),
            h.mode = 'S',   toNullable(ur.ur_max_out_bps),
            h.mode = 'P+S', toNullable(ur.ur_max_out_bps),
            NULL
        ) AS observed_bps_5m,
        multiIf(
            -- S: expected is the full group_total (only set when no missing publishers).
            h.mode = 'S' AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps),
            -- P+S: expected excludes self's contribution (PIM-SM RPF: source does
            -- not receive its own multicast back via the tree).
            h.mode = 'P+S' AND ur.ur_present = 1 AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps),
            NULL
        ) AS expected_bps_5m,
        multiIf(
            ur.ur_present = 0, 'no_data',

            -- Pure publisher: just observe activity.
            h.mode = 'P' AND ur.ur_max_in_bps > 0, 'active',
            h.mode = 'P', 'idle',

            -- Subscriber-side reconciliation (S and P+S).
            h.mode IN ('S', 'P+S') AND gpt.gpt_missing_publisher_count > 0, 'monitoring_gap',
            h.mode = 'P+S' AND (gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps) = 0, 'group_idle',
            h.mode = 'S'   AND gpt.gpt_total_publisher_rx_bps = 0, 'group_idle',

            h.mode = 'P+S' AND abs(ur.ur_max_out_bps - (gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps))
                                <= greatest(0.05 * (gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps), 1000000.0),
                'reconciled',
            h.mode = 'S'   AND abs(ur.ur_max_out_bps - gpt.gpt_total_publisher_rx_bps)
                                <= greatest(0.05 * gpt.gpt_total_publisher_rx_bps, 1000000.0),
                'reconciled',
            h.mode IN ('S', 'P+S'), 'mismatch',
            'no_data'
        ) AS rate_status_reason
    FROM health_multicast_user h
    LEFT JOIN user_rates ur
        ON ur.ur_device_pk = h.user_device_pk
       AND ur.ur_user_tunnel_id = h.user_tunnel_id
       AND ur.ur_user_pk = h.user_pk
    LEFT JOIN group_publisher_total gpt
        ON gpt.gpt_multicast_group_pk = h.multicast_group_pk
)
SELECT
    w.*,
    -- Roll rate_status_reason up into the three-value rate_status.
    if(rate_status_reason IN ('active', 'reconciled'), 'reconciled',
        if(rate_status_reason = 'mismatch', 'mismatch', 'unknown')
    ) AS rate_status,
    -- Combined verdict per the matrix.
    --                | rate=reconciled | rate=mismatch | rate=unknown
    --  CP=healthy    | healthy         | degraded      | unknown
    --  CP=degraded   | degraded        | unhealthy     | degraded
    --  CP=unhealthy  | unhealthy       | unhealthy     | unhealthy
    multiIf(
        control_plane_status = 'unhealthy', 'unhealthy',
        control_plane_status = 'degraded' AND rate_status_reason = 'mismatch', 'unhealthy',
        control_plane_status = 'degraded', 'degraded',
        control_plane_status = 'healthy' AND rate_status_reason IN ('active', 'reconciled'), 'healthy',
        control_plane_status = 'healthy' AND rate_status_reason = 'mismatch', 'degraded',
        control_plane_status = 'healthy', 'unknown',
        'unknown'
    ) AS health_status
FROM with_rate w;
-- +goose StatementEnd

-- +goose Down

-- Re-apply the previous view definition (with the fan-out bug and the
-- multi_group_ambiguity branches). Pulled from
-- 20260604000001_health_multicast_user_rate_view.sql.
-- +goose StatementBegin
CREATE OR REPLACE VIEW health_multicast_user_rate
AS
WITH
recent_bucket AS (
    SELECT
        device_pk AS rb_device_pk,
        user_tunnel_id AS rb_user_tunnel_id,
        user_pk AS rb_user_pk,
        max(bucket_ts) AS rb_bucket_ts
    FROM device_interface_rollup_5m
    WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
      AND user_tunnel_id IS NOT NULL
      AND user_pk != ''
    GROUP BY rb_device_pk, rb_user_tunnel_id, rb_user_pk
),
user_rates AS (
    SELECT
        r.device_pk AS ur_device_pk,
        r.user_tunnel_id AS ur_user_tunnel_id,
        r.user_pk AS ur_user_pk,
        r.bucket_ts AS ur_bucket_ts,
        r.max_in_bps AS ur_max_in_bps,
        r.max_out_bps AS ur_max_out_bps,
        1 AS ur_present
    FROM device_interface_rollup_5m r
    INNER JOIN recent_bucket rb
        ON r.device_pk = rb.rb_device_pk
       AND r.user_tunnel_id = rb.rb_user_tunnel_id
       AND r.user_pk = rb.rb_user_pk
       AND r.bucket_ts = rb.rb_bucket_ts
),
user_group_counts AS (
    SELECT
        h.user_device_pk AS ugc_device_pk,
        h.user_tunnel_id AS ugc_user_tunnel_id,
        h.user_pk        AS ugc_user_pk,
        countDistinct(h.multicast_group_pk) AS ugc_group_count
    FROM health_multicast_user h
    GROUP BY ugc_device_pk, ugc_user_tunnel_id, ugc_user_pk
),
group_publisher_total AS (
    SELECT
        h.multicast_group_pk AS gpt_multicast_group_pk,
        sumIf(ur.ur_max_in_bps, ur.ur_present = 1) AS gpt_total_publisher_rx_bps,
        countIf(ur.ur_present = 0) AS gpt_missing_publisher_count,
        countIf(ur.ur_present = 1 AND ugc.ugc_group_count > 1) AS gpt_ambiguous_publisher_count,
        count() AS gpt_publisher_count
    FROM health_multicast_user h
    LEFT JOIN user_rates ur
        ON ur.ur_device_pk = h.user_device_pk
       AND ur.ur_user_tunnel_id = h.user_tunnel_id
       AND ur.ur_user_pk = h.user_pk
    LEFT JOIN user_group_counts ugc
        ON ugc.ugc_device_pk = h.user_device_pk
       AND ugc.ugc_user_tunnel_id = h.user_tunnel_id
       AND ugc.ugc_user_pk = h.user_pk
    WHERE h.mode IN ('P', 'P+S')
    GROUP BY gpt_multicast_group_pk
),
with_rate AS (
    SELECT
        h.* EXCEPT (health_status),
        h.health_status AS control_plane_status,
        if(ur.ur_present = 1, ur.ur_bucket_ts, NULL) AS rate_bucket_ts,
        multiIf(
            ur.ur_present = 0, NULL,
            h.mode = 'P',   toNullable(ur.ur_max_in_bps),
            h.mode = 'S',   toNullable(ur.ur_max_out_bps),
            h.mode = 'P+S', toNullable(ur.ur_max_out_bps),
            NULL
        ) AS observed_bps_5m,
        multiIf(
            ugc.ugc_group_count > 1, NULL,
            gpt.gpt_ambiguous_publisher_count > 0, NULL,
            h.mode = 'S' AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps),
            h.mode = 'P+S' AND ur.ur_present = 1 AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps),
            NULL
        ) AS expected_bps_5m,
        multiIf(
            ur.ur_present = 0, 'no_data',
            ugc.ugc_group_count > 1 AND h.mode = 'P' AND ur.ur_max_in_bps = 0, 'idle',
            ugc.ugc_group_count > 1, 'multi_group_ambiguity',
            h.mode IN ('S', 'P+S') AND gpt.gpt_ambiguous_publisher_count > 0, 'multi_group_ambiguity',
            h.mode = 'P' AND ur.ur_max_in_bps > 0, 'active',
            h.mode = 'P', 'idle',
            h.mode IN ('S', 'P+S') AND gpt.gpt_missing_publisher_count > 0, 'monitoring_gap',
            h.mode = 'P+S' AND (gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps) = 0, 'group_idle',
            h.mode = 'S'   AND gpt.gpt_total_publisher_rx_bps = 0, 'group_idle',
            h.mode = 'P+S' AND abs(ur.ur_max_out_bps - (gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps))
                                <= greatest(0.05 * (gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps), 1000000.0),
                'reconciled',
            h.mode = 'S'   AND abs(ur.ur_max_out_bps - gpt.gpt_total_publisher_rx_bps)
                                <= greatest(0.05 * gpt.gpt_total_publisher_rx_bps, 1000000.0),
                'reconciled',
            h.mode IN ('S', 'P+S'), 'mismatch',
            'no_data'
        ) AS rate_status_reason
    FROM health_multicast_user h
    LEFT JOIN user_rates ur
        ON ur.ur_device_pk = h.user_device_pk
       AND ur.ur_user_tunnel_id = h.user_tunnel_id
       AND ur.ur_user_pk = h.user_pk
    LEFT JOIN user_group_counts ugc
        ON ugc.ugc_device_pk = h.user_device_pk
       AND ugc.ugc_user_tunnel_id = h.user_tunnel_id
       AND ugc.ugc_user_pk = h.user_pk
    LEFT JOIN group_publisher_total gpt
        ON gpt.gpt_multicast_group_pk = h.multicast_group_pk
)
SELECT
    w.*,
    if(rate_status_reason IN ('active', 'reconciled'), 'reconciled',
        if(rate_status_reason = 'mismatch', 'mismatch', 'unknown')
    ) AS rate_status,
    multiIf(
        control_plane_status = 'unhealthy', 'unhealthy',
        control_plane_status = 'degraded' AND rate_status_reason = 'mismatch', 'unhealthy',
        control_plane_status = 'degraded', 'degraded',
        control_plane_status = 'healthy' AND rate_status_reason IN ('active', 'reconciled'), 'healthy',
        control_plane_status = 'healthy' AND rate_status_reason = 'mismatch', 'degraded',
        control_plane_status = 'healthy', 'unknown',
        'unknown'
    ) AS health_status
FROM with_rate w;
-- +goose StatementEnd
