-- +goose Up
--
-- Drop rate reconciliation from the multicast health verdict. See the view
-- header for the rationale (per-group rate attribution is impossible from
-- per-tunnel counters when users span multiple groups).
--
-- ROLLBACK COUPLING: the Down body restores the reconciled/mismatch/
-- monitoring_gap/group_idle rate vocabulary, but this PR's web bundle removes
-- those keys from the badge/reason maps and narrows the TS unions. Roll this
-- migration back together with the web deploy — a DB-only rollback would leave
-- unstyled badges and blank reasons. (Same coupling documented in 20260708000003.)

-- +goose StatementBegin
CREATE OR REPLACE VIEW health_multicast_user_rate
AS
-- Rate reconciliation against sum-of-publishers has been removed. A user's
-- device_interface_rollup_5m counter is per-tunnel and aggregates every group
-- that user publishes/subscribes to, so a per-group expected rate cannot be
-- attributed when a user participates in more than one group — which is the
-- common case. In practice 100% of the "degraded" verdicts this produced were
-- multi-group attribution artifacts, not delivery problems, so the reconciliation
-- was pure noise.
--
-- The verdict is now the control-plane verdict (health_multicast_user, which
-- already includes 'disconnected' for BGP-down users). Rate is kept only as an
-- informational presence signal (active / idle / no_data) plus the observed
-- bps; expected_bps_5m is retained as NULL for output-schema stability. The UI
-- links out to the per-tunnel traffic charts for the actual rates.
WITH
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
)
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
    -- Retained as NULL: reconciliation removed (see header).
    CAST(NULL AS Nullable(Float64)) AS expected_bps_5m,
    -- Presence-only signal; does NOT gate health_status.
    multiIf(
        ur.ur_present = 0, 'no_data',
        (h.mode = 'P' AND ur.ur_max_in_bps > 0)
            OR (h.mode IN ('S', 'P+S') AND ur.ur_max_out_bps > 0), 'active',
        'idle'
    ) AS rate_status_reason,
    -- Derived from the reason so the two can't drift; the only difference is
    -- that 'no_data' surfaces as 'unknown' in the status.
    if(rate_status_reason = 'no_data', 'unknown', rate_status_reason) AS rate_status,
    -- Verdict = control-plane + disconnected. Rate never downgrades it.
    h.health_status AS health_status
FROM health_multicast_user h
LEFT JOIN user_rates ur
    ON ur.ur_device_pk = h.user_device_pk
   AND ur.ur_user_tunnel_id = h.user_tunnel_id
   AND ur.ur_user_pk = h.user_pk;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
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
    -- Exclude BGP-down (disconnected) publishers: no counter row, so counting them
    -- forces subscribers to monitoring_gap/unknown (the masking this feature removes).
    -- Mirrors health_mroute (000005).
    WHERE h.mode IN ('P', 'P+S') AND h.health_status != 'disconnected'
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
        control_plane_status = 'disconnected', 'disconnected',
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
