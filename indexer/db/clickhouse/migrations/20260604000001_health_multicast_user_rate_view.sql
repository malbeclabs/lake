-- +goose Up

-- +goose StatementBegin
-- health_multicast_user_rate
--
-- Extends health_multicast_user with a data-plane rate dimension. The
-- existing view verifies that the control plane (mroute state) reflects
-- the onchain expectation. This view adds:
--
--   1. observed_bps_5m — the 5-min max rate measured at the user's tunnel.
--      For publishers: max_in_bps (device's RX from the publisher).
--      For subscribers: max_out_bps (device's TX toward the subscriber).
--
--   2. expected_bps_5m — only defined for subscribers: the sum of every
--      publisher's RX rate in the same multicast group. A subscriber's TX
--      should equal this sum.
--
--   3. rate_status — reconciled | mismatch | unknown.
--      Tolerance: |observed - expected| <= max(5%, 1 Mbps).
--
--   4. rate_status_reason — a more specific cause behind rate_status.
--      Distinguishes "no_data" (monitoring gap), "idle" (registered but
--      zero traffic), "monitoring_gap" / "group_idle" (subscriber-side
--      reasons), from "reconciled" / "mismatch" / "active".
--
--   5. health_status — REPLACES the CP-only verdict with the combined
--      verdict (per the matrix). control_plane_status carries the
--      original CP-only value so consumers can drill in.
--
-- Strict idle handling: if any publisher in the group has no counter row
-- in the last 15 minutes, every subscriber's rate_status collapses to
-- unknown with reason 'monitoring_gap'. We refuse to reconcile against a
-- deflated expected. If all publishers are idle (sum = 0), subscribers go
-- unknown with reason 'group_idle'.
--
-- Track B1 of malbeclabs/infra#1501 (rate reconciliation).
CREATE OR REPLACE VIEW health_multicast_user_rate
AS
WITH
-- Latest 5-min bucket per (device, tunnel) within the freshness window.
recent_bucket AS (
    SELECT
        device_pk AS rb_device_pk,
        user_tunnel_id AS rb_user_tunnel_id,
        max(bucket_ts) AS rb_bucket_ts
    FROM device_interface_rollup_5m
    WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
      AND user_tunnel_id IS NOT NULL
    GROUP BY rb_device_pk, rb_user_tunnel_id
),
user_rates AS (
    -- ur_present = 1 lets the outer LEFT JOIN distinguish "no row" (0) from
    -- "row exists with rate 0" (1). ClickHouse LEFT JOIN with
    -- non-Nullable right-hand columns returns defaults (0 / epoch) instead
    -- of NULL, so an explicit sentinel column is needed.
    SELECT
        r.device_pk AS ur_device_pk,
        r.user_tunnel_id AS ur_user_tunnel_id,
        r.bucket_ts AS ur_bucket_ts,
        r.max_in_bps AS ur_max_in_bps,
        r.max_out_bps AS ur_max_out_bps,
        1 AS ur_present
    FROM device_interface_rollup_5m r
    INNER JOIN recent_bucket rb
        ON r.device_pk = rb.rb_device_pk
       AND r.user_tunnel_id = rb.rb_user_tunnel_id
       AND r.bucket_ts = rb.rb_bucket_ts
),
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
    WHERE h.mode IN ('P', 'P+S')
    GROUP BY gpt_multicast_group_pk
),
-- Inner layer: compute rate_status_reason / rate_status / control_plane_status
-- so the outer SELECT can reference them when computing the combined verdict.
with_rate AS (
    SELECT
        h.* EXCEPT (health_status),
        h.health_status AS control_plane_status,
        if(ur.ur_present = 1, ur.ur_bucket_ts, NULL) AS rate_bucket_ts,
        multiIf(
            ur.ur_present = 0, NULL,
            h.mode IN ('P', 'P+S'), toNullable(ur.ur_max_in_bps),
            h.mode = 'S',          toNullable(ur.ur_max_out_bps),
            NULL
        ) AS observed_bps_5m,
        multiIf(
            h.mode = 'S' AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps),
            NULL
        ) AS expected_bps_5m,
        multiIf(
            h.mode IN ('P', 'P+S') AND ur.ur_present = 0, 'no_data',
            h.mode IN ('P', 'P+S') AND ur.ur_max_in_bps > 0, 'active',
            h.mode IN ('P', 'P+S'), 'idle',
            h.mode = 'S' AND ur.ur_present = 0, 'no_data',
            h.mode = 'S' AND gpt.gpt_missing_publisher_count > 0, 'monitoring_gap',
            h.mode = 'S' AND gpt.gpt_total_publisher_rx_bps = 0, 'group_idle',
            h.mode = 'S' AND abs(ur.ur_max_out_bps - gpt.gpt_total_publisher_rx_bps)
                             <= greatest(0.05 * gpt.gpt_total_publisher_rx_bps, 1000000.0),
                'reconciled',
            h.mode = 'S', 'mismatch',
            'no_data'
        ) AS rate_status_reason
    FROM health_multicast_user h
    LEFT JOIN user_rates ur
        ON ur.ur_device_pk = h.user_device_pk
       AND ur.ur_user_tunnel_id = h.user_tunnel_id
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

DROP VIEW IF EXISTS health_multicast_user_rate;
