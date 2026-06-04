-- +goose Up

-- +goose StatementBegin
-- health_multicast_user_rate
--
-- Extends health_multicast_user with a data-plane rate dimension. The
-- existing view verifies that the control plane (mroute state) reflects
-- the onchain expectation. This view adds:
--
--   1. observed_bps_5m — the 5-min max rate measured at the user's tunnel.
--      For publishers (P):       max_in_bps (device's RX from the publisher).
--      For subscribers (S):      max_out_bps (device's TX toward the subscriber).
--      For dual-role users (P+S): max_out_bps — the side reconciled.
--
--   2. expected_bps_5m — only defined for the reconciled side.
--      For S:   sum of every publisher's RX in the same multicast group.
--      For P+S: sum of every OTHER publisher's RX in the same group
--               (i.e., gpt_total - self.max_in_bps). Excluding self matches
--               PIM-SM behaviour where a source does not receive its own
--               multicast back via the tree.
--      Tolerance: |observed - expected| <= max(5%, 1 Mbps).
--
--   3. rate_status — reconciled | mismatch | unknown.
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
-- deflated expected. If all publishers are idle (sum = 0, or for P+S the
-- subtracted sum = 0), subscribers go unknown with reason 'group_idle'.
--
-- Join keys: (device_pk, user_tunnel_id, user_pk). Including user_pk
-- defends against tunnel-ID reuse within the freshness window — without
-- it, a tunnel reassigned from user A to user B could pull B's rate for
-- A's row.
--
-- Multi-group ambiguity: device_interface_rollup_5m is keyed by
-- (device_pk, user_tunnel_id, user_pk) — it does NOT split rate by
-- multicast group. When a single (device, tunnel, user) participates in
-- more than one multicast group, the observed rate is a per-tunnel
-- aggregate that cannot be cleanly attributed to any one group, so this
-- view refuses to reconcile it: rate_status_reason becomes
-- 'multi_group_ambiguity' and rate_status becomes 'unknown'. The
-- 'multi_group_ambiguity' verdict only kicks in for rows whose mode
-- requires reading the observed rate (P with active/idle distinction
-- and S / P+S which reconcile against an expected). Pure 'idle' / 'no_data'
-- semantics still hold even with one shared tunnel, so we don't mask them.
-- A future schema split (per-group counters) would let this view drop the
-- guard.
--
-- Track B1 of malbeclabs/infra#1501 (rate reconciliation).
CREATE OR REPLACE VIEW health_multicast_user_rate
AS
WITH
-- Latest 5-min bucket per (device, tunnel, user) within the freshness window.
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
    -- ur_present = 1 lets the outer LEFT JOIN distinguish "no row" (0) from
    -- "row exists with rate 0" (1). ClickHouse LEFT JOIN with
    -- non-Nullable right-hand columns returns defaults (0 / epoch) instead
    -- of NULL, so an explicit sentinel column is needed.
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
-- Count distinct multicast groups per (device, tunnel, user). A count > 1
-- means the rollup row's bps is shared across groups and per-group
-- attribution is ambiguous (see view header). count() suffices because
-- health_multicast_user holds one row per (user, group, mode).
user_group_counts AS (
    SELECT
        h.user_device_pk AS ugc_device_pk,
        h.user_tunnel_id AS ugc_user_tunnel_id,
        h.user_pk        AS ugc_user_pk,
        countDistinct(h.multicast_group_pk) AS ugc_group_count
    FROM health_multicast_user h
    GROUP BY ugc_device_pk, ugc_user_tunnel_id, ugc_user_pk
),
-- Per-publisher RX so P+S subscriber expected can subtract self's contribution.
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
-- Inner layer: compute rate_status_reason / rate_status / control_plane_status
-- so the outer SELECT can reference them when computing the combined verdict.
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
            -- Multi-group ambiguity: there is no per-group expected for a
            -- tunnel shared across groups.
            ugc.ugc_group_count > 1, NULL,
            -- S: expected is the full group_total (only set when no missing publishers).
            h.mode = 'S' AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps),
            -- P+S: expected excludes self's contribution.
            h.mode = 'P+S' AND ur.ur_present = 1 AND gpt.gpt_missing_publisher_count = 0,
                toNullable(gpt.gpt_total_publisher_rx_bps - ur.ur_max_in_bps),
            NULL
        ) AS expected_bps_5m,
        multiIf(
            -- no_data wins over multi_group_ambiguity: if the rollup has
            -- nothing for this tunnel, we don't know any of it anyway.
            ur.ur_present = 0, 'no_data',

            -- Multi-group ambiguity: a single (device, tunnel, user) shared
            -- across N>1 groups has a per-tunnel aggregate rate that can't
            -- be cleanly attributed to any one group. Refuse to reconcile.
            -- Exempt: pure 'idle' (max_in_bps = 0) on a publisher still
            -- means "publishing nothing" per-group, which is correct.
            ugc.ugc_group_count > 1 AND h.mode = 'P' AND ur.ur_max_in_bps = 0, 'idle',
            ugc.ugc_group_count > 1, 'multi_group_ambiguity',

            -- Pure publisher: just observe activity.
            h.mode = 'P' AND ur.ur_max_in_bps > 0, 'active',
            h.mode = 'P', 'idle',

            -- Subscriber-side reconciliation (S and P+S).
            h.mode IN ('S', 'P+S') AND gpt.gpt_missing_publisher_count > 0, 'monitoring_gap',

            -- Expected for P+S excludes self; for S it's the full group total.
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
