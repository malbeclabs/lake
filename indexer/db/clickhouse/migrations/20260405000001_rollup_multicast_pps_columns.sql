-- +goose Up

-- Add multicast packet PPS percentile columns to device interface rollup.
-- Computed from in_multicast_pkts_delta / delta_duration and out_multicast_pkts_delta / delta_duration.
ALTER TABLE device_interface_rollup_5m
    ADD COLUMN IF NOT EXISTS avg_in_mcast_pps Float64 DEFAULT 0 AFTER max_out_pps,
    ADD COLUMN IF NOT EXISTS min_in_mcast_pps Float64 DEFAULT 0 AFTER avg_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS p50_in_mcast_pps Float64 DEFAULT 0 AFTER min_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS p90_in_mcast_pps Float64 DEFAULT 0 AFTER p50_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS p95_in_mcast_pps Float64 DEFAULT 0 AFTER p90_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS p99_in_mcast_pps Float64 DEFAULT 0 AFTER p95_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS max_in_mcast_pps Float64 DEFAULT 0 AFTER p99_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS avg_out_mcast_pps Float64 DEFAULT 0 AFTER max_in_mcast_pps,
    ADD COLUMN IF NOT EXISTS min_out_mcast_pps Float64 DEFAULT 0 AFTER avg_out_mcast_pps,
    ADD COLUMN IF NOT EXISTS p50_out_mcast_pps Float64 DEFAULT 0 AFTER min_out_mcast_pps,
    ADD COLUMN IF NOT EXISTS p90_out_mcast_pps Float64 DEFAULT 0 AFTER p50_out_mcast_pps,
    ADD COLUMN IF NOT EXISTS p95_out_mcast_pps Float64 DEFAULT 0 AFTER p90_out_mcast_pps,
    ADD COLUMN IF NOT EXISTS p99_out_mcast_pps Float64 DEFAULT 0 AFTER p95_out_mcast_pps,
    ADD COLUMN IF NOT EXISTS max_out_mcast_pps Float64 DEFAULT 0 AFTER p99_out_mcast_pps;

-- +goose Down

ALTER TABLE device_interface_rollup_5m
    DROP COLUMN IF EXISTS avg_in_mcast_pps,
    DROP COLUMN IF EXISTS min_in_mcast_pps,
    DROP COLUMN IF EXISTS p50_in_mcast_pps,
    DROP COLUMN IF EXISTS p90_in_mcast_pps,
    DROP COLUMN IF EXISTS p95_in_mcast_pps,
    DROP COLUMN IF EXISTS p99_in_mcast_pps,
    DROP COLUMN IF EXISTS max_in_mcast_pps,
    DROP COLUMN IF EXISTS avg_out_mcast_pps,
    DROP COLUMN IF EXISTS min_out_mcast_pps,
    DROP COLUMN IF EXISTS p50_out_mcast_pps,
    DROP COLUMN IF EXISTS p90_out_mcast_pps,
    DROP COLUMN IF EXISTS p95_out_mcast_pps,
    DROP COLUMN IF EXISTS p99_out_mcast_pps,
    DROP COLUMN IF EXISTS max_out_mcast_pps;
