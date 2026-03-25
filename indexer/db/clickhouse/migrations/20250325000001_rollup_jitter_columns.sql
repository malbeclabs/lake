-- +goose Up

-- Add jitter (IPDV) percentile columns to link rollup.
-- Jitter is computed as abs(ipdv_us) from raw probe data, matching the latency chart query.
ALTER TABLE link_rollup_5m
    ADD COLUMN IF NOT EXISTS a_avg_jitter_us Float64 DEFAULT 0 AFTER a_samples,
    ADD COLUMN IF NOT EXISTS a_min_jitter_us Float64 DEFAULT 0 AFTER a_avg_jitter_us,
    ADD COLUMN IF NOT EXISTS a_p50_jitter_us Float64 DEFAULT 0 AFTER a_min_jitter_us,
    ADD COLUMN IF NOT EXISTS a_p90_jitter_us Float64 DEFAULT 0 AFTER a_p50_jitter_us,
    ADD COLUMN IF NOT EXISTS a_p95_jitter_us Float64 DEFAULT 0 AFTER a_p90_jitter_us,
    ADD COLUMN IF NOT EXISTS a_p99_jitter_us Float64 DEFAULT 0 AFTER a_p95_jitter_us,
    ADD COLUMN IF NOT EXISTS a_max_jitter_us Float64 DEFAULT 0 AFTER a_p99_jitter_us,
    ADD COLUMN IF NOT EXISTS z_avg_jitter_us Float64 DEFAULT 0 AFTER z_samples,
    ADD COLUMN IF NOT EXISTS z_min_jitter_us Float64 DEFAULT 0 AFTER z_avg_jitter_us,
    ADD COLUMN IF NOT EXISTS z_p50_jitter_us Float64 DEFAULT 0 AFTER z_min_jitter_us,
    ADD COLUMN IF NOT EXISTS z_p90_jitter_us Float64 DEFAULT 0 AFTER z_p50_jitter_us,
    ADD COLUMN IF NOT EXISTS z_p95_jitter_us Float64 DEFAULT 0 AFTER z_p90_jitter_us,
    ADD COLUMN IF NOT EXISTS z_p99_jitter_us Float64 DEFAULT 0 AFTER z_p95_jitter_us,
    ADD COLUMN IF NOT EXISTS z_max_jitter_us Float64 DEFAULT 0 AFTER z_p99_jitter_us;

-- +goose Down

ALTER TABLE link_rollup_5m
    DROP COLUMN IF EXISTS a_avg_jitter_us,
    DROP COLUMN IF EXISTS a_min_jitter_us,
    DROP COLUMN IF EXISTS a_p50_jitter_us,
    DROP COLUMN IF EXISTS a_p90_jitter_us,
    DROP COLUMN IF EXISTS a_p95_jitter_us,
    DROP COLUMN IF EXISTS a_p99_jitter_us,
    DROP COLUMN IF EXISTS a_max_jitter_us,
    DROP COLUMN IF EXISTS z_avg_jitter_us,
    DROP COLUMN IF EXISTS z_min_jitter_us,
    DROP COLUMN IF EXISTS z_p50_jitter_us,
    DROP COLUMN IF EXISTS z_p90_jitter_us,
    DROP COLUMN IF EXISTS z_p95_jitter_us,
    DROP COLUMN IF EXISTS z_p99_jitter_us,
    DROP COLUMN IF EXISTS z_max_jitter_us;
