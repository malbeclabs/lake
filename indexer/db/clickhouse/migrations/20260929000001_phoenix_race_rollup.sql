-- +goose Up
DROP TABLE IF EXISTS phoenix_race_rollup_15m;

CREATE TABLE phoenix_race_rollup_15m (
    site LowCardinality(String),
    bucket_ts DateTime,
    ingested_at DateTime64(3),
    paired UInt64,
    dz_wins UInt64,
    venue_wins UInt64,
    signed_lead_p50_ms Float64,
    signed_lead_p95_ms Float64,
    signed_lead_p99_ms Float64,
    signed_lead_ms_state AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float64)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (site, bucket_ts);

-- +goose Down
DROP TABLE IF EXISTS phoenix_race_rollup_15m;
