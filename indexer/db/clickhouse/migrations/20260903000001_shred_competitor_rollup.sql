-- +goose Up

CREATE TABLE IF NOT EXISTS shred_competitor_rollup_1d (
    bucket_date Date,
    ingested_at DateTime64(3),
    leader_slots UInt64,
    win_typical_p50 Float64,
    lead_typical_ms Float64
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(bucket_date)
ORDER BY (bucket_date);

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS shred_competitor_rollup_1d;
-- +goose StatementEnd
