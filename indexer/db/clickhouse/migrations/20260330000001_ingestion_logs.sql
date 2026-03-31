-- +goose Up

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS log_ingestion_runs
(
    run_id               UUID,
    workflow             LowCardinality(String),
    activity             LowCardinality(String),
    network              LowCardinality(String),
    status               LowCardinality(String),
    started_at           DateTime64(3),
    finished_at          DateTime64(3),
    duration_ms          UInt64,
    rows_affected        Nullable(Int64),
    error_message        Nullable(String),
    source_min_event_ts  Nullable(DateTime64(3)),
    source_max_event_ts  Nullable(DateTime64(3))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (workflow, activity, network, started_at)
TTL started_at + INTERVAL 90 DAY;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP TABLE IF EXISTS log_ingestion_runs;
-- +goose StatementEnd
