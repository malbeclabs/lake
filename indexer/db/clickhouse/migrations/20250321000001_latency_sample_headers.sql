-- +goose Up

-- +goose StatementBegin
-- Stores the on-chain latency sample header for each device link circuit per epoch.
-- Each indexer refresh cycle persists the header it reads from the RPC, providing:
--   1. Freshness signal: written_at tells you when the indexer last read this circuit
--   2. Timestamp correction: written_at anchors latest_sample_index to wall-clock time,
--      allowing corrected timestamps via: written_at - (latest_index - sample_index) * sampling_interval_us
CREATE TABLE IF NOT EXISTS fact_dz_device_link_latency_sample_header
(
    written_at               DateTime64(3, 'UTC'),
    origin_device_pk         String,
    target_device_pk         String,
    link_pk                  String,
    epoch                    Int64,
    start_timestamp_us       Int64,
    sampling_interval_us     UInt64,
    latest_sample_index      Int32
) ENGINE = ReplacingMergeTree(written_at)
ORDER BY (origin_device_pk, target_device_pk, link_pk, epoch);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_dz_internet_metro_latency_sample_header
(
    written_at               DateTime64(3, 'UTC'),
    origin_metro_pk          String,
    target_metro_pk          String,
    data_provider            LowCardinality(String),
    epoch                    Int64,
    start_timestamp_us       Int64,
    sampling_interval_us     UInt64,
    latest_sample_index      Int32
) ENGINE = ReplacingMergeTree(written_at)
ORDER BY (origin_metro_pk, target_metro_pk, data_provider, epoch);
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_dz_internet_metro_latency_sample_header;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_dz_device_link_latency_sample_header;
-- +goose StatementEnd
