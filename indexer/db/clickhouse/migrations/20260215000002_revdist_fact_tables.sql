-- +goose Up

-- Revenue Distribution Fact Tables
-- Append-only time-series / per-epoch tables with ReplacingMergeTree for idempotent re-ingestion

-- +goose StatementBegin
-- Per-epoch validator debts
CREATE TABLE IF NOT EXISTS fact_dz_revdist_validator_debts
(
    dz_epoch Int64,
    node_id String,
    amount Int64,
    ingested_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (dz_epoch, node_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Per-epoch contributor reward shares
CREATE TABLE IF NOT EXISTS fact_dz_revdist_reward_shares
(
    dz_epoch Int64,
    contributor_key String,
    unit_share Int64,
    total_unit_shares Int64,
    is_blocked Bool,
    economic_burn_rate Int64,
    ingested_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (dz_epoch, contributor_key);
-- +goose StatementEnd

-- +goose StatementBegin
-- SOL/2Z price snapshots
CREATE TABLE IF NOT EXISTS fact_dz_revdist_prices
(
    ts DateTime64(3),
    sol_price_usd Float64,
    twoz_price_usd Float64,
    swap_rate Float64,
    ingested_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (ts);
-- +goose StatementEnd

-- +goose Down
-- Note: Down migrations would drop tables, which is destructive.
-- Since we use IF NOT EXISTS, re-running up is safe.
