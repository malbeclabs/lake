-- +goose Up

-- dim_dz_shred_feed_distributions: one row per (feed, calendar year, calendar
-- month), snapshotted from the feed-subscription program's FeedDistribution
-- accounts (program J9gupbyffs4XAoKn5NrJ4hrbdqW5ZfvMDaaas3FtH8yC).
--
-- collected_usdc is the account's collected_usdc_amount in USDC base units.
-- That field, not the feed's vault token balance, is the month's record: the
-- program only ever increments it, while the vault drains to zero once the
-- month settles.
--
-- The table accumulates every month ever observed, so a settled month keeps its
-- total after the on-chain account is closed.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_feed_distributions_snapshot (
    entity_id       String,
    snapshot_ts     DateTime64(3),
    ingested_at     DateTime64(3),
    op_id           UUID,
    is_deleted      UInt8 DEFAULT 0,
    attrs_hash      UInt64,
    pk              String,
    feed_key        String,
    year            UInt16,
    month           UInt8,
    collected_usdc  UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_feed_distributions_history (
    entity_id       String,
    snapshot_ts     DateTime64(3),
    ingested_at     DateTime64(3),
    op_id           UUID,
    is_deleted      UInt8 DEFAULT 0,
    attrs_hash      UInt64,
    pk              String,
    feed_key        String,
    year            UInt16,
    month           UInt8,
    collected_usdc  UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_feed_distributions_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_feed_distributions_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    feed_key, year, month, collected_usdc
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down
DROP VIEW IF EXISTS dim_dz_shred_feed_distributions_current;
DROP TABLE IF EXISTS dim_dz_shred_feed_distributions_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_feed_distributions_snapshot;
