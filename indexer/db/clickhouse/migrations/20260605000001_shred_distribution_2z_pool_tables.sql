-- +goose Up

-- dim_dz_shred_distribution_2z_pool: per-subscription_epoch 2Z reward pool read
-- from the ShredDistributionJournal seeded by the 2Z mint. tokens_received_2z
-- is the journal's post-Jupiter-swap 2Z balance (rewards_amount() upstream),
-- which is the validator-pool the per-leaf publisher shares are drawn from.
--
-- This replaces the defunct ShredDistribution.distributed_validator_2z_amount
-- field (removed from the on-chain struct; always 0 in the lake). Unlike the
-- leaf_distribution_status table this is NOT bounded to the recent window — it
-- accumulates every epoch ever observed so all-time earnings survive after the
-- on-chain journal is swept/closed.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_distribution_2z_pool_snapshot (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    tokens_received_2z   UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_distribution_2z_pool_history (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    tokens_received_2z   UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_distribution_2z_pool_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_distribution_2z_pool_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    subscription_epoch, tokens_received_2z
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down
DROP VIEW IF EXISTS dim_dz_shred_distribution_2z_pool_current;
DROP TABLE IF EXISTS dim_dz_shred_distribution_2z_pool_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_distribution_2z_pool_snapshot;
