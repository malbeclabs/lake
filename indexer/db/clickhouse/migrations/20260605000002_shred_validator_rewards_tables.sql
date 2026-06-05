-- +goose Up

-- dim_dz_shred_validator_rewards_leaves: per-(subscription_epoch, node_id) merkle leaf data
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_validator_rewards_leaves_snapshot (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    associated_dz_epoch  UInt64,
    node_id              String,
    leader_slots         UInt32,
    client_id            UInt16,
    leaf_index           UInt32
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_validator_rewards_leaves_history (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    associated_dz_epoch  UInt64,
    node_id              String,
    leader_slots         UInt32,
    client_id            UInt16,
    leaf_index           UInt32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_validator_rewards_leaves_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_validator_rewards_leaves_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    subscription_epoch, associated_dz_epoch, node_id,
    leader_slots, client_id, leaf_index
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_validator_leaf_distribution_status: per-(subscription_epoch, node_id) claimable bit
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_validator_leaf_distribution_status_snapshot (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    node_id              String,
    is_claimable         UInt8,
    journal_mint_key     String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_validator_leaf_distribution_status_history (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    node_id              String,
    is_claimable         UInt8,
    journal_mint_key     String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_validator_leaf_distribution_status_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_validator_leaf_distribution_status_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    subscription_epoch, node_id, is_claimable, journal_mint_key
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_distribution_client_proportions: per-(subscription_epoch, client_id) reward proportion
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_distribution_client_proportions_snapshot (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    client_id            UInt16,
    proportion           UInt16,
    default_proportion   UInt16
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_distribution_client_proportions_history (
    entity_id            String,
    snapshot_ts          DateTime64(3),
    ingested_at          DateTime64(3),
    op_id                UUID,
    is_deleted           UInt8 DEFAULT 0,
    attrs_hash           UInt64,
    pk                   String,
    subscription_epoch   UInt64,
    client_id            UInt16,
    proportion           UInt16,
    default_proportion   UInt16
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_distribution_client_proportions_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_distribution_client_proportions_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    subscription_epoch, client_id, proportion, default_proportion
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down
DROP VIEW IF EXISTS dim_dz_shred_distribution_client_proportions_current;
DROP TABLE IF EXISTS dim_dz_shred_distribution_client_proportions_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_distribution_client_proportions_snapshot;
DROP VIEW IF EXISTS dim_dz_shred_validator_leaf_distribution_status_current;
DROP TABLE IF EXISTS dim_dz_shred_validator_leaf_distribution_status_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_validator_leaf_distribution_status_snapshot;
DROP VIEW IF EXISTS dim_dz_shred_validator_rewards_leaves_current;
DROP TABLE IF EXISTS dim_dz_shred_validator_rewards_leaves_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_validator_rewards_leaves_snapshot;
