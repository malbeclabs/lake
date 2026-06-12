-- +goose Up

-- Fix: validator rewards collapsed multiple software clients in a single epoch.
--
-- The leaf grain is (subscription_epoch, node_id, client_id) — a validator that
-- publishes under more than one client (e.g. Agave + Firedancer) produces one
-- merkle leaf per client. The dim-type-2 entity_id is hash(pk), and pk omitted
-- client_id, so those leaves collapsed to a single row: the node's leader slots
-- (and therefore its earnings) were understated.
--
-- The leaves table columns are unchanged — only the pk/entity_id values now
-- include client_id — so we truncate the history + staging tables and let the
-- indexer rebuild from the public S3 export (which retains past epochs). The
-- refresh loop re-fetches every epoch once LeavesStatusForEpoch reports empty.
TRUNCATE TABLE IF EXISTS dim_dz_shred_validator_rewards_leaves_history;
TRUNCATE TABLE IF EXISTS stg_dim_dz_shred_validator_rewards_leaves_snapshot;

-- The leaf distribution status (claimable bit) shares the same defect: the
-- publisher-accumulation bitmap is per leaf (i.e. per node+client), but the
-- table keyed per (subscription_epoch, node_id), so one client's bit could
-- overwrite another's. Extend the grain to include client_id. We drop and
-- recreate (rather than ALTER ... ADD COLUMN) so client_id lands in the correct
-- column position for the dim-type-2 positional batch insert. The table is
-- window-bounded (~12 most-recent epochs) and repopulated every refresh, so the
-- rebuild is cheap.
DROP VIEW IF EXISTS dim_dz_shred_validator_leaf_distribution_status_current;
DROP TABLE IF EXISTS dim_dz_shred_validator_leaf_distribution_status_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_validator_leaf_distribution_status_snapshot;

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
    client_id            UInt16,
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
    client_id            UInt16,
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
    subscription_epoch, node_id, client_id, is_claimable, journal_mint_key
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- Revert the status table to the per-(subscription_epoch, node_id) grain.
DROP VIEW IF EXISTS dim_dz_shred_validator_leaf_distribution_status_current;
DROP TABLE IF EXISTS dim_dz_shred_validator_leaf_distribution_status_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_validator_leaf_distribution_status_snapshot;

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

TRUNCATE TABLE IF EXISTS dim_dz_shred_validator_rewards_leaves_history;
TRUNCATE TABLE IF EXISTS stg_dim_dz_shred_validator_rewards_leaves_snapshot;
