-- +goose Up

-- Add an is_verified flag to validator rewards leaves so we can index S3 data
-- before the on-chain merkle root is posted. When the root lands, the indexer
-- re-fetches and either flips the flag (match) or tombstones the row
-- (mismatch).

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_shred_validator_rewards_leaves_snapshot
    ADD COLUMN IF NOT EXISTS is_verified UInt8 DEFAULT 0;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_validator_rewards_leaves_history
    ADD COLUMN IF NOT EXISTS is_verified UInt8 DEFAULT 0;
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
    leader_slots, client_id, leaf_index, is_verified
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

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

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_validator_rewards_leaves_history DROP COLUMN IF EXISTS is_verified;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_shred_validator_rewards_leaves_snapshot DROP COLUMN IF EXISTS is_verified;
-- +goose StatementEnd
