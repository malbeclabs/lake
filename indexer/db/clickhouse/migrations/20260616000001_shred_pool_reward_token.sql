-- +goose Up

-- Clear the leaf distribution status history so the per-leaf reward-token
-- attribution rebuilds cleanly. The pre-multi-token projection marked any
-- cleared 2Z-journal bit in the accumulation prefix as a distributed 2Z leaf;
-- for epochs ≥ 968 that mistagged USDC/wSOL leaves as 2Z. The new projection
-- only marks a leaf distributed for the token it was previously attributed to,
-- so the stale 2Z rows must be cleared or they would perpetuate. The table is
-- window-bounded (~12 recent epochs) and repopulated every indexer refresh, so
-- this is cheap; older epochs whose journals are already swept cannot be
-- re-attributed regardless.
TRUNCATE TABLE IF EXISTS dim_dz_shred_validator_leaf_distribution_status_history;
TRUNCATE TABLE IF EXISTS stg_dim_dz_shred_validator_leaf_distribution_status_snapshot;

-- Multi-token reward pools.
--
-- From epoch 968 the shred-subscription program supports multiple reward tokens
-- (2Z, USDC, wSOL). Each token gets its own ShredDistributionJournal per epoch,
-- owning only the leaves of validators who picked that token. Two new columns
-- carry what the rewards page needs to split each token's pool correctly:
--
--   reward_mint               base58 of the token this journal pays out in. The
--                             API joins each leaf (tagged via the leaf
--                             distribution status' journal_mint_key) to the pool
--                             for its token. Empty on legacy rows → treated as 2Z.
--   accumulated_slots_scaled  the journal's authoritative per-token denominator,
--                             accumulated_publisher_slots_scaled +
--                             accumulated_client_slots_scaled (= this token's
--                             leader slots × 10000). The API divides by this
--                             rather than the epoch-wide total_leader_slots so a
--                             validator's reward is split over its journal's
--                             slots. Zero on legacy rows → API falls back to
--                             total_leader_slots × 10000, then summed leaves.
--
-- Appended as the last columns (matching the dim-type-2 positional batch insert,
-- whose payload order now ends with reward_mint, accumulated_slots_scaled) via
-- ADD COLUMN, so the existing pool history — which must survive journal sweeps
-- for all-time earnings — is preserved rather than rebuilt. Existing rows default
-- to empty/0; the indexer backfills the real values on its next refresh for every
-- still-readable journal. The 2Z journal keeps its bare epoch-{e} pk so its
-- historical series stays one entity per epoch; other tokens are keyed
-- epoch-{e}:mint-{mint}.
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    ADD COLUMN IF NOT EXISTS reward_mint String;
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    ADD COLUMN IF NOT EXISTS accumulated_slots_scaled UInt64;
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    ADD COLUMN IF NOT EXISTS distributed_amount UInt64;
ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    ADD COLUMN IF NOT EXISTS reward_mint String;
ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    ADD COLUMN IF NOT EXISTS accumulated_slots_scaled UInt64;
ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    ADD COLUMN IF NOT EXISTS distributed_amount UInt64;

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_distribution_2z_pool_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_distribution_2z_pool_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    subscription_epoch, tokens_received_2z, total_leader_slots,
    reward_mint, accumulated_slots_scaled, distributed_amount
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_distribution_2z_pool_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_distribution_2z_pool_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    subscription_epoch, tokens_received_2z, total_leader_slots
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    DROP COLUMN IF EXISTS distributed_amount;
ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    DROP COLUMN IF EXISTS accumulated_slots_scaled;
ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    DROP COLUMN IF EXISTS reward_mint;
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    DROP COLUMN IF EXISTS distributed_amount;
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    DROP COLUMN IF EXISTS accumulated_slots_scaled;
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    DROP COLUMN IF EXISTS reward_mint;
