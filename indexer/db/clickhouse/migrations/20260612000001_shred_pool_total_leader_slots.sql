-- +goose Up

-- Add the journal's authoritative leader-slot denominator to the 2Z pool table.
--
-- Validator earnings split the epoch's 2Z pool by leader_slots / total_leader_slots.
-- The query previously derived the denominator as sum(leader_slots) over the
-- indexed leaves, but those leaves are incomplete for older epochs (the S3 export
-- / verification lags), so the denominator was too small and every present
-- validator's share was over-credited by a per-epoch constant factor. The
-- ShredDistributionJournal carries the true total_leader_slots; record it here so
-- the API can divide by the authoritative value.
--
-- Appended as the last column (matching the dim-type-2 positional batch insert,
-- whose payload order now ends with total_leader_slots) via ADD COLUMN, so the
-- existing pool history — which must survive journal sweeps for all-time
-- earnings — is preserved rather than rebuilt. Existing rows default to 0; the
-- indexer backfills the real value on its next refresh for every still-readable
-- journal, and the API falls back to the summed-leaves denominator when it is 0.
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    ADD COLUMN IF NOT EXISTS total_leader_slots UInt64;
ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    ADD COLUMN IF NOT EXISTS total_leader_slots UInt64;

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

-- +goose Down

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

ALTER TABLE dim_dz_shred_distribution_2z_pool_history
    DROP COLUMN IF EXISTS total_leader_slots;
ALTER TABLE stg_dim_dz_shred_distribution_2z_pool_snapshot
    DROP COLUMN IF EXISTS total_leader_slots;
