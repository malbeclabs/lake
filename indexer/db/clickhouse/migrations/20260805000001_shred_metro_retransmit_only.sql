-- +goose Up
--
-- Add MetroHistory.retransmit_only_enabled to the shred metro dimension: bit 0
-- of the onchain flags, set when every device in the metro serves the retransmit
-- multicast group only (leader group excluded). DEFAULT 0 is accurate for
-- existing rows — the bit was reserved and unset, so no backfill is needed.
--
-- NOTE: the AFTER placement is load-bearing. loadSnapshotIntoStaging appends a
-- positional batch with no column list, so the physical column order MUST match
-- MetroHistorySchema.PayloadColumns() order (retransmit_only_enabled last,
-- after current_usdc_price_dollars).
--
-- Down drops the column, so it must run only after the indexer is rolled back to
-- a binary that does not write it.

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_metro_histories_history
    ADD COLUMN IF NOT EXISTS retransmit_only_enabled UInt8 DEFAULT 0 AFTER current_usdc_price_dollars;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_shred_metro_histories_snapshot
    ADD COLUMN IF NOT EXISTS retransmit_only_enabled UInt8 DEFAULT 0 AFTER current_usdc_price_dollars;
-- +goose StatementEnd

-- +goose StatementBegin
-- Recreate dim_dz_shred_metro_histories_current to expose retransmit_only_enabled.
CREATE OR REPLACE VIEW dim_dz_shred_metro_histories_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_metro_histories_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, exchange_key, is_current_price_finalized, total_initialized_devices,
    current_epoch, current_usdc_price_dollars, retransmit_only_enabled
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
-- Restore dim_dz_shred_metro_histories_current without retransmit_only_enabled.
CREATE OR REPLACE VIEW dim_dz_shred_metro_histories_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_metro_histories_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, exchange_key, is_current_price_finalized, total_initialized_devices,
    current_epoch, current_usdc_price_dollars
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_metro_histories_history
    DROP COLUMN IF EXISTS retransmit_only_enabled;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_shred_metro_histories_snapshot
    DROP COLUMN IF EXISTS retransmit_only_enabled;
-- +goose StatementEnd
