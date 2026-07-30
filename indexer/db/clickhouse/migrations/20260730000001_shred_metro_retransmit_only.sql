-- +goose Up

-- Add RetransmitOnlyEnabled to dim_dz_shred_metro_histories.
-- Bit 0 of the on-chain MetroHistory.flags: when set, every device in the metro
-- serves the retransmit multicast group only (leader group excluded).
-- DEFAULT 0 is accurate for existing rows — the bit was reserved and unset.

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_metro_histories_history
    ADD COLUMN IF NOT EXISTS retransmit_only_enabled UInt8 DEFAULT 0 AFTER current_usdc_price_dollars;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_shred_metro_histories_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_shred_metro_histories_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    exchange_key String,
    is_current_price_finalized UInt8,
    total_initialized_devices UInt16,
    current_epoch UInt64,
    current_usdc_price_dollars UInt16,
    retransmit_only_enabled UInt8 DEFAULT 0
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
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
DROP TABLE IF EXISTS stg_dim_dz_shred_metro_histories_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_shred_metro_histories_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    exchange_key String,
    is_current_price_finalized UInt8,
    total_initialized_devices UInt16,
    current_epoch UInt64,
    current_usdc_price_dollars UInt16
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_metro_histories_history
    DROP COLUMN IF EXISTS retransmit_only_enabled;
-- +goose StatementEnd
