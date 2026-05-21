-- +goose Up

-- Add SubscriptionStartSlot and LastUSDCPriceDollars to dim_dz_shred_client_seats.
-- These were added to the on-chain ClientSeat for prorated service support and
-- are needed to compute prorated USDC charges per epoch.

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_client_seats_history
    ADD COLUMN IF NOT EXISTS subscription_start_slot UInt64 DEFAULT 0 AFTER funding_authority_key,
    ADD COLUMN IF NOT EXISTS last_usdc_price_dollars UInt16 DEFAULT 0 AFTER subscription_start_slot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_shred_client_seats_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_shred_client_seats_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    device_key String,
    client_ip String,
    tenure_epochs UInt16,
    funded_epoch UInt64,
    active_epoch UInt64,
    has_price_override UInt8,
    override_usdc_price_dollars UInt16,
    escrow_count UInt32,
    funding_authority_key String,
    subscription_start_slot UInt64 DEFAULT 0,
    last_usdc_price_dollars UInt16 DEFAULT 0
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_client_seats_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_client_seats_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
    has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
    subscription_start_slot, last_usdc_price_dollars
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_client_seats_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_client_seats_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
    has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_shred_client_seats_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_shred_client_seats_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    device_key String,
    client_ip String,
    tenure_epochs UInt16,
    funded_epoch UInt64,
    active_epoch UInt64,
    has_price_override UInt8,
    override_usdc_price_dollars UInt16,
    escrow_count UInt32,
    funding_authority_key String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_shred_client_seats_history
    DROP COLUMN IF EXISTS last_usdc_price_dollars,
    DROP COLUMN IF EXISTS subscription_start_slot;
-- +goose StatementEnd
