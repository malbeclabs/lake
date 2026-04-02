-- +goose Up

-- Shred subscription program dimension tables.
-- These track on-chain state from the DZ shred subscription program.

-- dim_dz_shred_execution_controller: singleton epoch state machine
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_execution_controller_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    phase String,
    current_subscription_epoch UInt64,
    total_metros UInt16,
    total_enabled_devices UInt16,
    total_client_seats UInt32,
    updated_device_prices_count UInt16,
    settled_devices_count UInt16,
    settled_client_seats_count UInt16,
    last_settled_slot UInt64,
    last_updating_prices_slot UInt64,
    last_open_for_requests_slot UInt64,
    last_closed_for_requests_slot UInt64,
    next_seat_funding_index UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_execution_controller_history (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    phase String,
    current_subscription_epoch UInt64,
    total_metros UInt16,
    total_enabled_devices UInt16,
    total_client_seats UInt32,
    updated_device_prices_count UInt16,
    settled_devices_count UInt16,
    settled_client_seats_count UInt16,
    last_settled_slot UInt64,
    last_updating_prices_slot UInt64,
    last_open_for_requests_slot UInt64,
    last_closed_for_requests_slot UInt64,
    next_seat_funding_index UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_execution_controller_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_execution_controller_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, phase, current_subscription_epoch, total_metros, total_enabled_devices,
    total_client_seats, updated_device_prices_count, settled_devices_count,
    settled_client_seats_count, last_settled_slot, last_updating_prices_slot,
    last_open_for_requests_slot, last_closed_for_requests_slot, next_seat_funding_index
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_client_seats: client subscription seats on devices
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_client_seats_snapshot (
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
CREATE TABLE IF NOT EXISTS dim_dz_shred_client_seats_history (
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
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
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
    has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_payment_escrows: USDC payment escrows for client seats
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_payment_escrows_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    client_seat_key String,
    withdraw_authority_key String,
    usdc_balance UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_payment_escrows_history (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    client_seat_key String,
    withdraw_authority_key String,
    usdc_balance UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_payment_escrows_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_payment_escrows_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, client_seat_key, withdraw_authority_key, usdc_balance
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_metro_histories: metro pricing state
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_metro_histories_snapshot (
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
CREATE TABLE IF NOT EXISTS dim_dz_shred_metro_histories_history (
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
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
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
    current_epoch, current_usdc_price_dollars
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_device_histories: device subscription state
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_device_histories_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    device_key String,
    is_enabled UInt8,
    has_settled_seats UInt8,
    metro_exchange_key String,
    active_granted_seats UInt16,
    active_total_available_seats UInt16,
    current_epoch UInt64,
    current_requested_seat_count UInt16,
    current_granted_seat_count UInt16,
    current_total_available_seats UInt16,
    current_usdc_metro_premium_dollars Int16
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_device_histories_history (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    device_key String,
    is_enabled UInt8,
    has_settled_seats UInt8,
    metro_exchange_key String,
    active_granted_seats UInt16,
    active_total_available_seats UInt16,
    current_epoch UInt64,
    current_requested_seat_count UInt16,
    current_granted_seat_count UInt16,
    current_total_available_seats UInt16,
    current_usdc_metro_premium_dollars Int16
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_device_histories_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_device_histories_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, device_key, is_enabled, has_settled_seats, metro_exchange_key,
    active_granted_seats, active_total_available_seats, current_epoch,
    current_requested_seat_count, current_granted_seat_count,
    current_total_available_seats, current_usdc_metro_premium_dollars
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_validator_client_rewards: registered validator clients
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_validator_client_rewards_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    client_id UInt16,
    manager_key String,
    short_description String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_validator_client_rewards_history (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    client_id UInt16,
    manager_key String,
    short_description String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_validator_client_rewards_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_validator_client_rewards_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, client_id, manager_key, short_description
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- dim_dz_shred_distributions: per-epoch payment/reward distribution
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_distributions_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    subscription_epoch UInt64,
    associated_dz_epoch UInt64,
    device_count UInt16,
    client_seat_count UInt16,
    validator_rewards_proportion UInt16,
    total_publishing_validators UInt32,
    collected_usdc_payments UInt64,
    collected_2z_converted_from_usdc UInt64,
    distributed_validator_rewards_count UInt32,
    distributed_contributor_rewards_count UInt32,
    distributed_validator_2z_amount UInt64,
    distributed_contributor_2z_amount UInt64,
    burned_2z_amount UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_distributions_history (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    subscription_epoch UInt64,
    associated_dz_epoch UInt64,
    device_count UInt16,
    client_seat_count UInt16,
    validator_rewards_proportion UInt16,
    total_publishing_validators UInt32,
    collected_usdc_payments UInt64,
    collected_2z_converted_from_usdc UInt64,
    distributed_validator_rewards_count UInt32,
    distributed_contributor_rewards_count UInt32,
    distributed_validator_2z_amount UInt64,
    distributed_contributor_2z_amount UInt64,
    burned_2z_amount UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_distributions_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_distributions_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash,
    pk, subscription_epoch, associated_dz_epoch, device_count, client_seat_count,
    validator_rewards_proportion, total_publishing_validators,
    collected_usdc_payments, collected_2z_converted_from_usdc,
    distributed_validator_rewards_count, distributed_contributor_rewards_count,
    distributed_validator_2z_amount, distributed_contributor_2z_amount,
    burned_2z_amount
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

DROP VIEW IF EXISTS dim_dz_shred_distributions_current;
DROP TABLE IF EXISTS dim_dz_shred_distributions_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_distributions_snapshot;

DROP VIEW IF EXISTS dim_dz_shred_validator_client_rewards_current;
DROP TABLE IF EXISTS dim_dz_shred_validator_client_rewards_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_validator_client_rewards_snapshot;

DROP VIEW IF EXISTS dim_dz_shred_device_histories_current;
DROP TABLE IF EXISTS dim_dz_shred_device_histories_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_device_histories_snapshot;

DROP VIEW IF EXISTS dim_dz_shred_metro_histories_current;
DROP TABLE IF EXISTS dim_dz_shred_metro_histories_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_metro_histories_snapshot;

DROP VIEW IF EXISTS dim_dz_shred_payment_escrows_current;
DROP TABLE IF EXISTS dim_dz_shred_payment_escrows_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_payment_escrows_snapshot;

DROP VIEW IF EXISTS dim_dz_shred_client_seats_current;
DROP TABLE IF EXISTS dim_dz_shred_client_seats_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_client_seats_snapshot;

DROP VIEW IF EXISTS dim_dz_shred_execution_controller_current;
DROP TABLE IF EXISTS dim_dz_shred_execution_controller_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_execution_controller_snapshot;
