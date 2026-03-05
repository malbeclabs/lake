-- +goose Up

-- validatorsapp_validators
-- Dimension table for validator data from validators.app API
-- SCD2 design: _history (MergeTree) + staging table

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_validatorsapp_validators_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    account String,
    name String,
    vote_account String,
    software_version String,
    software_client String,
    software_client_id Int32,
    jito Int32,
    jito_commission Int32,
    is_active Int32,
    is_dz Int32,
    active_stake Int64,
    commission Int32,
    delinquent Int32,
    epoch Int64,
    epoch_credits Int64,
    skipped_slot_percent String,
    total_score Int32,
    data_center_key String,
    autonomous_system_number Int64,
    latitude String,
    longitude String,
    ip String,
    stake_pools_list String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_validatorsapp_validators_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    account String,
    name String,
    vote_account String,
    software_version String,
    software_client String,
    software_client_id Int32,
    jito Int32,
    jito_commission Int32,
    is_active Int32,
    is_dz Int32,
    active_stake Int64,
    commission Int32,
    delinquent Int32,
    epoch Int64,
    epoch_credits Int64,
    skipped_slot_percent String,
    total_score Int32,
    data_center_key String,
    autonomous_system_number Int64,
    latitude String,
    longitude String,
    ip String,
    stake_pools_list String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_validatorsapp_validators_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_validatorsapp_validators_history;
-- +goose StatementEnd
