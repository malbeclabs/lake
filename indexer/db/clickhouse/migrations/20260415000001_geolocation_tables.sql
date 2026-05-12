-- +goose Up

-- +goose StatementBegin
-- geoloc_probes
-- History table (immutable SCD2, single source of truth)
CREATE TABLE IF NOT EXISTS dim_geoloc_probes_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner String,
    exchange_pk String,
    public_ip String,
    location_offset_port UInt16,
    metrics_publisher_pk String,
    reference_count UInt32,
    code String,
    parent_devices String,
    target_update_count UInt32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Staging table (landing zone for snapshots, 7-day TTL)
CREATE TABLE IF NOT EXISTS stg_dim_geoloc_probes_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner String,
    exchange_pk String,
    public_ip String,
    location_offset_port UInt16,
    metrics_publisher_pk String,
    reference_count UInt32,
    code String,
    parent_devices String,
    target_update_count UInt32
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- geoloc_probes_current view
CREATE OR REPLACE VIEW geoloc_probes_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_geoloc_probes_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner,
    exchange_pk,
    public_ip,
    location_offset_port,
    metrics_publisher_pk,
    reference_count,
    code,
    parent_devices,
    target_update_count
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- geoloc_users
-- History table (immutable SCD2, single source of truth)
CREATE TABLE IF NOT EXISTS dim_geoloc_users_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner String,
    code String,
    token_account String,
    payment_status String,
    status String,
    target_count UInt32,
    billing_rate UInt64,
    last_deduction_dz_epoch UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Staging table (landing zone for snapshots, 7-day TTL)
CREATE TABLE IF NOT EXISTS stg_dim_geoloc_users_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner String,
    code String,
    token_account String,
    payment_status String,
    status String,
    target_count UInt32,
    billing_rate UInt64,
    last_deduction_dz_epoch UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- geoloc_users_current view
CREATE OR REPLACE VIEW geoloc_users_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_geoloc_users_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner,
    code,
    token_account,
    payment_status,
    status,
    target_count,
    billing_rate,
    last_deduction_dz_epoch
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- geoloc_targets
-- History table (immutable SCD2, single source of truth)
CREATE TABLE IF NOT EXISTS dim_geoloc_targets_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    geoloc_user_pk String,
    probe_pk String,
    target_type String,
    ip String,
    location_offset_port UInt16,
    target_pk String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Staging table (landing zone for snapshots, 7-day TTL)
CREATE TABLE IF NOT EXISTS stg_dim_geoloc_targets_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    geoloc_user_pk String,
    probe_pk String,
    target_type String,
    ip String,
    location_offset_port UInt16,
    target_pk String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- geoloc_targets_current view
CREATE OR REPLACE VIEW geoloc_targets_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_geoloc_targets_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    geoloc_user_pk,
    probe_pk,
    target_type,
    ip,
    location_offset_port,
    target_pk
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS geoloc_targets_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_geoloc_targets_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_geoloc_targets_history;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS geoloc_users_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_geoloc_users_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_geoloc_users_history;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS geoloc_probes_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_geoloc_probes_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_geoloc_probes_history;
-- +goose StatementEnd
