-- +goose Up

-- +goose StatementBegin
-- isis_adjacencies
-- History table (immutable SCD2, single source of truth)
CREATE TABLE IF NOT EXISTS dim_isis_adjacencies_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    system_id String,
    neighbor_system_id String,
    neighbor_addr String,
    device_pk String,
    link_pk String,
    hostname String,
    router_id String,
    local_addr String,
    metric Int64,
    adj_sids String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Staging table (landing zone for snapshots, 7-day TTL)
CREATE TABLE IF NOT EXISTS stg_dim_isis_adjacencies_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    system_id String,
    neighbor_system_id String,
    neighbor_addr String,
    device_pk String,
    link_pk String,
    hostname String,
    router_id String,
    local_addr String,
    metric Int64,
    adj_sids String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- isis_adjacencies_current view
CREATE OR REPLACE VIEW isis_adjacencies_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_isis_adjacencies_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    system_id,
    neighbor_system_id,
    neighbor_addr,
    device_pk,
    link_pk,
    hostname,
    router_id,
    local_addr,
    metric,
    adj_sids
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- isis_devices
-- History table (immutable SCD2, single source of truth)
CREATE TABLE IF NOT EXISTS dim_isis_devices_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    system_id String,
    device_pk String,
    hostname String,
    router_id String,
    overload UInt8,
    node_unreachable UInt8,
    sequence Int64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Staging table (landing zone for snapshots, 7-day TTL)
CREATE TABLE IF NOT EXISTS stg_dim_isis_devices_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    system_id String,
    device_pk String,
    hostname String,
    router_id String,
    overload UInt8,
    node_unreachable UInt8,
    sequence Int64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- isis_devices_current view
CREATE OR REPLACE VIEW isis_devices_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_isis_devices_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    system_id,
    device_pk,
    hostname,
    router_id,
    overload,
    node_unreachable,
    sequence
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS isis_devices_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_isis_devices_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_isis_devices_history;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS isis_adjacencies_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_isis_adjacencies_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_isis_adjacencies_history;
-- +goose StatementEnd
