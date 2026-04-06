-- +goose Up

-- Device interfaces dimension table
-- Stores per-interface metadata from on-chain device data,
-- joinable with fact_dz_device_interface_counters and device_interface_rollup_5m
-- on (device_pk, intf).

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_device_interfaces_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pk String,
    intf String,
    status String,
    interface_type String,
    cyoa_type String,
    dia_type String,
    loopback_type String,
    routing_mode String,
    bandwidth UInt64,
    cir UInt64,
    mtu UInt16,
    vlan_id UInt16,
    node_segment_idx UInt16,
    user_tunnel_endpoint UInt8 DEFAULT 0
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_device_interfaces_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pk String,
    intf String,
    status String,
    interface_type String,
    cyoa_type String,
    dia_type String,
    loopback_type String,
    routing_mode String,
    bandwidth UInt64,
    cir UInt64,
    mtu UInt16,
    vlan_id UInt16,
    node_segment_idx UInt16,
    user_tunnel_endpoint UInt8 DEFAULT 0
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_device_interfaces_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_device_interfaces_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    device_pk,
    intf,
    status,
    interface_type,
    cyoa_type,
    dia_type,
    loopback_type,
    routing_mode,
    bandwidth,
    cir,
    mtu,
    vlan_id,
    node_segment_idx,
    user_tunnel_endpoint
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_device_interfaces_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_device_interfaces_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_device_interfaces_history;
-- +goose StatementEnd
