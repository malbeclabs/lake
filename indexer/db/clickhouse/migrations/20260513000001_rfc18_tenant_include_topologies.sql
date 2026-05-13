-- +goose Up

-- RFC-18: Add include_topologies to tenants for flex-algo topology assignment

-- 1. Add include_topologies column to tenants history table
-- +goose StatementBegin
ALTER TABLE dim_dz_tenants_history ADD COLUMN IF NOT EXISTS include_topologies String DEFAULT '[]' AFTER billing_rate;
-- +goose StatementEnd

-- 2. Recreate tenants staging table with new column
-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_tenants_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_tenants_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner_pubkey String,
    code String,
    payment_status String,
    vrf_id UInt16,
    metro_routing UInt8 DEFAULT 0,
    route_liveness UInt8 DEFAULT 0,
    billing_rate UInt64,
    include_topologies String DEFAULT '[]'
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- 3. Update tenants current view to include new column
-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_tenants_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_tenants_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner_pubkey,
    code,
    payment_status,
    vrf_id,
    metro_routing,
    route_liveness,
    billing_rate,
    include_topologies
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_tenants_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_tenants_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner_pubkey,
    code,
    payment_status,
    vrf_id,
    metro_routing,
    route_liveness,
    billing_rate
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_tenants_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_tenants_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner_pubkey String,
    code String,
    payment_status String,
    vrf_id UInt16,
    metro_routing UInt8,
    route_liveness UInt8,
    billing_rate UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_tenants_history DROP COLUMN IF EXISTS include_topologies;
-- +goose StatementEnd
