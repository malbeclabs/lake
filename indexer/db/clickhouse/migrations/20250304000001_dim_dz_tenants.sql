-- +goose Up

-- +goose StatementBegin
-- dz_tenants
-- History table (immutable SCD2, single source of truth)
CREATE TABLE IF NOT EXISTS dim_dz_tenants_history
(
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
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
-- Staging table (landing zone for snapshots, 7-day TTL)
CREATE TABLE IF NOT EXISTS stg_dim_dz_tenants_snapshot
(
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
-- dz_tenants_current view
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

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_tenants_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_tenants_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_tenants_history;
-- +goose StatementEnd
