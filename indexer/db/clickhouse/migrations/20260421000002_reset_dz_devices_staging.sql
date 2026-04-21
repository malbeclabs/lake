-- +goose Up
-- Recreate stg_dim_dz_devices_snapshot to match DeviceSchema and add location_pk.
-- Some environments had extra columns that caused INSERT failures.

-- +goose StatementBegin
ALTER TABLE dim_dz_devices_history ADD COLUMN IF NOT EXISTS location_pk String DEFAULT '' AFTER metro_pk;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_devices_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_devices_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    status String,
    device_type String,
    code String,
    public_ip String,
    contributor_pk String,
    metro_pk String,
    location_pk String DEFAULT '',
    max_users Int32,
    interfaces String DEFAULT '[]'
) ENGINE = MergeTree()
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_devices_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_devices_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    status,
    device_type,
    code,
    public_ip,
    contributor_pk,
    metro_pk,
    location_pk,
    max_users,
    interfaces
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_devices_current;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_devices_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_devices_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    status,
    device_type,
    code,
    public_ip,
    contributor_pk,
    metro_pk,
    max_users,
    interfaces
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_devices_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_devices_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    status String,
    device_type String,
    code String,
    public_ip String,
    contributor_pk String,
    metro_pk String,
    max_users Int32,
    interfaces String DEFAULT '[]'
) ENGINE = MergeTree()
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd
