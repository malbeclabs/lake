-- +goose Up
--
-- Add the serviceability Feed account (Generalized Payments) as a Type-2 SCD
-- dimension, mirroring every other serviceability entity. A feed is a catalog
-- entry (SKU): one feed scoped to a single metro (exchange), holding the
-- multicast groups joinable there.
--
-- Columns follow lake conventions: metro_pk is the base58 of the onchain
-- Exchange field (lake renames "exchange" to "metro" everywhere), groups is a
-- JSON array of base58 multicast-group PKs (same shape as dz_users.publishers).

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_feeds_history
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
    name String,
    metro_pk String,
    groups String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_feeds_snapshot
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
    name String,
    metro_pk String,
    groups String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_feeds_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_feeds_history
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
    name,
    metro_pk,
    groups
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_feeds_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_feeds_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_feeds_history;
-- +goose StatementEnd
