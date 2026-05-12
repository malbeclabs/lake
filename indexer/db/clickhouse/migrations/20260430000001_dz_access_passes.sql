-- +goose Up

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_access_passes_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner_pubkey String,
    type_tag String,
    associated_pubkey String,
    others_type_name String,
    others_key String,
    client_ip String,
    user_payer String,
    last_access_epoch UInt64,
    connection_count UInt16,
    status String,
    mgroup_pub_allowlist String,
    mgroup_sub_allowlist String,
    flags UInt8
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_access_passes_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    owner_pubkey String,
    type_tag String,
    associated_pubkey String,
    others_type_name String,
    others_key String,
    client_ip String,
    user_payer String,
    last_access_epoch UInt64,
    connection_count UInt16,
    status String,
    mgroup_pub_allowlist String,
    mgroup_sub_allowlist String,
    flags UInt8
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_access_passes_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_access_passes_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner_pubkey,
    type_tag,
    associated_pubkey,
    others_type_name,
    others_key,
    client_ip,
    user_payer,
    last_access_epoch,
    connection_count,
    status,
    mgroup_pub_allowlist,
    mgroup_sub_allowlist,
    flags
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_access_passes_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_access_passes_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_access_passes_history;
-- +goose StatementEnd
