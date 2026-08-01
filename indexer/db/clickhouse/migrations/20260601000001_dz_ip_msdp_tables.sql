-- +goose Up

-- ============================================================================
-- MSDP state from doublezero-telemetry --state-collect-enable.
-- Three independent dimensions, one per `show ip msdp ...` command kind:
--   - dz_ip_msdp_peers           ← `show ip msdp summary`
--   - dz_ip_msdp_pim_sa_cache    ← `show ip msdp pim sa-cache`
--   - dz_ip_msdp_sa_cache        ← `show ip msdp sa-cache rejected`
--                                  (single command returns both accepted
--                                  and rejected SAs; status column
--                                  distinguishes them.)
-- ============================================================================

-- ------------------------------------------------------------------------
-- dz_ip_msdp_peers  ← `show ip msdp summary`
-- ------------------------------------------------------------------------

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_ip_msdp_peers_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    peer_address String,
    state String,
    session_start_time DateTime64(3),
    sa_count Int64,
    reset_count Int64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_ip_msdp_peers_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    peer_address String,
    state String,
    session_start_time DateTime64(3),
    sa_count Int64,
    reset_count Int64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_ip_msdp_peers_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_ip_msdp_peers_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    device_pubkey,
    peer_address,
    state,
    session_start_time,
    sa_count,
    reset_count
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- ------------------------------------------------------------------------
-- dz_ip_msdp_pim_sa_cache  ← `show ip msdp pim sa-cache`
-- ------------------------------------------------------------------------

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_ip_msdp_pim_sa_cache_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    group_address String,
    source_address String,
    rp_address String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_ip_msdp_pim_sa_cache_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    group_address String,
    source_address String,
    rp_address String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_ip_msdp_pim_sa_cache_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_ip_msdp_pim_sa_cache_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    device_pubkey,
    group_address,
    source_address,
    rp_address
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- ------------------------------------------------------------------------
-- dz_ip_msdp_sa_cache  ← `show ip msdp sa-cache rejected`
-- Combined accepted + rejected SAs distinguished by the `status` column.
-- ------------------------------------------------------------------------

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_ip_msdp_sa_cache_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    group_address String,
    source_address String,
    remote_address String,
    status String,
    rp_address String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_ip_msdp_sa_cache_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    group_address String,
    source_address String,
    remote_address String,
    status String,
    rp_address String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_ip_msdp_sa_cache_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_ip_msdp_sa_cache_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    device_pubkey,
    group_address,
    source_address,
    remote_address,
    status,
    rp_address
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_ip_msdp_sa_cache_current;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_ip_msdp_sa_cache_snapshot;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_ip_msdp_sa_cache_history;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_ip_msdp_pim_sa_cache_current;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_ip_msdp_pim_sa_cache_snapshot;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_ip_msdp_pim_sa_cache_history;
-- +goose StatementEnd

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_ip_msdp_peers_current;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_ip_msdp_peers_snapshot;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_ip_msdp_peers_history;
-- +goose StatementEnd
