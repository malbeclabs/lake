-- +goose Up

-- RFC-18: Add topology dimension tables and extend links with topology/drain fields

-- 1. Add link_topologies and unicast_drained columns to links history table
-- +goose StatementBegin
ALTER TABLE dim_dz_links_history ADD COLUMN IF NOT EXISTS link_topologies String DEFAULT '[]' AFTER isis_delay_override_ns;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_links_history ADD COLUMN IF NOT EXISTS unicast_drained UInt8 DEFAULT 0 AFTER link_topologies;
-- +goose StatementEnd

-- 2. Recreate links staging table with new columns
-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_links_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_links_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    status String,
    code String,
    tunnel_net String,
    contributor_pk String,
    side_a_pk String,
    side_z_pk String,
    side_a_iface_name String,
    side_z_iface_name String,
    side_a_ip String DEFAULT '',
    side_z_ip String DEFAULT '',
    link_type String,
    committed_rtt_ns Int64,
    committed_jitter_ns Int64,
    bandwidth_bps Int64,
    isis_delay_override_ns Int64,
    link_topologies String DEFAULT '[]',
    unicast_drained UInt8 DEFAULT 0
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- 3. Update links current view to include new columns
-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_links_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_links_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    status,
    code,
    tunnel_net,
    contributor_pk,
    side_a_pk,
    side_z_pk,
    side_a_iface_name,
    side_z_iface_name,
    side_a_ip,
    side_z_ip,
    link_type,
    committed_rtt_ns,
    committed_jitter_ns,
    bandwidth_bps,
    isis_delay_override_ns,
    link_topologies,
    unicast_drained
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- 4. Create topology dimension tables
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_topologies_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    name String,
    admin_group_bit UInt8,
    flex_algo_number UInt8,
    color UInt8,
    topo_constraint String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_topologies_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    name String,
    admin_group_bit UInt8,
    flex_algo_number UInt8,
    color UInt8,
    topo_constraint String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_topologies_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_topologies_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    name,
    admin_group_bit,
    flex_algo_number,
    color,
    topo_constraint
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- 5. Add include_topologies column to tenants history table
-- +goose StatementBegin
ALTER TABLE dim_dz_tenants_history ADD COLUMN IF NOT EXISTS include_topologies String DEFAULT '[]' AFTER billing_rate;
-- +goose StatementEnd

-- 6. Recreate tenants staging table with new column
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

-- 7. Update tenants current view to include new column
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

-- Rollback tenants: recreate staging without include_topologies, recreate view, drop column
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

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_topologies_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_topologies_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_topologies_history;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_links_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_links_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    status,
    code,
    tunnel_net,
    contributor_pk,
    side_a_pk,
    side_z_pk,
    side_a_iface_name,
    side_z_iface_name,
    side_a_ip,
    side_z_ip,
    link_type,
    committed_rtt_ns,
    committed_jitter_ns,
    bandwidth_bps,
    isis_delay_override_ns
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_links_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE stg_dim_dz_links_snapshot (
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    pk String,
    status String,
    code String,
    tunnel_net String,
    contributor_pk String,
    side_a_pk String,
    side_z_pk String,
    side_a_iface_name String,
    side_z_iface_name String,
    side_a_ip String DEFAULT '',
    side_z_ip String DEFAULT '',
    link_type String,
    committed_rtt_ns Int64,
    committed_jitter_ns Int64,
    bandwidth_bps Int64,
    isis_delay_override_ns Int64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_links_history DROP COLUMN IF EXISTS unicast_drained;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_links_history DROP COLUMN IF EXISTS link_topologies;
-- +goose StatementEnd
