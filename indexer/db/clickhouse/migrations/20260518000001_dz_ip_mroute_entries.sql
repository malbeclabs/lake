-- +goose Up

-- +goose StatementBegin
-- dz_ip_mroute_entries
-- Type-2 dimension capturing per-device PIM multicast forwarding state.
-- Rows are scoped by (device_pubkey, vrf, mode, group_address, source_address);
-- a new row lands only when one of the payload columns changes between
-- snapshots, so steady-state OIL keeps the table compact.
--
-- Sourced from `show ip mroute | json` snapshots uploaded by
-- doublezero-telemetry --state-collect-enable.
CREATE TABLE IF NOT EXISTS dim_dz_ip_mroute_entries_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    vrf String,
    mode String,
    group_address String,
    source_address String,
    route_flags String,
    register_in_oif_list UInt8,
    rpf_interface String,
    rpf_rib String,
    rpf_prefix String,
    rpf_preference Int64,
    rpf_metric Int64,
    rpf_neighbor String,
    rpf_attached UInt8,
    rpf_has_block UInt8,
    oif_list String,
    oif_count Int64,
    creation_time DateTime64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_ip_mroute_entries_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    device_pubkey String,
    vrf String,
    mode String,
    group_address String,
    source_address String,
    route_flags String,
    register_in_oif_list UInt8,
    rpf_interface String,
    rpf_rib String,
    rpf_prefix String,
    rpf_preference Int64,
    rpf_metric Int64,
    rpf_neighbor String,
    rpf_attached UInt8,
    rpf_has_block UInt8,
    oif_list String,
    oif_count Int64,
    creation_time DateTime64(3)
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_ip_mroute_entries_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_ip_mroute_entries_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    device_pubkey,
    vrf,
    mode,
    group_address,
    source_address,
    route_flags,
    register_in_oif_list,
    rpf_interface,
    rpf_rib,
    rpf_prefix,
    rpf_preference,
    rpf_metric,
    rpf_neighbor,
    rpf_attached,
    rpf_has_block,
    oif_list,
    oif_count,
    creation_time
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP VIEW IF EXISTS dz_ip_mroute_entries_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS stg_dim_dz_ip_mroute_entries_snapshot;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS dim_dz_ip_mroute_entries_history;
-- +goose StatementEnd
