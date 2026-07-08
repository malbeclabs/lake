-- +goose Up

-- Add onchain BGP session state (User account: bgp_status, last_bgp_up_at,
-- last_bgp_reported_at) to the users dimension so consumers can distinguish a
-- user that is not connected (BGP down) from a multicast forwarding fault.

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history
    ADD COLUMN IF NOT EXISTS bgp_status String DEFAULT '' AFTER subscribers,
    ADD COLUMN IF NOT EXISTS last_bgp_up_at UInt64 DEFAULT 0 AFTER bgp_status,
    ADD COLUMN IF NOT EXISTS last_bgp_reported_at UInt64 DEFAULT 0 AFTER last_bgp_up_at;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    ADD COLUMN IF NOT EXISTS bgp_status String DEFAULT '' AFTER subscribers,
    ADD COLUMN IF NOT EXISTS last_bgp_up_at UInt64 DEFAULT 0 AFTER bgp_status,
    ADD COLUMN IF NOT EXISTS last_bgp_reported_at UInt64 DEFAULT 0 AFTER last_bgp_up_at;
-- +goose StatementEnd

-- +goose StatementBegin
-- Recreate dz_users_current to expose the BGP columns.
CREATE OR REPLACE VIEW dz_users_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_users_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner_pubkey,
    status,
    kind,
    client_ip,
    dz_ip,
    device_pk,
    tenant_pk,
    tunnel_id,
    publishers,
    subscribers,
    bgp_status,
    last_bgp_up_at,
    last_bgp_reported_at
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
-- Restore dz_users_current without the BGP columns.
CREATE OR REPLACE VIEW dz_users_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_users_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    pk,
    owner_pubkey,
    status,
    kind,
    client_ip,
    dz_ip,
    device_pk,
    tenant_pk,
    tunnel_id,
    publishers,
    subscribers
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history
    DROP COLUMN IF EXISTS bgp_status,
    DROP COLUMN IF EXISTS last_bgp_up_at,
    DROP COLUMN IF EXISTS last_bgp_reported_at;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    DROP COLUMN IF EXISTS bgp_status,
    DROP COLUMN IF EXISTS last_bgp_up_at,
    DROP COLUMN IF EXISTS last_bgp_reported_at;
-- +goose StatementEnd
