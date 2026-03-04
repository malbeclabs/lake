-- +goose Up

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history
    ADD COLUMN IF NOT EXISTS tenant_pk String DEFAULT '' AFTER device_pk;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    ADD COLUMN IF NOT EXISTS tenant_pk String DEFAULT '' AFTER device_pk;
-- +goose StatementEnd

-- +goose StatementBegin
-- Update dz_users_current view to include tenant_pk
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

-- +goose Down

-- +goose StatementBegin
-- Restore dz_users_current view without tenant_pk
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
    tunnel_id,
    publishers,
    subscribers
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history DROP COLUMN IF EXISTS tenant_pk;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot DROP COLUMN IF EXISTS tenant_pk;
-- +goose StatementEnd
