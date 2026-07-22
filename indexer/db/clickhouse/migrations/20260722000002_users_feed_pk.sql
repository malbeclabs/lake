-- +goose Up
--
-- Add User.FeedPk (Generalized Payments) to the users dimension: the base58
-- EdgeSeat feed whose per-feed seat the user consumed at connect. DEFAULT ''
-- means "no feed" (non-EdgeSeat/unicast users, whose onchain value is the zero
-- pubkey), keeping `WHERE feed_pk != ''` filters sane.
--
-- NOTE: the AFTER placement is load-bearing. loadSnapshotIntoStaging appends a
-- positional batch with no column list, so the physical column order MUST match
-- UserSchema.PayloadColumns() order (feed_pk last, after bgp_status).

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history
    ADD COLUMN IF NOT EXISTS feed_pk String DEFAULT '' AFTER bgp_status;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    ADD COLUMN IF NOT EXISTS feed_pk String DEFAULT '' AFTER bgp_status;
-- +goose StatementEnd

-- +goose StatementBegin
-- Recreate dz_users_current to expose feed_pk.
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
    feed_pk
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
-- Restore dz_users_current without feed_pk.
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
    bgp_status
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history
    DROP COLUMN IF EXISTS feed_pk;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    DROP COLUMN IF EXISTS feed_pk;
-- +goose StatementEnd
