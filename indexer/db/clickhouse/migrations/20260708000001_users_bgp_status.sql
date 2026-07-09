-- +goose Up
--
-- Add the onchain BGP session status (User account) to the users dimension so
-- consumers can distinguish a user that is not connected (BGP down) from a
-- multicast forwarding fault.
--
-- Only bgp_status is ingested. The onchain last_bgp_up_at / last_bgp_reported_at
-- slots are deliberately NOT ingested: last_bgp_reported_at is rewritten by a
-- ~6-hourly onchain keepalive, and since attrs_hash covers every payload column
-- it would churn a new dim_dz_users_history row per user ~4x/day forever. If a
-- freshness signal is needed later, add it as a non-hashed column.
--
-- Column values: 'up' | 'down' | 'unknown' (agent never reported). DEFAULT
-- 'unknown' means "not yet reported"; consumers treat anything != 'down' as
-- fail-open (old behavior), so an unreported user is never masked.
--
-- NOTE: the AFTER placement is load-bearing. loadSnapshotIntoStaging appends a
-- positional batch with no column list, so the physical column order MUST match
-- UserSchema.PayloadColumns() order (bgp_status last, after subscribers).

-- +goose StatementBegin
ALTER TABLE dim_dz_users_history
    ADD COLUMN IF NOT EXISTS bgp_status String DEFAULT 'unknown' AFTER subscribers;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    ADD COLUMN IF NOT EXISTS bgp_status String DEFAULT 'unknown' AFTER subscribers;
-- +goose StatementEnd

-- +goose StatementBegin
-- Recreate dz_users_current to expose bgp_status.
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

-- +goose Down

-- +goose StatementBegin
-- Restore dz_users_current without bgp_status.
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
    DROP COLUMN IF EXISTS bgp_status;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_users_snapshot
    DROP COLUMN IF EXISTS bgp_status;
-- +goose StatementEnd
