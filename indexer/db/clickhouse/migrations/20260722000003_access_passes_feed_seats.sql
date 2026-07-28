-- +goose Up
--
-- Add AccessPass.FeedSeats (Generalized Payments) to the access-passes
-- dimension: a JSON array of the per-feed seats purchased on an EdgeSeat pass,
-- each carrying the feed's billing lifecycle
-- ({feed_pk, max_users, max_future_users, current_users, anniversary_day,
-- window_end, terminates_at}; the last two are unix seconds). DEFAULT '[]' for
-- passes with no seats. A JSON column matches how mgroup_*_allowlist are stored.
--
-- NOTE: the AFTER placement is load-bearing. loadSnapshotIntoStaging appends a
-- positional batch with no column list, so the physical column order MUST match
-- AccessPassSchema.PayloadColumns() order (feed_seats last, after flags).

-- +goose StatementBegin
ALTER TABLE dim_dz_access_passes_history
    ADD COLUMN IF NOT EXISTS feed_seats String DEFAULT '[]' AFTER flags;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_access_passes_snapshot
    ADD COLUMN IF NOT EXISTS feed_seats String DEFAULT '[]' AFTER flags;
-- +goose StatementEnd

-- +goose StatementBegin
-- Recreate dz_access_passes_current to expose feed_seats.
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
    flags,
    feed_seats
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
-- Restore dz_access_passes_current without feed_seats.
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

-- +goose StatementBegin
ALTER TABLE dim_dz_access_passes_history
    DROP COLUMN IF EXISTS feed_seats;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE stg_dim_dz_access_passes_snapshot
    DROP COLUMN IF EXISTS feed_seats;
-- +goose StatementEnd
