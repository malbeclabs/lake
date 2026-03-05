-- +goose Up

-- +goose StatementBegin
-- validatorsapp_validators_current
-- Current view for validators.app dimension table
CREATE OR REPLACE VIEW validatorsapp_validators_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_validatorsapp_validators_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    account,
    name,
    vote_account,
    software_version,
    software_client,
    software_client_id,
    jito,
    jito_commission,
    is_active,
    is_dz,
    active_stake,
    commission,
    delinquent,
    epoch,
    epoch_credits,
    skipped_slot_percent,
    total_score,
    data_center_key,
    autonomous_system_number,
    latitude,
    longitude,
    ip,
    stake_pools_list
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS validatorsapp_validators_current;
-- +goose StatementEnd
