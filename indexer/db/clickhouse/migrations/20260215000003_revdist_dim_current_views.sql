-- +goose Up

-- Current Views for Revenue Distribution Dimension Tables

-- +goose StatementBegin
-- dz_revdist_config_current
CREATE OR REPLACE VIEW dz_revdist_config_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_revdist_config_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    program_id,
    flags,
    next_completed_epoch,
    admin_key,
    debt_accountant_key,
    rewards_accountant_key,
    contributor_manager_key,
    sol_2z_swap_program_id,
    burn_rate_limit,
    burn_rate_dz_epochs_to_increasing,
    burn_rate_dz_epochs_to_limit,
    base_block_rewards_pct,
    priority_block_rewards_pct,
    inflation_rewards_pct,
    jito_tips_pct,
    fixed_sol_amount,
    relay_placeholder_lamports,
    relay_distribute_rewards_lamports,
    debt_write_off_feature_activation_epoch
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_distributions_current
CREATE OR REPLACE VIEW dz_revdist_distributions_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_revdist_distributions_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    dz_epoch,
    flags,
    community_burn_rate,
    total_solana_validators,
    solana_validator_payments_count,
    total_solana_validator_debt,
    collected_solana_validator_payments,
    total_contributors,
    distributed_rewards_count,
    collected_prepaid_2z_payments,
    collected_2z_converted_from_sol,
    uncollectible_sol_debt,
    distributed_2z_amount,
    burned_2z_amount,
    solana_validator_write_off_count,
    base_block_rewards_pct,
    priority_block_rewards_pct,
    inflation_rewards_pct,
    jito_tips_pct,
    fixed_sol_amount,
    sol_price_usd,
    twoz_price_usd
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_journal_current
CREATE OR REPLACE VIEW dz_revdist_journal_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_revdist_journal_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    program_id,
    total_sol_balance,
    total_2z_balance,
    swap_2z_destination_balance,
    swapped_sol_amount,
    next_dz_epoch_to_sweep_tokens
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_validator_deposits_current
CREATE OR REPLACE VIEW dz_revdist_validator_deposits_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_revdist_validator_deposits_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    node_id,
    written_off_sol_debt
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_contributor_rewards_current
CREATE OR REPLACE VIEW dz_revdist_contributor_rewards_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_revdist_contributor_rewards_history
)
SELECT
    entity_id,
    snapshot_ts,
    ingested_at,
    op_id,
    attrs_hash,
    service_key,
    rewards_manager_key,
    flags,
    recipient_shares
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down
-- Note: Down migrations would drop views.
-- Since we use CREATE OR REPLACE, re-running up is safe.
