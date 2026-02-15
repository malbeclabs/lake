-- +goose Up

-- Revenue Distribution Dimension Tables
-- SCD2 design: _history (MergeTree) + staging tables

-- +goose StatementBegin
-- dz_revdist_config
CREATE TABLE IF NOT EXISTS dim_dz_revdist_config_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    program_id String,
    flags Int64,
    next_completed_epoch Int64,
    admin_key String,
    debt_accountant_key String,
    rewards_accountant_key String,
    contributor_manager_key String,
    sol_2z_swap_program_id String,
    burn_rate_limit Int64,
    burn_rate_dz_epochs_to_increasing Int64,
    burn_rate_dz_epochs_to_limit Int64,
    base_block_rewards_pct Int32,
    priority_block_rewards_pct Int32,
    inflation_rewards_pct Int32,
    jito_tips_pct Int32,
    fixed_sol_amount Int64,
    relay_placeholder_lamports Int64,
    relay_distribute_rewards_lamports Int64,
    debt_write_off_feature_activation_epoch Int64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_revdist_config_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    program_id String,
    flags Int64,
    next_completed_epoch Int64,
    admin_key String,
    debt_accountant_key String,
    rewards_accountant_key String,
    contributor_manager_key String,
    sol_2z_swap_program_id String,
    burn_rate_limit Int64,
    burn_rate_dz_epochs_to_increasing Int64,
    burn_rate_dz_epochs_to_limit Int64,
    base_block_rewards_pct Int32,
    priority_block_rewards_pct Int32,
    inflation_rewards_pct Int32,
    jito_tips_pct Int32,
    fixed_sol_amount Int64,
    relay_placeholder_lamports Int64,
    relay_distribute_rewards_lamports Int64,
    debt_write_off_feature_activation_epoch Int64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_distributions
CREATE TABLE IF NOT EXISTS dim_dz_revdist_distributions_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    dz_epoch Int64,
    flags Int64,
    community_burn_rate Int64,
    total_solana_validators Int64,
    solana_validator_payments_count Int64,
    total_solana_validator_debt Int64,
    collected_solana_validator_payments Int64,
    total_contributors Int64,
    distributed_rewards_count Int64,
    collected_prepaid_2z_payments Int64,
    collected_2z_converted_from_sol Int64,
    uncollectible_sol_debt Int64,
    distributed_2z_amount Int64,
    burned_2z_amount Int64,
    solana_validator_write_off_count Int64,
    base_block_rewards_pct Int32,
    priority_block_rewards_pct Int32,
    inflation_rewards_pct Int32,
    jito_tips_pct Int32,
    fixed_sol_amount Int64,
    sol_price_usd Float64,
    twoz_price_usd Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_revdist_distributions_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    dz_epoch Int64,
    flags Int64,
    community_burn_rate Int64,
    total_solana_validators Int64,
    solana_validator_payments_count Int64,
    total_solana_validator_debt Int64,
    collected_solana_validator_payments Int64,
    total_contributors Int64,
    distributed_rewards_count Int64,
    collected_prepaid_2z_payments Int64,
    collected_2z_converted_from_sol Int64,
    uncollectible_sol_debt Int64,
    distributed_2z_amount Int64,
    burned_2z_amount Int64,
    solana_validator_write_off_count Int64,
    base_block_rewards_pct Int32,
    priority_block_rewards_pct Int32,
    inflation_rewards_pct Int32,
    jito_tips_pct Int32,
    fixed_sol_amount Int64,
    sol_price_usd Float64,
    twoz_price_usd Float64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_journal
CREATE TABLE IF NOT EXISTS dim_dz_revdist_journal_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    program_id String,
    total_sol_balance Int64,
    total_2z_balance Int64,
    swap_2z_destination_balance Int64,
    swapped_sol_amount Int64,
    next_dz_epoch_to_sweep_tokens Int64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_revdist_journal_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    program_id String,
    total_sol_balance Int64,
    total_2z_balance Int64,
    swap_2z_destination_balance Int64,
    swapped_sol_amount Int64,
    next_dz_epoch_to_sweep_tokens Int64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_validator_deposits
CREATE TABLE IF NOT EXISTS dim_dz_revdist_validator_deposits_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    node_id String,
    written_off_sol_debt Int64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_revdist_validator_deposits_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    node_id String,
    written_off_sol_debt Int64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
-- dz_revdist_contributor_rewards
CREATE TABLE IF NOT EXISTS dim_dz_revdist_contributor_rewards_history
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    service_key String,
    rewards_manager_key String,
    flags Int64,
    recipient_shares String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_revdist_contributor_rewards_snapshot
(
    entity_id String,
    snapshot_ts DateTime64(3),
    ingested_at DateTime64(3),
    op_id UUID,
    is_deleted UInt8 DEFAULT 0,
    attrs_hash UInt64,
    service_key String,
    rewards_manager_key String,
    flags Int64,
    recipient_shares String
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose Down
-- Note: Down migrations would drop tables, which is destructive.
-- Since we use IF NOT EXISTS, re-running up is safe.
