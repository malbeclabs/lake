package dzrevdist

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// --- Dimension schemas (on-chain data) ---

// ConfigSchema defines the schema for the revdist program config.
type ConfigSchema struct{}

func (s *ConfigSchema) Name() string                { return "dz_revdist_config" }
func (s *ConfigSchema) PrimaryKeyColumns() []string { return []string{"program_id:VARCHAR"} }
func (s *ConfigSchema) PayloadColumns() []string {
	return []string{
		"flags:BIGINT",
		"next_completed_epoch:BIGINT",
		"admin_key:VARCHAR",
		"debt_accountant_key:VARCHAR",
		"rewards_accountant_key:VARCHAR",
		"contributor_manager_key:VARCHAR",
		"sol_2z_swap_program_id:VARCHAR",
		"burn_rate_limit:BIGINT",
		"burn_rate_dz_epochs_to_increasing:BIGINT",
		"burn_rate_dz_epochs_to_limit:BIGINT",
		"base_block_rewards_pct:INTEGER",
		"priority_block_rewards_pct:INTEGER",
		"inflation_rewards_pct:INTEGER",
		"jito_tips_pct:INTEGER",
		"fixed_sol_amount:BIGINT",
		"relay_placeholder_lamports:BIGINT",
		"relay_distribute_rewards_lamports:BIGINT",
		"debt_write_off_feature_activation_epoch:BIGINT",
	}
}
func (s *ConfigSchema) ToRow(c Config) []any {
	return []any{
		c.ProgramID,
		int64(c.Flags),
		int64(c.NextCompletedEpoch),
		c.AdminKey,
		c.DebtAccountantKey,
		c.RewardsAccountantKey,
		c.ContributorManagerKey,
		c.SOL2ZSwapProgramID,
		int64(c.BurnRateLimit),
		int64(c.BurnRateDZEpochsToIncreasing),
		int64(c.BurnRateDZEpochsToLimit),
		int(c.BaseBlockRewardsPct),
		int(c.PriorityBlockRewardsPct),
		int(c.InflationRewardsPct),
		int(c.JitoTipsPct),
		int64(c.FixedSOLAmount),
		int64(c.RelayPlaceholderLamports),
		int64(c.RelayDistributeRewardsLamports),
		int64(c.DebtWriteOffFeatureActivationEpoch),
	}
}
func (s *ConfigSchema) GetPrimaryKey(c Config) string { return c.ProgramID }

// DistributionSchema defines the schema for per-epoch distributions.
type DistributionSchema struct{}

func (s *DistributionSchema) Name() string                { return "dz_revdist_distributions" }
func (s *DistributionSchema) PrimaryKeyColumns() []string { return []string{"dz_epoch:BIGINT"} }
func (s *DistributionSchema) PayloadColumns() []string {
	return []string{
		"flags:BIGINT",
		"community_burn_rate:BIGINT",
		"total_solana_validators:BIGINT",
		"solana_validator_payments_count:BIGINT",
		"total_solana_validator_debt:BIGINT",
		"collected_solana_validator_payments:BIGINT",
		"total_contributors:BIGINT",
		"distributed_rewards_count:BIGINT",
		"collected_prepaid_2z_payments:BIGINT",
		"collected_2z_converted_from_sol:BIGINT",
		"uncollectible_sol_debt:BIGINT",
		"distributed_2z_amount:BIGINT",
		"burned_2z_amount:BIGINT",
		"solana_validator_write_off_count:BIGINT",
		"base_block_rewards_pct:INTEGER",
		"priority_block_rewards_pct:INTEGER",
		"inflation_rewards_pct:INTEGER",
		"jito_tips_pct:INTEGER",
		"fixed_sol_amount:BIGINT",
		"sol_price_usd:DOUBLE",
		"twoz_price_usd:DOUBLE",
	}
}
func (s *DistributionSchema) ToRow(d Distribution) []any {
	return []any{
		int64(d.DZEpoch),
		int64(d.Flags),
		int64(d.CommunityBurnRate),
		int64(d.TotalSolanaValidators),
		int64(d.SolanaValidatorPaymentsCount),
		int64(d.TotalSolanaValidatorDebt),
		int64(d.CollectedSolanaValidatorPayments),
		int64(d.TotalContributors),
		int64(d.DistributedRewardsCount),
		int64(d.CollectedPrepaid2ZPayments),
		int64(d.Collected2ZConvertedFromSOL),
		int64(d.UncollectibleSOLDebt),
		int64(d.Distributed2ZAmount),
		int64(d.Burned2ZAmount),
		int64(d.SolanaValidatorWriteOffCount),
		int(d.BaseBlockRewardsPct),
		int(d.PriorityBlockRewardsPct),
		int(d.InflationRewardsPct),
		int(d.JitoTipsPct),
		int64(d.FixedSOLAmount),
		d.SOLPriceUSD,
		d.TwoZPriceUSD,
	}
}
func (s *DistributionSchema) GetPrimaryKey(d Distribution) string {
	return fmt.Sprintf("%d", d.DZEpoch)
}

// JournalSchema defines the schema for the revdist journal.
type JournalSchema struct{}

func (s *JournalSchema) Name() string                { return "dz_revdist_journal" }
func (s *JournalSchema) PrimaryKeyColumns() []string { return []string{"program_id:VARCHAR"} }
func (s *JournalSchema) PayloadColumns() []string {
	return []string{
		"total_sol_balance:BIGINT",
		"total_2z_balance:BIGINT",
		"swap_2z_destination_balance:BIGINT",
		"swapped_sol_amount:BIGINT",
		"next_dz_epoch_to_sweep_tokens:BIGINT",
	}
}
func (s *JournalSchema) ToRow(j Journal) []any {
	return []any{
		j.ProgramID,
		int64(j.TotalSOLBalance),
		int64(j.Total2ZBalance),
		int64(j.Swap2ZDestinationBalance),
		int64(j.SwappedSOLAmount),
		int64(j.NextDZEpochToSweepTokens),
	}
}
func (s *JournalSchema) GetPrimaryKey(j Journal) string { return j.ProgramID }

// ValidatorDepositSchema defines the schema for validator deposits.
type ValidatorDepositSchema struct{}

func (s *ValidatorDepositSchema) Name() string                { return "dz_revdist_validator_deposits" }
func (s *ValidatorDepositSchema) PrimaryKeyColumns() []string { return []string{"node_id:VARCHAR"} }
func (s *ValidatorDepositSchema) PayloadColumns() []string {
	return []string{"written_off_sol_debt:BIGINT"}
}
func (s *ValidatorDepositSchema) ToRow(v ValidatorDeposit) []any {
	return []any{v.NodeID, int64(v.WrittenOffSOLDebt)}
}
func (s *ValidatorDepositSchema) GetPrimaryKey(v ValidatorDeposit) string { return v.NodeID }

// ContributorRewardsSchema defines the schema for contributor rewards.
type ContributorRewardsSchema struct{}

func (s *ContributorRewardsSchema) Name() string { return "dz_revdist_contributor_rewards" }
func (s *ContributorRewardsSchema) PrimaryKeyColumns() []string {
	return []string{"service_key:VARCHAR"}
}
func (s *ContributorRewardsSchema) PayloadColumns() []string {
	return []string{
		"rewards_manager_key:VARCHAR",
		"flags:BIGINT",
		"recipient_shares:VARCHAR",
	}
}
func (s *ContributorRewardsSchema) ToRow(c ContributorReward) []any {
	sharesJSON, _ := json.Marshal(c.RecipientShares)
	return []any{c.ServiceKey, c.RewardsManagerKey, int64(c.Flags), string(sharesJSON)}
}
func (s *ContributorRewardsSchema) GetPrimaryKey(c ContributorReward) string { return c.ServiceKey }

// --- Fact schemas (off-chain data) ---

// ValidatorDebtFactSchema defines the schema for per-epoch validator debts.
type ValidatorDebtFactSchema struct{}

func (s *ValidatorDebtFactSchema) Name() string { return "dz_revdist_validator_debts" }
func (s *ValidatorDebtFactSchema) UniqueKeyColumns() []string {
	return []string{"dz_epoch", "node_id"}
}
func (s *ValidatorDebtFactSchema) Columns() []string {
	return []string{
		"dz_epoch:BIGINT",
		"node_id:VARCHAR",
		"amount:BIGINT",
		"ingested_at:TIMESTAMP",
	}
}
func (s *ValidatorDebtFactSchema) TimeColumn() string           { return "" }
func (s *ValidatorDebtFactSchema) PartitionByTime() bool        { return false }
func (s *ValidatorDebtFactSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }
func (s *ValidatorDebtFactSchema) DedupVersionColumn() string   { return "ingested_at" }

// RewardShareFactSchema defines the schema for per-epoch reward shares.
type RewardShareFactSchema struct{}

func (s *RewardShareFactSchema) Name() string { return "dz_revdist_reward_shares" }
func (s *RewardShareFactSchema) UniqueKeyColumns() []string {
	return []string{"dz_epoch", "contributor_key"}
}
func (s *RewardShareFactSchema) Columns() []string {
	return []string{
		"dz_epoch:BIGINT",
		"contributor_key:VARCHAR",
		"unit_share:BIGINT",
		"total_unit_shares:BIGINT",
		"is_blocked:BOOLEAN",
		"economic_burn_rate:BIGINT",
		"ingested_at:TIMESTAMP",
	}
}
func (s *RewardShareFactSchema) TimeColumn() string           { return "" }
func (s *RewardShareFactSchema) PartitionByTime() bool        { return false }
func (s *RewardShareFactSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }
func (s *RewardShareFactSchema) DedupVersionColumn() string   { return "ingested_at" }

// PriceSnapshotFactSchema defines the schema for periodic price snapshots.
type PriceSnapshotFactSchema struct{}

func (s *PriceSnapshotFactSchema) Name() string { return "dz_revdist_prices" }
func (s *PriceSnapshotFactSchema) UniqueKeyColumns() []string {
	return []string{"ts"}
}
func (s *PriceSnapshotFactSchema) Columns() []string {
	return []string{
		"ts:TIMESTAMP",
		"sol_price_usd:DOUBLE",
		"twoz_price_usd:DOUBLE",
		"swap_rate:DOUBLE",
		"ingested_at:TIMESTAMP",
	}
}
func (s *PriceSnapshotFactSchema) TimeColumn() string           { return "ts" }
func (s *PriceSnapshotFactSchema) PartitionByTime() bool        { return true }
func (s *PriceSnapshotFactSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }
func (s *PriceSnapshotFactSchema) DedupVersionColumn() string   { return "ingested_at" }

// Schema instances
var (
	configSchema             = &ConfigSchema{}
	distributionSchema       = &DistributionSchema{}
	journalSchema            = &JournalSchema{}
	validatorDepositSchema   = &ValidatorDepositSchema{}
	contributorRewardsSchema = &ContributorRewardsSchema{}
	validatorDebtFactSchema  = &ValidatorDebtFactSchema{}
	rewardShareFactSchema    = &RewardShareFactSchema{}
	priceSnapshotFactSchema  = &PriceSnapshotFactSchema{}
)

// Dataset constructors

func NewConfigDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, configSchema)
}

func NewDistributionDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, distributionSchema)
}

func NewJournalDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, journalSchema)
}

func NewValidatorDepositDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, validatorDepositSchema)
}

func NewContributorRewardsDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, contributorRewardsSchema)
}

func NewValidatorDebtFactDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, validatorDebtFactSchema)
}

func NewRewardShareFactDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, rewardShareFactSchema)
}

func NewPriceSnapshotFactDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, priceSnapshotFactSchema)
}
