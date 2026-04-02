package dzshreds

import (
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// ExecutionControllerSchema defines the schema for the execution controller singleton.
type ExecutionControllerSchema struct{}

func (s *ExecutionControllerSchema) Name() string { return "dz_shred_execution_controller" }

func (s *ExecutionControllerSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *ExecutionControllerSchema) PayloadColumns() []string {
	return []string{
		"phase:VARCHAR",
		"current_subscription_epoch:BIGINT",
		"total_metros:INTEGER",
		"total_enabled_devices:INTEGER",
		"total_client_seats:INTEGER",
		"updated_device_prices_count:INTEGER",
		"settled_devices_count:INTEGER",
		"settled_client_seats_count:INTEGER",
		"last_settled_slot:BIGINT",
		"last_updating_prices_slot:BIGINT",
		"last_open_for_requests_slot:BIGINT",
		"last_closed_for_requests_slot:BIGINT",
		"next_seat_funding_index:BIGINT",
	}
}

func (s *ExecutionControllerSchema) ToRow(e ExecutionControllerRow) []any {
	return []any{
		e.PK,
		e.Phase,
		e.CurrentSubscriptionEpoch,
		e.TotalMetros,
		e.TotalEnabledDevices,
		e.TotalClientSeats,
		e.UpdatedDevicePricesCount,
		e.SettledDevicesCount,
		e.SettledClientSeatsCount,
		e.LastSettledSlot,
		e.LastUpdatingPricesSlot,
		e.LastOpenForRequestsSlot,
		e.LastClosedForRequestsSlot,
		e.NextSeatFundingIndex,
	}
}

func (s *ExecutionControllerSchema) GetPrimaryKey(e ExecutionControllerRow) string {
	return e.PK
}

// ClientSeatSchema defines the schema for client seats.
type ClientSeatSchema struct{}

func (s *ClientSeatSchema) Name() string { return "dz_shred_client_seats" }

func (s *ClientSeatSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *ClientSeatSchema) PayloadColumns() []string {
	return []string{
		"device_key:VARCHAR",
		"client_ip:VARCHAR",
		"tenure_epochs:INTEGER",
		"funded_epoch:BIGINT",
		"active_epoch:BIGINT",
		"has_price_override:BOOLEAN",
		"override_usdc_price_dollars:INTEGER",
		"escrow_count:INTEGER",
		"funding_authority_key:VARCHAR",
	}
}

func (s *ClientSeatSchema) ToRow(c ClientSeatRow) []any {
	return []any{
		c.PK,
		c.DeviceKey,
		c.ClientIP,
		c.TenureEpochs,
		c.FundedEpoch,
		c.ActiveEpoch,
		c.HasPriceOverride,
		c.OverrideUSDCPriceDollars,
		c.EscrowCount,
		c.FundingAuthorityKey,
	}
}

func (s *ClientSeatSchema) GetPrimaryKey(c ClientSeatRow) string {
	return c.PK
}

// PaymentEscrowSchema defines the schema for payment escrows.
type PaymentEscrowSchema struct{}

func (s *PaymentEscrowSchema) Name() string { return "dz_shred_payment_escrows" }

func (s *PaymentEscrowSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *PaymentEscrowSchema) PayloadColumns() []string {
	return []string{
		"client_seat_key:VARCHAR",
		"withdraw_authority_key:VARCHAR",
		"usdc_balance:BIGINT",
	}
}

func (s *PaymentEscrowSchema) ToRow(p PaymentEscrowRow) []any {
	return []any{
		p.PK,
		p.ClientSeatKey,
		p.WithdrawAuthorityKey,
		p.USDCBalance,
	}
}

func (s *PaymentEscrowSchema) GetPrimaryKey(p PaymentEscrowRow) string {
	return p.PK
}

// MetroHistorySchema defines the schema for metro pricing histories.
type MetroHistorySchema struct{}

func (s *MetroHistorySchema) Name() string { return "dz_shred_metro_histories" }

func (s *MetroHistorySchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *MetroHistorySchema) PayloadColumns() []string {
	return []string{
		"exchange_key:VARCHAR",
		"is_current_price_finalized:BOOLEAN",
		"total_initialized_devices:INTEGER",
		"current_epoch:BIGINT",
		"current_usdc_price_dollars:INTEGER",
	}
}

func (s *MetroHistorySchema) ToRow(m MetroHistoryRow) []any {
	return []any{
		m.PK,
		m.ExchangeKey,
		m.IsCurrentPriceFinalized,
		m.TotalInitializedDevices,
		m.CurrentEpoch,
		m.CurrentUSDCPriceDollars,
	}
}

func (s *MetroHistorySchema) GetPrimaryKey(m MetroHistoryRow) string {
	return m.PK
}

// DeviceHistorySchema defines the schema for device subscription histories.
type DeviceHistorySchema struct{}

func (s *DeviceHistorySchema) Name() string { return "dz_shred_device_histories" }

func (s *DeviceHistorySchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *DeviceHistorySchema) PayloadColumns() []string {
	return []string{
		"device_key:VARCHAR",
		"is_enabled:BOOLEAN",
		"has_settled_seats:BOOLEAN",
		"metro_exchange_key:VARCHAR",
		"active_granted_seats:INTEGER",
		"active_total_available_seats:INTEGER",
		"current_epoch:BIGINT",
		"current_requested_seat_count:INTEGER",
		"current_granted_seat_count:INTEGER",
		"current_total_available_seats:INTEGER",
		"current_usdc_metro_premium_dollars:INTEGER",
	}
}

func (s *DeviceHistorySchema) ToRow(d DeviceHistoryRow) []any {
	return []any{
		d.PK,
		d.DeviceKey,
		d.IsEnabled,
		d.HasSettledSeats,
		d.MetroExchangeKey,
		d.ActiveGrantedSeats,
		d.ActiveTotalAvailableSeats,
		d.CurrentEpoch,
		d.CurrentRequestedSeatCount,
		d.CurrentGrantedSeatCount,
		d.CurrentTotalAvailableSeats,
		d.CurrentUSDCMetroPremiumDollars,
	}
}

func (s *DeviceHistorySchema) GetPrimaryKey(d DeviceHistoryRow) string {
	return d.PK
}

// ValidatorClientRewardsSchema defines the schema for validator client rewards.
type ValidatorClientRewardsSchema struct{}

func (s *ValidatorClientRewardsSchema) Name() string { return "dz_shred_validator_client_rewards" }

func (s *ValidatorClientRewardsSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *ValidatorClientRewardsSchema) PayloadColumns() []string {
	return []string{
		"client_id:INTEGER",
		"manager_key:VARCHAR",
		"short_description:VARCHAR",
	}
}

func (s *ValidatorClientRewardsSchema) ToRow(v ValidatorClientRewardsRow) []any {
	return []any{
		v.PK,
		v.ClientID,
		v.ManagerKey,
		v.ShortDescription,
	}
}

func (s *ValidatorClientRewardsSchema) GetPrimaryKey(v ValidatorClientRewardsRow) string {
	return v.PK
}

// ShredDistributionSchema defines the schema for shred distributions.
type ShredDistributionSchema struct{}

func (s *ShredDistributionSchema) Name() string { return "dz_shred_distributions" }

func (s *ShredDistributionSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *ShredDistributionSchema) PayloadColumns() []string {
	return []string{
		"subscription_epoch:BIGINT",
		"associated_dz_epoch:BIGINT",
		"device_count:INTEGER",
		"client_seat_count:INTEGER",
		"validator_rewards_proportion:INTEGER",
		"total_publishing_validators:INTEGER",
		"collected_usdc_payments:BIGINT",
		"collected_2z_converted_from_usdc:BIGINT",
		"distributed_validator_rewards_count:INTEGER",
		"distributed_contributor_rewards_count:INTEGER",
		"distributed_validator_2z_amount:BIGINT",
		"distributed_contributor_2z_amount:BIGINT",
		"burned_2z_amount:BIGINT",
	}
}

func (s *ShredDistributionSchema) ToRow(d ShredDistributionRow) []any {
	return []any{
		d.PK,
		d.SubscriptionEpoch,
		d.AssociatedDZEpoch,
		d.DeviceCount,
		d.ClientSeatCount,
		d.ValidatorRewardsProportion,
		d.TotalPublishingValidators,
		d.CollectedUSDCPayments,
		d.Collected2ZConvertedFromUSDC,
		d.DistributedValidatorRewardsCount,
		d.DistributedContributorRewardsCount,
		d.DistributedValidator2ZAmount,
		d.DistributedContributor2ZAmount,
		d.Burned2ZAmount,
	}
}

func (s *ShredDistributionSchema) GetPrimaryKey(d ShredDistributionRow) string {
	return d.PK
}

var (
	executionControllerSchema    = &ExecutionControllerSchema{}
	clientSeatSchema             = &ClientSeatSchema{}
	paymentEscrowSchema          = &PaymentEscrowSchema{}
	metroHistorySchema           = &MetroHistorySchema{}
	deviceHistorySchema          = &DeviceHistorySchema{}
	validatorClientRewardsSchema = &ValidatorClientRewardsSchema{}
	shredDistributionSchema      = &ShredDistributionSchema{}
)

func NewExecutionControllerDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, executionControllerSchema)
}

func NewClientSeatDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, clientSeatSchema)
}

func NewPaymentEscrowDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, paymentEscrowSchema)
}

func NewMetroHistoryDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, metroHistorySchema)
}

func NewDeviceHistoryDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, deviceHistorySchema)
}

func NewValidatorClientRewardsDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, validatorClientRewardsSchema)
}

func NewShredDistributionDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, shredDistributionSchema)
}
