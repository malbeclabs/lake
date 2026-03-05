package validatorsapp

import (
	"encoding/json"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// ValidatorSchema implements dataset.DimensionSchema for validators.app validator data.
type ValidatorSchema struct{}

func (s *ValidatorSchema) Name() string {
	return "validatorsapp_validators"
}

func (s *ValidatorSchema) PrimaryKeyColumns() []string {
	return []string{"account:VARCHAR"}
}

func (s *ValidatorSchema) PayloadColumns() []string {
	return []string{
		"name:VARCHAR",
		"vote_account:VARCHAR",
		"software_version:VARCHAR",
		"software_client:VARCHAR",
		"software_client_id:INTEGER",
		"jito:INTEGER",
		"jito_commission:INTEGER",
		"is_active:INTEGER",
		"is_dz:INTEGER",
		"active_stake:BIGINT",
		"commission:INTEGER",
		"delinquent:INTEGER",
		"epoch:BIGINT",
		"epoch_credits:BIGINT",
		"skipped_slot_percent:VARCHAR",
		"total_score:INTEGER",
		"data_center_key:VARCHAR",
		"autonomous_system_number:BIGINT",
		"latitude:VARCHAR",
		"longitude:VARCHAR",
		"ip:VARCHAR",
		"stake_pools_list:VARCHAR",
	}
}

// ToRow converts a Validator into a row for ClickHouse insertion.
// The order is: primary key columns first, then payload columns in order.
func (s *ValidatorSchema) ToRow(v Validator) []any {
	stakePoolsJSON, _ := json.Marshal(v.StakePoolsList)

	return []any{
		v.Account,
		v.Name,
		v.VoteAccount,
		v.SoftwareVersion,
		v.SoftwareClient,
		int32(v.SoftwareClientID),
		boolToInt32(v.Jito),
		int32(v.JitoCommission),
		boolToInt32(v.IsActive),
		boolToInt32(v.IsDZ),
		int64(v.ActiveStake),
		int32(v.Commission),
		boolToInt32(v.Delinquent),
		int64(v.Epoch),
		int64(v.EpochCredits),
		v.SkippedSlotPercent,
		int32(v.TotalScore),
		v.DataCenterKey,
		int64(v.AutonomousSystemNumber),
		v.Latitude,
		v.Longitude,
		v.IP,
		string(stakePoolsJSON),
	}
}

// GetPrimaryKey returns the primary key value for a Validator.
func (s *ValidatorSchema) GetPrimaryKey(v Validator) string {
	return v.Account
}

func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

var validatorSchema = &ValidatorSchema{}

// NewValidatorDataset creates a new DimensionType2Dataset for validator data.
func NewValidatorDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, validatorSchema)
}
