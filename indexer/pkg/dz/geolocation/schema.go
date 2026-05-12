package geolocation

import (
	"encoding/json"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// ProbeSchema defines the schema for geolocation probes.
type ProbeSchema struct{}

func (s *ProbeSchema) Name() string {
	return "geoloc_probes"
}

func (s *ProbeSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *ProbeSchema) PayloadColumns() []string {
	return []string{
		"owner:VARCHAR",
		"exchange_pk:VARCHAR",
		"public_ip:VARCHAR",
		"location_offset_port:INTEGER",
		"metrics_publisher_pk:VARCHAR",
		"reference_count:INTEGER",
		"code:VARCHAR",
		"parent_devices:VARCHAR",
		"target_update_count:INTEGER",
	}
}

func (s *ProbeSchema) ToRow(p Probe) []any {
	parentDevicesJSON, _ := json.Marshal(p.ParentDevices)
	return []any{
		p.PK,
		p.Owner,
		p.ExchangePK,
		p.PublicIP,
		p.LocationOffsetPort,
		p.MetricsPublisherPK,
		p.ReferenceCount,
		p.Code,
		string(parentDevicesJSON),
		p.TargetUpdateCount,
	}
}

func (s *ProbeSchema) GetPrimaryKey(p Probe) string {
	return p.PK
}

// UserSchema defines the schema for geolocation users.
type UserSchema struct{}

func (s *UserSchema) Name() string {
	return "geoloc_users"
}

func (s *UserSchema) PrimaryKeyColumns() []string {
	return []string{"pk:VARCHAR"}
}

func (s *UserSchema) PayloadColumns() []string {
	return []string{
		"owner:VARCHAR",
		"code:VARCHAR",
		"token_account:VARCHAR",
		"payment_status:VARCHAR",
		"status:VARCHAR",
		"target_count:INTEGER",
		"billing_rate:BIGINT",
		"last_deduction_dz_epoch:BIGINT",
	}
}

func (s *UserSchema) ToRow(u User) []any {
	return []any{
		u.PK,
		u.Owner,
		u.Code,
		u.TokenAccount,
		u.PaymentStatus,
		u.Status,
		u.TargetCount,
		u.BillingRate,
		u.LastDeductionDzEpoch,
	}
}

func (s *UserSchema) GetPrimaryKey(u User) string {
	return u.PK
}

// TargetSchema defines the schema for geolocation targets.
type TargetSchema struct{}

func (s *TargetSchema) Name() string {
	return "geoloc_targets"
}

func (s *TargetSchema) PrimaryKeyColumns() []string {
	return []string{
		"geoloc_user_pk:VARCHAR",
		"probe_pk:VARCHAR",
		"target_type:VARCHAR",
		"ip:VARCHAR",
		"location_offset_port:INTEGER",
		"target_pk:VARCHAR",
	}
}

func (s *TargetSchema) PayloadColumns() []string {
	return nil
}

func (s *TargetSchema) ToRow(t Target) []any {
	return []any{
		t.GeolocUserPK,
		t.ProbePK,
		t.TargetType,
		t.IP,
		t.LocationOffsetPort,
		t.TargetPK,
	}
}

func (s *TargetSchema) GetPrimaryKey(t Target) string {
	return t.EntityID()
}

var (
	probeSchema  = &ProbeSchema{}
	userSchema   = &UserSchema{}
	targetSchema = &TargetSchema{}
)

func NewProbeDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, probeSchema)
}

func NewUserDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, userSchema)
}

func NewTargetDataset(log *slog.Logger) (*dataset.DimensionType2Dataset, error) {
	return dataset.NewDimensionType2Dataset(log, targetSchema)
}
