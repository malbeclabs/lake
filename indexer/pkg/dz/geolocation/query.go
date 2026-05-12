package geolocation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

type probeRow struct {
	PK                 string `ch:"pk"`
	Owner              string `ch:"owner"`
	ExchangePK         string `ch:"exchange_pk"`
	PublicIP           string `ch:"public_ip"`
	LocationOffsetPort uint16 `ch:"location_offset_port"`
	MetricsPublisherPK string `ch:"metrics_publisher_pk"`
	ReferenceCount     uint32 `ch:"reference_count"`
	Code               string `ch:"code"`
	ParentDevices      string `ch:"parent_devices"`
	TargetUpdateCount  uint32 `ch:"target_update_count"`
}

type userRow struct {
	PK                   string `ch:"pk"`
	Owner                string `ch:"owner"`
	Code                 string `ch:"code"`
	TokenAccount         string `ch:"token_account"`
	PaymentStatus        string `ch:"payment_status"`
	Status               string `ch:"status"`
	TargetCount          uint32 `ch:"target_count"`
	BillingRate          uint64 `ch:"billing_rate"`
	LastDeductionDzEpoch uint64 `ch:"last_deduction_dz_epoch"`
}

type targetRow struct {
	GeolocUserPK       string `ch:"geoloc_user_pk"`
	ProbePK            string `ch:"probe_pk"`
	TargetType         string `ch:"target_type"`
	IP                 string `ch:"ip"`
	LocationOffsetPort uint16 `ch:"location_offset_port"`
	TargetPK           string `ch:"target_pk"`
}

func QueryCurrentProbes(ctx context.Context, log *slog.Logger, db clickhouse.Client) ([]Probe, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	d, err := NewProbeDataset(log)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	typed := dataset.NewTypedDimensionType2Dataset[probeRow](d)
	rows, err := typed.GetCurrentRows(ctx, conn, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query probes: %w", err)
	}

	probes := make([]Probe, len(rows))
	for i, row := range rows {
		var parentDevices []string
		if row.ParentDevices != "" {
			if err := json.Unmarshal([]byte(row.ParentDevices), &parentDevices); err != nil {
				log.Debug("geolocation: failed to unmarshal parent_devices", "pk", row.PK, "error", err)
			}
		}
		probes[i] = Probe{
			PK:                 row.PK,
			Owner:              row.Owner,
			ExchangePK:         row.ExchangePK,
			PublicIP:           row.PublicIP,
			LocationOffsetPort: row.LocationOffsetPort,
			MetricsPublisherPK: row.MetricsPublisherPK,
			ReferenceCount:     row.ReferenceCount,
			Code:               row.Code,
			ParentDevices:      parentDevices,
			TargetUpdateCount:  row.TargetUpdateCount,
		}
	}

	return probes, nil
}

func QueryCurrentUsers(ctx context.Context, log *slog.Logger, db clickhouse.Client) ([]User, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	d, err := NewUserDataset(log)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	typed := dataset.NewTypedDimensionType2Dataset[userRow](d)
	rows, err := typed.GetCurrentRows(ctx, conn, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query users: %w", err)
	}

	users := make([]User, len(rows))
	for i, row := range rows {
		users[i] = User{
			PK:                   row.PK,
			Owner:                row.Owner,
			Code:                 row.Code,
			TokenAccount:         row.TokenAccount,
			PaymentStatus:        row.PaymentStatus,
			Status:               row.Status,
			TargetCount:          row.TargetCount,
			BillingRate:          row.BillingRate,
			LastDeductionDzEpoch: row.LastDeductionDzEpoch,
		}
	}

	return users, nil
}

func QueryCurrentTargets(ctx context.Context, log *slog.Logger, db clickhouse.Client) ([]Target, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	d, err := NewTargetDataset(log)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	typed := dataset.NewTypedDimensionType2Dataset[targetRow](d)
	rows, err := typed.GetCurrentRows(ctx, conn, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query targets: %w", err)
	}

	targets := make([]Target, len(rows))
	for i, row := range rows {
		targets[i] = Target{
			GeolocUserPK:       row.GeolocUserPK,
			ProbePK:            row.ProbePK,
			TargetType:         row.TargetType,
			IP:                 row.IP,
			LocationOffsetPort: row.LocationOffsetPort,
			TargetPK:           row.TargetPK,
		}
	}

	return targets, nil
}
