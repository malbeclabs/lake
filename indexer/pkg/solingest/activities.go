package solingest

import (
	"context"
	"fmt"
	"log/slog"

	mcpgeoip "github.com/malbeclabs/lake/indexer/pkg/geoip"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/sol"
	"github.com/malbeclabs/lake/indexer/pkg/validatorsapp"
)

// Activities holds dependencies for Solana ingest activities.
type Activities struct {
	Log           *slog.Logger
	IngestionLog  *ingestionlog.Writer
	Network       string
	Solana        *sol.View
	GeoIP         *mcpgeoip.View      // nil when GeoIP is not configured
	ValidatorsApp *validatorsapp.View // nil when validators.app is not configured
}

// RefreshSolana fetches the latest Solana validator state from RPC
// and writes it to ClickHouse fact tables.
func (a *Activities) RefreshSolana(ctx context.Context) error {
	return a.IngestionLog.Wrap(ctx, "solingest", "RefreshSolana", a.Network, func() error {
		if err := a.Solana.Refresh(ctx); err != nil {
			return fmt.Errorf("solana refresh: %w", err)
		}
		return nil
	})
}

// RefreshBlockProduction fetches Solana block production data from RPC
// and writes it to ClickHouse fact tables.
func (a *Activities) RefreshBlockProduction(ctx context.Context) error {
	return a.IngestionLog.Wrap(ctx, "solingest", "RefreshBlockProduction", a.Network, func() error {
		if err := a.Solana.RefreshBlockProduction(ctx); err != nil {
			return fmt.Errorf("block production refresh: %w", err)
		}
		return nil
	})
}

// RefreshGeoIP resolves GeoIP data for known IPs (from Solana gossip and DZ
// users) and writes it to ClickHouse. No-op if GeoIP is not configured.
func (a *Activities) RefreshGeoIP(ctx context.Context) error {
	if a.GeoIP == nil {
		a.IngestionLog.WrapSkipped(ctx, "solingest", "RefreshGeoIP", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "solingest", "RefreshGeoIP", a.Network, func() error {
		if err := a.GeoIP.Refresh(ctx); err != nil {
			return fmt.Errorf("geoip refresh: %w", err)
		}
		return nil
	})
}

// RefreshValidatorsApp fetches validator metadata from validators.app
// and writes it to ClickHouse. No-op if validators.app is not configured.
func (a *Activities) RefreshValidatorsApp(ctx context.Context) error {
	if a.ValidatorsApp == nil {
		a.IngestionLog.WrapSkipped(ctx, "solingest", "RefreshValidatorsApp", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "solingest", "RefreshValidatorsApp", a.Network, func() error {
		if err := a.ValidatorsApp.Refresh(ctx); err != nil {
			return fmt.Errorf("validatorsapp refresh: %w", err)
		}
		return nil
	})
}
