package dzingest

import (
	"context"
	"fmt"
	"log/slog"

	dzgraph "github.com/malbeclabs/lake/indexer/pkg/dz/graph"
	"github.com/malbeclabs/lake/indexer/pkg/dz/isis"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	dzshreds "github.com/malbeclabs/lake/indexer/pkg/dz/shreds"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/escrowevents"
	dztelemlatency "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/latency"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
)

// Activities holds dependencies for DZ ingest activities.
type Activities struct {
	Log            *slog.Logger
	IngestionLog   *ingestionlog.Writer
	Network        string
	Serviceability *dzsvc.View
	Shreds         *dzshreds.View     // nil when shreds is not configured
	EscrowEvents   *escrowevents.View // nil when shreds is not configured
	TelemLatency   *dztelemlatency.View
	TelemUsage     *dztelemusage.View // nil when InfluxDB is not configured
	GraphStore     *dzgraph.Store     // nil when Neo4j is not configured
	ISISSource     isis.Source        // nil when ISIS is not enabled
	ISISStore      *isis.Store        // nil when ISIS is not enabled
}

// RefreshServiceability fetches the latest DZ serviceability state from RPC
// and writes it to ClickHouse dimension tables.
func (a *Activities) RefreshServiceability(ctx context.Context) error {
	return a.IngestionLog.Wrap(ctx, "dzingest", "RefreshServiceability", a.Network, func() (ingestionlog.RefreshResult, error) {
		result, err := a.Serviceability.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("serviceability refresh: %w", err)
		}
		return result, nil
	})
}

// RefreshShreds fetches shred subscription program state from RPC
// and writes it to ClickHouse dimension tables. No-op if shreds is not configured.
func (a *Activities) RefreshShreds(ctx context.Context) error {
	if a.Shreds == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "RefreshShreds", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "dzingest", "RefreshShreds", a.Network, func() (ingestionlog.RefreshResult, error) {
		result, err := a.Shreds.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("shreds refresh: %w", err)
		}
		return result, nil
	})
}

// RefreshTelemetryLatency fetches device link latency samples from RPC
// and writes them to ClickHouse fact tables.
func (a *Activities) RefreshTelemetryLatency(ctx context.Context) error {
	return a.IngestionLog.Wrap(ctx, "dzingest", "RefreshTelemetryLatency", a.Network, func() (ingestionlog.RefreshResult, error) {
		result, err := a.TelemLatency.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("telemetry latency refresh: %w", err)
		}
		return result, nil
	})
}

// RefreshTelemetryUsage fetches device interface counters from InfluxDB
// and writes them to ClickHouse fact tables. No-op if InfluxDB is not configured.
func (a *Activities) RefreshTelemetryUsage(ctx context.Context) error {
	if a.TelemUsage == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "RefreshTelemetryUsage", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "dzingest", "RefreshTelemetryUsage", a.Network, func() (ingestionlog.RefreshResult, error) {
		result, err := a.TelemUsage.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("telemetry usage refresh: %w", err)
		}
		return result, nil
	})
}

// SyncGraph syncs the Neo4j topology graph from ClickHouse state.
// When ISIS is enabled, it fetches ISIS data and syncs atomically with the
// base graph. No-op if Neo4j is not configured.
func (a *Activities) SyncGraph(ctx context.Context) error {
	if a.GraphStore == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "SyncGraph", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "dzingest", "SyncGraph", a.Network, func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		if a.ISISStore != nil {
			return result, a.GraphStore.SyncWithISIS(ctx)
		}
		return result, a.GraphStore.Sync(ctx)
	})
}

// SyncISIS fetches IS-IS topology from S3 and writes adjacency/device data
// to ClickHouse. Independent of Neo4j. No-op if ISIS is not enabled.
func (a *Activities) SyncISIS(ctx context.Context) error {
	if a.ISISSource == nil || a.ISISStore == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "SyncISIS", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "dzingest", "SyncISIS", a.Network, func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		lsps, err := a.fetchISISData(ctx)
		if err != nil {
			return result, fmt.Errorf("isis sync: %w", err)
		}
		return result, a.ISISStore.Sync(ctx, lsps)
	})
}

// RefreshShredEscrowEvents fetches on-chain transaction history for payment escrows
// and writes parsed events to ClickHouse. No-op if shreds is not configured.
func (a *Activities) RefreshShredEscrowEvents(ctx context.Context) error {
	if a.EscrowEvents == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "RefreshShredEscrowEvents", a.Network)
		return nil
	}
	return a.IngestionLog.Wrap(ctx, "dzingest", "RefreshShredEscrowEvents", a.Network, func() (ingestionlog.RefreshResult, error) {
		result, err := a.EscrowEvents.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("escrow events refresh: %w", err)
		}
		return result, nil
	})
}

func (a *Activities) fetchISISData(ctx context.Context) ([]isis.LSP, error) {
	dump, err := a.ISISSource.FetchLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ISIS dump: %w", err)
	}
	lsps, err := isis.Parse(dump.RawJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ISIS dump: %w", err)
	}
	return lsps, nil
}
