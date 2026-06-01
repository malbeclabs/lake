package dzingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	dzgeoloc "github.com/malbeclabs/lake/indexer/pkg/dz/geolocation"
	dzgraph "github.com/malbeclabs/lake/indexer/pkg/dz/graph"
	"github.com/malbeclabs/lake/indexer/pkg/dz/isis"
	"github.com/malbeclabs/lake/indexer/pkg/dz/mroute"
	"github.com/malbeclabs/lake/indexer/pkg/dz/msdp"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	dzshreds "github.com/malbeclabs/lake/indexer/pkg/dz/shreds"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/escrowevents"
	dztelemlatency "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/latency"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorAfterFailures is the number of consecutive failed refreshes after
// which we escalate the log to ERROR. Below the threshold, transient
// failures (deploys, ClickHouse restarts, InfluxDB rate limits) log at WARN.
// At ~60s per workflow iteration, 3 means ~3 minutes of sustained failure
// before paging anyone.
const errorAfterFailures = 3

// Activities holds dependencies for DZ ingest activities.
type Activities struct {
	Log            *slog.Logger
	IngestionLog   *ingestionlog.Writer
	Network        string
	Serviceability *dzsvc.View
	Geolocation    *dzgeoloc.View     // nil when geolocation is not configured
	Shreds         *dzshreds.View     // nil when shreds is not configured
	EscrowEvents   *escrowevents.View // nil when shreds is not configured
	TelemLatency   *dztelemlatency.View
	TelemUsage     *dztelemusage.View // nil when InfluxDB is not configured
	GraphStore     *dzgraph.Store     // nil when Neo4j is not configured
	ISISSource     isis.Source        // nil when ISIS is not enabled
	ISISStore      *isis.Store        // nil when ISIS is not enabled
	MrouteSource   mroute.Source      // nil when mroute ingest is not enabled
	MrouteStore    *mroute.Store      // nil when mroute ingest is not enabled
	MSDPSource     msdp.Source        // nil when MSDP ingest is not enabled
	MSDPStore      *msdp.Store        // nil when MSDP ingest is not enabled

	// failures tracks consecutive-failure counts per activity name.
	// map[string]*atomic.Int32; populated lazily.
	failures sync.Map
}

// refresh runs fn under the IngestionLog wrapper and tracks consecutive
// failures per activity. On error, increments the counter and logs at WARN
// below errorAfterFailures, ERROR at/above. On success, resets the counter.
//
// Always returns nil to Temporal. Transient failures (in-flight ClickHouse
// during a pod restart, InfluxDB rate limits, brief network blips) don't
// benefit from immediate retries within a single iteration — the next
// workflow iteration (~60s) is the natural retry cadence. Returning nil
// suppresses both Temporal's per-attempt "Activity error" log and the
// workflow-level error log; the ingestion log table still records every
// attempt.
func (a *Activities) refresh(ctx context.Context, name string, fn func() (ingestionlog.RefreshResult, error)) error {
	err := a.IngestionLog.Wrap(ctx, "dzingest", name, a.Network, fn)
	if err != nil {
		n := a.incFailures(name)
		args := []any{"activity", name, "consecutive_failures", n, "error", err}
		// Annotate the well-known InfluxDB rate-limit cause for readability.
		if status.Code(err) == codes.ResourceExhausted {
			args = append(args, "cause", "influxdb_rate_limit")
		}
		if n >= errorAfterFailures {
			a.Log.Error("activity refresh failed", args...)
		} else {
			a.Log.Warn("activity refresh failed", args...)
		}
		return nil
	}
	a.resetFailures(name)
	return nil
}

func (a *Activities) incFailures(name string) int32 {
	v, _ := a.failures.LoadOrStore(name, &atomic.Int32{})
	return v.(*atomic.Int32).Add(1)
}

func (a *Activities) resetFailures(name string) {
	if v, ok := a.failures.Load(name); ok {
		v.(*atomic.Int32).Store(0)
	}
}

// RefreshServiceability fetches the latest DZ serviceability state from RPC
// and writes it to ClickHouse dimension tables.
func (a *Activities) RefreshServiceability(ctx context.Context) error {
	return a.refresh(ctx, "RefreshServiceability", func() (ingestionlog.RefreshResult, error) {
		result, err := a.Serviceability.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("serviceability refresh: %w", err)
		}
		return result, nil
	})
}

// RefreshGeolocation fetches geolocation program state from RPC
// and writes it to ClickHouse dimension tables. No-op if geolocation is not configured.
func (a *Activities) RefreshGeolocation(ctx context.Context) error {
	if a.Geolocation == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "RefreshGeolocation", a.Network)
		return nil
	}
	return a.refresh(ctx, "RefreshGeolocation", func() (ingestionlog.RefreshResult, error) {
		result, err := a.Geolocation.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("geolocation refresh: %w", err)
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
	return a.refresh(ctx, "RefreshShreds", func() (ingestionlog.RefreshResult, error) {
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
	return a.refresh(ctx, "RefreshTelemetryLatency", func() (ingestionlog.RefreshResult, error) {
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
	return a.refresh(ctx, "RefreshTelemetryUsage", func() (ingestionlog.RefreshResult, error) {
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
	return a.refresh(ctx, "SyncGraph", func() (ingestionlog.RefreshResult, error) {
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
	return a.refresh(ctx, "SyncISIS", func() (ingestionlog.RefreshResult, error) {
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
	return a.refresh(ctx, "RefreshShredEscrowEvents", func() (ingestionlog.RefreshResult, error) {
		result, err := a.EscrowEvents.Refresh(ctx)
		if err != nil {
			return result, fmt.Errorf("escrow events refresh: %w", err)
		}
		return result, nil
	})
}

// SyncIPMroute fetches per-device `show ip mroute` snapshots from S3 (uploaded
// by doublezero-telemetry --state-collect-enable) and replaces the
// dz_ip_mroute_entries dimension in ClickHouse. No-op if mroute ingest is not
// configured.
func (a *Activities) SyncIPMroute(ctx context.Context) error {
	if a.MrouteSource == nil || a.MrouteStore == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "SyncIPMroute", a.Network)
		return nil
	}
	return a.refresh(ctx, "SyncIPMroute", func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		dumps, err := a.MrouteSource.FetchLatest(ctx)
		if err != nil {
			return result, fmt.Errorf("mroute fetch: %w", err)
		}
		return result, a.MrouteStore.Sync(ctx, dumps)
	})
}

// SyncMSDP fetches per-device `show ip msdp ...` snapshots from S3
// (uploaded by doublezero-telemetry --state-collect-enable) for all
// three MSDP kinds and replaces the dz_ip_msdp_* dimensions in
// ClickHouse. No-op if MSDP ingest is not configured.
func (a *Activities) SyncMSDP(ctx context.Context) error {
	if a.MSDPSource == nil || a.MSDPStore == nil {
		a.IngestionLog.WrapSkipped(ctx, "dzingest", "SyncMSDP", a.Network)
		return nil
	}
	return a.refresh(ctx, "SyncMSDP", func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		dumps, err := a.MSDPSource.FetchLatest(ctx)
		if err != nil {
			return result, fmt.Errorf("msdp fetch: %w", err)
		}
		return result, a.MSDPStore.Sync(ctx, dumps)
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
