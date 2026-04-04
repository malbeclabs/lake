package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	dzgraph "github.com/malbeclabs/lake/indexer/pkg/dz/graph"
	"github.com/malbeclabs/lake/indexer/pkg/dz/isis"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	dzshreds "github.com/malbeclabs/lake/indexer/pkg/dz/shreds"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/escrowevents"
	dztelemlatency "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/latency"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
	mcpgeoip "github.com/malbeclabs/lake/indexer/pkg/geoip"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/malbeclabs/lake/indexer/pkg/sol"
	"github.com/malbeclabs/lake/indexer/pkg/validatorsapp"
)

type Indexer struct {
	log *slog.Logger
	cfg Config

	svc           *dzsvc.View
	shreds        *dzshreds.View
	escrowEvents  *escrowevents.View
	graphStore    *dzgraph.Store
	telemLatency  *dztelemlatency.View
	telemUsage    *dztelemusage.View
	sol           *sol.View
	geoip         *mcpgeoip.View
	isisSource    isis.Source
	isisStore     *isis.Store
	validatorsApp *validatorsapp.View
}

func New(ctx context.Context, cfg Config) (*Indexer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.MigrationsEnable {
		// Run ClickHouse migrations to ensure tables exist
		if err := clickhouse.RunMigrations(ctx, cfg.Logger, cfg.MigrationsConfig); err != nil {
			return nil, fmt.Errorf("failed to run ClickHouse migrations: %w", err)
		}
		cfg.Logger.Info("ClickHouse migrations completed")
	}

	// Check ClickHouse env lock
	if err := checkClickHouseEnvLock(ctx, cfg.ClickHouse, cfg.DZEnv); err != nil {
		return nil, fmt.Errorf("clickhouse env lock check failed: %w", err)
	}
	cfg.Logger.Info("ClickHouse env lock verified", "dz_env", cfg.DZEnv)

	if cfg.Neo4jMigrationsEnable && cfg.Neo4j != nil {
		if err := neo4j.RunMigrations(ctx, cfg.Logger, cfg.Neo4jMigrationsConfig); err != nil {
			return nil, fmt.Errorf("failed to run Neo4j migrations: %w", err)
		}
		cfg.Logger.Info("Neo4j migrations completed")
	}

	// Check Neo4j env lock if configured
	if cfg.Neo4j != nil {
		if err := checkNeo4jEnvLock(ctx, cfg.Neo4j, cfg.DZEnv); err != nil {
			return nil, fmt.Errorf("neo4j env lock check failed: %w", err)
		}
		cfg.Logger.Info("Neo4j env lock verified", "dz_env", cfg.DZEnv)
	}

	// Initialize serviceability view
	svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
		Logger:            cfg.Logger,
		Clock:             cfg.Clock,
		ServiceabilityRPC: cfg.ServiceabilityRPC,
		RefreshInterval:   cfg.RefreshInterval,
		ClickHouse:        cfg.ClickHouse,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create serviceability view: %w", err)
	}

	// Initialize shreds subscription view (optional, mainnet-beta + testnet only)
	var shredsView *dzshreds.View
	if cfg.ShredsRPC != nil {
		shredsView, err = dzshreds.NewView(dzshreds.ViewConfig{
			Logger:          cfg.Logger,
			Clock:           cfg.Clock,
			ShredsRPC:       cfg.ShredsRPC,
			ShredsRawRPC:    cfg.ShredsRawRPC,
			ProgramID:       cfg.ShredsProgramID,
			RefreshInterval: cfg.RefreshInterval,
			ClickHouse:      cfg.ClickHouse,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create shreds view: %w", err)
		}
	}

	// Initialize escrow events view (optional, depends on shreds + escrow events RPC).
	var escrowEventsView *escrowevents.View
	if shredsView != nil && cfg.EscrowEventsRPC != nil {
		escrowEventsView, err = escrowevents.NewView(escrowevents.ViewConfig{
			Logger:          cfg.Logger,
			Clock:           cfg.Clock,
			RPC:             cfg.EscrowEventsRPC,
			ProgramID:       cfg.ShredsProgramID,
			RefreshInterval: cfg.RefreshInterval,
			ClickHouse:      cfg.ClickHouse,
			EscrowProvider:  shredsView.PaymentEscrowInfos,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create escrow events view: %w", err)
		}
	}

	// Initialize telemetry view
	telemView, err := dztelemlatency.NewView(dztelemlatency.ViewConfig{
		Logger:                 cfg.Logger,
		Clock:                  cfg.Clock,
		TelemetryRPC:           cfg.TelemetryRPC,
		EpochRPC:               cfg.DZEpochRPC,
		MaxConcurrency:         cfg.MaxConcurrency,
		InternetLatencyAgentPK: cfg.InternetLatencyAgentPK,
		InternetDataProviders:  cfg.InternetDataProviders,
		ClickHouse:             cfg.ClickHouse,
		Serviceability:         svcView,
		RefreshInterval:        cfg.RefreshInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry view: %w", err)
	}

	// Initialize solana view (optional)
	var solanaView *sol.View
	if cfg.SolanaRPC != nil {
		solanaView, err = sol.NewView(sol.ViewConfig{
			Logger:          cfg.Logger,
			Clock:           cfg.Clock,
			RPC:             cfg.SolanaRPC,
			ClickHouse:      cfg.ClickHouse,
			RefreshInterval: cfg.RefreshInterval,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create solana view: %w", err)
		}
	}

	// Initialize geoip view (optional, requires solana)
	var geoipView *mcpgeoip.View
	if cfg.GeoIPResolver != nil {
		geoIPStore, err := mcpgeoip.NewStore(mcpgeoip.StoreConfig{
			Logger:     cfg.Logger,
			ClickHouse: cfg.ClickHouse,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create GeoIP store: %w", err)
		}

		geoipView, err = mcpgeoip.NewView(mcpgeoip.ViewConfig{
			Logger:              cfg.Logger,
			Clock:               cfg.Clock,
			GeoIPStore:          geoIPStore,
			GeoIPResolver:       cfg.GeoIPResolver,
			ServiceabilityStore: svcView.Store(),
			SolanaStore:         solanaView.Store(),
			RefreshInterval:     cfg.RefreshInterval,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create geoip view: %w", err)
		}
	}

	// Initialize graph store if Neo4j is configured
	var graphStore *dzgraph.Store
	if cfg.Neo4j != nil {
		graphStore, err = dzgraph.NewStore(dzgraph.StoreConfig{
			Logger:     cfg.Logger,
			Neo4j:      cfg.Neo4j,
			ClickHouse: cfg.ClickHouse,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create graph store: %w", err)
		}
		cfg.Logger.Info("Neo4j graph store initialized")
	}

	// Initialize telemetry usage view if influx client is configured
	var telemetryUsageView *dztelemusage.View
	if cfg.DeviceUsageInfluxClient != nil {
		telemetryUsageView, err = dztelemusage.NewView(dztelemusage.ViewConfig{
			Logger:          cfg.Logger,
			Clock:           cfg.Clock,
			ClickHouse:      cfg.ClickHouse,
			RefreshInterval: cfg.DeviceUsageRefreshInterval,
			InfluxDB:        cfg.DeviceUsageInfluxClient,
			Bucket:          cfg.DeviceUsageInfluxBucket,
			QueryWindow:     cfg.DeviceUsageInfluxQueryWindow,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create telemetry usage view: %w", err)
		}
	}

	// Initialize ISIS source and store if enabled
	var isisSource isis.Source
	var isisStore *isis.Store
	if cfg.ISISEnabled {
		isisSource, err = isis.NewS3Source(ctx, isis.S3SourceConfig{
			Bucket:      cfg.ISISS3Bucket,
			Region:      cfg.ISISS3Region,
			EndpointURL: cfg.ISISS3EndpointURL,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create ISIS S3 source: %w", err)
		}
		isisStore, err = isis.NewStore(isis.StoreConfig{
			Logger:     cfg.Logger,
			ClickHouse: cfg.ClickHouse,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create ISIS store: %w", err)
		}
		cfg.Logger.Info("ISIS S3 source initialized",
			"bucket", cfg.ISISS3Bucket,
			"region", cfg.ISISS3Region)
	}

	// Initialize validators.app view (optional)
	var validatorsAppView *validatorsapp.View
	if cfg.ValidatorsAppClient != nil {
		validatorsAppView, err = validatorsapp.NewView(validatorsapp.ViewConfig{
			Logger:          cfg.Logger,
			Clock:           cfg.Clock,
			Client:          cfg.ValidatorsAppClient,
			ClickHouse:      cfg.ClickHouse,
			RefreshInterval: cfg.ValidatorsAppRefreshInterval,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create validatorsapp view: %w", err)
		}
	}

	i := &Indexer{
		log: cfg.Logger,
		cfg: cfg,

		svc:           svcView,
		shreds:        shredsView,
		escrowEvents:  escrowEventsView,
		graphStore:    graphStore,
		telemLatency:  telemView,
		telemUsage:    telemetryUsageView,
		sol:           solanaView,
		geoip:         geoipView,
		isisSource:    isisSource,
		isisStore:     isisStore,
		validatorsApp: validatorsAppView,
	}

	return i, nil
}

func (i *Indexer) Ready() bool {
	// In preview/dev environments, skip waiting for views to be ready for faster startup.
	if i.cfg.SkipReadyWait {
		return true
	}
	svcReady := i.svc.Ready()
	telemLatencyReady := i.telemLatency.Ready()
	solReady := i.sol == nil || i.sol.Ready()
	geoipReady := i.geoip == nil || i.geoip.Ready()
	// Don't wait for telemUsage to be ready, it takes too long to refresh from scratch.
	return svcReady && telemLatencyReady && solReady && geoipReady
}

func (i *Indexer) Close() error {
	var errs []error
	if i.isisSource != nil {
		if err := i.isisSource.Close(); err != nil {
			i.log.Warn("failed to close ISIS source", "error", err)
			errs = append(errs, fmt.Errorf("failed to close ISIS source: %w", err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// View getters for Temporal workflow activities.

// Serviceability returns the serviceability view.
func (i *Indexer) Serviceability() *dzsvc.View {
	return i.svc
}

// TelemLatency returns the telemetry latency view.
func (i *Indexer) TelemLatency() *dztelemlatency.View {
	return i.telemLatency
}

// TelemUsage returns the telemetry usage view, or nil if not configured.
func (i *Indexer) TelemUsage() *dztelemusage.View {
	return i.telemUsage
}

// Solana returns the Solana view, or nil if not configured.
func (i *Indexer) Solana() *sol.View {
	return i.sol
}

// GeoIP returns the GeoIP view, or nil if not configured.
func (i *Indexer) GeoIP() *mcpgeoip.View {
	return i.geoip
}

// GraphStore returns the Neo4j graph store, or nil if Neo4j is not configured.
func (i *Indexer) GraphStore() *dzgraph.Store {
	return i.graphStore
}

// ISISSource returns the ISIS data source, or nil if not configured.
func (i *Indexer) ISISSource() isis.Source {
	return i.isisSource
}

// ISISStore returns the ISIS ClickHouse store, or nil if not configured.
func (i *Indexer) ISISStore() *isis.Store {
	return i.isisStore
}

// Shreds returns the shreds subscription view, or nil if not configured.
func (i *Indexer) Shreds() *dzshreds.View {
	return i.shreds
}

// EscrowEvents returns the escrow events view, or nil if not configured.
func (i *Indexer) EscrowEvents() *escrowevents.View {
	return i.escrowEvents
}

// ValidatorsApp returns the validators.app view, or nil if not configured.
func (i *Indexer) ValidatorsApp() *validatorsapp.View {
	return i.validatorsApp
}
