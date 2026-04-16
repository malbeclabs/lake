package geolocation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/jonboulle/clockwork"
	geolocsdk "github.com/malbeclabs/doublezero/sdk/geolocation/go"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"golang.org/x/sync/errgroup"
)

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	GeolocationRPC  GeolocationRPC
	RefreshInterval time.Duration
	ClickHouse      clickhouse.Client
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.GeolocationRPC == nil {
		return errors.New("geolocation rpc is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.RefreshInterval <= 0 {
		return errors.New("refresh interval must be greater than 0")
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

type View struct {
	log       *slog.Logger
	cfg       ViewConfig
	store     *Store
	refreshMu sync.Mutex

	fetchedAt time.Time
	readyOnce sync.Once
	readyCh   chan struct{}
}

func NewView(cfg ViewConfig) (*View, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	store, err := NewStore(StoreConfig{
		Logger:     cfg.Logger,
		ClickHouse: cfg.ClickHouse,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	v := &View{
		log:     cfg.Logger,
		cfg:     cfg,
		store:   store,
		readyCh: make(chan struct{}),
	}

	return v, nil
}

func (v *View) Store() *Store {
	return v.store
}

func (v *View) Ready() bool {
	select {
	case <-v.readyCh:
		return true
	default:
		return false
	}
}

func (v *View) WaitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for geolocation view: %w", ctx.Err())
	}
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("geolocation: starting refresh loop", "interval", v.cfg.RefreshInterval)

		v.safeRefresh(ctx)

		ticker := v.cfg.Clock.NewTicker(v.cfg.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				v.safeRefresh(ctx)
			}
		}
	}()
}

func (v *View) safeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			v.log.Error("geolocation: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("geolocation", "panic").Inc()
		}
	}()

	if _, err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("geolocation: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	v.log.Debug("geolocation: refresh started", "start_time", refreshStart)
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("geolocation: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("geolocation").Observe(duration.Seconds())
	}()

	// Fetch probes and users concurrently.
	var (
		onchainProbes []geolocsdk.KeyedGeoProbe
		onchainUsers  []geolocsdk.KeyedGeolocationUser
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		onchainProbes, err = v.cfg.GeolocationRPC.GetGeoProbes(gctx)
		return err
	})
	g.Go(func() error {
		var err error
		onchainUsers, err = v.cfg.GeolocationRPC.GetGeolocationUsers(gctx)
		return err
	})
	if err := g.Wait(); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("geolocation", "error").Inc()
		return result, err
	}

	v.log.Debug("geolocation: fetched program data",
		"probes", len(onchainProbes),
		"users", len(onchainUsers))

	// Validate that we received data — empty responses would tombstone all existing entities.
	if len(onchainProbes) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("geolocation", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no probes (possible RPC issue)")
	}
	if len(onchainUsers) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("geolocation", "error").Inc()
		return result, fmt.Errorf("refusing to write snapshot: RPC returned no users (possible RPC issue)")
	}

	probes := convertProbes(onchainProbes)
	users := convertUsers(onchainUsers)
	targets := convertTargets(onchainUsers)

	fetchedAt := time.Now().UTC()

	if err := v.store.ReplaceProbes(ctx, probes); err != nil {
		return result, fmt.Errorf("failed to replace probes: %w", err)
	}

	if err := v.store.ReplaceUsers(ctx, users); err != nil {
		return result, fmt.Errorf("failed to replace users: %w", err)
	}

	if err := v.store.ReplaceTargets(ctx, targets); err != nil {
		return result, fmt.Errorf("failed to replace targets: %w", err)
	}

	result.RowsAffected = int64(len(probes) + len(users) + len(targets))
	result.SourceMaxEventTS = &fetchedAt

	v.fetchedAt = fetchedAt
	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("geolocation: view is now ready")
	})

	v.log.Debug("geolocation: refresh completed", "fetched_at", fetchedAt)
	metrics.ViewRefreshTotal.WithLabelValues("geolocation", "success").Inc()
	return result, nil
}

func convertProbes(onchain []geolocsdk.KeyedGeoProbe) []Probe {
	result := make([]Probe, len(onchain))
	for i, p := range onchain {
		parentDevices := make([]string, len(p.ParentDevices))
		for j, pd := range p.ParentDevices {
			parentDevices[j] = pd.String()
		}

		result[i] = Probe{
			PK:                 p.Pubkey.String(),
			Owner:              solana.PublicKeyFromBytes(p.Owner[:]).String(),
			ExchangePK:         solana.PublicKeyFromBytes(p.ExchangePK[:]).String(),
			PublicIP:           net.IP(p.PublicIP[:]).String(),
			LocationOffsetPort: p.LocationOffsetPort,
			MetricsPublisherPK: solana.PublicKeyFromBytes(p.MetricsPublisherPK[:]).String(),
			ReferenceCount:     p.ReferenceCount,
			Code:               p.Code,
			ParentDevices:      parentDevices,
			TargetUpdateCount:  p.TargetUpdateCount,
		}
	}
	return result
}

func convertUsers(onchain []geolocsdk.KeyedGeolocationUser) []User {
	result := make([]User, len(onchain))
	for i, u := range onchain {
		result[i] = User{
			PK:                   u.Pubkey.String(),
			Owner:                solana.PublicKeyFromBytes(u.Owner[:]).String(),
			Code:                 u.Code,
			TokenAccount:         solana.PublicKeyFromBytes(u.TokenAccount[:]).String(),
			PaymentStatus:        u.PaymentStatus.String(),
			Status:               u.Status.String(),
			TargetCount:          uint32(len(u.Targets)),
			BillingRate:          u.Billing.FlatPerEpoch.Rate,
			LastDeductionDzEpoch: u.Billing.FlatPerEpoch.LastDeductionDzEpoch,
		}
	}
	return result
}

func convertTargets(onchain []geolocsdk.KeyedGeolocationUser) []Target {
	var result []Target
	for _, u := range onchain {
		userPK := u.Pubkey.String()
		for _, t := range u.Targets {
			result = append(result, Target{
				GeolocUserPK:       userPK,
				ProbePK:            solana.PublicKeyFromBytes(t.GeoProbePK[:]).String(),
				TargetType:         t.TargetType.String(),
				IP:                 net.IP(t.IPAddress[:]).String(),
				LocationOffsetPort: t.LocationOffsetPort,
				TargetPK:           solana.PublicKeyFromBytes(t.TargetPK[:]).String(),
			})
		}
	}
	return result
}
