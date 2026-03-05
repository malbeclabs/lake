package validatorsapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// ViewConfig holds configuration for the validatorsapp View.
type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	Client          Client
	ClickHouse      clickhouse.Client
	RefreshInterval time.Duration
}

// Validate ensures the ViewConfig has all required fields and sets defaults.
func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.Client == nil {
		return errors.New("client is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 5 * time.Minute
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

// View manages periodic ingestion of validator data from validators.app into ClickHouse.
type View struct {
	log   *slog.Logger
	cfg   ViewConfig
	store *Store

	readyOnce sync.Once
	readyCh   chan struct{}
}

// NewView creates a new View after validating the provided config.
func NewView(cfg ViewConfig) (*View, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate config: %w", err)
	}

	store, err := NewStore(StoreConfig{
		Logger:     cfg.Logger,
		ClickHouse: cfg.ClickHouse,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	return &View{
		log:     cfg.Logger,
		cfg:     cfg,
		store:   store,
		readyCh: make(chan struct{}),
	}, nil
}

// Ready returns true if the view has completed at least one successful refresh.
func (v *View) Ready() bool {
	select {
	case <-v.readyCh:
		return true
	default:
		return false
	}
}

// WaitReady blocks until the view is ready or the context is cancelled.
func (v *View) WaitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for validatorsapp view: %w", ctx.Err())
	}
}

// Start begins the periodic refresh loop in a background goroutine.
func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("validatorsapp: starting refresh loop", "interval", v.cfg.RefreshInterval)

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

// safeRefresh wraps Refresh with panic recovery to prevent the refresh loop from dying.
func (v *View) safeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			v.log.Error("validatorsapp: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("validatorsapp", "panic").Inc()
		}
	}()

	if err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("validatorsapp: refresh failed", "error", err)
	}
}

// Refresh fetches validators from the API and writes them to ClickHouse.
func (v *View) Refresh(ctx context.Context) error {
	refreshStart := time.Now()
	v.log.Debug("validatorsapp: refresh started")
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("validatorsapp: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("validatorsapp").Observe(duration.Seconds())
	}()

	validators, err := v.cfg.Client.GetValidators(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("validatorsapp", "error").Inc()
		return fmt.Errorf("failed to get validators: %w", err)
	}

	if len(validators) == 0 {
		metrics.ViewRefreshTotal.WithLabelValues("validatorsapp", "error").Inc()
		return fmt.Errorf("rejecting empty validator response")
	}

	if err := v.store.ReplaceValidators(ctx, validators); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("validatorsapp", "error").Inc()
		return fmt.Errorf("failed to replace validators: %w", err)
	}

	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("validatorsapp: view is now ready")
	})

	metrics.ViewRefreshTotal.WithLabelValues("validatorsapp", "success").Inc()
	return nil
}
