package dztelemlatency

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/telemetry"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"github.com/malbeclabs/lake/utils/pkg/logger"
)

// fetchOutcome classifies a telemetry sample-fetch error. Both the device-link
// and internet-metro fan-outs need the same three-way split, so it lives here
// rather than being spelled out (and drifting) at each call site.
type fetchOutcome int

const (
	// fetchAbort means the refresh context is done — worker shutdown or the refresh
	// deadline — so stop and record nothing. Shutdown is not a collection failure and
	// must not be counted as one.
	fetchAbort fetchOutcome = iota
	// fetchExpectedMiss means there is simply no telemetry account for this
	// circuit/epoch. Routine and very common — not worth counting.
	fetchExpectedMiss
	// fetchUnclassified is any other error. The circuit resumes from its own stored
	// max sample index, so the next refresh re-fetches what this one missed — but only
	// while the epoch is still in the fetch window (current and current-1). Past that,
	// roughly two days, only the admin backfill recovers it. So the skip must be
	// counted and logged: the refresh still reports success, and an uncounted skip lets
	// a failing circuit under-collect with no signal anywhere.
	fetchUnclassified
)

// classifyFetchErr decides what a sample-fetch error means. The context decides
// shutdown, never the error's shape.
//
// The invariant: an http.Client timeout also satisfies
// errors.Is(err, context.DeadlineExceeded). Keying the abort on the error therefore
// reads a slow endpoint as shutdown and returns from the worker goroutine, and that
// return sits before the append of everything already collected for the circuit. The
// refresh still reports success, so the cycle looks clean with a hole in it.
//
// This has not fired in production. The deployed client timeout and the activity
// StartToCloseTimeout are both 5 minutes and the HTTP request starts later, so the
// context always expired first and the abort was genuine. It bites as soon as the
// client timeout drops below the activity deadline, which the SDK bump in #757 does
// (10s), or when Refresh runs from a context with no deadline.
//
// A live context therefore means the fetch genuinely failed: count it, log it, and let
// the circuit resume from its stored max sample index. Only a cancelled or expired
// context aborts, and the loop's own select on ctx.Done handles that first.
func classifyFetchErr(ctx context.Context, err error) fetchOutcome {
	if ctx.Err() != nil {
		return fetchAbort
	}
	if errors.Is(err, telemetry.ErrAccountNotFound) {
		return fetchExpectedMiss
	}
	return fetchUnclassified
}

type TelemetryRPC interface {
	GetDeviceLatencySamplesTail(ctx context.Context, originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error)
	GetInternetLatencySamples(ctx context.Context, dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error)
}

type EpochRPC interface {
	GetEpochInfo(ctx context.Context, commitment solanarpc.CommitmentType) (*solanarpc.GetEpochInfoResult, error)
}

type ViewConfig struct {
	Logger                     *slog.Logger
	Clock                      clockwork.Clock
	TelemetryRPC               TelemetryRPC
	EpochRPC                   EpochRPC
	MaxConcurrency             int
	InternetLatencyAgentPK     solana.PublicKey
	InternetDataProviders      []string
	ClickHouse                 clickhouse.Client
	Serviceability             *dzsvc.View
	RefreshInterval            time.Duration
	ServiceabilityReadyTimeout time.Duration
	// DZEnv labels the store's Prometheus metrics; it does not affect behavior.
	DZEnv string
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.TelemetryRPC == nil {
		return errors.New("telemetry rpc is required")
	}
	if cfg.EpochRPC == nil {
		return errors.New("epoch rpc is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.Serviceability == nil {
		return errors.New("serviceability view is required")
	}
	if cfg.RefreshInterval <= 0 {
		return errors.New("refresh interval must be greater than 0")
	}
	if cfg.InternetLatencyAgentPK.IsZero() {
		return errors.New("internet latency agent pk is required")
	}
	if len(cfg.InternetDataProviders) == 0 {
		return errors.New("internet data providers are required")
	}
	if cfg.MaxConcurrency <= 0 {
		return errors.New("max concurrency must be greater than 0")
	}

	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	if cfg.ServiceabilityReadyTimeout <= 0 {
		cfg.ServiceabilityReadyTimeout = 2 * cfg.RefreshInterval
	}
	return nil
}

type View struct {
	log       *slog.Logger
	cfg       ViewConfig
	store     *Store
	readyOnce sync.Once
	readyCh   chan struct{}
	refreshMu sync.Mutex // prevents concurrent refreshes

	// esc escalates consecutive refresh failures from WARN to ERROR so a
	// single blip doesn't page on-call (see logger.Escalator).
	esc logger.Escalator
}

func NewView(cfg ViewConfig) (*View, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	store, err := NewStore(StoreConfig{
		Logger:     cfg.Logger,
		ClickHouse: cfg.ClickHouse,
		DZEnv:      cfg.DZEnv,
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

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("telemetry/latency: starting refresh loop", "interval", v.cfg.RefreshInterval)

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

// safeRefresh wraps Refresh with panic recovery to prevent the refresh loop from dying
func (v *View) safeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			v.log.Error("telemetry/latency: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("telemetry", "panic").Inc()
		}
	}()

	_, err := v.Refresh(ctx)
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	v.esc.Observe(v.log, "refresh", "telemetry/latency: refresh failed", err)
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	var result ingestionlog.RefreshResult

	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	refreshStart := time.Now()
	v.log.Debug("telemetry/latency: refresh started", "start_time", refreshStart)
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("telemetry/latency: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("telemetry").Observe(duration.Seconds())
	}()

	if !v.cfg.Serviceability.Ready() {
		waitCtx, cancel := context.WithTimeout(ctx, v.cfg.ServiceabilityReadyTimeout)
		defer cancel()

		if err := v.cfg.Serviceability.WaitReady(waitCtx); err != nil {
			metrics.ViewRefreshTotal.WithLabelValues("telemetry", "error").Inc()
			return result, fmt.Errorf("serviceability view not ready: %w", err)
		}
	}

	devices, err := dzsvc.QueryCurrentDevices(ctx, v.log, v.cfg.ClickHouse)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("telemetry", "error").Inc()
		return result, fmt.Errorf("failed to query devices: %w", err)
	}

	links, err := dzsvc.QueryCurrentLinks(ctx, v.log, v.cfg.ClickHouse)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("telemetry", "error").Inc()
		return result, fmt.Errorf("failed to query links: %w", err)
	}

	// Refresh device-link latency samples
	if err := v.refreshDeviceLinkTelemetrySamples(ctx, devices, links); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("telemetry", "error").Inc()
		return result, fmt.Errorf("failed to refresh device-link latency samples: %w", err)
	}

	// Refresh internet-metro latency samples if configured
	if !v.cfg.InternetLatencyAgentPK.IsZero() && len(v.cfg.InternetDataProviders) > 0 {
		metros, err := dzsvc.QueryCurrentMetros(ctx, v.log, v.cfg.ClickHouse)
		if err != nil {
			metrics.ViewRefreshTotal.WithLabelValues("telemetry", "error").Inc()
			return result, fmt.Errorf("failed to query metros: %w", err)
		}

		if err := v.refreshInternetMetroLatencySamples(ctx, metros); err != nil {
			metrics.ViewRefreshTotal.WithLabelValues("telemetry", "error").Inc()
			return result, fmt.Errorf("failed to refresh internet-metro latency samples: %w", err)
		}
	}

	// Signal readiness once (close channel) - safe to call multiple times
	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("telemetry/latency: view is now ready")
	})

	metrics.ViewRefreshTotal.WithLabelValues("telemetry", "success").Inc()
	fetchedAt := time.Now().UTC()
	result.SourceMaxEventTS = &fetchedAt
	return result, nil
}

// Ready returns true if the view has completed at least one successful refresh
func (v *View) Ready() bool {
	select {
	case <-v.readyCh:
		return true
	default:
		return false
	}
}

// WaitReady waits for the view to be ready (has completed at least one successful refresh)
// It returns immediately if already ready, or blocks until ready or context is cancelled.
func (v *View) WaitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for telemetry view: %w", ctx.Err())
	}
}

// noteSkip records a skipped fetch for the per-refresh summary. Both fan-outs call it
// from the same place, the arm that counts an unclassified error, so the two cannot
// drift: a previous version of this accounting sat in the wrong branch on one path and
// silenced that path's summary entirely.
//
// It stores a copy of err rather than the caller's variable, so the retained error
// cannot be affected by later loop iterations.
func noteSkip(count *atomic.Int64, first *atomic.Pointer[error], err error) {
	count.Add(1)
	e := err
	first.CompareAndSwap(nil, &e)
}
