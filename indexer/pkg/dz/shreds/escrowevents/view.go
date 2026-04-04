package escrowevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"golang.org/x/sync/errgroup"
)

const (
	// maxConcurrentFetches limits parallel RPC calls per refresh.
	maxConcurrentFetches = 10
	// maxSignaturesPerRequest is the Solana RPC limit.
	maxSignaturesPerRequest = 1000
)

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	RPC             SolanaRPC
	ProgramID       solana.PublicKey
	RefreshInterval time.Duration
	ClickHouse      clickhouse.Client
	// EscrowProvider returns the current list of known escrow accounts.
	EscrowProvider func() []EscrowInfo
	// SkipHighWaterMarks ignores existing high-water marks, forcing a full
	// re-fetch of all transaction history. Used by the backfill command.
	SkipHighWaterMarks bool
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.RPC == nil {
		return errors.New("rpc is required")
	}
	if cfg.ProgramID.IsZero() {
		return errors.New("program id is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.RefreshInterval <= 0 {
		return errors.New("refresh interval must be greater than 0")
	}
	if cfg.EscrowProvider == nil {
		return errors.New("escrow provider is required")
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

	return &View{
		log:     cfg.Logger,
		cfg:     cfg,
		store:   store,
		readyCh: make(chan struct{}),
	}, nil
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
		return fmt.Errorf("context cancelled while waiting for escrow events view: %w", ctx.Err())
	}
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("shreds/escrow-events: starting refresh loop", "interval", v.cfg.RefreshInterval)

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
			v.log.Error("shreds/escrow-events: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "panic").Inc()
		}
	}()

	if _, err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("shreds/escrow-events: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	v.log.Debug("shreds/escrow-events: refresh started")
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("shreds/escrow-events: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("shreds_escrow_events").Observe(duration.Seconds())
	}()

	// Get known escrow accounts.
	escrows := v.cfg.EscrowProvider()
	if len(escrows) == 0 {
		v.log.Debug("shreds/escrow-events: no escrows found, skipping")
		v.markReady()
		metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "success").Inc()
		return result, nil
	}

	// Get high water marks to know where to resume (unless skipping for backfill).
	var hwms map[string]HighWaterMark
	if v.cfg.SkipHighWaterMarks {
		hwms = make(map[string]HighWaterMark)
	} else {
		var err error
		hwms, err = v.store.GetHighWaterMarks(ctx)
		if err != nil {
			metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "error").Inc()
			return result, fmt.Errorf("get high water marks: %w", err)
		}
	}

	v.log.Debug("shreds/escrow-events: fetching transactions",
		"escrows", len(escrows),
		"high_water_marks", len(hwms),
	)

	// Fetch and parse transactions for each escrow with concurrency limit.
	var (
		mu        sync.Mutex
		allEvents []EscrowEventRow
	)

	programID := v.cfg.ProgramID.String()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetches)

	for _, escrow := range escrows {
		g.Go(func() error {
			events, err := v.fetchEscrowEvents(gctx, escrow, hwms[escrow.EscrowPK], programID)
			if err != nil {
				v.log.Warn("shreds/escrow-events: failed to fetch for escrow",
					"escrow_pk", escrow.EscrowPK,
					"error", err,
				)
				return nil // Don't fail the entire refresh for one escrow.
			}
			if len(events) > 0 {
				mu.Lock()
				allEvents = append(allEvents, events...)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "error").Inc()
		return result, fmt.Errorf("fetch shreds/escrow-events: %w", err)
	}

	// Insert all events.
	if err := v.store.InsertEvents(ctx, allEvents); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "error").Inc()
		return result, fmt.Errorf("insert shreds/escrow-events: %w", err)
	}

	result.RowsAffected = int64(len(allEvents))
	fetchedAt := time.Now().UTC()
	result.SourceMaxEventTS = &fetchedAt

	v.markReady()
	metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "success").Inc()

	if len(allEvents) > 0 {
		v.log.Info("shreds/escrow-events: indexed new events", "count", len(allEvents))
	}

	return result, nil
}

// BackfillRefresh runs a full refresh ignoring high-water marks. Existing events
// are safely overwritten via ReplacingMergeTree deduplication.
func (v *View) BackfillRefresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	orig := v.cfg.SkipHighWaterMarks
	v.cfg.SkipHighWaterMarks = true
	defer func() { v.cfg.SkipHighWaterMarks = orig }()
	return v.Refresh(ctx)
}

// ClickHouse returns the ClickHouse client for direct operations (e.g. truncate).
func (v *View) ClickHouse() clickhouse.Client {
	return v.cfg.ClickHouse
}

func (v *View) markReady() {
	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("shreds/escrow-events: view is now ready")
	})
}

// fetchEscrowEvents fetches all new transactions for a single escrow account
// and parses them into events.
func (v *View) fetchEscrowEvents(ctx context.Context, escrow EscrowInfo, hwm HighWaterMark, programID string) ([]EscrowEventRow, error) {
	escrowPK, err := solana.PublicKeyFromBase58(escrow.EscrowPK)
	if err != nil {
		return nil, fmt.Errorf("invalid escrow pubkey %q: %w", escrow.EscrowPK, err)
	}

	// Build opts for incremental fetching.
	var untilSig solana.Signature
	if hwm.TxSignature != "" {
		untilSig, err = solana.SignatureFromBase58(hwm.TxSignature)
		if err != nil {
			return nil, fmt.Errorf("invalid high water mark signature %q: %w", hwm.TxSignature, err)
		}
	}

	// Fetch all new signatures, paginating backwards if needed.
	var allSigs []*rpc.TransactionSignature
	var beforeSig solana.Signature

	for {
		opts := &rpc.GetSignaturesForAddressOpts{
			Commitment: rpc.CommitmentFinalized,
		}
		if !untilSig.IsZero() {
			opts.Until = untilSig
		}
		if !beforeSig.IsZero() {
			opts.Before = beforeSig
		}

		sigs, err := v.cfg.RPC.GetSignaturesForAddressWithOpts(ctx, escrowPK, opts)
		if err != nil {
			return nil, fmt.Errorf("get signatures: %w", err)
		}

		allSigs = append(allSigs, sigs...)

		// If we got fewer than the max, we've reached the end.
		if len(sigs) < maxSignaturesPerRequest {
			break
		}

		// Paginate: set before to the last (oldest) signature.
		beforeSig = sigs[len(sigs)-1].Signature
	}

	if len(allSigs) == 0 {
		return nil, nil
	}

	v.log.Debug("shreds/escrow-events: fetching transaction details",
		"escrow_pk", escrow.EscrowPK,
		"new_signatures", len(allSigs),
	)

	// Fetch transaction details and parse logs.
	var events []EscrowEventRow
	maxVersion := uint64(0)
	for _, sig := range allSigs {
		txResult, err := v.cfg.RPC.GetTransaction(ctx, sig.Signature, &rpc.GetTransactionOpts{
			MaxSupportedTransactionVersion: &maxVersion,
		})
		if err != nil {
			v.log.Warn("shreds/escrow-events: failed to fetch transaction",
				"signature", sig.Signature.String(),
				"error", err,
			)
			continue
		}

		var logs []string
		if txResult.Meta != nil {
			logs = txResult.Meta.LogMessages
		}

		// Extract fee payer (first account key / signer).
		var signer string
		if txResult.Transaction != nil {
			if tx, err := txResult.Transaction.GetTransaction(); err == nil && tx != nil {
				if len(tx.Message.AccountKeys) > 0 {
					signer = tx.Message.AccountKeys[0].String()
				}
			}
		}

		var blockTime time.Time
		if sig.BlockTime != nil {
			blockTime = sig.BlockTime.Time()
		}

		failed := sig.Err != nil

		parsed := ParseTransactionLogs(
			v.log,
			escrow.EscrowPK,
			escrow.ClientSeatPK,
			sig.Signature.String(),
			sig.Slot,
			blockTime,
			logs,
			failed,
			programID,
			signer,
		)
		events = append(events, parsed...)
	}

	return events, nil
}
