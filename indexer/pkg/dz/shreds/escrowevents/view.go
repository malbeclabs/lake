package escrowevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"github.com/malbeclabs/lake/utils/pkg/logger"
	"golang.org/x/sync/errgroup"
)

const (
	// maxConcurrentFetches limits parallel RPC calls per refresh.
	maxConcurrentFetches = 10
	// maxSignaturesPerRequest is the Solana RPC limit.
	maxSignaturesPerRequest = 1000
	// scanChunkSize is how many signatures are decoded and durably written before the
	// walk continues. It bounds what a mid-walk failure costs: the fact-derived
	// high-water mark advances over every committed chunk, so a retry resumes rather
	// than restarting. The largest escrow in production holds 4471 signatures and grows
	// unbounded, so an all-or-nothing walk over it never converges once anything fails
	// part-way. Mirrors permissionevents.scanChunkSize.
	scanChunkSize = 200
	// notFoundSkipSlotLag is how far below the newest fetched signature a not-found
	// transaction must sit before it is skipped as pruned history. A getTransaction null
	// near the tip is a load-balanced backend lagging finalization: transient, and it
	// must be retried rather than skipped, or the event is lost for good. One this many
	// slots back is genuinely unretrievable and would wedge the escrow forever.
	// Same value and reasoning as permissionevents.
	notFoundSkipSlotLag = 300
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

	_, err := v.Refresh(ctx)
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	v.esc.Observe(v.log, "refresh", "shreds/escrow-events: refresh failed", err)
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	return v.refresh(ctx, v.cfg.SkipHighWaterMarks)
}

func (v *View) refresh(ctx context.Context, ignoreCursor bool) (ingestionlog.RefreshResult, error) {
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
	if ignoreCursor {
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

	// Drain each escrow with a concurrency limit. Each drain commits its own chunks, so
	// there is no aggregate insert here: a failure mid-walk leaves the chunks before it
	// durable and the next refresh resumes from the advanced high-water mark.
	var (
		totalInserted int64
		failedEscrows int64
		firstErr      atomic.Pointer[error]
	)

	programID := v.cfg.ProgramID.String()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetches)

	for _, escrow := range escrows {
		g.Go(func() error {
			n, err := v.drainEscrow(gctx, escrow, hwms[escrow.EscrowPK], programID)
			atomic.AddInt64(&totalInserted, n)
			if err != nil {
				// One escrow's failure must not fail the refresh: the others are
				// independent and their committed chunks are already durable. But it
				// must be visible, so it is counted and summarised after the barrier
				// rather than left as a per-escrow line nobody aggregates.
				if ctx.Err() != nil {
					return err
				}
				atomic.AddInt64(&failedEscrows, 1)
				firstErr.CompareAndSwap(nil, &err)
				v.log.Debug("shreds/escrow-events: escrow drain failed, resuming next refresh",
					"escrow_pk", escrow.EscrowPK, "inserted", n, "error", err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "error").Inc()
		return result, fmt.Errorf("fetch shreds/escrow-events: %w", err)
	}

	result.RowsAffected = totalInserted

	// One summary line per refresh rather than one per escrow, so a partial run is
	// visible without burying the log. A failed escrow keeps its committed chunks and
	// resumes next refresh, so the count matters more than any single error.
	if n := failedEscrows; n > 0 {
		var first error
		if ptr := firstErr.Load(); ptr != nil {
			first = *ptr
		}
		metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "partial").Inc()
		v.log.Warn("shreds/escrow-events: some escrows did not finish, resuming next refresh",
			"failed_escrows", n, "total_escrows", len(escrows),
			"inserted", totalInserted, "first_error", first)
	} else {
		// Only a clean sweep may claim current freshness. Restamping this while an
		// escrow is still behind is what let a stalled drain look healthy.
		fetchedAt := time.Now().UTC()
		result.SourceMaxEventTS = &fetchedAt
		metrics.ViewRefreshTotal.WithLabelValues("shreds_escrow_events", "success").Inc()
	}

	v.markReady()

	if totalInserted > 0 {
		v.log.Info("shreds/escrow-events: indexed new events", "count", totalInserted)
	}

	return result, nil
}

// BackfillRefresh runs a full refresh ignoring high-water marks. Existing events
// are safely overwritten via ReplacingMergeTree deduplication.
func (v *View) BackfillRefresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	return v.refresh(ctx, true)
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
func (v *View) drainEscrow(ctx context.Context, escrow EscrowInfo, hwm HighWaterMark, programID string) (int64, error) {
	escrowPK, err := solana.PublicKeyFromBase58(escrow.EscrowPK)
	if err != nil {
		return 0, fmt.Errorf("invalid escrow pubkey %q: %w", escrow.EscrowPK, err)
	}

	// Build opts for incremental fetching.
	var untilSig solana.Signature
	if hwm.TxSignature != "" {
		untilSig, err = solana.SignatureFromBase58(hwm.TxSignature)
		if err != nil {
			return 0, fmt.Errorf("invalid high water mark signature %q: %w", hwm.TxSignature, err)
		}
	}

	// Fetch all new signatures, paginating backwards if needed.
	var allSigs []*rpc.TransactionSignature
	var beforeSig solana.Signature

	// Paginate backward (via Before) until an empty page. Terminating on an empty page
	// rather than a short page is robust to providers/gateways that cap pages below the
	// requested limit — stopping on the first short page would skip older history.
	limit := maxSignaturesPerRequest
	for {
		opts := &rpc.GetSignaturesForAddressOpts{
			Commitment: rpc.CommitmentFinalized,
			Limit:      &limit,
		}
		if !untilSig.IsZero() {
			opts.Until = untilSig
		}
		if !beforeSig.IsZero() {
			opts.Before = beforeSig
		}

		sigs, err := v.cfg.RPC.GetSignaturesForAddressWithOpts(ctx, escrowPK, opts)
		if err != nil {
			return 0, fmt.Errorf("get signatures: %w", err)
		}
		if len(sigs) == 0 {
			break
		}

		allSigs = append(allSigs, sigs...)

		// Paginate: set before to the last (oldest) signature.
		beforeSig = sigs[len(sigs)-1].Signature
	}

	if len(allSigs) == 0 {
		return 0, nil
	}

	v.log.Debug("shreds/escrow-events: fetching transaction details",
		"escrow_pk", escrow.EscrowPK,
		"new_signatures", len(allSigs),
	)

	// Walk oldest-first in durable chunks. allSigs is newest-first from pagination, and
	// GetHighWaterMarks derives the resume point from max(slot) of rows actually
	// written, so committing a prefix of the *oldest* signatures advances the mark over
	// exactly the range that is done. Committing newest-first would push the mark past
	// signatures never decoded, which is the permanent gap this whole change is about.
	for i, j := 0, len(allSigs)-1; i < j; i, j = i+1, j-1 {
		allSigs[i], allSigs[j] = allSigs[j], allSigs[i]
	}

	// The tip is now the last element. Not-founds more than notFoundSkipSlotLag below it
	// are pruned history and safe to skip.
	var skipNotFoundBelow uint64
	if tip := allSigs[len(allSigs)-1].Slot; tip > notFoundSkipSlotLag {
		skipNotFoundBelow = tip - notFoundSkipSlotLag
	}

	maxVersion := uint64(0)
	var inserted int64

	for start := 0; start < len(allSigs); start += scanChunkSize {
		chunk := allSigs[start:min(start+scanChunkSize, len(allSigs))]
		var events []EscrowEventRow

		for _, sig := range chunk {
			// Shutdown is decided by the context, not by the error. Without this a
			// cancelled context walks every remaining signature against a dead
			// connection, emitting one line each: thousands that read like data loss.
			if err := ctx.Err(); err != nil {
				return inserted, err
			}

			txResult, err := v.cfg.RPC.GetTransaction(ctx, sig.Signature, &rpc.GetTransactionOpts{
				MaxSupportedTransactionVersion: &maxVersion,
			})
			if err != nil {
				// One narrow escape hatch: a finalized signature the RPC cannot serve
				// from far enough below the tip is pruned history. Retrying can never
				// recover it and failing forever would wedge the escrow, so it is
				// skipped and counted. Every other error fails the chunk.
				//
				// Failing closed is what closes the gap. The previous version gated on
				// dberror.IsTransient, a message-substring classifier written for
				// ClickHouse and S3 errors: of the shapes solana-go actually returns,
				// only RPCError{429} matched, so ErrNotFound, HTTPError{429,502,503} and
				// RPCError{-32005,-32004,-32603} all still took the skip path and still
				// lost events.
				if errors.Is(err, rpc.ErrNotFound) && sig.Slot < skipNotFoundBelow {
					metrics.EscrowEventsSkippedTx.Inc()
					v.log.Warn("shreds/escrow-events: transaction unretrievable upstream, skipping",
						"escrow_pk", escrow.EscrowPK, "signature", sig.Signature.String(),
						"slot", sig.Slot, "error", err)
					continue
				}
				return inserted, fmt.Errorf("get transaction %s: %w", sig.Signature.String(), err)
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

			parsed := ParseTransactionLogs(
				v.log,
				escrow.EscrowPK,
				escrow.ClientSeatPK,
				sig.Signature.String(),
				sig.Slot,
				blockTime,
				logs,
				sig.Err != nil,
				programID,
				signer,
			)
			events = append(events, parsed...)
		}

		// Commit the chunk before moving on. This is what makes a later failure cost one
		// chunk rather than the whole walk.
		if len(events) > 0 {
			if err := v.store.InsertEvents(ctx, events); err != nil {
				return inserted, fmt.Errorf("insert escrow events: %w", err)
			}
			inserted += int64(len(events))
		}
	}

	return inserted, nil
}
