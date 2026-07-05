package permissionevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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
	// maxConcurrentFetches limits parallel getTransaction calls per refresh.
	maxConcurrentFetches = 10
	// maxSignaturesPerRequest is the Solana RPC page limit.
	maxSignaturesPerRequest = 1000
	// metricSource labels this view's refresh metrics.
	metricSource = "serviceability_permission_events"
)

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	RPC             SolanaRPC
	ProgramID       solana.PublicKey // the serviceability program id
	RefreshInterval time.Duration
	ClickHouse      clickhouse.Client
	// SkipHighWaterMarks ignores the stored scan cursor, forcing a full re-scan of all
	// history. Used by the backfill command. Existing rows are safely overwritten via
	// ReplacingMergeTree dedup.
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
	store, err := NewStore(StoreConfig{Logger: cfg.Logger, ClickHouse: cfg.ClickHouse})
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
		return fmt.Errorf("context cancelled while waiting for permission events view: %w", ctx.Err())
	}
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("serviceability/permission-events: starting refresh loop", "interval", v.cfg.RefreshInterval)

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
			v.log.Error("serviceability/permission-events: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues(metricSource, "panic").Inc()
		}
	}()

	if _, err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("serviceability/permission-events: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	v.log.Debug("serviceability/permission-events: refresh started")
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("serviceability/permission-events: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues(metricSource).Observe(duration.Seconds())
	}()

	programPK := v.cfg.ProgramID.String()

	// Resolve where to resume from (unless backfilling).
	var cursor ScanCursor
	if !v.cfg.SkipHighWaterMarks {
		var err error
		cursor, err = v.store.GetScanCursor(ctx, programPK)
		if err != nil {
			metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
			return result, fmt.Errorf("get scan cursor: %w", err)
		}
	}

	// Fetch all new signatures for the program, paginating backward to the cursor.
	sigs, err := v.fetchNewSignatures(ctx, cursor)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("fetch signatures: %w", err)
	}
	if len(sigs) == 0 {
		v.markReady()
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "success").Inc()
		return result, nil
	}

	// The newest signature (first, since results are newest-first) becomes the new cursor.
	newCursor := ScanCursor{TxSignature: sigs[0].Signature.String(), Slot: sigs[0].Slot}

	// Fetch and decode each transaction concurrently.
	var (
		mu        sync.Mutex
		allEvents []PermissionEventRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetches)
	for _, sig := range sigs {
		g.Go(func() error {
			events, err := v.decodeTransaction(gctx, sig)
			if err != nil {
				v.log.Warn("serviceability/permission-events: failed to decode transaction",
					"signature", sig.Signature.String(), "error", err)
				return nil // one bad tx shouldn't fail the whole refresh
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
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("decode transactions: %w", err)
	}

	if err := v.store.InsertEvents(ctx, allEvents); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("insert permission events: %w", err)
	}

	// Advance the cursor only after events are durably written, so a crash mid-refresh
	// re-scans (idempotent via ReplacingMergeTree) rather than skipping.
	if err := v.store.SetScanCursor(ctx, programPK, newCursor); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("set scan cursor: %w", err)
	}

	result.RowsAffected = int64(len(allEvents))
	fetchedAt := time.Now().UTC()
	result.SourceMaxEventTS = &fetchedAt

	v.markReady()
	metrics.ViewRefreshTotal.WithLabelValues(metricSource, "success").Inc()

	if len(allEvents) > 0 {
		v.log.Info("serviceability/permission-events: indexed new events",
			"count", len(allEvents), "scanned_signatures", len(sigs))
	}
	return result, nil
}

// BackfillRefresh runs a full re-scan ignoring the stored cursor. Existing events are
// safely overwritten via ReplacingMergeTree dedup.
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
		v.log.Info("serviceability/permission-events: view is now ready")
	})
}

// fetchNewSignatures returns all program signatures newer than the cursor, newest-first.
func (v *View) fetchNewSignatures(ctx context.Context, cursor ScanCursor) ([]*rpc.TransactionSignature, error) {
	var untilSig solana.Signature
	if cursor.TxSignature != "" {
		var err error
		untilSig, err = solana.SignatureFromBase58(cursor.TxSignature)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor signature %q: %w", cursor.TxSignature, err)
		}
	}

	var allSigs []*rpc.TransactionSignature
	var beforeSig solana.Signature
	for {
		opts := &rpc.GetSignaturesForAddressOpts{Commitment: rpc.CommitmentFinalized}
		if !untilSig.IsZero() {
			opts.Until = untilSig
		}
		if !beforeSig.IsZero() {
			opts.Before = beforeSig
		}

		page, err := v.cfg.RPC.GetSignaturesForAddressWithOpts(ctx, v.cfg.ProgramID, opts)
		if err != nil {
			return nil, fmt.Errorf("get signatures: %w", err)
		}
		allSigs = append(allSigs, page...)
		if len(page) < maxSignaturesPerRequest {
			break
		}
		beforeSig = page[len(page)-1].Signature
	}
	return allSigs, nil
}

// decodeTransaction fetches one transaction and decodes any permission-management
// instructions it contains into audit rows.
func (v *View) decodeTransaction(ctx context.Context, sig *rpc.TransactionSignature) ([]PermissionEventRow, error) {
	maxVersion := uint64(0)
	txResult, err := v.cfg.RPC.GetTransaction(ctx, sig.Signature, &rpc.GetTransactionOpts{
		Encoding:                       solana.EncodingBase64,
		Commitment:                     rpc.CommitmentFinalized,
		MaxSupportedTransactionVersion: &maxVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("get transaction: %w", err)
	}
	if txResult == nil || txResult.Transaction == nil {
		return nil, nil
	}
	tx, err := txResult.Transaction.GetTransaction()
	if err != nil {
		return nil, fmt.Errorf("parse transaction: %w", err)
	}
	if tx == nil || len(tx.Message.AccountKeys) == 0 {
		return nil, nil
	}

	accountKeys := tx.Message.AccountKeys
	feePayer := accountKeys[0].String()

	var blockTime time.Time
	if sig.BlockTime != nil {
		blockTime = sig.BlockTime.Time().UTC()
	}
	success := uint8(1)
	if sig.Err != nil {
		success = 0
	}

	var rows []PermissionEventRow
	for idx, ci := range tx.Message.Instructions {
		if int(ci.ProgramIDIndex) >= len(accountKeys) {
			continue
		}
		if !accountKeys[ci.ProgramIDIndex].Equals(v.cfg.ProgramID) {
			continue
		}

		decoded, ok, err := DecodePermissionInstruction(ci.Data)
		if err != nil {
			v.log.Warn("serviceability/permission-events: malformed permission instruction",
				"signature", sig.Signature.String(), "instruction_index", idx, "error", err)
			continue
		}
		if !ok {
			continue
		}

		// account[0] is the target Permission PDA.
		var permissionPK string
		if len(ci.Accounts) > 0 && int(ci.Accounts[0]) < len(accountKeys) {
			permissionPK = accountKeys[ci.Accounts[0]].String()
		}

		rows = append(rows, PermissionEventRow{
			EventTS:                blockTime,
			TxSignature:            sig.Signature.String(),
			Slot:                   sig.Slot,
			InstructionIndex:       uint16(idx),
			Signer:                 feePayer,
			PermissionPK:           permissionPK,
			TargetPubkey:           decoded.TargetUserPayer,
			EventType:              decoded.EventType,
			PermissionsAdded:       FlagNames(decoded.Added),
			PermissionsRemoved:     FlagNames(decoded.Removed),
			PermissionsAddedMask:   decoded.Added.Hex(),
			PermissionsRemovedMask: decoded.Removed.Hex(),
			Success:                success,
		})
	}

	// Deterministic order within a tx (stable across concurrent refreshes).
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].InstructionIndex < rows[j].InstructionIndex })
	return rows, nil
}
