package permissionevents

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
	// maxConcurrentFetches limits parallel getTransaction calls per refresh.
	maxConcurrentFetches = 10
	// maxSignaturesPerRequest is the Solana RPC page limit.
	maxSignaturesPerRequest = 1000
	// scanChunkSize is how many signatures are fetched, decoded, and durably written
	// before the scan cursor is advanced. Chunking (oldest-first) bounds how much work
	// is lost if the refresh is cancelled — e.g. a first-run full-history sweep that
	// exceeds the Temporal activity timeout resumes from the last completed chunk instead
	// of restarting from scratch.
	scanChunkSize = 200
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
	return v.refresh(ctx, v.cfg.SkipHighWaterMarks)
}

// BackfillRefresh runs a full re-scan ignoring the stored cursor. Existing events are
// safely overwritten via ReplacingMergeTree dedup.
func (v *View) BackfillRefresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	return v.refresh(ctx, true)
}

// refresh scans the program's transaction history and writes any permission events.
// ignoreCursor forces a full re-scan (backfill); otherwise it resumes from the stored
// scan cursor.
//
// The scan is processed oldest-first in chunks, and the cursor is advanced only after
// each chunk is durably written. This means (a) a cancelled/timed-out refresh resumes
// from the last completed chunk rather than restarting, and (b) a transient fetch/decode
// failure fails the refresh without advancing past the un-indexed signatures, so the
// events are retried on the next run rather than silently dropped from the audit trail.
func (v *View) refresh(ctx context.Context, ignoreCursor bool) (ingestionlog.RefreshResult, error) {
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
	if !ignoreCursor {
		var err error
		cursor, err = v.store.GetScanCursor(ctx, programPK)
		if err != nil {
			metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
			return result, fmt.Errorf("get scan cursor: %w", err)
		}
	}

	// Fetch all new signatures for the program, paginating backward to the cursor.
	// Results are newest-first.
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

	// Reverse to oldest-first so the cursor advances monotonically as we checkpoint each
	// chunk; a partial scan then leaves the cursor at the newest fully-indexed signature.
	for i, j := 0, len(sigs)-1; i < j; i, j = i+1, j-1 {
		sigs[i], sigs[j] = sigs[j], sigs[i]
	}

	var totalEvents int64
	for start := 0; start < len(sigs); start += scanChunkSize {
		end := start + scanChunkSize
		if end > len(sigs) {
			end = len(sigs)
		}
		chunk := sigs[start:end]

		events, decodeErr := v.decodeChunk(ctx, chunk)

		// Persist whatever decoded cleanly (idempotent via ReplacingMergeTree) before
		// deciding whether to advance the cursor.
		if err := v.store.InsertEvents(ctx, events); err != nil {
			metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
			return result, fmt.Errorf("insert permission events: %w", err)
		}
		totalEvents += int64(len(events))

		// A transient fetch/decode error must not advance the cursor past the affected
		// signatures — fail the refresh so Temporal retries and the events are re-scanned.
		if decodeErr != nil {
			metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
			return result, fmt.Errorf("decode transactions: %w", decodeErr)
		}

		// Chunk is oldest-first, so its last element is the newest signature indexed so far.
		newest := chunk[len(chunk)-1]
		newCursor := ScanCursor{TxSignature: newest.Signature.String(), Slot: newest.Slot}
		if err := v.store.SetScanCursor(ctx, programPK, newCursor); err != nil {
			metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
			return result, fmt.Errorf("set scan cursor: %w", err)
		}
	}

	result.RowsAffected = totalEvents
	fetchedAt := time.Now().UTC()
	result.SourceMaxEventTS = &fetchedAt

	v.markReady()
	metrics.ViewRefreshTotal.WithLabelValues(metricSource, "success").Inc()

	if totalEvents > 0 {
		v.log.Info("serviceability/permission-events: indexed new events",
			"count", totalEvents, "scanned_signatures", len(sigs))
	}
	return result, nil
}

// decodeChunk fetches and decodes a batch of transactions concurrently. It returns the
// events that decoded cleanly along with the first fetch/decode error encountered, if any.
// All transactions are attempted even when one fails (no early cancellation) so a single
// transient error doesn't discard the rest of the chunk's successfully decoded events.
func (v *View) decodeChunk(ctx context.Context, sigs []*rpc.TransactionSignature) ([]PermissionEventRow, error) {
	var (
		mu        sync.Mutex
		allEvents []PermissionEventRow
	)
	var g errgroup.Group
	g.SetLimit(maxConcurrentFetches)
	for _, sig := range sigs {
		g.Go(func() error {
			events, err := v.decodeTransaction(ctx, sig)
			if err != nil {
				v.log.Warn("serviceability/permission-events: failed to decode transaction",
					"signature", sig.Signature.String(), "error", err)
				return err
			}
			if len(events) > 0 {
				mu.Lock()
				allEvents = append(allEvents, events...)
				mu.Unlock()
			}
			return nil
		})
	}
	err := g.Wait()
	return allEvents, err
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

	// Paginate backward (via Before) until an empty page. We terminate only on an empty
	// page rather than on a short page: a provider/gateway may cap pages below our
	// requested limit, and stopping on the first short page would skip older history and
	// advance the cursor past it permanently.
	limit := maxSignaturesPerRequest
	var allSigs []*rpc.TransactionSignature
	var beforeSig solana.Signature
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

		page, err := v.cfg.RPC.GetSignaturesForAddressWithOpts(ctx, v.cfg.ProgramID, opts)
		if err != nil {
			return nil, fmt.Errorf("get signatures: %w", err)
		}
		if len(page) == 0 {
			break
		}
		allSigs = append(allSigs, page...)
		beforeSig = page[len(page)-1].Signature
	}
	return allSigs, nil
}

// decodeTransaction fetches one transaction and decodes any permission-management
// instructions it contains into audit rows. Both top-level and CPI (inner) instructions
// are scanned, since permission ops invoked via a multisig/governance program appear only
// in the transaction's inner instructions.
//
// Signer is the transaction fee-payer (accountKeys[0]). For the common case — an admin
// signing and paying for their own permission tx — this is the acting authority. When a
// relayer or multisig pays fees, the fee-payer is not the acting admin; the true signer
// set is not recorded here.
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

	// Resolve the full account key list. For v0 transactions the instruction account
	// indices also reference address-lookup-table entries, which the RPC returns in
	// Meta.LoadedAddresses (writable first, then read-only). Without merging them, a
	// looked-up Permission PDA would resolve to an empty pubkey.
	accountKeys := tx.Message.AccountKeys
	if txResult.Meta != nil {
		accountKeys = append(accountKeys, txResult.Meta.LoadedAddresses.Writable...)
		accountKeys = append(accountKeys, txResult.Meta.LoadedAddresses.ReadOnly...)
	}
	feePayer := accountKeys[0].String()

	// Prefer the signature's block time; fall back to the transaction result's block time
	// (either may be nil on some RPC states). event_ts may still be zero in the rare case
	// both are absent — the API sorts by slot first so such rows remain visible.
	var blockTime time.Time
	if sig.BlockTime != nil {
		blockTime = sig.BlockTime.Time().UTC()
	} else if txResult.BlockTime != nil {
		blockTime = txResult.BlockTime.Time().UTC()
	}
	success := uint8(1)
	if sig.Err != nil {
		success = 0
	}

	var rows []PermissionEventRow
	// seq assigns a stable, unique index to each permission row within the tx (top-level
	// instructions in order, then inner instructions). It backs the (permission_pk, slot,
	// tx_signature, instruction_index) dedup key, so two permission instructions in one tx
	// stay distinct rows.
	// decodeInto decodes a single (top-level or inner) instruction. Top-level and inner
	// instructions are distinct Go types (solana.CompiledInstruction vs
	// rpc.CompiledInstruction) with the same shape, so it takes the fields directly.
	var seq uint16
	decodeInto := func(programIDIndex uint16, accounts []uint16, data []byte) {
		if int(programIDIndex) >= len(accountKeys) {
			return
		}
		if !accountKeys[programIDIndex].Equals(v.cfg.ProgramID) {
			return
		}

		decoded, ok, err := DecodePermissionInstruction(data)
		if err != nil {
			v.log.Warn("serviceability/permission-events: malformed permission instruction",
				"signature", sig.Signature.String(), "instruction_index", seq, "error", err)
			return
		}
		if !ok {
			return
		}

		// account[0] is the target Permission PDA.
		var permissionPK string
		if len(accounts) > 0 && int(accounts[0]) < len(accountKeys) {
			permissionPK = accountKeys[accounts[0]].String()
		}

		rows = append(rows, PermissionEventRow{
			EventTS:                blockTime,
			TxSignature:            sig.Signature.String(),
			Slot:                   sig.Slot,
			InstructionIndex:       seq,
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
		seq++
	}

	for _, ci := range tx.Message.Instructions {
		decodeInto(ci.ProgramIDIndex, ci.Accounts, ci.Data)
	}
	if txResult.Meta != nil {
		for _, inner := range txResult.Meta.InnerInstructions {
			for _, ci := range inner.Instructions {
				decodeInto(ci.ProgramIDIndex, ci.Accounts, ci.Data)
			}
		}
	}

	return rows, nil
}
