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
	"github.com/malbeclabs/lake/utils/pkg/logger"
	"golang.org/x/sync/errgroup"
)

const (
	// maxConcurrentFetches limits in-flight getTransaction calls across the whole view
	// via a shared semaphore: accounts drain concurrently, but the RPC never sees more
	// than this many transaction fetches at once.
	maxConcurrentFetches = 10
	// maxSignaturesPerRequest is the Solana RPC page limit.
	maxSignaturesPerRequest = 1000
	// scanChunkSize is how many signatures the program backfill scan and the per-account
	// drain decode and durably write before advancing their cursor. Chunking
	// (oldest-first) bounds how much work is lost if a refresh is cancelled — a
	// backlog that exceeds the Temporal activity timeout resumes from the last
	// completed chunk instead of restarting from scratch.
	scanChunkSize = 200
	// drainBudgetMargin is how much of the refresh context's deadline the per-account
	// drain leaves unspent when deciding to start another chunk: enough to decode,
	// insert, and checkpoint that chunk. When less than this remains, the drain stops
	// and the next refresh continues from the durable cursor.
	drainBudgetMargin = 30 * time.Second
	// permissionAccountType is the serviceability account_type discriminator (first byte
	// of account data) for Permission accounts — AccountType::Permission = 15 in
	// state/permission.rs. Used to enumerate Permission PDAs via getProgramAccounts.
	permissionAccountType byte = 15
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

	// decodeSem bounds in-flight getTransaction calls view-wide (see
	// maxConcurrentFetches); per-chunk errgroup limits alone would multiply
	// by the number of concurrently draining accounts.
	decodeSem chan struct{}

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
	store, err := NewStore(StoreConfig{Logger: cfg.Logger, ClickHouse: cfg.ClickHouse})
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	return &View{
		log:       cfg.Logger,
		cfg:       cfg,
		store:     store,
		decodeSem: make(chan struct{}, maxConcurrentFetches),
		readyCh:   make(chan struct{}),
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

	_, err := v.Refresh(ctx)
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	v.esc.Observe(v.log, "refresh", "serviceability/permission-events: refresh failed", err)
}

// Refresh is the steady-state refresh. Permission-management instructions are sporadic
// among a high volume of other serviceability transactions, so instead of fetching every
// program transaction just to check its variant byte, it watches each Permission account
// directly:
//
//  1. discover the current Permission PDAs via getProgramAccounts (one cheap call), then
//  2. per PDA, getSignaturesForAddress since that account's high-water mark and decode only
//     those transactions.
//
// This fetches only transactions touching Permission PDAs rather than the whole program's
// history. Note a Permission PDA is not only referenced by permission-management
// instructions: other serviceability instructions (e.g. multicast allowlist ops) reference
// it too and decode to zero audit rows — which is why resume progress is tracked in a
// durable per-account cursor rather than derived from indexed rows. New grants are
// picked up once the PDA is discovered (its create tx references the account). The only gap
// is an account created and deleted between two discovery polls (a deleted account no longer
// appears in getProgramAccounts) — BackfillRefresh's full-history program scan is the
// completeness backstop for that.
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

	// Discover the Permission accounts to watch.
	pdas, err := v.discoverPermissionAccounts(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("discover permission accounts: %w", err)
	}
	if len(pdas) == 0 {
		v.markReady()
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "success").Inc()
		return result, nil
	}

	// Resume each account from its durable cursor, falling back to the fact-derived
	// high-water mark for accounts indexed before the cursor table existed. When both
	// exist take the newer: the program-wide backfill can insert rows beyond the cursor.
	hwms, err := v.store.GetHighWaterMarks(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("get high water marks: %w", err)
	}
	cursors, err := v.store.GetAccountCursors(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("get account cursors: %w", err)
	}
	resume := func(pk string) HighWaterMark {
		cur, hasCur := cursors[pk]
		if hwm, ok := hwms[pk]; ok && (!hasCur || hwm.Slot > cur.Slot) {
			return hwm
		}
		return cur
	}

	// Drain each account in durable oldest-first chunks (see drainAccount). A refresh cut
	// short by the Temporal activity timeout keeps every committed chunk, so even a single
	// account whose backlog exceeds one timeout window makes forward progress each cycle
	// instead of re-fetching the same work forever: the poison-loop failure mode.
	var (
		mu            sync.Mutex
		totalInserted int64
		totalPending  int
	)
	var g errgroup.Group
	g.SetLimit(maxConcurrentFetches)
	for _, pda := range pdas {
		g.Go(func() error {
			inserted, pending, drainErr := v.drainAccount(ctx, pda, resume(pda.String()))
			mu.Lock()
			totalInserted += inserted
			totalPending += pending
			mu.Unlock()
			if drainErr != nil {
				v.log.Warn("serviceability/permission-events: failed to drain account",
					"permission_pk", pda.String(), "error", drainErr)
				return fmt.Errorf("drain account %s: %w", pda.String(), drainErr)
			}
			return nil
		})
	}
	// Committed chunks are durable regardless of the refresh outcome — report them, and
	// let freshness advance on partial progress (overstates by at most one cycle).
	err = g.Wait()
	result.RowsAffected = totalInserted
	if totalInserted > 0 || err == nil {
		fetchedAt := time.Now().UTC()
		result.SourceMaxEventTS = &fetchedAt
	}
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("refresh permission accounts: %w", err)
	}

	v.markReady()
	// "partial" = every account made progress but at least one stopped at the refresh
	// budget with backlog left. A drain that stays partial forever is a stalled drain —
	// the metric is what keeps that visible, since each cycle reports success.
	if totalPending > 0 {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "partial").Inc()
		v.log.Info("serviceability/permission-events: drain stopped at refresh budget",
			"inserted", totalInserted, "pending_signatures", totalPending, "watched_accounts", len(pdas))
	} else {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "success").Inc()
	}

	if totalInserted > 0 {
		v.log.Info("serviceability/permission-events: indexed new events",
			"count", totalInserted, "watched_accounts", len(pdas))
	}
	return result, nil
}

// BackfillRefresh scans the serviceability program's entire transaction history for
// permission instructions, ignoring the per-account cursors. This is the completeness
// path: it catches historical events and permission accounts that were created and later
// deleted (which the steady-state per-account watch can't discover). Existing rows are
// safely overwritten via ReplacingMergeTree dedup.
//
// The scan is processed oldest-first in chunks, advancing a program scan cursor after each
// chunk is durably written. So (a) a cancelled/timed-out backfill resumes from the last
// completed chunk rather than restarting, and (b) a transient fetch/decode failure fails
// the run without advancing past the un-indexed signatures.
func (v *View) BackfillRefresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	var result ingestionlog.RefreshResult

	refreshStart := time.Now()
	v.log.Debug("serviceability/permission-events: backfill started")
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("serviceability/permission-events: backfill completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues(metricSource).Observe(duration.Seconds())
	}()

	programPK := v.cfg.ProgramID.String()

	cursor, err := v.store.GetScanCursor(ctx, programPK)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("get scan cursor: %w", err)
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

// discoverPermissionAccounts returns the current Permission PDAs owned by the serviceability
// program, via a single getProgramAccounts filtered on the account_type discriminator. The
// data slice is zero-length: we only need the pubkeys, not the account contents.
func (v *View) discoverPermissionAccounts(ctx context.Context) ([]solana.PublicKey, error) {
	zero := uint64(0)
	res, err := v.cfg.RPC.GetProgramAccountsWithOpts(ctx, v.cfg.ProgramID, &rpc.GetProgramAccountsOpts{
		Commitment: rpc.CommitmentFinalized,
		Encoding:   solana.EncodingBase64,
		DataSlice:  &rpc.DataSlice{Offset: &zero, Length: &zero},
		Filters: []rpc.RPCFilter{{
			Memcmp: &rpc.RPCFilterMemcmp{Offset: 0, Bytes: solana.Base58{permissionAccountType}},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("get program accounts: %w", err)
	}
	pdas := make([]solana.PublicKey, 0, len(res))
	for _, acc := range res {
		if acc != nil {
			pdas = append(pdas, acc.Pubkey)
		}
	}
	return pdas, nil
}

// drainAccount incrementally indexes one Permission PDA's transaction history newer than
// resume, oldest-first in chunks. Each fully-decoded chunk is persisted and the account's
// durable cursor advanced before the next chunk starts, so a refresh cut short by
// cancellation or the deadline budget re-does at most one chunk of work — an account whose
// backlog exceeds one refresh window drains across cycles instead of poison-looping.
// Returns the audit rows inserted (meaningful even alongside an error) and how many
// signatures were left unprocessed.
//
// Signature pagination is O(remaining backlog) each refresh: getSignaturesForAddress pages
// newest-first, so reaching the oldest unprocessed chunk requires paging everything newer
// than the cursor. Pagination checks the same deadline budget per page and fails fast when
// it can't finish in time — a pagination-starved account is loud, never a silent no-op.
func (v *View) drainAccount(ctx context.Context, pda solana.PublicKey, resume HighWaterMark) (inserted int64, pending int, err error) {
	sigs, err := v.fetchAccountSignatures(ctx, pda, resume)
	if err != nil {
		return 0, 0, err
	}
	if len(sigs) == 0 {
		return 0, 0, nil
	}

	// Reverse to oldest-first so the cursor advances monotonically as chunks commit; a
	// partial drain then leaves the cursor at the newest fully-processed signature.
	for i, j := 0, len(sigs)-1; i < j; i, j = i+1, j-1 {
		sigs[i], sigs[j] = sigs[j], sigs[i]
	}

	for start := 0; start < len(sigs); start += scanChunkSize {
		if err := ctx.Err(); err != nil {
			return inserted, len(sigs) - start, err
		}
		// Stop when too little of the deadline remains to decode, insert, and checkpoint
		// another chunk. Progress so far is committed, so stopping with progress is a
		// success — the next refresh continues from the cursor. Stopping with none is an
		// error so a persistently starved account escalates instead of no-op succeeding.
		if deadline, ok := ctx.Deadline(); ok && deadline.Sub(v.cfg.Clock.Now()) < drainBudgetMargin {
			if start == 0 {
				return 0, len(sigs), fmt.Errorf("refresh budget exhausted before first chunk (%d signatures pending)", len(sigs))
			}
			v.log.Info("serviceability/permission-events: account drain stopping at refresh budget",
				"permission_pk", pda.String(), "processed", start, "pending", len(sigs)-start)
			return inserted, len(sigs) - start, nil
		}

		chunk := sigs[start:min(start+scanChunkSize, len(sigs))]
		events, decodeErr := v.decodeChunk(ctx, chunk)
		if decodeErr != nil {
			// Decode order within a chunk is not cursor order: persisting a partially
			// decoded chunk could advance the fact-derived high-water mark past
			// never-decoded signatures — a silent, permanent gap. Drop the chunk's
			// events; the cursor stays put and the chunk is re-fetched next refresh.
			return inserted, len(sigs) - start, fmt.Errorf("decode transactions: %w", decodeErr)
		}
		if err := v.store.InsertEvents(ctx, events); err != nil {
			return inserted, len(sigs) - start, fmt.Errorf("insert permission events: %w", err)
		}
		inserted += int64(len(events))

		// Chunk is oldest-first, so its last element is the newest processed signature.
		// The cursor must advance even when the chunk produced no rows: non-permission
		// instructions also reference Permission PDAs, and a backlog of them would
		// otherwise be re-fetched forever.
		newest := chunk[len(chunk)-1]
		cur := HighWaterMark{TxSignature: newest.Signature.String(), Slot: newest.Slot}
		if err := v.store.SetAccountCursor(ctx, pda.String(), cur); err != nil {
			return inserted, len(sigs) - start, fmt.Errorf("set account cursor: %w", err)
		}
	}
	return inserted, 0, nil
}

// fetchAccountSignatures returns all signatures touching pda newer than resume, newest-first.
func (v *View) fetchAccountSignatures(ctx context.Context, pda solana.PublicKey, resume HighWaterMark) ([]*rpc.TransactionSignature, error) {
	var untilSig solana.Signature
	if resume.TxSignature != "" {
		var err error
		untilSig, err = solana.SignatureFromBase58(resume.TxSignature)
		if err != nil {
			return nil, fmt.Errorf("invalid resume cursor signature %q: %w", resume.TxSignature, err)
		}
	}

	// Paginate backward (via Before) until an empty page — robust to gateways that cap
	// pages below the requested limit (a short page would otherwise end pagination early).
	limit := maxSignaturesPerRequest
	var allSigs []*rpc.TransactionSignature
	var beforeSig solana.Signature
	for {
		// Pagination cost is O(remaining backlog) and yields nothing processable until it
		// reaches the cursor (pages are newest-first, chunks drain oldest-first), so a
		// backlog whose pagination alone fills the window must fail fast here rather than
		// burn the whole activity before the zero-progress check.
		if deadline, ok := ctx.Deadline(); ok && deadline.Sub(v.cfg.Clock.Now()) < drainBudgetMargin {
			return nil, fmt.Errorf("refresh budget exhausted during signature pagination (%d signatures paged)", len(allSigs))
		}
		opts := &rpc.GetSignaturesForAddressOpts{Commitment: rpc.CommitmentFinalized, Limit: &limit}
		if !untilSig.IsZero() {
			opts.Until = untilSig
		}
		if !beforeSig.IsZero() {
			opts.Before = beforeSig
		}
		page, err := v.cfg.RPC.GetSignaturesForAddressWithOpts(ctx, pda, opts)
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

// decodeChunk fetches and decodes a batch of transactions concurrently. It returns the
// events that decoded cleanly along with the first fetch/decode error encountered, if any.
// All transactions are attempted even when one fails (no early cancellation) so a single
// transient error doesn't discard the rest of the chunk's successfully decoded events.
//
// A finalized signature whose transaction the RPC cannot serve (rpc.ErrNotFound — pruned
// or inconsistent upstream history) is skipped with a warning rather than failing the
// chunk: retrying can never recover it, and failing would wedge the drain (and the
// backfill, which advances past skipped signatures identically) at that point forever.
// A skip is therefore a permanently missing audit row unless re-backfilled against an
// archival node — the PermissionEventsSkippedTx metric is what keeps that loss visible.
func (v *View) decodeChunk(ctx context.Context, sigs []*rpc.TransactionSignature) ([]PermissionEventRow, error) {
	var (
		mu        sync.Mutex
		allEvents []PermissionEventRow
	)
	var g errgroup.Group
	g.SetLimit(maxConcurrentFetches)
	for _, sig := range sigs {
		g.Go(func() error {
			// The view-wide semaphore bounds total RPC pressure: without it, chunks
			// draining in parallel across accounts would each fetch at the group limit.
			select {
			case v.decodeSem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-v.decodeSem }()

			events, err := v.decodeTransaction(ctx, sig)
			if err != nil {
				if errors.Is(err, rpc.ErrNotFound) {
					metrics.PermissionEventsSkippedTx.Inc()
					v.log.Warn("serviceability/permission-events: transaction unretrievable upstream, skipping",
						"signature", sig.Signature.String(), "slot", sig.Slot, "error", err)
					return nil
				}
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
