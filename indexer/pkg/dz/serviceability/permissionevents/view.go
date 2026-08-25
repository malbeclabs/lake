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
	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/malbeclabs/lake/utils/pkg/logger"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	// maxConcurrentFetches limits in-flight getTransaction calls across the whole view
	// via a shared semaphore: accounts drain concurrently, but the RPC never sees more
	// than this many transaction fetches at once.
	maxConcurrentFetches = 10
	// defaultFetchesPerSecond paces getTransaction across the whole view. A
	// concurrency cap alone does not bound request *rate*: 10 in-flight calls
	// against a fast endpoint is an unbounded rate, which is how the drain earned
	// "Too many requests for a specific RPC call" (a per-method provider limit) on
	// every cycle for hours. Pacing keeps us under the limit instead of discovering
	// it, so the retry budget is spent on real blips rather than on throttles we
	// caused ourselves.
	//
	// This is a calibration knob, not a derived constant: the provider publishes no
	// per-method number and it differs per endpoint and plan. 25/s is chosen to sit
	// well under observed limits while still draining a chunk (scanChunkSize=200) in
	// ~8s, so the ~4.5min usable drain window fits ~30 chunks — enough to catch up on
	// a multi-hour backlog in a few cycles. Override via ViewConfig.FetchesPerSecond
	// if an endpoint proves tighter or more generous.
	defaultFetchesPerSecond = 25
	// maxSignaturesPerRequest is the Solana RPC page limit.
	maxSignaturesPerRequest = 1000
	// scanChunkSize is how many signatures the program backfill scan and the per-account
	// drain decode and durably write before advancing their cursor. Chunking
	// (oldest-first) bounds how much work is lost if a refresh is cancelled — a
	// backlog that exceeds the Temporal activity timeout resumes from the last
	// completed chunk instead of restarting from scratch.
	scanChunkSize = 200
	// drainCommitReserve is the fixed part of a chunk's deadline requirement: enough
	// to insert the decoded rows and checkpoint the cursor once fetching is done. The
	// fetch time itself is not fixed — it follows the configured fetch rate — so the
	// full requirement is computed per chunk by View.chunkBudget.
	drainCommitReserve = 30 * time.Second
	// notFoundSkipSlotLag is how far below the newest fetched signature a not-found
	// transaction must sit before it is skipped as pruned history. A getTransaction
	// null near the tip is usually a load-balanced backend lagging finalization — a
	// transient, recoverable miss that must fail the chunk (the un-advanced cursor
	// retries it next refresh) rather than permanently skip an audit row. One this
	// many slots back (~minutes of ledger time) is genuinely unretrievable and would
	// wedge the drain forever if it kept failing.
	notFoundSkipSlotLag = 300
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

	// FetchesPerSecond caps the view-wide getTransaction rate. Zero means
	// defaultFetchesPerSecond.
	FetchesPerSecond float64
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
	// A negative rate is not a slower drain, it is a limiter that refuses every
	// reservation. Zero is the documented "use the package default".
	if cfg.FetchesPerSecond < 0 {
		return errors.New("fetches per second must not be negative")
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

	// fetchLimiter bounds the view-wide getTransaction *rate*, which decodeSem
	// cannot (see defaultFetchesPerSecond).
	fetchLimiter *rate.Limiter

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
	fetchesPerSecond := cfg.FetchesPerSecond
	if fetchesPerSecond <= 0 {
		fetchesPerSecond = defaultFetchesPerSecond
	}
	return &View{
		log:   cfg.Logger,
		cfg:   cfg,
		store: store,
		// Burst equals the concurrency cap so a chunk's fan-out fills the semaphore
		// immediately and steady state is then governed by the rate, not the burst.
		fetchLimiter: rate.NewLimiter(rate.Limit(fetchesPerSecond), maxConcurrentFetches),
		decodeSem:    make(chan struct{}, maxConcurrentFetches),
		readyCh:      make(chan struct{}),
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

	// Resume each account from its durable cursor; the fact-derived high-water mark is
	// only the fallback for accounts indexed before the cursor table existed. The cursor
	// always wins once present — comparing slots would let stale fact rows (e.g. rows
	// surviving a ledger reset, whose old slots exceed any new-ledger slot forever)
	// permanently shadow the cursor and force a full re-page + re-decode every cycle.
	// The bounded cost: after a program-wide backfill inserts rows beyond the cursor,
	// one drain pass re-decodes that span (idempotent) and the cursor catches up.
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
		if cur, ok := cursors[pk]; ok {
			return cur
		}
		return hwms[pk]
	}

	// Drain each account in durable oldest-first chunks (see drainAccount). A refresh cut
	// short by the Temporal activity timeout keeps every committed chunk, so even a single
	// account whose backlog exceeds one timeout window makes forward progress each cycle
	// instead of re-fetching the same work forever: the poison-loop failure mode.
	var (
		mu            sync.Mutex
		totalInserted int64
		totalPending  int
		frontier      time.Time // oldest committedTip among accounts left with a backlog
	)
	var g errgroup.Group
	g.SetLimit(maxConcurrentFetches)
	for _, pda := range pdas {
		g.Go(func() error {
			res, drainErr := v.drainAccount(ctx, pda, resume(pda.String()))
			mu.Lock()
			totalInserted += res.inserted
			totalPending += res.pending
			if res.pending > 0 && !res.committedTip.IsZero() &&
				(frontier.IsZero() || res.committedTip.Before(frontier)) {
				frontier = res.committedTip
			}
			mu.Unlock()
			if drainErr != nil {
				v.log.Warn("serviceability/permission-events: failed to drain account",
					"permission_pk", pda.String(), "error", drainErr)
				return fmt.Errorf("drain account %s: %w", pda.String(), drainErr)
			}
			return nil
		})
	}
	// Committed chunks are durable regardless of the refresh outcome — report them.
	// Freshness must not overstate during a multi-cycle drain: with backlog pending,
	// the honest frontier is the oldest committed chunk tip across backlogged accounts
	// (their newest events stay unindexed until the drain converges), not "now".
	err = g.Wait()
	result.RowsAffected = totalInserted
	// A cycle that banked committed chunks and stopped is not a success: the staleness
	// alert asks for the last successful run's finished_at, so calling this "success"
	// resets that clock while the data it covers stays behind. status="partial" keeps
	// the clock running until the drain actually converges.
	result.Partial = totalPending > 0
	switch {
	case err == nil && totalPending == 0:
		fetchedAt := time.Now().UTC()
		result.SourceMaxEventTS = &fetchedAt
	case !frontier.IsZero():
		result.SourceMaxEventTS = &frontier
	}
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues(metricSource, "error").Inc()
		return result, fmt.Errorf("refresh permission accounts: %w", err)
	}

	v.markReady()
	// "partial" = every account made progress but at least one stopped at the refresh
	// budget with backlog left. A drain that stays partial forever is a stalled drain,
	// and it stays visible because the run is recorded with status="partial" (see
	// result.Partial above), which never advances the staleness clock. The metric is the
	// same signal in Prometheus.
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

	// sigs is newest-first here, so sigs[0] is the scan tip; see drainAccount for the
	// not-found age-gate rationale.
	var skipNotFoundBelow uint64
	if tip := sigs[0].Slot; tip > notFoundSkipSlotLag {
		skipNotFoundBelow = tip - notFoundSkipSlotLag
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

		events, decodeErr := v.decodeChunk(ctx, chunk, skipNotFoundBelow)

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

// drainResult reports one account's drain outcome.
type drainResult struct {
	inserted int64
	// pending is how many fetched signatures were left unprocessed (budget stop,
	// cancellation, or a failed chunk).
	pending int
	// committedTip is the block time of the newest committed chunk's tail — the
	// account's honest indexing frontier when pending > 0. Zero when no chunk
	// committed this pass or the tail carried no block time.
	committedTip time.Time
}

// drainAccount incrementally indexes one Permission PDA's transaction history newer than
// resume, oldest-first in chunks. Each fully-decoded chunk is persisted and the account's
// durable cursor advanced before the next chunk starts, so a refresh cut short by
// cancellation or the deadline budget re-does at most one chunk of work — an account whose
// backlog exceeds one refresh window drains across cycles instead of poison-looping.
// The returned result is meaningful even alongside an error.
//
// Signature pagination is O(remaining backlog) each refresh: getSignaturesForAddress pages
// newest-first, so reaching the oldest unprocessed chunk requires paging everything newer
// than the cursor. Pagination checks the same deadline budget per page and fails fast when
// it can't finish in time — a pagination-starved account is loud, never a silent no-op.
func (v *View) drainAccount(ctx context.Context, pda solana.PublicKey, resume HighWaterMark) (drainResult, error) {
	var res drainResult
	sigs, err := v.fetchAccountSignatures(ctx, pda, resume)
	if err != nil {
		return res, err
	}
	if len(sigs) == 0 {
		return res, nil
	}

	// sigs is newest-first here, so sigs[0] is the account's fetch tip: not-founds more
	// than notFoundSkipSlotLag below it are pruned history, safe to skip.
	var skipNotFoundBelow uint64
	if tip := sigs[0].Slot; tip > notFoundSkipSlotLag {
		skipNotFoundBelow = tip - notFoundSkipSlotLag
	}

	// Reverse to oldest-first so the cursor advances monotonically as chunks commit; a
	// partial drain then leaves the cursor at the newest fully-processed signature.
	for i, j := 0, len(sigs)-1; i < j; i, j = i+1, j-1 {
		sigs[i], sigs[j] = sigs[j], sigs[i]
	}

	for start := 0; start < len(sigs); start += scanChunkSize {
		if err := ctx.Err(); err != nil {
			res.pending = len(sigs) - start
			return res, err
		}
		chunk := sigs[start:min(start+scanChunkSize, len(sigs))]

		// Stop when too little of the deadline remains to decode, insert, and checkpoint
		// this chunk. Progress so far is committed, so stopping with progress returns no
		// error and the refresh is recorded as partial; the next cycle continues from the
		// cursor. Stopping with none is an error so a persistently starved account
		// escalates instead of no-op succeeding.
		//
		// The requirement is derived from the chunk rather than fixed, because pacing put
		// the fetch rate in charge of how long a chunk takes (see chunkBudget). A fixed
		// reserve sized for unpaced fetches would let the drain start a chunk it cannot
		// finish: rate.Limiter.Wait refuses a reservation that would outlast the deadline
		// rather than blocking, returning "would exceed context deadline", which
		// classifies transient on the "context deadline" substring and so takes the
		// stop-with-progress path below, having fetched a chunk's worth of transactions
		// that commit nothing.
		if deadline, ok := ctx.Deadline(); ok && deadline.Sub(v.cfg.Clock.Now()) < v.chunkBudget(len(chunk)) {
			res.pending = len(sigs) - start
			if start == 0 {
				// Name the rate as well as the requirement. This branch is also where a
				// misconfigured rate lands, and the two causes read identically without it:
				// a genuinely starved backlog and a rate too low to move one chunk in the
				// window. Below roughly 0.74/s no chunk fits a 300s activity at all.
				return res, fmt.Errorf("refresh budget exhausted before first chunk (%d signatures pending, %s needed at %g fetches/second)",
					len(sigs), v.chunkBudget(len(chunk)), float64(v.fetchLimiter.Limit()))
			}
			v.log.Info("serviceability/permission-events: account drain stopping at refresh budget",
				"permission_pk", pda.String(), "processed", start, "pending", res.pending)
			return res, nil
		}

		events, decodeErr := v.decodeChunk(ctx, chunk, skipNotFoundBelow)
		if decodeErr != nil {
			// Decode order within a chunk is not cursor order: persisting a partially
			// decoded chunk could advance the fact-derived high-water mark past
			// never-decoded signatures — a silent, permanent gap. Drop the chunk's
			// events; the cursor stays put and the chunk is re-fetched next refresh.
			res.pending = len(sigs) - start

			// A transient upstream failure (provider throttle, connection blip) is a
			// budget exhaustion, not a fault: the endpoint will serve this same chunk
			// later. Treat it like the deadline stop above — committed chunks are
			// durable, so stopping with progress is a success and the next refresh
			// resumes from the cursor. Failing the whole cycle instead is what made a
			// throttled account unable to catch up: it committed one chunk per cycle
			// and was recorded as an error every time, so the drain fell permanently
			// behind while the retry-exhausted 429 read as a hard failure.
			//
			// Zero progress still errors (below): a persistently throttled account must
			// escalate rather than no-op succeed.
			if start > 0 && dberror.IsTransient(decodeErr) {
				v.log.Warn("serviceability/permission-events: account drain stopping on transient upstream failure",
					"permission_pk", pda.String(), "processed", start, "pending", res.pending, "error", decodeErr)
				return res, nil
			}
			return res, fmt.Errorf("decode transactions: %w", decodeErr)
		}
		if err := v.store.InsertEvents(ctx, events); err != nil {
			res.pending = len(sigs) - start
			return res, fmt.Errorf("insert permission events: %w", err)
		}
		res.inserted += int64(len(events))

		// Chunk is oldest-first, so its last element is the newest processed signature.
		// The cursor must advance even when the chunk produced no rows: non-permission
		// instructions also reference Permission PDAs, and a backlog of them would
		// otherwise be re-fetched forever.
		newest := chunk[len(chunk)-1]
		cur := HighWaterMark{TxSignature: newest.Signature.String(), Slot: newest.Slot}
		if err := v.store.SetAccountCursor(ctx, pda.String(), cur); err != nil {
			res.pending = len(sigs) - start
			return res, fmt.Errorf("set account cursor: %w", err)
		}
		// Take the newest block time the chunk actually carries, not only the last
		// element's. A partial cycle whose committedTip stays zero reports no freshness
		// value at all — Refresh's switch leaves SourceMaxEventTS nil — so the run lands
		// as a clean success with nothing to show how far behind it is. Falling back
		// through the chunk closes that for every case except a chunk where no signature
		// is dated, which the RPC only produces for states it cannot serve a block time
		// for at all.
		for i := len(chunk) - 1; i >= 0; i-- {
			if chunk[i].BlockTime != nil {
				res.committedTip = chunk[i].BlockTime.Time().UTC()
				break
			}
		}
	}
	return res, nil
}

// chunkBudget is how much of the refresh deadline a chunk of n signatures needs:
// the time pacing alone will take to fetch them, plus drainCommitReserve for the
// insert and checkpoint that follow. Both drain phases test this one number, so
// pagination never hands the chunk loop a backlog the chunk loop then refuses.
//
// The fetch term is the uncontended cost. The limiter is view-wide, so a chunk drained
// alongside others finishes slower than this — 200 signatures at 25/s is 8s alone and
// up to 80s against nine other draining accounts. Reserving for that worst case
// instead is what an earlier version did, and it costs more than it buys: it rejects
// every cycle holding between 8s and 80s of deadline, which is most of them, and it
// puts the whole activity window out of reach below about 7.4/s. What it buys is only
// avoiding one chunk of wasted fetches, because an overrun is no longer silent — the
// limiter refuses a reservation that would outlast the deadline, the chunk's rows are
// dropped uncommitted, and the cycle reports partial (with progress) or errors (with
// none). Contention also varies during a chunk, so no static factor is right; the
// uncontended cost is at least the honest lower bound with a visible failure.
func (v *View) chunkBudget(n int) time.Duration {
	perSecond := float64(v.fetchLimiter.Limit())
	if perSecond <= 0 {
		return drainCommitReserve
	}
	fetch := time.Duration(float64(n) / perSecond * float64(time.Second))
	return fetch + drainCommitReserve
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

		// Pagination costs O(remaining backlog) and yields nothing processable until it
		// reaches the cursor: pages come newest-first and chunks drain oldest-first. A
		// truncated page set is not usable either, because oldest-first over the newest
		// N signatures would checkpoint the cursor past the older ones it never saw. So
		// pagination either completes or fails, and it fails as soon as the deadline can
		// no longer fit a chunk of the backlog already discovered.
		//
		// The reserve is the same chunkBudget the chunk loop tests, over the same count.
		// The two phases agreeing on one number is what stops a cycle from paging an
		// entire backlog and then erroring with no chunk started.
		//
		// The check sits after the first page, not before: an account with nothing new
		// costs one request and succeeds no matter how little deadline is left.
		reserve := v.chunkBudget(min(len(allSigs), scanChunkSize))
		if deadline, ok := ctx.Deadline(); ok && deadline.Sub(v.cfg.Clock.Now()) < reserve {
			return nil, fmt.Errorf("refresh budget exhausted during signature pagination (%d signatures paged, %s needed to drain a chunk of them at %g fetches/second)",
				len(allSigs), reserve, float64(v.fetchLimiter.Limit()))
		}
	}
	return allSigs, nil
}

// decodeChunk fetches and decodes a batch of transactions concurrently. It returns the
// events that decoded cleanly along with the first fetch/decode error encountered, if any.
// All transactions are attempted even when one fails (no early cancellation) so a single
// transient error doesn't discard the rest of the chunk's successfully decoded events.
//
// A finalized signature whose transaction the RPC cannot serve (rpc.ErrNotFound) is
// handled by age: below skipNotFoundBelow it is pruned history — skipped with a warning,
// since retrying can never recover it and failing would wedge the drain (and the backfill,
// which advances past skipped signatures identically) at that point forever; such a skip
// is a permanently missing audit row unless re-backfilled against an archival node, kept
// visible by the PermissionEventsSkippedTx metric. At or above the threshold the null is
// most likely a load-balanced backend lagging finalization, so the chunk fails and the
// un-advanced cursor retries the signature next refresh.
func (v *View) decodeChunk(ctx context.Context, sigs []*rpc.TransactionSignature, skipNotFoundBelow uint64) ([]PermissionEventRow, error) {
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

			// Pace the call. Held inside the semaphore so a waiting fetch also holds
			// its concurrency slot: the rate is the binding constraint, and releasing
			// the slot while waiting would let the queue grow without bound.
			if err := v.fetchLimiter.Wait(ctx); err != nil {
				return err
			}

			events, err := v.decodeTransaction(ctx, sig)
			if err != nil {
				if errors.Is(err, rpc.ErrNotFound) {
					if sig.Slot < skipNotFoundBelow {
						metrics.PermissionEventsSkippedTx.Inc()
						v.log.Warn("serviceability/permission-events: transaction unretrievable upstream, skipping",
							"signature", sig.Signature.String(), "slot", sig.Slot, "error", err)
						return nil
					}
					v.log.Warn("serviceability/permission-events: transaction not found near tip, retrying next refresh",
						"signature", sig.Signature.String(), "slot", sig.Slot)
					// Mark transient: this near-tip null is a load-balanced backend
					// lagging finalization and self-heals on retry, so escalation
					// should use the higher transient threshold rather than paging
					// after a few consecutive refreshes. Still unwraps to
					// rpc.ErrNotFound for any upstream errors.Is checks.
					return fmt.Errorf("%w (%w)", err, dberror.ErrTransient)
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
