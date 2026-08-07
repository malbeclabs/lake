package permissionevents

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	soljsonrpc "github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

var (
	testProgramID = solana.MustPublicKeyFromBase58("DZtnuQ839pSaDMFG5q1ad2V95G82S5EC4RrB3Ndw2Heb")
	testFeePayer  = solana.MustPublicKeyFromBase58("BUtAWK4GaUV42YRp7jSHZhchspsshabn67HnBHnKxzsY")
	testPDA       = solana.MustPublicKeyFromBase58("9onLAjzQx38ajKMbyfPs5L2jXFRtioXJBsFZNduxc4jA")
)

// fakeRPC serves Permission PDAs with synthetic per-account transaction histories.
// Each history is newest-first, mirroring getSignaturesForAddress. Until/Before/Limit
// are honored like the real RPC. txErrs/txResults may only be mutated between
// Refresh calls (decode goroutines read them concurrently).
type fakeRPC struct {
	pdas      []solana.PublicKey
	histories map[solana.PublicKey][]*solanarpc.TransactionSignature
	txResults map[solana.Signature]*solanarpc.GetTransactionResult
	txErrs    map[solana.Signature]error

	decodes   atomic.Int64
	cancelAt  int64 // if >0, the Nth GetTransaction call invokes cancel and fails
	cancel    context.CancelFunc
	onDecode  func() // if set, invoked on every GetTransaction (e.g. to advance a fake clock)
	onSigPage func() // if set, invoked on every GetSignaturesForAddress page

	inflight     atomic.Int64 // current concurrent GetTransaction calls
	peakInflight atomic.Int64 // high-water mark of inflight
}

// notePeak records cur into peakInflight if it is a new high-water mark.
func (f *fakeRPC) notePeak(cur int64) {
	for {
		peak := f.peakInflight.Load()
		if cur <= peak || f.peakInflight.CompareAndSwap(peak, cur) {
			return
		}
	}
}

func (f *fakeRPC) GetProgramAccountsWithOpts(ctx context.Context, program solana.PublicKey, opts *solanarpc.GetProgramAccountsOpts) (solanarpc.GetProgramAccountsResult, error) {
	res := make(solanarpc.GetProgramAccountsResult, 0, len(f.pdas))
	for _, pda := range f.pdas {
		res = append(res, &solanarpc.KeyedAccount{Pubkey: pda})
	}
	return res, nil
}

func (f *fakeRPC) GetSignaturesForAddressWithOpts(ctx context.Context, account solana.PublicKey, opts *solanarpc.GetSignaturesForAddressOpts) ([]*solanarpc.TransactionSignature, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.onSigPage != nil {
		f.onSigPage()
	}
	hist := f.histories[account]
	if !opts.Until.IsZero() {
		for i, e := range hist {
			if e.Signature == opts.Until {
				hist = hist[:i]
				break
			}
		}
	}
	if !opts.Before.IsZero() {
		rest := hist[:0:0]
		for i, e := range hist {
			if e.Signature == opts.Before {
				rest = hist[i+1:]
				break
			}
		}
		hist = rest
	}
	limit := len(hist)
	if opts.Limit != nil && *opts.Limit < limit {
		limit = *opts.Limit
	}
	return hist[:limit], nil
}

func (f *fakeRPC) GetTransaction(ctx context.Context, txSig solana.Signature, opts *solanarpc.GetTransactionOpts) (*solanarpc.GetTransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.notePeak(f.inflight.Add(1))
	defer f.inflight.Add(-1)
	n := f.decodes.Add(1)
	if f.onDecode != nil {
		f.onDecode()
	}
	if f.cancelAt > 0 && n >= f.cancelAt {
		f.cancel()
		return nil, context.Canceled
	}
	if err, ok := f.txErrs[txSig]; ok {
		return nil, err
	}
	res, ok := f.txResults[txSig]
	if !ok {
		return nil, solanarpc.ErrNotFound
	}
	return res, nil
}

// mkSig returns a deterministic distinct signature for a slot.
func mkSig(slot uint64) solana.Signature {
	var sig solana.Signature
	binary.BigEndian.PutUint64(sig[:8], slot)
	sig[8] = 1 // avoid the zero signature for slot 0
	return sig
}

// newHistory builds a newest-first history of len(slots) entries. slots must be
// ascending; entry i gets signature mkSig(slot) and a non-nil block time.
func newHistory(slots []uint64) []*solanarpc.TransactionSignature {
	out := make([]*solanarpc.TransactionSignature, 0, len(slots))
	for i := len(slots) - 1; i >= 0; i-- {
		slot := slots[i]
		bt := solana.UnixTimeSeconds(1753200000 + int64(slot))
		out = append(out, &solanarpc.TransactionSignature{
			Signature: mkSig(slot),
			Slot:      slot,
			BlockTime: &bt,
		})
	}
	return out
}

func seqSlots(from, to uint64) []uint64 {
	out := make([]uint64, 0, to-from+1)
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// createPermissionIxData builds CreatePermission instruction data: variant 97,
// user_payer pubkey, permissions mask (bit 0 set).
func createPermissionIxData() []byte {
	data := make([]byte, 49)
	data[0] = variantCreatePermission
	copy(data[1:33], testFeePayer[:])
	data[33] = 1
	return data
}

// rowlessIxData is a serviceability instruction that references the Permission
// PDA but is not a permission-management variant (like the multicast allowlist
// ops observed in prod), so it decodes to zero audit rows.
func rowlessIxData() []byte {
	return []byte{0x2a, 0, 0, 0}
}

// buildTxResult wraps a single-instruction transaction touching the PDA into a
// base64 GetTransactionResult envelope, as the real RPC returns it.
func buildTxResult(t *testing.T, ixData []byte) *solanarpc.GetTransactionResult {
	t.Helper()
	tx := &solana.Transaction{
		Signatures: []solana.Signature{mkSig(1)},
		Message: solana.Message{
			Header:          solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys:     []solana.PublicKey{testFeePayer, testPDA, testProgramID},
			RecentBlockhash: solana.Hash{},
			Instructions: []solana.CompiledInstruction{{
				ProgramIDIndex: 2,
				Accounts:       []uint16{1},
				Data:           solana.Base58(ixData),
			}},
		},
	}
	raw, err := tx.MarshalBinary()
	require.NoError(t, err)

	var env solanarpc.TransactionResultEnvelope
	require.NoError(t, json.Unmarshal(fmt.Appendf(nil, `[%q,"base64"]`, base64.StdEncoding.EncodeToString(raw)), &env))
	return &solanarpc.GetTransactionResult{Transaction: &env}
}

// newFakeRPC serves the given slots for testPDA, all with the same instruction data.
func newFakeRPC(t *testing.T, slots []uint64, ixData []byte) *fakeRPC {
	t.Helper()
	f := &fakeRPC{
		histories: make(map[solana.PublicKey][]*solanarpc.TransactionSignature),
		txResults: make(map[solana.Signature]*solanarpc.GetTransactionResult, len(slots)),
		txErrs:    make(map[solana.Signature]error),
	}
	f.addAccount(t, testPDA, slots, ixData)
	return f
}

// addAccount registers another watched PDA with its own history. Slot ranges must not
// overlap across accounts (mkSig derives signatures from slots).
func (f *fakeRPC) addAccount(t *testing.T, pda solana.PublicKey, slots []uint64, ixData []byte) {
	t.Helper()
	f.pdas = append(f.pdas, pda)
	f.histories[pda] = newHistory(slots)
	for _, slot := range slots {
		f.txResults[mkSig(slot)] = buildTxResult(t, ixData)
	}
}

// testFetchesPerSecond effectively disables getTransaction pacing. The fake RPC has
// no rate limit to respect, and the production default (25/s) would add tens of
// seconds to the multi-hundred-signature drain tests for no coverage. The pacing
// itself is covered by TestLake_PermissionEvents_View_PacesTransactionFetches.
const testFetchesPerSecond = 1e6

func newTestView(t *testing.T, ch clickhouse.Client, rpc *fakeRPC) *View {
	t.Helper()
	view, err := NewView(ViewConfig{
		Logger:           laketesting.NewLogger(),
		Clock:            clockwork.NewRealClock(),
		RPC:              rpc,
		ProgramID:        testProgramID,
		RefreshInterval:  time.Second,
		ClickHouse:       ch,
		FetchesPerSecond: testFetchesPerSecond,
	})
	require.NoError(t, err)
	return view
}

func factRowCount(t *testing.T, ch clickhouse.Client) uint64 {
	t.Helper()
	return queryUInt64(t, ch, `SELECT count() FROM fact_dz_permission_events`)
}

func factDistinctSlots(t *testing.T, ch clickhouse.Client) uint64 {
	t.Helper()
	return queryUInt64(t, ch, `SELECT uniqExact(slot) FROM fact_dz_permission_events`)
}

func queryUInt64(t *testing.T, ch clickhouse.Client, query string) uint64 {
	t.Helper()
	conn, err := ch.Conn(t.Context())
	require.NoError(t, err)
	rows, err := conn.Query(t.Context(), query)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var n uint64
	require.NoError(t, rows.Scan(&n))
	return n
}

// accountCursor reads the durable per-account cursor. found is false when no
// cursor row has been written for the PDA yet.
func accountCursor(t *testing.T, ch clickhouse.Client, pk string) (sig string, slot uint64, found bool) {
	t.Helper()
	conn, err := ch.Conn(t.Context())
	require.NoError(t, err)
	rows, err := conn.Query(t.Context(), `
		SELECT argMax(last_tx_signature, (updated_at, last_slot)), argMax(last_slot, (updated_at, last_slot))
		FROM dz_permission_events_account_cursor
		WHERE permission_pk = ?
		GROUP BY permission_pk
	`, pk)
	require.NoError(t, err)
	defer rows.Close()
	if !rows.Next() {
		return "", 0, false
	}
	require.NoError(t, rows.Scan(&sig, &slot))
	return sig, slot, true
}

// TestLake_PermissionEvents_View_DrainProgressPersistsAcrossTimedOutRefreshes is
// the poison-loop regression: an account whose backlog exceeds one refresh window
// must not have completed work re-fetched by the next refresh — even when its
// transactions decode to zero audit rows (so the fact table cannot serve as the
// cursor). Refresh #1 is cancelled mid-drain (as the Temporal activity deadline
// does); refresh #2 must only process the remainder.
func TestLake_PermissionEvents_View_DrainProgressPersistsAcrossTimedOutRefreshes(t *testing.T) {
	t.Parallel()

	total := uint64(2*scanChunkSize + 50)
	rpc := newFakeRPC(t, seqSlots(1, total), rowlessIxData())
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	// Refresh #1: cancel once the drain is 50 decodes into the second chunk.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rpc.cancelAt = int64(scanChunkSize + 50)
	rpc.cancel = cancel
	_, err := view.Refresh(ctx)
	require.Error(t, err, "cancelled mid-drain refresh should surface an error")

	// Refresh #2: no cancellation. Only the un-committed remainder may be
	// re-fetched; the first chunk's work must have been durably committed.
	rpc.cancelAt = 0
	rpc.decodes.Store(0)
	_, err = view.Refresh(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, total-scanChunkSize, rpc.decodes.Load(),
		"refresh #2 must resume after the committed chunk instead of re-fetching the whole backlog")

	// The durable cursor sits at the newest signature and no audit rows exist
	// (every transaction was row-less).
	sig, slot, found := accountCursor(t, ch, testPDA.String())
	require.True(t, found, "drain must leave a durable per-account cursor")
	require.Equal(t, total, slot)
	require.Equal(t, mkSig(total).String(), sig)
	require.EqualValues(t, 0, factRowCount(t, ch))
}

// TestLake_PermissionEvents_View_SkipsUnretrievableOldTransaction: a not-found
// transaction far below the account's fetch tip is pruned history — skipped with a
// warning instead of wedging the account forever.
func TestLake_PermissionEvents_View_SkipsUnretrievableOldTransaction(t *testing.T) {
	t.Parallel()

	slots := append([]uint64{1000}, seqSlots(10001, 10004)...)
	rpc := newFakeRPC(t, slots, createPermissionIxData())
	delete(rpc.txResults, mkSig(1000)) // fake returns solanarpc.ErrNotFound for it
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.NoError(t, err, "an old unretrievable transaction must not fail the refresh")
	require.EqualValues(t, 4, factRowCount(t, ch))

	_, slot, found := accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.EqualValues(t, 10004, slot)
}

// TestLake_PermissionEvents_View_NearTipNotFoundFailsChunkAndRetries: a not-found
// near the fetch tip is usually a load-balanced backend lagging finalization — a
// transient null, not pruned history. Skipping it would advance the cursor past a
// recoverable audit row permanently; the chunk must fail so the next refresh
// retries, and the event must land once the transaction becomes retrievable.
func TestLake_PermissionEvents_View_NearTipNotFoundFailsChunkAndRetries(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 5), createPermissionIxData())
	tipTx := rpc.txResults[mkSig(5)]
	delete(rpc.txResults, mkSig(5)) // near-tip null from a lagging backend
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.Error(t, err, "a near-tip not-found must fail the chunk, not be skipped")
	// The failure is transient-by-design (a lagging backend that self-heals),
	// so it must classify as transient — the activity escalator then pages at
	// the higher transient threshold instead of after a few refreshes — while
	// still unwrapping to the original not-found cause.
	require.True(t, dberror.IsTransient(err), "near-tip not-found must be classified transient")
	require.True(t, errors.Is(err, solanarpc.ErrNotFound), "error must still unwrap to ErrNotFound")
	require.EqualValues(t, 0, factRowCount(t, ch))
	_, _, found := accountCursor(t, ch, testPDA.String())
	require.False(t, found, "cursor must not advance past a recoverable signature")

	// The lagging backend catches up; nothing may have been lost.
	rpc.txResults[mkSig(5)] = tipTx
	_, err = view.Refresh(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 5, factRowCount(t, ch))
	_, slot, found := accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.EqualValues(t, 5, slot)
}

// TestLake_PermissionEvents_View_DurableCursorWinsOverStaleHighWaterMark: once a
// durable cursor exists it must be the resume point even when fact rows carry higher
// slots (a program-wide backfill wrote ahead, or pre-ledger-reset rows survive with
// old high slots). Preferring the higher slot would let a stale high-water mark
// shadow the cursor forever and re-page the account's full history every cycle.
func TestLake_PermissionEvents_View_DurableCursorWinsOverStaleHighWaterMark(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(95, 110), createPermissionIxData())
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	ctx := context.Background()
	require.NoError(t, view.store.SetAccountCursor(ctx, testPDA.String(),
		HighWaterMark{TxSignature: mkSig(100).String(), Slot: 100}))
	bt := time.Unix(1753200105, 0).UTC()
	require.NoError(t, view.store.InsertEvents(ctx, []PermissionEventRow{{
		EventTS:      bt,
		TxSignature:  mkSig(105).String(),
		Slot:         105,
		PermissionPK: testPDA.String(),
		EventType:    EventCreate,
		Success:      1,
	}}))

	_, err := view.Refresh(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 10, rpc.decodes.Load(),
		"resume must start at the durable cursor (slot 100), not the higher fact high-water mark")
}

// TestLake_PermissionEvents_View_FailedChunkKeepsCursorAndBackfillsOnRetry: a
// transient decode failure mid-chunk must not persist a partial chunk. Decode
// order is not cursor order, so inserting a partial chunk would advance the
// fact-derived high-water mark past never-decoded older signatures — a silent,
// permanent gap once the fault clears.
func TestLake_PermissionEvents_View_FailedChunkKeepsCursorAndBackfillsOnRetry(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 10), createPermissionIxData())
	rpc.txErrs[mkSig(5)] = errors.New("boom")
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.Error(t, err)
	require.EqualValues(t, 0, factRowCount(t, ch),
		"a chunk that did not fully decode must not be persisted")

	// Fault clears; every event must land — nothing skipped by a corrupted cursor.
	delete(rpc.txErrs, mkSig(5))
	_, err = view.Refresh(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 10, factRowCount(t, ch))
	require.EqualValues(t, 10, factDistinctSlots(t, ch))
}

// TestLake_PermissionEvents_View_PacesTransactionFetches: the concurrency semaphore
// bounds in-flight calls but not request rate, which is what a per-method provider
// limit actually measures. The view-wide limiter must hold the drain to
// FetchesPerSecond so we stay under the limit instead of discovering it.
func TestLake_PermissionEvents_View_PacesTransactionFetches(t *testing.T) {
	t.Parallel()

	const (
		fetches         = 40
		perSecond       = 20
		burst           = maxConcurrentFetches
		wantMinDuration = (fetches - burst) * time.Second / perSecond
	)

	rpc := newFakeRPC(t, seqSlots(1, fetches), rowlessIxData())
	ch := testClient(t)
	view, err := NewView(ViewConfig{
		Logger:           laketesting.NewLogger(),
		Clock:            clockwork.NewRealClock(),
		RPC:              rpc,
		ProgramID:        testProgramID,
		RefreshInterval:  time.Second,
		ClickHouse:       ch,
		FetchesPerSecond: perSecond,
	})
	require.NoError(t, err)

	start := time.Now()
	_, err = view.Refresh(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.EqualValues(t, fetches, rpc.decodes.Load(), "every signature must still be fetched")
	// The burst is spent immediately; the remainder is paced at perSecond.
	require.GreaterOrEqual(t, elapsed, wantMinDuration,
		"drain finished in %v — faster than %d fetches/s allows, so pacing is not applied", elapsed, perSecond)
}

// TestLake_PermissionEvents_View_ThrottledChunkStopsWithProgressAndResumes is the
// catch-up-livelock regression. A rate-limited getTransaction (RPCPool answers a
// throttled call with an *RPCError carrying HTTP 429 in its code field) used to
// abort the whole refresh cycle, so an account with a backlog committed only its
// first chunk per cycle and was recorded as a failure. With the drain cycle running
// every ~27min and a backlog growing faster than one chunk, the account could never
// catch up: prod sat 3h26m behind and widening.
//
// An upstream throttle is a budget exhaustion, not a fault — the same shape the
// deadline check already handles. Committed chunks are durable, so the drain must
// stop, report the honest committed frontier, and resume from the cursor next cycle.
func TestLake_PermissionEvents_View_ThrottledChunkStopsWithProgressAndResumes(t *testing.T) {
	t.Parallel()

	total := uint64(2 * scanChunkSize)
	rpc := newFakeRPC(t, seqSlots(1, total), createPermissionIxData())
	// Throttle a signature in the second chunk: the first commits, the second cannot.
	throttled := mkSig(scanChunkSize + 50)
	rpc.txErrs[throttled] = &soljsonrpc.RPCError{
		Code:    429,
		Message: "Too many requests for a specific RPC call",
	}
	require.True(t, dberror.IsTransient(rpc.txErrs[throttled]),
		"the injected error must classify as transient, else the test proves nothing")

	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	res, err := view.Refresh(context.Background())
	require.NoError(t, err,
		"a throttle after committed progress is a budget stop, not a cycle failure")
	require.True(t, res.Partial,
		"a cycle that stopped with backlog left must record status=partial, not success — "+
			"the staleness alert reads the last successful run's finished_at")
	require.EqualValues(t, scanChunkSize, factRowCount(t, ch),
		"the first chunk must stay committed")

	_, slot, found := accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.EqualValues(t, scanChunkSize, slot, "cursor must sit at the committed chunk boundary")

	// Freshness must not overstate: with backlog pending the frontier is the committed
	// chunk's block time, not now — this is what feeds the ingest-staleness alert.
	require.NotNil(t, res.SourceMaxEventTS)
	require.True(t, res.SourceMaxEventTS.Equal(time.Unix(1753200000+int64(scanChunkSize), 0)),
		"partial-progress freshness must be the committed frontier, got %v", res.SourceMaxEventTS)

	// Throttle lifts; the next cycle drains the remainder from the cursor.
	delete(rpc.txErrs, throttled)
	res, err = view.Refresh(context.Background())
	require.NoError(t, err)
	require.False(t, res.Partial,
		"a converged cycle must report success so the staleness clock resets")
	require.EqualValues(t, total, factRowCount(t, ch))
	require.EqualValues(t, total, factDistinctSlots(t, ch), "no signature may be skipped or double-counted")
	_, slot, found = accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.EqualValues(t, total, slot)
}

// TestLake_PermissionEvents_View_ThrottledFirstChunkStillErrors pins the guard on the
// graceful-stop path above: an account that is throttled before committing anything
// has made no progress, so it must still surface an error. Otherwise a persistently
// throttled account would no-op succeed forever and the staleness signal would be the
// only thing left.
func TestLake_PermissionEvents_View_ThrottledFirstChunkStillErrors(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 10), createPermissionIxData())
	rpc.txErrs[mkSig(5)] = &soljsonrpc.RPCError{
		Code:    429,
		Message: "Too many requests for a specific RPC call",
	}
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.Error(t, err, "zero-progress throttle must still escalate")
	require.EqualValues(t, 0, factRowCount(t, ch))
}

// TestLake_PermissionEvents_View_RefusesChunkWhenBudgetExhausted: when the
// context deadline is too close to safely decode and commit a chunk, the drain
// stops before starting one and reports an error (no silent no-op success).
func TestLake_PermissionEvents_View_RefusesChunkWhenBudgetExhausted(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 3), rowlessIxData())
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	// Deadline closer than the drain budget margin: no chunk may start.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	_, err := view.Refresh(ctx)
	require.Error(t, err, "no-progress budget exhaustion must surface an error")
	require.EqualValues(t, 0, rpc.decodes.Load(), "no transaction decode may start on an exhausted budget")
}

// TestLake_PermissionEvents_View_BudgetStopWithProgressResumesNextRefresh: when the
// deadline budget runs out after at least one committed chunk, the refresh reports
// success (the committed work is durable, stopping is the designed drain behavior)
// and the next refresh continues from the cursor. Time is driven by a fake clock the
// RPC advances per decode, so the stop point is deterministic.
func TestLake_PermissionEvents_View_BudgetStopWithProgressResumesNextRefresh(t *testing.T) {
	t.Parallel()

	total := uint64(2 * scanChunkSize)
	rpc := newFakeRPC(t, seqSlots(1, total), rowlessIxData())
	ch := testClient(t)

	start := time.Now()
	clock := clockwork.NewFakeClockAt(start)
	// Each decode advances the clock so the first chunk consumes 20s of budget.
	rpc.onDecode = func() { clock.Advance(100 * time.Millisecond) }

	view, err := NewView(ViewConfig{
		Logger:           laketesting.NewLogger(),
		Clock:            clock,
		RPC:              rpc,
		ProgramID:        testProgramID,
		RefreshInterval:  time.Second,
		ClickHouse:       ch,
		FetchesPerSecond: testFetchesPerSecond,
	})
	require.NoError(t, err)

	// Deadline 45s out: the first chunk's budget check sees 45s > margin (30s); after
	// the chunk the fake clock has advanced 20s, so 25s < margin stops the drain.
	ctx, cancel := context.WithDeadline(context.Background(), start.Add(45*time.Second))
	defer cancel()
	res, err := view.Refresh(ctx)
	require.NoError(t, err, "budget stop after committed progress is a success, not an error")
	require.EqualValues(t, scanChunkSize, rpc.decodes.Load(),
		"only the first chunk may be decoded before the budget stop")
	// Freshness must not overstate during a drain: with backlog pending, the source
	// timestamp is the committed chunk's frontier (block time of slot 200), not now.
	require.NotNil(t, res.SourceMaxEventTS)
	require.True(t, res.SourceMaxEventTS.Equal(time.Unix(1753200000+int64(scanChunkSize), 0)),
		"partial-progress freshness must be the committed frontier, got %v", res.SourceMaxEventTS)

	_, slot, found := accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.EqualValues(t, scanChunkSize, slot, "cursor must sit at the committed chunk boundary")

	// Next refresh (fresh deadline) drains the remainder.
	rpc.decodes.Store(0)
	ctx2, cancel2 := context.WithDeadline(context.Background(), clock.Now().Add(10*time.Minute))
	defer cancel2()
	_, err = view.Refresh(ctx2)
	require.NoError(t, err)
	require.EqualValues(t, total-scanChunkSize, rpc.decodes.Load())
	_, slot, found = accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.Equal(t, total, slot)
}

// TestLake_PermissionEvents_View_PaginationRespectsBudget: signature pagination is
// O(remaining backlog) and runs before any chunk work, so it must respect the deadline
// budget too — otherwise a backlog whose pagination alone fills the window would burn
// the whole activity every cycle before failing. It must fail fast, before any decode.
func TestLake_PermissionEvents_View_PaginationRespectsBudget(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 10), rowlessIxData())
	ch := testClient(t)

	start := time.Now()
	clock := clockwork.NewFakeClockAt(start)
	// Each signature page consumes 20s of budget; deadline 45s out with a 30s margin
	// means the second page's check sees 25s remaining and stops.
	rpc.onSigPage = func() { clock.Advance(20 * time.Second) }

	view, err := NewView(ViewConfig{
		Logger:           laketesting.NewLogger(),
		Clock:            clock,
		RPC:              rpc,
		ProgramID:        testProgramID,
		RefreshInterval:  time.Second,
		ClickHouse:       ch,
		FetchesPerSecond: testFetchesPerSecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithDeadline(context.Background(), start.Add(45*time.Second))
	defer cancel()
	_, err = view.Refresh(ctx)
	require.Error(t, err, "pagination that exhausts the budget must fail the account loudly")
	require.ErrorContains(t, err, "pagination")
	require.EqualValues(t, 0, rpc.decodes.Load(), "no decode work may start after pagination exhausts the budget")
}

// TestLake_PermissionEvents_View_BoundsConcurrentTransactionFetches: the view-wide
// semaphore must cap in-flight getTransaction calls at maxConcurrentFetches even with
// several accounts draining chunks concurrently — without it, nested per-chunk limits
// multiply by the number of accounts (the 10×10=100 hazard).
func TestLake_PermissionEvents_View_BoundsConcurrentTransactionFetches(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 250), rowlessIxData())
	rpc.addAccount(t, solana.NewWallet().PublicKey(), seqSlots(1001, 1250), rowlessIxData())
	rpc.addAccount(t, solana.NewWallet().PublicKey(), seqSlots(2001, 2250), rowlessIxData())
	// Hold each fetch briefly so goroutines genuinely overlap.
	rpc.onDecode = func() { time.Sleep(2 * time.Millisecond) }
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 750, rpc.decodes.Load())
	require.LessOrEqual(t, rpc.peakInflight.Load(), int64(maxConcurrentFetches),
		"in-flight getTransaction calls must stay within the view-wide bound")
}

// TestLake_PermissionEvents_View_ResumesFromFactHighWaterMark guards the upgrade
// path: an account indexed before the durable cursor existed resumes from the
// fact table's high-water mark rather than re-fetching its full history.
func TestLake_PermissionEvents_View_ResumesFromFactHighWaterMark(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(95, 110), createPermissionIxData())
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	// Seed one already-indexed event at slot 100, as the pre-cursor code left it.
	bt := time.Unix(1753200100, 0).UTC()
	require.NoError(t, view.store.InsertEvents(context.Background(), []PermissionEventRow{{
		EventTS:      bt,
		TxSignature:  mkSig(100).String(),
		Slot:         100,
		PermissionPK: testPDA.String(),
		EventType:    EventCreate,
		Success:      1,
	}}))

	_, err := view.Refresh(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 10, rpc.decodes.Load(),
		"only signatures newer than the fact high-water mark may be fetched")
	require.EqualValues(t, 11, factRowCount(t, ch))
}

// TestLake_PermissionEvents_View_ChunkBudgetScalesWithFetchRate: pacing put the
// fetch rate in charge of how long a chunk takes, so the deadline reserve has to
// follow it. A fixed reserve lets the drain start a chunk it cannot finish, and the
// overrun is silent — rate.Limiter.Wait refuses a reservation that would outlast the
// deadline and returns "would exceed context deadline", which classifies transient
// and so takes the stop-with-progress path, reporting success after fetching a
// chunk's worth of transactions that commit nothing.
func TestLake_PermissionEvents_View_ChunkBudgetScalesWithFetchRate(t *testing.T) {
	t.Parallel()

	ch := testClient(t)
	rpc := newFakeRPC(t, seqSlots(1, 10), rowlessIxData())

	newAt := func(perSecond float64) *View {
		v, err := NewView(ViewConfig{
			Logger: laketesting.NewLogger(), Clock: clockwork.NewRealClock(), RPC: rpc,
			ProgramID: testProgramID, RefreshInterval: time.Second, ClickHouse: ch,
			FetchesPerSecond: perSecond,
		})
		require.NoError(t, err)
		return v
	}

	// Worst-case contention: the limiter is view-wide, so a chunk can complete at a
	// fraction of the configured rate when other accounts drain through it.
	fast := newAt(100)
	slow := newAt(10)
	wantFast := time.Duration(float64(scanChunkSize*maxConcurrentFetches)/100*float64(time.Second)) + drainCommitReserve
	wantSlow := time.Duration(float64(scanChunkSize*maxConcurrentFetches)/10*float64(time.Second)) + drainCommitReserve

	require.Equal(t, wantFast, fast.chunkBudget(scanChunkSize))
	require.Equal(t, wantSlow, slow.chunkBudget(scanChunkSize))
	require.Greater(t, slow.chunkBudget(scanChunkSize), fast.chunkBudget(scanChunkSize),
		"a slower fetch rate must reserve more of the deadline, not the same fixed amount")

	// A short final chunk must not be charged for a full one.
	require.Less(t, fast.chunkBudget(1), fast.chunkBudget(scanChunkSize))
	// The reserve is always at least the insert+checkpoint cost.
	require.GreaterOrEqual(t, fast.chunkBudget(0), drainCommitReserve)
}

// TestLake_PermissionEvents_View_PartialCycleAlwaysReportsAFrontier: a partial cycle
// whose committedTip stays zero reports no freshness value at all — Refresh's switch
// leaves SourceMaxEventTS nil — so the run lands as a clean success with nothing to
// show how far behind it is. The tip must come from the newest dated signature in the
// chunk, not only its last element.
func TestLake_PermissionEvents_View_PartialCycleAlwaysReportsAFrontier(t *testing.T) {
	t.Parallel()

	total := uint64(2 * scanChunkSize)
	rpc := newFakeRPC(t, seqSlots(1, total), createPermissionIxData())

	// The first chunk's newest signature carries no block time, as the RPC returns for
	// states it cannot date. Earlier entries in that chunk are still dated.
	for _, h := range rpc.histories {
		for _, e := range h {
			if e.Slot == scanChunkSize {
				e.BlockTime = nil
			}
		}
	}
	// Throttle into the second chunk so the cycle stops with progress.
	rpc.txErrs[mkSig(scanChunkSize+50)] = &soljsonrpc.RPCError{
		Code: 429, Message: "Too many requests for a specific RPC call",
	}

	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	res, err := view.Refresh(context.Background())
	require.NoError(t, err)
	require.NotNil(t, res.SourceMaxEventTS,
		"a partial cycle must report a frontier even when its last committed signature is undated")
	// Slot scanChunkSize is undated, so the frontier falls back to scanChunkSize-1.
	require.True(t, res.SourceMaxEventTS.Equal(time.Unix(1753200000+int64(scanChunkSize)-1, 0)),
		"frontier should be the newest dated signature in the committed chunk, got %v", res.SourceMaxEventTS)
}
