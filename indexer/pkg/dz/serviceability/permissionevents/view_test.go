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
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

var (
	testProgramID = solana.MustPublicKeyFromBase58("DZtnuQ839pSaDMFG5q1ad2V95G82S5EC4RrB3Ndw2Heb")
	testFeePayer  = solana.MustPublicKeyFromBase58("BUtAWK4GaUV42YRp7jSHZhchspsshabn67HnBHnKxzsY")
	testPDA       = solana.MustPublicKeyFromBase58("9onLAjzQx38ajKMbyfPs5L2jXFRtioXJBsFZNduxc4jA")
)

// fakeRPC serves a single Permission PDA with a synthetic transaction history.
// history is newest-first, mirroring getSignaturesForAddress. Until/Before/Limit
// are honored like the real RPC. txErrs/txResults may only be mutated between
// Refresh calls (decode goroutines read them concurrently).
type fakeRPC struct {
	pdas      []solana.PublicKey
	history   []*solanarpc.TransactionSignature
	txResults map[solana.Signature]*solanarpc.GetTransactionResult
	txErrs    map[solana.Signature]error

	decodes  atomic.Int64
	cancelAt int64 // if >0, the Nth GetTransaction call invokes cancel and fails
	cancel   context.CancelFunc
	onDecode func() // if set, invoked on every GetTransaction (e.g. to advance a fake clock)
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
	hist := f.history
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
		pdas:      []solana.PublicKey{testPDA},
		history:   newHistory(slots),
		txResults: make(map[solana.Signature]*solanarpc.GetTransactionResult, len(slots)),
		txErrs:    make(map[solana.Signature]error),
	}
	for _, slot := range slots {
		f.txResults[mkSig(slot)] = buildTxResult(t, ixData)
	}
	return f
}

func newTestView(t *testing.T, ch clickhouse.Client, rpc *fakeRPC) *View {
	t.Helper()
	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clockwork.NewRealClock(),
		RPC:             rpc,
		ProgramID:       testProgramID,
		RefreshInterval: time.Second,
		ClickHouse:      ch,
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

// TestLake_PermissionEvents_View_SkipsUnretrievableTransaction: a signature whose
// transaction the RPC no longer serves (pruned history) is skipped with a warning
// instead of wedging the account forever.
func TestLake_PermissionEvents_View_SkipsUnretrievableTransaction(t *testing.T) {
	t.Parallel()

	rpc := newFakeRPC(t, seqSlots(1, 5), createPermissionIxData())
	delete(rpc.txResults, mkSig(3)) // fake returns solanarpc.ErrNotFound for it
	ch := testClient(t)
	view := newTestView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.NoError(t, err, "an unretrievable transaction must not fail the refresh")
	require.EqualValues(t, 4, factRowCount(t, ch))

	_, slot, found := accountCursor(t, ch, testPDA.String())
	require.True(t, found)
	require.EqualValues(t, 5, slot)
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
		Logger:          laketesting.NewLogger(),
		Clock:           clock,
		RPC:             rpc,
		ProgramID:       testProgramID,
		RefreshInterval: time.Second,
		ClickHouse:      ch,
	})
	require.NoError(t, err)

	// Deadline 45s out: the first chunk's budget check sees 45s > margin (30s); after
	// the chunk the fake clock has advanced 20s, so 25s < margin stops the drain.
	ctx, cancel := context.WithDeadline(context.Background(), start.Add(45*time.Second))
	defer cancel()
	_, err = view.Refresh(ctx)
	require.NoError(t, err, "budget stop after committed progress is a success, not an error")
	require.EqualValues(t, scanChunkSize, rpc.decodes.Load(),
		"only the first chunk may be decoded before the budget stop")

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
