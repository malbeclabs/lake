package escrowevents

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	soljsonrpc "github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

var (
	testViewProgramID = solana.MustPublicKeyFromBase58("DZtnuQ839pSaDMFG5q1ad2V95G82S5EC4RrB3Ndw2Heb")
	testEscrowAccount = solana.MustPublicKeyFromBase58("9onLAjzQx38ajKMbyfPs5L2jXFRtioXJBsFZNduxc4jA")
	testFeePayerKey   = solana.MustPublicKeyFromBase58("BUtAWK4GaUV42YRp7jSHZhchspsshabn67HnBHnKxzsY")
)

// fakeViewRPC serves one escrow's transaction history newest-first, honoring
// Until/Before/Limit like getSignaturesForAddress. txErrs injects a per-signature
// GetTransaction failure.
type fakeViewRPC struct {
	history []*solanarpc.TransactionSignature
	txErrs  map[solana.Signature]error
}

func (f *fakeViewRPC) GetSignaturesForAddressWithOpts(ctx context.Context, account solana.PublicKey, opts *solanarpc.GetSignaturesForAddressOpts) ([]*solanarpc.TransactionSignature, error) {
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

func (f *fakeViewRPC) GetTransaction(ctx context.Context, txSig solana.Signature, opts *solanarpc.GetTransactionOpts) (*solanarpc.GetTransactionResult, error) {
	if err, ok := f.txErrs[txSig]; ok {
		return nil, err
	}
	return buildFundTxResult(txSig), nil
}

// mkViewSig returns a deterministic distinct signature for a slot.
func mkViewSig(slot uint64) solana.Signature {
	var sig solana.Signature
	binary.BigEndian.PutUint64(sig[:8], slot)
	sig[8] = 1
	return sig
}

// buildFundTxResult returns a transaction whose logs parse to exactly one fund
// event, with the fee payer as the only account key.
func buildFundTxResult(sig solana.Signature) *solanarpc.GetTransactionResult {
	tx := &solana.Transaction{
		Signatures: []solana.Signature{sig},
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{testFeePayerKey, testViewProgramID},
		},
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		panic(err)
	}
	var env solanarpc.TransactionResultEnvelope
	if err := json.Unmarshal(fmt.Appendf(nil, `[%q,"base64"]`,
		base64.StdEncoding.EncodeToString(raw)), &env); err != nil {
		panic(err)
	}
	pid := testViewProgramID.String()
	return &solanarpc.GetTransactionResult{
		Transaction: &env,
		Meta: &solanarpc.TransactionMeta{
			LogMessages: []string{
				"Program " + pid + " invoke [1]",
				"Program log: Fund payment escrow with USDC",
				"Program log: Funded payment escrow for client seat " + testClientSeatPK + " with 1000000 USDC",
				"Program log: USDC balance after funding: 2000000",
				"Program " + pid + " success",
			},
		},
	}
}

// newViewHistory builds a newest-first history over the given ascending slots.
func newViewHistory(slots []uint64) []*solanarpc.TransactionSignature {
	out := make([]*solanarpc.TransactionSignature, 0, len(slots))
	for i := len(slots) - 1; i >= 0; i-- {
		slot := slots[i]
		bt := solana.UnixTimeSeconds(1753200000 + int64(slot))
		out = append(out, &solanarpc.TransactionSignature{
			Signature: mkViewSig(slot),
			Slot:      slot,
			BlockTime: &bt,
		})
	}
	return out
}

func newTestEscrowView(t *testing.T, ch clickhouse.Client, rpc *fakeViewRPC) *View {
	t.Helper()
	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clockwork.NewRealClock(),
		RPC:             rpc,
		ProgramID:       testViewProgramID,
		RefreshInterval: time.Second,
		ClickHouse:      ch,
		EscrowProvider: func() []EscrowInfo {
			return []EscrowInfo{{EscrowPK: testEscrowAccount.String(), ClientSeatPK: testClientSeatPK}}
		},
	})
	require.NoError(t, err)
	return view
}

func escrowRowSlots(t *testing.T, ch clickhouse.Client) []uint64 {
	t.Helper()
	conn, err := ch.Conn(t.Context())
	require.NoError(t, err)
	rows, err := conn.Query(t.Context(),
		`SELECT slot FROM fact_dz_shred_escrow_events FINAL ORDER BY slot`)
	require.NoError(t, err)
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var slot uint64
		require.NoError(t, rows.Scan(&slot))
		out = append(out, slot)
	}
	return out
}

// TestLake_EscrowEvents_View_RealRPCErrorShapesConverge is the regression that the
// first version of this fix did not actually cover.
//
// That version gated the retry on dberror.IsTransient, a message-substring classifier
// written for ClickHouse and S3 errors. Run the shapes solana-go really returns through
// it and only RPCError{429} matches — which happened to be the shape the old test
// injected. Every other real failure still took the skip path and still lost the event
// permanently, because GetHighWaterMarks derives the resume point from max(slot) of the
// rows actually written.
//
// Each case here fails against that version.
func TestLake_EscrowEvents_View_RealRPCErrorShapesConverge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"rpc.ErrNotFound near tip", solanarpc.ErrNotFound},
		{"HTTPError 429", soljsonrpc.NewHTTPError(429, errors.New("rpc call getTransaction() on https://x status code: 429"))},
		{"HTTPError 503", soljsonrpc.NewHTTPError(503, errors.New("rpc call getTransaction() on https://x status code: 503"))},
		{"RPCError -32005 node behind", &soljsonrpc.RPCError{Code: -32005, Message: "Node is behind by 100 slots"}},
		{"RPCError -32603 internal", &soljsonrpc.RPCError{Code: -32603, Message: "Service unavailable, please try again later."}},
		{"RPCError 429 too many requests", &soljsonrpc.RPCError{Code: 429, Message: "Too many requests for a specific RPC call"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rpc := &fakeViewRPC{
				history: newViewHistory([]uint64{100, 200, 300}),
				txErrs:  map[solana.Signature]error{mkViewSig(200): tt.err},
			}
			ch := testClient(t)
			view := newTestEscrowView(t, ch, rpc)

			_, err := view.Refresh(context.Background())
			require.NoError(t, err, "one failing escrow must not fail the refresh")

			// The fault clears. Every signature must eventually be indexed.
			delete(rpc.txErrs, mkViewSig(200))
			_, err = view.Refresh(context.Background())
			require.NoError(t, err)

			require.Equal(t, []uint64{100, 200, 300}, escrowRowSlots(t, ch),
				"slot 200 must be re-fetched after the fault clears; a missing 200 means "+
					"it was skipped and the high-water mark moved past it")
		})
	}
}

// TestLake_EscrowEvents_View_PrunedNotFoundIsSkippedAndCommitted pins the one escape
// hatch. A not-found far below the tip is pruned history: retrying cannot recover it and
// failing forever would wedge the escrow, so it is skipped and the rest still commits.
func TestLake_EscrowEvents_View_PrunedNotFoundIsSkippedAndCommitted(t *testing.T) {
	t.Parallel()

	// Slot 100 sits well over notFoundSkipSlotLag below the tip at 100000.
	rpc := &fakeViewRPC{
		history: newViewHistory([]uint64{100, 99000, 100000}),
		txErrs:  map[solana.Signature]error{mkViewSig(100): solanarpc.ErrNotFound},
	}
	ch := testClient(t)
	view := newTestEscrowView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.NoError(t, err)

	require.Equal(t, []uint64{99000, 100000}, escrowRowSlots(t, ch),
		"a pruned not-found is skipped, and the remaining signatures still commit")
}

// TestLake_EscrowEvents_View_PartialWalkKeepsCommittedChunks is the convergence
// regression for the largest escrow.
//
// Production holds 693 escrows; the median has 4 signatures but the largest has 4471 and
// grows unbounded. Failing the walk without committing anything meant one blip discarded
// all of it and the retry restarted from zero, so on a cold start or BackfillRefresh it
// never converged. Chunks now commit as the walk proceeds.
func TestLake_EscrowEvents_View_PartialWalkKeepsCommittedChunks(t *testing.T) {
	t.Parallel()

	// Two chunks plus a remainder, oldest-first after the reversal.
	slots := make([]uint64, 0, scanChunkSize+50)
	for i := uint64(1); i <= uint64(scanChunkSize)+50; i++ {
		slots = append(slots, i)
	}

	// Fail inside the second chunk, so the first must already be durable.
	failAt := uint64(scanChunkSize + 10)
	rpc := &fakeViewRPC{
		history: newViewHistory(slots),
		txErrs:  map[solana.Signature]error{mkViewSig(failAt): errors.New("read tcp: connection reset by peer")},
	}
	ch := testClient(t)
	view := newTestEscrowView(t, ch, rpc)

	_, err := view.Refresh(context.Background())
	require.NoError(t, err)

	got := escrowRowSlots(t, ch)
	require.NotEmpty(t, got,
		"the first chunk completed before the failure and must be committed; empty means "+
			"the whole walk was discarded and a large escrow can never converge")
	require.GreaterOrEqual(t, len(got), scanChunkSize,
		"at least the first full chunk must be durable, got %d rows", len(got))

	// The fault clears and the walk resumes from the advanced mark rather than zero.
	delete(rpc.txErrs, mkViewSig(failAt))
	_, err = view.Refresh(context.Background())
	require.NoError(t, err)
	require.Len(t, escrowRowSlots(t, ch), len(slots), "the walk must converge on a retry")
}
