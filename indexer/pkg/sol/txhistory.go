package sol

import (
	"context"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

// TxHistoryRPC abstracts the Solana RPC methods needed to fetch and decode an
// account/program's transaction history (getSignaturesForAddress + getTransaction).
// It is shared by the escrowevents and permissionevents audit indexers, which both
// scan serviceability/escrow transaction history.
//
// Satisfied by *solanarpc.Client (gagliardetto solana-go), including the retrying
// client returned by doublezero's rpc.NewWithRetries.
type TxHistoryRPC interface {
	GetSignaturesForAddressWithOpts(ctx context.Context, account solana.PublicKey, opts *solanarpc.GetSignaturesForAddressOpts) ([]*solanarpc.TransactionSignature, error)
	GetTransaction(ctx context.Context, txSig solana.Signature, opts *solanarpc.GetTransactionOpts) (*solanarpc.GetTransactionResult, error)
}

// Compile-time check that *solanarpc.Client satisfies TxHistoryRPC.
var _ TxHistoryRPC = (*solanarpc.Client)(nil)
