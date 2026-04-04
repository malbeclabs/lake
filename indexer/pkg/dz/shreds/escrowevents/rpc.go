package escrowevents

import (
	"context"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// SolanaRPC abstracts the Solana RPC methods needed for fetching escrow
// transaction history.
type SolanaRPC interface {
	GetSignaturesForAddressWithOpts(ctx context.Context, account solana.PublicKey, opts *rpc.GetSignaturesForAddressOpts) ([]*rpc.TransactionSignature, error)
	GetTransaction(ctx context.Context, txSig solana.Signature, opts *rpc.GetTransactionOpts) (*rpc.GetTransactionResult, error)
}

// Compile-time check that *rpc.Client satisfies SolanaRPC.
var _ SolanaRPC = (*rpc.Client)(nil)
