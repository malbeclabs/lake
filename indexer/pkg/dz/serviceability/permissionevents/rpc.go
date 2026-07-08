package permissionevents

import (
	"context"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/malbeclabs/lake/indexer/pkg/sol"
)

// SolanaRPC is the RPC surface the permission-events indexer needs: the shared tx-history
// methods (getSignaturesForAddress + getTransaction, via sol.TxHistoryRPC) plus
// getProgramAccounts for discovering Permission PDAs to watch. Satisfied by *rpc.Client.
type SolanaRPC interface {
	sol.TxHistoryRPC
	GetProgramAccountsWithOpts(ctx context.Context, program solana.PublicKey, opts *rpc.GetProgramAccountsOpts) (rpc.GetProgramAccountsResult, error)
}

// Compile-time check that *rpc.Client satisfies SolanaRPC.
var _ SolanaRPC = (*rpc.Client)(nil)
