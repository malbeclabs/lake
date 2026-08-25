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

// MaxConcurrentRPCRequests is the peak number of in-flight HTTP requests this view
// makes, so a client serving it can size its connection pool to match. Time queued
// waiting for a free connection counts against the client's per-request timeout, so a
// pool below this turns a slow endpoint into terminal timeouts rather than slow
// successes.
//
// Two independent limits add up. Up to maxConcurrentFetches accounts drain at once, and
// each one paginates getSignaturesForAddress itself, ungated. Separately, decodeSem caps
// in-flight getTransaction at maxConcurrentFetches across the whole view. Signature
// pagination and transaction decoding therefore overlap.
const MaxConcurrentRPCRequests = 2 * maxConcurrentFetches
