package permissionevents

import "github.com/malbeclabs/lake/indexer/pkg/sol"

// SolanaRPC abstracts the Solana RPC methods needed for fetching serviceability
// transaction history. It aliases the shared sol.TxHistoryRPC so the escrowevents
// and permissionevents indexers stay on one interface. Satisfied by *rpc.Client.
type SolanaRPC = sol.TxHistoryRPC
