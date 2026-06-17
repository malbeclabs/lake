package validatorrewards

// Supported reward-token mints. From epoch 968 onward validators may choose to
// be rewarded in one of these tokens; each (subscription_epoch, mint) gets its
// own ShredDistributionJournal that owns the leaves of validators who picked it.
//
// DoubleZeroMintKey (2Z) is defined in journal.go.
const (
	// USDCMintKey is the canonical mainnet USDC mint.
	USDCMintKey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	// WSOLMintKey is the canonical wrapped-SOL mint.
	WSOLMintKey = "So11111111111111111111111111111111111111112"
)

// TokenInfo describes how to render and scale a reward token's on-chain base
// units into whole tokens.
type TokenInfo struct {
	Symbol   string
	Decimals uint8
}

// tokenInfoByMint maps a reward mint (base58) to its display symbol and decimal
// scale. Amounts in the journals are in base units; whole tokens = base units /
// 10^Decimals.
var tokenInfoByMint = map[string]TokenInfo{
	DoubleZeroMintKey: {Symbol: "2Z", Decimals: 8},
	USDCMintKey:       {Symbol: "USDC", Decimals: 6},
	WSOLMintKey:       {Symbol: "wSOL", Decimals: 9},
}

// TokenInfoForMint returns the (symbol, decimals) for a known reward mint and a
// bool reporting whether the mint is recognized. Unknown mints fall back to the
// 2Z scale so a newly-added token never silently produces NaN amounts, but the
// ok=false return lets callers log/flag it.
func TokenInfoForMint(mint string) (TokenInfo, bool) {
	if info, ok := tokenInfoByMint[mint]; ok {
		return info, true
	}
	return TokenInfo{Symbol: "?", Decimals: 8}, false
}
