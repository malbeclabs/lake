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
