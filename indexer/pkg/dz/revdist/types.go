package dzrevdist

// Config represents the revdist program configuration (on-chain).
type Config struct {
	ProgramID                          string
	Flags                              uint64
	NextCompletedEpoch                 uint64
	AdminKey                           string
	DebtAccountantKey                  string
	RewardsAccountantKey               string
	ContributorManagerKey              string
	SOL2ZSwapProgramID                 string
	BurnRateLimit                      uint32
	BurnRateDZEpochsToIncreasing       uint32
	BurnRateDZEpochsToLimit            uint32
	BaseBlockRewardsPct                uint16
	PriorityBlockRewardsPct            uint16
	InflationRewardsPct                uint16
	JitoTipsPct                        uint16
	FixedSOLAmount                     uint32
	RelayPlaceholderLamports           uint32
	RelayDistributeRewardsLamports     uint32
	DebtWriteOffFeatureActivationEpoch uint64
}

// Distribution represents a per-epoch revenue distribution (on-chain).
type Distribution struct {
	DZEpoch                          uint64
	Flags                            uint64
	CommunityBurnRate                uint32
	TotalSolanaValidators            uint32
	SolanaValidatorPaymentsCount     uint32
	TotalSolanaValidatorDebt         uint64
	CollectedSolanaValidatorPayments uint64
	TotalContributors                uint32
	DistributedRewardsCount          uint32
	CollectedPrepaid2ZPayments       uint64
	Collected2ZConvertedFromSOL      uint64
	UncollectibleSOLDebt             uint64
	Distributed2ZAmount              uint64
	Burned2ZAmount                   uint64
	SolanaValidatorWriteOffCount     uint32
	BaseBlockRewardsPct              uint16
	PriorityBlockRewardsPct          uint16
	InflationRewardsPct              uint16
	JitoTipsPct                      uint16
	FixedSOLAmount                   uint32
}

// Journal represents the revdist journal (on-chain).
type Journal struct {
	ProgramID                string
	TotalSOLBalance          uint64
	Total2ZBalance           uint64
	Swap2ZDestinationBalance uint64
	SwappedSOLAmount         uint64
	NextDZEpochToSweepTokens uint64
}

// ValidatorDeposit represents a validator deposit (on-chain).
type ValidatorDeposit struct {
	NodeID            string
	WrittenOffSOLDebt uint64
}

// RecipientShare represents a single recipient share entry.
type RecipientShare struct {
	RecipientKey string `json:"recipient_key"`
	Share        uint16 `json:"share"`
}

// ContributorReward represents contributor rewards configuration (on-chain).
type ContributorReward struct {
	ServiceKey        string
	RewardsManagerKey string
	Flags             uint64
	RecipientShares   []RecipientShare
}

// ValidatorDebt represents a per-epoch validator debt (off-chain).
type ValidatorDebt struct {
	DZEpoch uint64
	NodeID  string
	Amount  uint64
}

// RewardShare represents a per-epoch reward share (off-chain).
type RewardShare struct {
	DZEpoch          uint64
	ContributorKey   string
	UnitShare        uint32
	TotalUnitShares  uint32
	IsBlocked        bool
	EconomicBurnRate uint32
}
