package validatorsapp

// Validator represents a validator entry from the validators.app API,
// transformed into our domain model.
type Validator struct {
	Account                string
	Name                   string
	VoteAccount            string
	SoftwareVersion        string
	SoftwareClient         string
	SoftwareClientID       uint16
	Jito                   bool
	JitoCommission         uint32
	IsActive               bool
	IsDZ                   bool
	ActiveStake            uint64
	Commission             uint8
	Delinquent             bool
	Epoch                  uint64
	EpochCredits           uint64
	SkippedSlotPercent     string
	TotalScore             int16
	DataCenterKey          string
	AutonomousSystemNumber uint32
	Latitude               string
	Longitude              string
	IP                     string
	StakePoolsList         []string
}
