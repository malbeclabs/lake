package handlers

// SeatMetrics is the minimal seat state needed to evaluate an alert condition.
type SeatMetrics struct {
	TotalUSDCBalance     uint64 // micro-USDC (6 decimals)
	PricePerEpochDollars int64  // whole USDC per epoch
	ActiveEpoch          uint64
	EscrowCount          uint32
}

// PrepaidEpochs returns how many more epochs the balance can pay for (floor).
// Mirrors the frontend prepaidEpochs() and the shreds.go prepaidExpr SQL.
func PrepaidEpochs(balanceMicro uint64, pricePerEpochDollars int64) int64 {
	if pricePerEpochDollars <= 0 || balanceMicro == 0 {
		return 0
	}
	return int64(balanceMicro/1_000_000) / pricePerEpochDollars
}

func seatIsActive(s SeatMetrics, currentSolanaEpoch uint64) bool {
	return s.EscrowCount > 0 && s.ActiveEpoch >= currentSolanaEpoch
}

// AlertConditionMet reports whether the alert should fire for this seat right now.
// Only currently-active seats can trigger (this is an early warning, not a post-lapse notice).
func AlertConditionMet(triggerType string, threshold float64, s SeatMetrics, currentSolanaEpoch uint64) bool {
	if !seatIsActive(s, currentSolanaEpoch) {
		return false
	}
	switch triggerType {
	case "epochs_left":
		return float64(PrepaidEpochs(s.TotalUSDCBalance, s.PricePerEpochDollars)) <= threshold
	case "balance_below_usdc":
		return float64(s.TotalUSDCBalance)/1_000_000 < threshold
	}
	return false
}
