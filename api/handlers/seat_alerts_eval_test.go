package handlers

import "testing"

func TestPrepaidEpochs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		balance uint64
		price   int64
		want    int64
	}{
		{"zero balance", 0, 100, 0},
		{"zero price", 500_000_000, 0, 0},
		{"exact", 300_000_000, 100, 3},    // $300 / $100 = 3
		{"floor", 250_000_000, 100, 2},    // $250 / $100 = 2 (floor)
		{"under one", 50_000_000, 100, 0}, // $50 / $100 = 0
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := PrepaidEpochs(c.balance, c.price); got != c.want {
				t.Fatalf("PrepaidEpochs(%d,%d)=%d want %d", c.balance, c.price, got, c.want)
			}
		})
	}
}

func TestAlertConditionMet(t *testing.T) {
	t.Parallel()
	const epoch = 1000
	active := SeatMetrics{TotalUSDCBalance: 150_000_000, PricePerEpochDollars: 100, ActiveEpoch: epoch, EscrowCount: 1} // 1 epoch left
	cases := []struct {
		name    string
		trigger string
		thresh  float64
		s       SeatMetrics
		want    bool
	}{
		{"epochs_left triggers at threshold", "epochs_left", 2, active, true}, // 1 <= 2
		{"epochs_left not yet", "epochs_left", 0, active, false},              // 1 <= 0 false
		{"balance_below triggers", "balance_below_usdc", 200, active, true},   // 150 < 200
		{"balance_below not yet", "balance_below_usdc", 100, active, false},   // 150 < 100 false
		{"inactive seat never triggers", "epochs_left", 5,
			SeatMetrics{TotalUSDCBalance: 0, PricePerEpochDollars: 100, ActiveEpoch: epoch - 1, EscrowCount: 1}, false},
		{"closed seat never triggers", "epochs_left", 5,
			SeatMetrics{TotalUSDCBalance: 150_000_000, PricePerEpochDollars: 100, ActiveEpoch: epoch, EscrowCount: 0}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := AlertConditionMet(c.trigger, c.thresh, c.s, epoch); got != c.want {
				t.Fatalf("AlertConditionMet(%q,%v)=%v want %v", c.trigger, c.thresh, got, c.want)
			}
		})
	}
}
