package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/solana"
	"github.com/stretchr/testify/require"
)

func samples(n int, periodSec, numSlots uint64) []solana.PerformanceSample {
	out := make([]solana.PerformanceSample, 0, n)
	for i := range n {
		out = append(out, solana.PerformanceSample{
			Slot:            uint64(n - i),
			NumTransactions: 1000,
			NumSlots:        numSlots,
			SamplePeriodSec: periodSec,
		})
	}
	return out
}

// TestSlotDurationFromSamples covers the derivation and every guardrail.
func TestSlotDurationFromSamples(t *testing.T) {
	tests := []struct {
		name     string
		samples  []solana.PerformanceSample
		fallback float64
		want     float64
	}{
		// Derivation, across the spread of rates these chains run at. Ten 60s samples
		// in each case, so the window is 600s.
		{"400ms chain", samples(10, 60, 150), solanaFallbackSlotDurationSec, 0.400},
		{"300ms chain", samples(10, 60, 200), solanaFallbackSlotDurationSec, 0.300},
		{"200ms chain", samples(10, 60, 300), solanaFallbackSlotDurationSec, 0.200},
		// Nothing snaps to a round figure: a chain that misses its target is reported
		// at the rate it actually runs at, which is the whole point of measuring.
		{"350ms chain", samples(10, 60, 171), solanaFallbackSlotDurationSec, 0.351},
		{"chain at an untargeted rate", samples(10, 60, 163), dzFallbackSlotDurationSec, 0.368},
		// A node that has only just started sampling still reports its real rate.
		{"a single sample is enough", samples(1, 60, 150), solanaFallbackSlotDurationSec, 0.400},

		// Summed across the window, not averaged per sample: uneven samples must
		// weight by their own period.
		{
			name: "uneven samples sum rather than average per-sample",
			samples: append(
				samples(9, 60, 150), // 540s / 1350 slots
				solana.PerformanceSample{SamplePeriodSec: 60, NumSlots: 300}, // one fast sample
			),
			fallback: solanaFallbackSlotDurationSec,
			want:     600.0 / 1650.0, // ~0.364, not the 0.38 a per-sample mean gives
		},

		// Guardrails.
		{"no samples falls back", nil, solanaFallbackSlotDurationSec, solanaFallbackSlotDurationSec},
		{"zero slots falls back", samples(10, 60, 0), dzFallbackSlotDurationSec, dzFallbackSlotDurationSec},
		// 600s / 6000 slots = 0.1s: a node replaying history, not keeping up with it.
		{"below the floor falls back", samples(10, 60, 600), solanaFallbackSlotDurationSec, solanaFallbackSlotDurationSec},
		// A chain that advanced one slot in 600s is stalled, not running at 600s/slot.
		{"above the ceiling falls back", samples(1, 600, 1), dzFallbackSlotDurationSec, dzFallbackSlotDurationSec},

		// Inclusive bounds: an off-by-one here silently discards legitimate readings.
		// Each fallback differs from the expected reading, so a rejected bound fails.
		{"exactly the floor is accepted", samples(10, 60, 400), solanaFallbackSlotDurationSec, minSlotDurationSec},
		{"exactly the ceiling is accepted", samples(10, 60, 60), solanaFallbackSlotDurationSec, maxSlotDurationSec},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.InDelta(t, tt.want, slotDurationFromSamples(tt.samples, tt.fallback), 0.001)
		})
	}
}

// ledgerBackend serves a minimal ledger RPC with a configurable slot rate.
func ledgerBackend(t *testing.T, slotIndex, slotsInEpoch, numSlotsPerSample uint64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		switch req.Method {
		case "getEpochInfo":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"epoch":700,"slotIndex":%d,`+
				`"slotsInEpoch":%d,"absoluteSlot":300000000,"blockHeight":280000000,"transactionCount":9}}`,
				slotIndex, slotsInEpoch)
		case "getRecentPerformanceSamples":
			out, err := json.Marshal(samples(10, 60, numSlotsPerSample))
			require.NoError(t, err)
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":%s}`, out)
		case "getInflationRate":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"total":0.05,"validator":0.045,"foundation":0.005,"epoch":700}}`)
		case "getVersion":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"solana-core":"2.0.0"}}`)
		case "getVoteAccounts":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"current":[],"delinquent":[]}}`)
		case "getSupply":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},`+
				`"value":{"total":0,"circulating":0,"nonCirculating":0,"nonCirculatingAccounts":[]}}}`)
		default:
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestFetchLedgerData_EpochETAUsesMeasuredSlotDuration: a chain running twice as fast
// finishes its epoch in half the time. Under the fixed constant this replaced, both
// reported the same number.
func TestFetchLedgerData_EpochETAUsesMeasuredSlotDuration(t *testing.T) {
	resetSupplyCache()

	const slotIndex, slotsInEpoch = 32000, 432000

	// 600s / 1500 slots = 0.4s.
	slow, err := fetchLedgerData(context.Background(),
		ledgerBackend(t, slotIndex, slotsInEpoch, 150).URL, solanaFallbackSlotDurationSec)
	require.NoError(t, err)
	require.InDelta(t, 0.4, slow.SlotDurationSec, 0.0001)
	require.InDelta(t, float64(slotsInEpoch-slotIndex)*slow.SlotDurationSec, slow.EpochETASec, 0.01)

	resetSupplyCache()

	// 600s / 3000 slots = 0.2s.
	fast, err := fetchLedgerData(context.Background(),
		ledgerBackend(t, slotIndex, slotsInEpoch, 300).URL, solanaFallbackSlotDurationSec)
	require.NoError(t, err)
	require.InDelta(t, 0.2, fast.SlotDurationSec, 0.0001)
	require.InDelta(t, slow.EpochETASec/2, fast.EpochETASec, 0.01,
		"double the slot rate halves the epoch ETA")
}

// TestFetchLedgerData_UnusableSamplesUseTheCallersFallback proves the per-chain constant
// reaches the response rather than a shared default: the same unusable samples must
// produce a different answer per chain.
func TestFetchLedgerData_UnusableSamplesUseTheCallersFallback(t *testing.T) {
	for _, tt := range []struct {
		name     string
		fallback float64
	}{
		{"dz", dzFallbackSlotDurationSec},
		{"solana", solanaFallbackSlotDurationSec},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resetSupplyCache()

			// numSlots of 0 is the shape a chain reports when the field is unpopulated.
			got, err := fetchLedgerData(context.Background(),
				ledgerBackend(t, 32000, 432000, 0).URL, tt.fallback)
			require.NoError(t, err)
			require.InDelta(t, tt.fallback, got.SlotDurationSec, 0.0001)
			require.InDelta(t, float64(432000-32000)*tt.fallback, got.EpochETASec, 0.01)
		})
	}
}
