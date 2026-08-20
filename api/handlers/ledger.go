package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/malbeclabs/lake/api/solana"
	"github.com/malbeclabs/lake/utils/pkg/redact"
	"golang.org/x/sync/errgroup"
)

// LedgerResponse contains ledger/chain telemetry for a Solana-compatible chain.
type LedgerResponse struct {
	// Epoch info
	Epoch        uint64  `json:"epoch"`
	SlotIndex    uint64  `json:"slot_index"`
	SlotsInEpoch uint64  `json:"slots_in_epoch"`
	EpochPct     float64 `json:"epoch_pct"`
	EpochETASec  float64 `json:"epoch_eta_sec"`

	// SlotDurationSec is the chain's current slot time, measured from the recent
	// performance samples. EpochETASec is derived from it, and the web derives
	// "epoch started N ago" from it too, so both figures come from one number.
	SlotDurationSec float64 `json:"slot_duration_sec"`

	// Chain state
	AbsoluteSlot     uint64  `json:"absolute_slot"`
	BlockHeight      uint64  `json:"block_height"`
	TransactionCount uint64  `json:"transaction_count"`
	SkipRate         float64 `json:"skip_rate"`

	// TPS (average over recent samples)
	TPS float64 `json:"tps"`

	// Supply (in SOL)
	TotalSupply       float64 `json:"total_supply"`
	CirculatingSupply float64 `json:"circulating_supply"`

	// Inflation
	InflationTotal      float64 `json:"inflation_total"`
	InflationValidator  float64 `json:"inflation_validator"`
	InflationFoundation float64 `json:"inflation_foundation"`

	// Validator summary
	ActiveValidators     int     `json:"active_validators"`
	DelinquentValidators int     `json:"delinquent_validators"`
	TotalStakeSOL        float64 `json:"total_stake_sol"`

	// Node version (of the RPC node we're talking to)
	NodeVersion string `json:"node_version"`

	Error string `json:"error,omitempty"`
}

const defaultDZLedgerRPCURL = "https://doublezero-mainnet-beta.rpcpool.com/db336024-e7a8-46b1-80e5-352dd77060ab"

// Bounds on a slot duration measured from the performance samples. Outside them the
// reading is not a slot rate — the chain is paused, or the sample window is too short
// to be anything but RPC latency and commitment jitter — and the caller's fallback is
// used instead.
const (
	// Below SIMD-0525's terminal 200ms stage, so a future sub-200ms stage needs no edit.
	minSlotDurationSec = 0.150
	maxSlotDurationSec = 1.0
	// Ten 60s samples span ~600s, so this only bites when an endpoint returns a
	// partial set.
	minSampleSpanSec = 300
)

// Fallbacks, used only when the chain's own slot rate cannot be measured from the
// performance samples — empty, too short a span, or out of plausible bounds. This is a
// rarely-hit path and these are approximations by design: they do not need to track
// their chain precisely, they need to be sane.
const (
	// The DoubleZero ledger's *observed* rate, not its 400ms target, which it beats by
	// ~8%: it lands 432k-slot epochs in ~44h. Basis: the note on fallbackSlotDuration in
	// malbeclabs/doublezero controlplane/telemetry/internal/telemetry/epoch.go. Not 0.400.
	DZFallbackSlotDurationSec = 0.370

	// Solana's SIMD-0525 stage as of the 350ms gate, effective epoch 1020 (2026-08-21).
	// Not 0.400 — that is the pre-gate value this issue was filed to stop using.
	SolanaFallbackSlotDurationSec = 0.350
)

// slotDurationFromSamples derives the chain's slot time from the performance samples,
// falling back to the caller's per-chain constant when they cannot support a reading.
//
// agave's SamplePerformanceService populates NumSlots as the delta of the highest slot
// across the sample window — a slot *index* delta, so skipped slots are counted. That is
// the unit the epoch ETA needs: it projects over slots_in_epoch - slot_index, and a
// skipped slot still consumes its wall-clock slot time.
func slotDurationFromSamples(samples []solana.PerformanceSample, fallback float64) float64 {
	var totalSec, totalSlots uint64
	for _, s := range samples {
		totalSec += s.SamplePeriodSec
		totalSlots += s.NumSlots
	}
	if totalSec < minSampleSpanSec || totalSlots == 0 {
		return fallback
	}
	d := float64(totalSec) / float64(totalSlots)
	if d < minSlotDurationSec || d > maxSlotDurationSec {
		return fallback
	}
	return d
}

func GetDZLedgerRPCURL() string {
	if url := os.Getenv("DZ_LEDGER_RPC_URL"); url != "" {
		return url
	}
	return defaultDZLedgerRPCURL
}

func GetSolanaRPCURL() string {
	return solana.GetRPCURL()
}

// FetchLedgerData fetches ledger telemetry from the given RPC URL.
//
// fallbackSlotDurationSec is the chain's slot time to assume when it cannot be measured
// from the performance samples; pass DZFallbackSlotDurationSec or
// SolanaFallbackSlotDurationSec.
func FetchLedgerData(ctx context.Context, rpcURL string, fallbackSlotDurationSec float64) (*LedgerResponse, error) {
	client := solana.NewClient(rpcURL)

	var (
		epochInfo   *solana.EpochInfo
		perfSamples []solana.PerformanceSample
		supply      *solana.Supply
		inflation   *solana.InflationRate
		version     *solana.Version
		voteAccts   *solana.VoteAccountsResult
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		epochInfo, err = client.GetEpochInfo(gctx)
		return err
	})

	g.Go(func() error {
		var err error
		perfSamples, err = client.GetRecentPerformanceSamples(gctx, 10)
		return err
	})

	// getSupply is best-effort and deliberately outside this group's error path.
	// It costs ~6.4s against an endpoint where every other call here takes ~45ms, so
	// letting it return an error cancels gctx and discards five already-finished
	// results to report nothing. Its own failure now costs only the supply fields,
	// which keep their last known value.
	g.Go(func() error {
		supply = cachedSupply(gctx, client)
		return nil
	})

	g.Go(func() error {
		var err error
		inflation, err = client.GetInflationRate(gctx)
		return err
	})

	g.Go(func() error {
		var err error
		version, err = client.GetVersion(gctx)
		return err
	})

	g.Go(func() error {
		var err error
		voteAccts, err = client.GetVoteAccounts(gctx)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Compute TPS from performance samples
	var tps float64
	if len(perfSamples) > 0 {
		var totalTxn, totalSec uint64
		for _, s := range perfSamples {
			totalTxn += s.NumTransactions
			totalSec += s.SamplePeriodSec
		}
		if totalSec > 0 {
			tps = float64(totalTxn) / float64(totalSec)
		}
	}

	// supply is nil when getSupply has never succeeded since startup. Report zero
	// rather than fail: every other field on this response is present and useful.
	var totalSupplySOL, circulatingSupplySOL float64
	if supply != nil {
		totalSupplySOL = float64(supply.Value.Total) / 1e9
		circulatingSupplySOL = float64(supply.Value.Circulating) / 1e9
	}

	// Skip rate
	var skipRate float64
	if epochInfo.AbsoluteSlot > 0 {
		skipRate = float64(epochInfo.AbsoluteSlot-epochInfo.BlockHeight) / float64(epochInfo.AbsoluteSlot) * 100
	}

	// Epoch progress
	var epochPct float64
	if epochInfo.SlotsInEpoch > 0 {
		epochPct = float64(epochInfo.SlotIndex) / float64(epochInfo.SlotsInEpoch) * 100
	}

	// ETA in seconds, at the chain's own measured slot rate. A fixed 0.4s put mainnet's
	// 350ms epochs ~6h long, and the DZ ledger — which runs ~8% ahead of its own 400ms
	// target — up to ~4h long right after a rollover.
	slotDurationSec := slotDurationFromSamples(perfSamples, fallbackSlotDurationSec)
	remainingSlots := epochInfo.SlotsInEpoch - epochInfo.SlotIndex
	epochETASec := float64(remainingSlots) * slotDurationSec

	// Validator summary
	var totalStakeLamports uint64
	for _, v := range voteAccts.Current {
		totalStakeLamports += v.ActivatedStake
	}
	for _, v := range voteAccts.Delinquent {
		totalStakeLamports += v.ActivatedStake
	}

	return &LedgerResponse{
		Epoch:        epochInfo.Epoch,
		SlotIndex:    epochInfo.SlotIndex,
		SlotsInEpoch: epochInfo.SlotsInEpoch,
		EpochPct:     epochPct,
		EpochETASec:  epochETASec,

		SlotDurationSec: slotDurationSec,

		AbsoluteSlot:     epochInfo.AbsoluteSlot,
		BlockHeight:      epochInfo.BlockHeight,
		TransactionCount: epochInfo.TransactionCount,
		SkipRate:         skipRate,

		TPS: tps,

		TotalSupply:       totalSupplySOL,
		CirculatingSupply: circulatingSupplySOL,

		InflationTotal:      inflation.Total * 100,
		InflationValidator:  inflation.Validator * 100,
		InflationFoundation: inflation.Foundation * 100,

		ActiveValidators:     len(voteAccts.Current),
		DelinquentValidators: len(voteAccts.Delinquent),
		TotalStakeSOL:        float64(totalStakeLamports) / 1e9,

		NodeVersion: version.SolanaCore,
	}, nil
}

// GetDZLedger returns ledger telemetry for the DZ chain.
func (a *API) GetDZLedger(w http.ResponseWriter, r *http.Request) {
	if data, err := a.readPageCache(r.Context(), "dz_ledger"); err == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := FetchLedgerData(ctx, GetDZLedgerRPCURL(), DZFallbackSlotDurationSec)
	if err != nil {
		logError("DZ ledger RPC request failed", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(LedgerResponse{Error: redact.Error(err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// GetSolanaLedger returns ledger telemetry for Solana.
func (a *API) GetSolanaLedger(w http.ResponseWriter, r *http.Request) {
	if data, err := a.readPageCache(r.Context(), "solana_ledger"); err == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := FetchLedgerData(ctx, GetSolanaRPCURL(), SolanaFallbackSlotDurationSec)
	if err != nil {
		logError("Solana ledger RPC request failed", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(LedgerResponse{Error: redact.Error(err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ValidatorPerfGroup holds aggregated performance metrics for a group of validators.
type ValidatorPerfGroup struct {
	ValidatorCount  uint64  `json:"validator_count"`
	AvgVoteLag      float64 `json:"avg_vote_lag"`
	AvgSkipRate     float64 `json:"avg_skip_rate"`
	DelinquentCount uint64  `json:"delinquent_count"`
	TotalStakeSOL   float64 `json:"total_stake_sol"`
}

// ValidatorPerfResponse compares DZ vs non-DZ validator performance.
type ValidatorPerfResponse struct {
	OnDZ  ValidatorPerfGroup `json:"on_dz"`
	OffDZ ValidatorPerfGroup `json:"off_dz"`
	Error string             `json:"error,omitempty"`
}

const validatorPerfQuery = `
SELECT
	dz_status,
	count(*) AS validator_count,
	round(avg(avg_vote_lag_slots), 2) AS avg_vote_lag,
	round(avg(skip_rate_pct), 2) AS avg_skip_rate,
	countIf(is_delinquent) AS delinquent_count,
	round(sum(activated_stake_sol), 0) AS total_stake_sol
FROM solana_validators_performance_current
GROUP BY dz_status
`

// FetchValidatorPerfData fetches aggregated validator performance data.
func (a *API) FetchValidatorPerfData(ctx context.Context) (*ValidatorPerfResponse, error) {
	rows, err := a.DB.Query(ctx, validatorPerfQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp ValidatorPerfResponse
	for rows.Next() {
		var (
			dzStatus       string
			validatorCount uint64
			avgVoteLag     float64
			avgSkipRate    float64
			delinquentCnt  uint64
			totalStakeSOL  float64
		)
		if err := rows.Scan(&dzStatus, &validatorCount, &avgVoteLag, &avgSkipRate, &delinquentCnt, &totalStakeSOL); err != nil {
			logError("validator performance scan failed", "error", err)
			continue
		}
		group := ValidatorPerfGroup{
			ValidatorCount:  validatorCount,
			AvgVoteLag:      avgVoteLag,
			AvgSkipRate:     avgSkipRate,
			DelinquentCount: delinquentCnt,
			TotalStakeSOL:   totalStakeSOL,
		}
		switch dzStatus {
		case "on_dz":
			resp.OnDZ = group
		case "off_dz":
			resp.OffDZ = group
		}
	}

	return &resp, nil
}

// GetValidatorPerformance returns aggregated validator performance comparing DZ vs non-DZ.
func (a *API) GetValidatorPerformance(w http.ResponseWriter, r *http.Request) {
	if data, err := a.readPageCache(r.Context(), "validator_perf"); err == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := a.FetchValidatorPerfData(ctx)
	if err != nil {
		logError("validator performance query failed", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ValidatorPerfResponse{Error: redact.Error(err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
