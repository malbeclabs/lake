package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/solana"
	"golang.org/x/sync/errgroup"
)

// ledgerCache holds a cached LedgerResponse for a given RPC URL.
type ledgerCache struct {
	mu      sync.RWMutex
	data    []byte // pre-encoded JSON
	expires time.Time
}

const ledgerCacheTTL = 60 * time.Second

var (
	dzLedgerCache     = &ledgerCache{}
	solanaLedgerCache = &ledgerCache{}
)

// LedgerResponse contains ledger/chain telemetry for a Solana-compatible chain.
type LedgerResponse struct {
	// Epoch info
	Epoch        uint64  `json:"epoch"`
	SlotIndex    uint64  `json:"slot_index"`
	SlotsInEpoch uint64  `json:"slots_in_epoch"`
	EpochPct     float64 `json:"epoch_pct"`
	EpochETASec  float64 `json:"epoch_eta_sec"`

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

const (
	defaultDZLedgerRPCURL = "https://doublezero-mainnet-beta.rpcpool.com/db336024-e7a8-46b1-80e5-352dd77060ab"
	avgSlotDurationSec    = 0.4
)

func getDZLedgerRPCURL() string {
	if url := os.Getenv("DZ_LEDGER_RPC_URL"); url != "" {
		return url
	}
	return defaultDZLedgerRPCURL
}

func getSolanaRPCURL() string {
	return solana.GetRPCURL()
}

func getLedger(w http.ResponseWriter, r *http.Request, rpcURL string, cache *ledgerCache) {
	// Check cache first
	cache.mu.RLock()
	if time.Now().Before(cache.expires) && cache.data != nil {
		data := cache.data
		cache.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}
	cache.mu.RUnlock()

	ctx := r.Context()
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

	g.Go(func() error {
		var err error
		supply, err = client.GetSupply(gctx)
		return err
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
		log.Printf("ledger RPC error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(LedgerResponse{Error: err.Error()})
		return
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

	// ETA in seconds
	remainingSlots := epochInfo.SlotsInEpoch - epochInfo.SlotIndex
	epochETASec := float64(remainingSlots) * avgSlotDurationSec

	// Validator summary
	var totalStakeLamports uint64
	for _, v := range voteAccts.Current {
		totalStakeLamports += v.ActivatedStake
	}
	for _, v := range voteAccts.Delinquent {
		totalStakeLamports += v.ActivatedStake
	}

	resp := LedgerResponse{
		Epoch:        epochInfo.Epoch,
		SlotIndex:    epochInfo.SlotIndex,
		SlotsInEpoch: epochInfo.SlotsInEpoch,
		EpochPct:     epochPct,
		EpochETASec:  epochETASec,

		AbsoluteSlot:     epochInfo.AbsoluteSlot,
		BlockHeight:      epochInfo.BlockHeight,
		TransactionCount: epochInfo.TransactionCount,
		SkipRate:         skipRate,

		TPS: tps,

		TotalSupply:       float64(supply.Value.Total) / 1e9,
		CirculatingSupply: float64(supply.Value.Circulating) / 1e9,

		InflationTotal:      inflation.Total * 100,
		InflationValidator:  inflation.Validator * 100,
		InflationFoundation: inflation.Foundation * 100,

		ActiveValidators:     len(voteAccts.Current),
		DelinquentValidators: len(voteAccts.Delinquent),
		TotalStakeSOL:        float64(totalStakeLamports) / 1e9,

		NodeVersion: version.SolanaCore,
	}

	encoded, err := json.Marshal(resp)
	if err != nil {
		log.Printf("ledger marshal error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(LedgerResponse{Error: "internal error"})
		return
	}

	cache.mu.Lock()
	cache.data = encoded
	cache.expires = time.Now().Add(ledgerCacheTTL)
	cache.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

// GetDZLedger returns ledger telemetry for the DZ chain.
func GetDZLedger(w http.ResponseWriter, r *http.Request) {
	getLedger(w, r, getDZLedgerRPCURL(), dzLedgerCache)
}

// GetSolanaLedger returns ledger telemetry for Solana.
func GetSolanaLedger(w http.ResponseWriter, r *http.Request) {
	getLedger(w, r, getSolanaRPCURL(), solanaLedgerCache)
}

// ValidatorPerfGroup holds aggregated performance metrics for a group of validators.
type ValidatorPerfGroup struct {
	ValidatorCount  int     `json:"validator_count"`
	AvgVoteLag      float64 `json:"avg_vote_lag"`
	AvgSkipRate     float64 `json:"avg_skip_rate"`
	DelinquentCount int     `json:"delinquent_count"`
	TotalStakeSOL   float64 `json:"total_stake_sol"`
}

// ValidatorPerfResponse compares DZ vs non-DZ validator performance.
type ValidatorPerfResponse struct {
	OnDZ  ValidatorPerfGroup `json:"on_dz"`
	OffDZ ValidatorPerfGroup `json:"off_dz"`
	Error string             `json:"error,omitempty"`
}

var validatorPerfCache = &ledgerCache{}

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

// GetValidatorPerformance returns aggregated validator performance comparing DZ vs non-DZ.
func GetValidatorPerformance(w http.ResponseWriter, r *http.Request) {
	cache := validatorPerfCache

	cache.mu.RLock()
	if time.Now().Before(cache.expires) && cache.data != nil {
		data := cache.data
		cache.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}
	cache.mu.RUnlock()

	ctx := r.Context()
	rows, err := config.DB.Query(ctx, validatorPerfQuery)
	if err != nil {
		log.Printf("validator performance query error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ValidatorPerfResponse{Error: err.Error()})
		return
	}
	defer rows.Close()

	var resp ValidatorPerfResponse
	for rows.Next() {
		var (
			dzStatus       string
			validatorCount int
			avgVoteLag     float64
			avgSkipRate    float64
			delinquentCnt  int
			totalStakeSOL  float64
		)
		if err := rows.Scan(&dzStatus, &validatorCount, &avgVoteLag, &avgSkipRate, &delinquentCnt, &totalStakeSOL); err != nil {
			log.Printf("validator performance scan error: %v", err)
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

	encoded, err := json.Marshal(resp)
	if err != nil {
		log.Printf("validator performance marshal error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ValidatorPerfResponse{Error: "internal error"})
		return
	}

	cache.mu.Lock()
	cache.data = encoded
	cache.expires = time.Now().Add(ledgerCacheTTL)
	cache.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}
