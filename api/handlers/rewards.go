package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/api/rewards"
)

// maxRewardsBody limits request body size for rewards POST endpoints (5 MB).
const maxRewardsBody = 5 * 1024 * 1024

// rewardsParams extracts common simulation parameters from query string.
func rewardsParams(r *http.Request) (operatorUptime, contiguityBonus, demandMultiplier float64) {
	operatorUptime = rewards.DefaultOperatorUptime
	contiguityBonus = rewards.DefaultContiguityBonus
	demandMultiplier = rewards.DefaultDemandMultiplier

	if v := r.URL.Query().Get("operator_uptime"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			operatorUptime = f
		}
	}
	if v := r.URL.Query().Get("contiguity_bonus"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			contiguityBonus = f
		}
	}
	if v := r.URL.Query().Get("demand_multiplier"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			demandMultiplier = f
		}
	}
	return
}

// applyParams sets simulation parameters on a ShapleyInput, keeping existing values as defaults.
func applyParams(input *rewards.ShapleyInput, uptime, bonus, multiplier float64) {
	input.OperatorUptime = uptime
	input.ContiguityBonus = bonus
	input.DemandMultiplier = multiplier
}

// GetRewardsSimulate handles GET /api/rewards/simulate.
// Fetches live network from ClickHouse and runs Shapley simulation.
func GetRewardsSimulate(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	uptime, bonus, multiplier := rewardsParams(r)

	start := time.Now()
	liveNet, err := rewards.FetchLiveNetwork(ctx, envDB(ctx))
	metrics.RecordClickHouseQuery(time.Since(start), err)
	if err != nil {
		log.Printf("rewards: fetch live network: %v", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	applyParams(&liveNet.Network, uptime, bonus, multiplier)

	// By default, collapse small operators into "Others" to reduce coalition
	// count (2^n) and keep simulation tractable. Pass full=true to disable.
	const collapseThreshold = 5
	network := rewards.CollapseSmallOperators(liveNet.Network, collapseThreshold)
	if r.URL.Query().Get("full") == "true" {
		network = liveNet.Network
	}

	results, err := rewards.Simulate(ctx, network)
	if err != nil {
		log.Printf("rewards: simulate: %v", err)
		http.Error(w, "simulation failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"results":     results,
		"total_value": totalValue(results),
	})
}

// PostRewardsCompare handles POST /api/rewards/compare.
// Body: { "baseline": <ShapleyInput>, "modified": <ShapleyInput> }
func PostRewardsCompare(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRewardsBody)

	var req struct {
		Baseline rewards.ShapleyInput `json:"baseline"`
		Modified rewards.ShapleyInput `json:"modified"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	result, err := rewards.Compare(ctx, req.Baseline, req.Modified)
	if err != nil {
		log.Printf("rewards: compare: %v", err)
		http.Error(w, "comparison failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// PostRewardsLinkEstimate handles POST /api/rewards/link-estimate.
// Body: { "operator": "name", "network": <ShapleyInput> }
func PostRewardsLinkEstimate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRewardsBody)

	var req struct {
		Operator string               `json:"operator"`
		Network  rewards.ShapleyInput `json:"network"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Operator == "" {
		http.Error(w, "operator is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	result, err := rewards.LinkEstimate(ctx, req.Operator, req.Network)
	if err != nil {
		log.Printf("rewards: link estimate: %v", err)
		http.Error(w, "link estimate failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// GetRewardsLiveNetwork handles GET /api/rewards/live-network.
// Returns the current network topology from ClickHouse as JSON.
func GetRewardsLiveNetwork(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	liveNet, err := rewards.FetchLiveNetwork(ctx, envDB(ctx))
	metrics.RecordClickHouseQuery(time.Since(start), err)
	if err != nil {
		log.Printf("rewards: fetch live network: %v", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(liveNet)
}

func totalValue(results []rewards.OperatorValue) float64 {
	var total float64
	for _, r := range results {
		total += r.Value
	}
	return total
}
