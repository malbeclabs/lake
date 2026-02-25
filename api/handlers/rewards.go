package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/api/rewards"
	"golang.org/x/sync/errgroup"
)

// rewardsCache holds the background-computed Shapley results.
var rewardsCache *rewards.RewardsCache

// SetRewardsCache sets the global rewards cache instance.
func SetRewardsCache(rc *rewards.RewardsCache) {
	rewardsCache = rc
}

// maxRewardsBody limits request body size for rewards POST endpoints (5 MB).
const maxRewardsBody = 5 * 1024 * 1024

// GetRewardsSimulate handles GET /api/rewards/simulate.
// Returns pre-computed Shapley results from the background cache.
func GetRewardsSimulate(w http.ResponseWriter, r *http.Request) {
	if rewardsCache == nil || !rewardsCache.IsReady() {
		http.Error(w, "rewards simulation is computing, please try again shortly", http.StatusServiceUnavailable)
		return
	}

	results, total, computedAt, epoch := rewardsCache.GetSimulation()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"results":     results,
		"total_value": total,
		"computed_at": computedAt.UTC().Format(time.RFC3339),
		"epoch":       epoch,
	})
}

// PostRewardsCompare handles POST /api/rewards/compare.
// Body: { "baseline": <ShapleyInput>, "modified": <ShapleyInput> }
// Uses cached baseline results when available (skips one ~2min simulation).
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

	var baselineResults, modifiedResults []rewards.OperatorValue

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Use cached baseline when available (the frontend always sends the
		// unmodified live network as baseline, which matches the cache).
		if rewardsCache != nil && rewardsCache.IsReady() {
			cachedResults, _, _, _ := rewardsCache.GetSimulation()
			baselineResults = cachedResults
			return nil
		}
		const collapseThreshold = 5
		baseline := rewards.CollapseSmallOperators(req.Baseline, collapseThreshold)
		var err error
		baselineResults, err = rewards.Simulate(gctx, baseline)
		if err != nil {
			return fmt.Errorf("baseline simulation: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		const collapseThreshold = 5
		modified := rewards.CollapseSmallOperators(req.Modified, collapseThreshold)
		var err error
		modifiedResults, err = rewards.Simulate(gctx, modified)
		if err != nil {
			return fmt.Errorf("modified simulation: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Printf("rewards: compare: %v", err)
		http.Error(w, "comparison failed", http.StatusInternalServerError)
		return
	}

	result := rewards.BuildCompareResult(baselineResults, modifiedResults)

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

	// Pass cached baseline results to skip redundant baseline simulation (approx path)
	var cachedBaseline []rewards.OperatorValue
	if rewardsCache != nil && rewardsCache.IsReady() {
		cachedBaseline, _, _, _ = rewardsCache.GetSimulation()
	}

	result, err := rewards.LinkEstimate(ctx, req.Operator, req.Network, cachedBaseline)
	if err != nil {
		log.Printf("rewards: link estimate: %v", err)
		http.Error(w, "link estimate failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// GetRewardsLiveNetwork handles GET /api/rewards/live-network.
// Returns the current network topology, preferring the cache.
func GetRewardsLiveNetwork(w http.ResponseWriter, r *http.Request) {
	// Serve from cache if available
	if rewardsCache != nil {
		if cached := rewardsCache.GetLiveNetwork(); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(cached)
			return
		}
	}

	// Fall back to direct ClickHouse query
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
