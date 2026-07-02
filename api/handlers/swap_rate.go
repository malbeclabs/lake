package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	swapRateCacheTTL      = 60 * time.Second
	defaultTwoZOracleURL  = "https://sol-2z-oracle-api-v1.mainnet-beta.doublezero.xyz"
	twoZOracleURLEnvVar   = "TWOZ_ORACLE_URL"
	twoZOracleSwapRateKey = "/swap-rate"
	swapRateMaxAttempts   = 3
)

var swapRateRetryBackoffs = []time.Duration{200 * time.Millisecond, 500 * time.Millisecond}

type swapRateResponse struct {
	SOLPriceUSD  float64 `json:"sol_price_usd"`
	TwoZPriceUSD float64 `json:"twoz_price_usd"`
	SwapRate     float64 `json:"swap_rate"`
	FetchedAt    int64   `json:"fetched_at"`
}

type swapRateCacheEntry struct {
	resp      swapRateResponse
	fetchedAt time.Time
}

var (
	swapRateHTTPClient = &http.Client{Timeout: 10 * time.Second}
	swapRateMu         sync.Mutex
	swapRateCache      *swapRateCacheEntry
)

func twoZOracleURL() string {
	if u := os.Getenv(twoZOracleURLEnvVar); u != "" {
		return u
	}
	return defaultTwoZOracleURL
}

func (a *API) GetSwapRate(w http.ResponseWriter, r *http.Request) {
	swapRateMu.Lock()
	entry := swapRateCache
	swapRateMu.Unlock()

	if entry != nil && time.Since(entry.fetchedAt) < swapRateCacheTTL {
		writeSwapRateJSON(w, entry.resp, true)
		return
	}

	out, err := fetchSwapRateWithRetry(r.Context())
	if err != nil {
		if entry != nil {
			logWarn("swap rate: oracle fetch failed, serving stale cache", "error", err)
			writeSwapRateJSON(w, entry.resp, true)
			return
		}
		logError("swap rate: oracle fetch failed with no cached fallback", "error", err)
		http.Error(w, "failed to fetch from oracle", http.StatusBadGateway)
		return
	}

	swapRateMu.Lock()
	swapRateCache = &swapRateCacheEntry{resp: out, fetchedAt: time.Now()}
	swapRateMu.Unlock()

	writeSwapRateJSON(w, out, false)
}

func fetchSwapRateWithRetry(ctx context.Context) (swapRateResponse, error) {
	var lastErr error
	for attempt := range swapRateMaxAttempts {
		if attempt > 0 {
			backoff := swapRateRetryBackoffs[attempt-1]
			select {
			case <-ctx.Done():
				return swapRateResponse{}, ctx.Err()
			case <-time.After(backoff):
			}
		}

		out, retry, err := fetchSwapRateOnce(ctx)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if !retry {
			return swapRateResponse{}, err
		}
	}
	return swapRateResponse{}, lastErr
}

func fetchSwapRateOnce(ctx context.Context) (swapRateResponse, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, twoZOracleURL()+twoZOracleSwapRateKey, nil)
	if err != nil {
		return swapRateResponse{}, false, fmt.Errorf("build request: %w", err)
	}

	resp, err := swapRateHTTPClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return swapRateResponse{}, false, err
		}
		return swapRateResponse{}, true, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return swapRateResponse{}, true, fmt.Errorf("oracle status %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return swapRateResponse{}, false, fmt.Errorf("oracle status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return swapRateResponse{}, true, fmt.Errorf("read body: %w", err)
	}

	var raw struct {
		SwapRate     float64 `json:"swapRate"`
		SOLPriceUSD  string  `json:"solPriceUsd"`
		TwoZPriceUSD string  `json:"twozPriceUsd"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return swapRateResponse{}, false, fmt.Errorf("parse body: %w", err)
	}

	solUSD, _ := strconv.ParseFloat(raw.SOLPriceUSD, 64)
	twozUSD, _ := strconv.ParseFloat(raw.TwoZPriceUSD, 64)
	return swapRateResponse{
		SOLPriceUSD:  solUSD,
		TwoZPriceUSD: twozUSD,
		SwapRate:     raw.SwapRate,
		FetchedAt:    time.Now().Unix(),
	}, false, nil
}

func writeSwapRateJSON(w http.ResponseWriter, p swapRateResponse, cached bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	if cached {
		w.Header().Set("X-Cache", "HIT")
	} else {
		w.Header().Set("X-Cache", "MISS")
	}
	if err := json.NewEncoder(w).Encode(p); err != nil {
		logError("failed to encode swap rate response", "error", err)
	}
}
