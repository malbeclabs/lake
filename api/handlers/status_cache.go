package handlers

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const cacheStopTimeout = 5 * time.Second

// maxConcurrentRefreshes limits how many cache refreshes can run simultaneously.
// With a limit of 2, worst case is status (10 queries) + timeline (10 queries)
// = 20 connections, leaving 30 of the 50-connection pool for API/agent requests.
const maxConcurrentRefreshes = 2

// refreshCheckInterval is how often the refresh loop checks for due refreshes.
const refreshCheckInterval = 5 * time.Second

// maxBackoffMultiplier caps exponential backoff at 8x the normal interval.
const maxBackoffMultiplier = 8

// StatusCache provides periodic background caching for status endpoints.
// This ensures fast initial page loads by pre-computing expensive queries.
type StatusCache struct {
	mu sync.RWMutex

	// Cached responses
	status            *StatusResponse
	linkHistory       map[string]*LinkHistoryResponse      // keyed by "range:buckets" e.g. "24h:72"
	deviceHistory     map[string]*DeviceHistoryResponse    // keyed by "range:buckets" e.g. "24h:72"
	timeline          *TimelineResponse                    // default 24h timeline
	incidents         *LinkIncidentsResponse               // default 24h incidents
	deviceIncidents   *DeviceIncidentsResponse             // default 24h device incidents
	latencyComparison *LatencyComparisonResponse           // DZ vs Internet latency comparison
	metroPathLatency  map[string]*MetroPathLatencyResponse // keyed by optimize strategy (hops, latency, bandwidth)
	dzLedger          *LedgerResponse
	solanaLedger      *LedgerResponse
	validatorPerf     *ValidatorPerfResponse
	stakeOverview     *StakeOverview

	// Refresh intervals
	statusInterval      time.Duration
	linkHistoryInterval time.Duration
	timelineInterval    time.Duration
	incidentsInterval   time.Duration
	performanceInterval time.Duration // for latency comparison and metro path latency
	ledgerInterval      time.Duration // for ledger, validator perf, and stake overview

	// Last refresh times (for observability)
	statusLastRefresh            time.Time
	linkHistoryLastRefresh       time.Time
	deviceHistoryLastRefresh     time.Time
	timelineLastRefresh          time.Time
	incidentsLastRefresh         time.Time
	deviceIncidentsLastRefresh   time.Time
	latencyComparisonLastRefresh time.Time
	metroPathLatencyLastRefresh  time.Time
	ledgerLastRefresh            time.Time

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// WaitGroup to track running goroutines
	wg sync.WaitGroup
}

// Link history configuration to pre-cache (default only)
var linkHistoryConfigs = []struct {
	timeRange string
	buckets   int
}{
	{"24h", 72}, // 24-hour view (default)
}

// Device history configuration to pre-cache (default only)
var deviceHistoryConfigs = []struct {
	timeRange string
	buckets   int
}{
	{"24h", 72}, // 24-hour view (default)
}

// refreshEntry defines a cache refresh with its scheduling metadata.
type refreshEntry struct {
	name     string
	interval time.Duration
	fn       func() bool // returns true on success, false on failure
}

// NewStatusCache creates a new cache with the specified refresh intervals.
func NewStatusCache(statusInterval, linkHistoryInterval, timelineInterval, incidentsInterval, performanceInterval, ledgerInterval time.Duration) *StatusCache {
	ctx, cancel := context.WithCancel(context.Background())
	return &StatusCache{
		linkHistory:         make(map[string]*LinkHistoryResponse),
		deviceHistory:       make(map[string]*DeviceHistoryResponse),
		metroPathLatency:    make(map[string]*MetroPathLatencyResponse),
		statusInterval:      statusInterval,
		linkHistoryInterval: linkHistoryInterval,
		timelineInterval:    timelineInterval,
		incidentsInterval:   incidentsInterval,
		performanceInterval: performanceInterval,
		ledgerInterval:      ledgerInterval,
		ctx:                 ctx,
		cancel:              cancel,
	}
}

// Start begins the background refresh loop.
// It performs an initial refresh synchronously to ensure cache is warm before returning.
func (c *StatusCache) Start() {
	log.Printf("Starting status cache with intervals: status=%v, linkHistory=%v, timeline=%v, incidents=%v, performance=%v, ledger=%v",
		c.statusInterval, c.linkHistoryInterval, c.timelineInterval, c.incidentsInterval, c.performanceInterval, c.ledgerInterval)

	// Initial refresh (concurrent to reduce startup time, but cache is warm before returning)
	start := time.Now()
	g, _ := errgroup.WithContext(c.ctx)
	// Use higher concurrency at startup since no API requests are competing
	// for the connection pool yet. Keep it moderate to avoid saturating
	// ClickHouse CPU/IO which causes query timeouts.
	g.SetLimit(4)
	for _, fn := range []func() bool{
		c.refreshStatus,
		c.refreshLinkHistory,
		c.refreshDeviceHistory,
		c.refreshTimeline,
		c.refreshIncidents,
		c.refreshDeviceIncidents,
		c.refreshLatencyComparison,
		c.refreshMetroPathLatency,
		c.refreshDZLedger,
		c.refreshSolanaLedger,
		c.refreshValidatorPerf,
		c.refreshStakeOverview,
	} {
		g.Go(func() error {
			fn()
			return nil
		})
	}
	_ = g.Wait()
	log.Printf("All caches warmed in %v", time.Since(start).Round(time.Millisecond))

	// Start a single coordinated refresh loop
	c.wg.Add(1)
	go c.refreshLoop()
}

// refreshLoop is a single coordinated loop that schedules all cache refreshes.
// It replaces 7 independent goroutines with one loop that:
//   - Checks every refreshCheckInterval which refreshes are due
//   - Runs due refreshes in priority order (status/timeline first since they gate readyz)
//   - Limits concurrent refreshes to maxConcurrentRefreshes via errgroup
//   - Guarantees fair scheduling: all refresh types get turns, not just the frequent ones
func (c *StatusCache) refreshLoop() {
	defer c.wg.Done()

	// Priority-ordered: status and timeline gate readyz, so they run first.
	entries := []refreshEntry{
		{"status", c.statusInterval, c.refreshStatus},
		{"timeline", c.timelineInterval, c.refreshTimeline},
		{"incidents", c.incidentsInterval, c.refreshIncidents},
		{"device incidents", c.incidentsInterval, c.refreshDeviceIncidents},
		{"link history", c.linkHistoryInterval, c.refreshLinkHistory},
		{"device history", c.linkHistoryInterval, c.refreshDeviceHistory},
		{"latency comparison", c.performanceInterval, c.refreshLatencyComparison},
		{"metro path latency", c.performanceInterval, c.refreshMetroPathLatency},
		{"dz ledger", c.ledgerInterval, c.refreshDZLedger},
		{"solana ledger", c.ledgerInterval, c.refreshSolanaLedger},
		{"validator perf", c.ledgerInterval, c.refreshValidatorPerf},
		{"stake overview", c.ledgerInterval, c.refreshStakeOverview},
	}

	// Track when each refresh last ran. Initialized to now since Start()
	// already completed the initial synchronous refresh.
	lastRefresh := make([]time.Time, len(entries))
	// Track consecutive failures per entry for exponential backoff.
	// When a refresh keeps failing, we progressively increase the delay
	// to stop it from monopolizing the connection pool.
	consecutiveFailures := make([]int, len(entries))
	now := time.Now()
	for i := range lastRefresh {
		lastRefresh[i] = now
	}

	ticker := time.NewTicker(refreshCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()

			// Collect due refreshes in priority order
			g, _ := errgroup.WithContext(c.ctx)
			g.SetLimit(maxConcurrentRefreshes)

			for i, entry := range entries {
				// Apply exponential backoff: interval * 2^min(failures, maxBackoffMultiplier)
				effectiveInterval := entry.interval
				if consecutiveFailures[i] > 0 {
					multiplier := 1 << min(consecutiveFailures[i], maxBackoffMultiplier)
					effectiveInterval = entry.interval * time.Duration(multiplier)
				}

				if now.Sub(lastRefresh[i]) < effectiveInterval {
					continue
				}
				if c.ctx.Err() != nil {
					break
				}
				i, entry := i, entry
				g.Go(func() error {
					if c.ctx.Err() != nil {
						return nil
					}
					if entry.fn() {
						consecutiveFailures[i] = 0
					} else {
						consecutiveFailures[i]++
						if consecutiveFailures[i] == 3 {
							log.Printf("Cache %q: 3 consecutive failures, backing off", entry.name)
						}
					}
					lastRefresh[i] = time.Now()
					return nil
				})
			}

			_ = g.Wait()

		case <-c.ctx.Done():
			return
		}
	}
}

// Stop cancels the background refresh goroutines and waits for them to exit.
func (c *StatusCache) Stop() {
	log.Println("Stopping status cache...")
	c.cancel()

	// Wait for goroutines to exit with a timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Status cache stopped")
	case <-time.After(cacheStopTimeout):
		log.Println("Status cache stop timed out, continuing shutdown")
	}
}

// IsReady returns true if the cache has been populated with initial data.
func (c *StatusCache) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status != nil && c.timeline != nil && c.incidents != nil
}

// GetStatus returns the cached status response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *StatusCache) GetStatus() *StatusResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// GetLinkHistory returns the cached link history response for the given parameters.
// Returns nil if the specific configuration is not cached.
func (c *StatusCache) GetLinkHistory(timeRange string, buckets int) *LinkHistoryResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := linkHistoryCacheKey(timeRange, buckets)
	return c.linkHistory[key]
}

// GetDeviceHistory returns the cached device history response for the given parameters.
// Returns nil if the specific configuration is not cached.
func (c *StatusCache) GetDeviceHistory(timeRange string, buckets int) *DeviceHistoryResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := deviceHistoryCacheKey(timeRange, buckets)
	return c.deviceHistory[key]
}

// GetTimeline returns the cached default timeline response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *StatusCache) GetTimeline() *TimelineResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timeline
}

// GetIncidents returns the cached default incidents response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *StatusCache) GetIncidents() *LinkIncidentsResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.incidents
}

// GetDeviceIncidents returns the cached default device incidents response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *StatusCache) GetDeviceIncidents() *DeviceIncidentsResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.deviceIncidents
}

// GetLatencyComparison returns the cached DZ vs Internet latency comparison.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *StatusCache) GetLatencyComparison() *LatencyComparisonResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latencyComparison
}

// GetMetroPathLatency returns the cached metro path latency for the given optimize strategy.
// Returns nil if the specific strategy is not cached.
func (c *StatusCache) GetMetroPathLatency(optimize string) *MetroPathLatencyResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metroPathLatency[optimize]
}

// refreshStatus fetches fresh status data and updates the cache.
func (c *StatusCache) refreshStatus() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp := fetchStatusData(ctx)

	if resp.Error != "" {
		log.Printf("Status cache refresh error: %v (keeping stale data)", resp.Error)
		return false
	}

	c.mu.Lock()
	c.status = resp
	c.statusLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Status cache refreshed in %v", time.Since(start))
	return true
}

// refreshLinkHistory fetches fresh link history data for all configured ranges.
func (c *StatusCache) refreshLinkHistory() bool {
	start := time.Now()
	success := true

	// Refresh all common configurations
	for _, cfg := range linkHistoryConfigs {
		ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
		resp, err := fetchLinkHistoryData(ctx, cfg.timeRange, cfg.buckets)
		cancel()

		if err != nil {
			log.Printf("Link history cache refresh error (range=%s, buckets=%d): %v", cfg.timeRange, cfg.buckets, err)
			success = false
			continue
		}
		key := linkHistoryCacheKey(cfg.timeRange, cfg.buckets)
		c.mu.Lock()
		c.linkHistory[key] = resp
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.linkHistoryLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Link history cache refreshed in %v (%d configs)",
		time.Since(start), len(linkHistoryConfigs))
	return success
}

// refreshDeviceHistory fetches fresh device history data for all configured ranges.
func (c *StatusCache) refreshDeviceHistory() bool {
	start := time.Now()
	success := true

	// Refresh all common configurations
	for _, cfg := range deviceHistoryConfigs {
		ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
		resp, err := fetchDeviceHistoryData(ctx, cfg.timeRange, cfg.buckets)
		cancel()

		if err != nil {
			log.Printf("Device history cache refresh error (range=%s, buckets=%d): %v", cfg.timeRange, cfg.buckets, err)
			success = false
			continue
		}
		key := deviceHistoryCacheKey(cfg.timeRange, cfg.buckets)
		c.mu.Lock()
		c.deviceHistory[key] = resp
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.deviceHistoryLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Device history cache refreshed in %v (%d configs)",
		time.Since(start), len(deviceHistoryConfigs))
	return success
}

// refreshTimeline fetches fresh timeline data for the default 24h view.
func (c *StatusCache) refreshTimeline() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultTimelineData(ctx)

	if ctx.Err() != nil {
		log.Printf("Timeline cache refresh error: %v (keeping stale data)", ctx.Err())
		return false
	}

	c.mu.Lock()
	c.timeline = resp
	c.timelineLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Timeline cache refreshed in %v (%d events)", time.Since(start), len(resp.Events))
	return true
}

// refreshIncidents fetches fresh incidents data for the default 24h view.
func (c *StatusCache) refreshIncidents() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultIncidentsData(ctx)

	if ctx.Err() != nil {
		log.Printf("Incidents cache refresh error: %v (keeping stale data)", ctx.Err())
		return false
	}

	if resp == nil {
		log.Printf("Incidents cache refresh returned nil (keeping stale data)")
		return false
	}

	c.mu.Lock()
	c.incidents = resp
	c.incidentsLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Incidents cache refreshed in %v (%d active, %d drained)", time.Since(start), len(resp.Active), len(resp.Drained))
	return true
}

// refreshDeviceIncidents fetches fresh device incidents data for the default 24h view.
func (c *StatusCache) refreshDeviceIncidents() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultDeviceIncidentsData(ctx)

	if ctx.Err() != nil {
		log.Printf("Device incidents cache refresh error: %v (keeping stale data)", ctx.Err())
		return false
	}

	if resp == nil {
		log.Printf("Device incidents cache refresh returned nil (keeping stale data)")
		return false
	}

	c.mu.Lock()
	c.deviceIncidents = resp
	c.deviceIncidentsLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Device incidents cache refreshed in %v (%d active, %d drained)", time.Since(start), len(resp.Active), len(resp.Drained))
	return true
}

// refreshLatencyComparison fetches fresh DZ vs Internet latency comparison data.
func (c *StatusCache) refreshLatencyComparison() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp, err := fetchLatencyComparisonData(ctx)
	if err != nil {
		log.Printf("Latency comparison cache refresh error: %v", err)
		return false
	}

	c.mu.Lock()
	c.latencyComparison = resp
	c.latencyComparisonLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Latency comparison cache refreshed in %v (%d comparisons)", time.Since(start), len(resp.Comparisons))
	return true
}

// refreshMetroPathLatency fetches fresh metro path latency data for all optimization strategies.
func (c *StatusCache) refreshMetroPathLatency() bool {
	start := time.Now()
	success := true

	// Cache all three optimization strategies
	strategies := []string{"latency", "hops", "bandwidth"}
	for _, strategy := range strategies {
		ctx, cancel := context.WithTimeout(c.ctx, 45*time.Second)
		resp, err := fetchMetroPathLatencyData(ctx, strategy)
		cancel()

		if err != nil {
			log.Printf("Metro path latency cache refresh error (optimize=%s): %v", strategy, err)
			success = false
			continue
		}

		c.mu.Lock()
		c.metroPathLatency[strategy] = resp
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.metroPathLatencyLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Metro path latency cache refreshed in %v (%d strategies)", time.Since(start), len(strategies))
	return success
}

// GetDZLedger returns the cached DZ ledger response.
func (c *StatusCache) GetDZLedger() *LedgerResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dzLedger
}

// GetSolanaLedger returns the cached Solana ledger response.
func (c *StatusCache) GetSolanaLedger() *LedgerResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.solanaLedger
}

// GetValidatorPerf returns the cached validator performance response.
func (c *StatusCache) GetValidatorPerf() *ValidatorPerfResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.validatorPerf
}

// GetStakeOverview returns the cached stake overview response.
func (c *StatusCache) GetStakeOverview() *StakeOverview {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stakeOverview
}

func (c *StatusCache) refreshDZLedger() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchLedgerData(ctx, getDZLedgerRPCURL())
	if err != nil {
		log.Printf("DZ ledger cache refresh error: %v (keeping stale data)", err)
		return false
	}

	c.mu.Lock()
	c.dzLedger = resp
	c.ledgerLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("DZ ledger cache refreshed in %v", time.Since(start))
	return true
}

func (c *StatusCache) refreshSolanaLedger() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchLedgerData(ctx, getSolanaRPCURL())
	if err != nil {
		log.Printf("Solana ledger cache refresh error: %v (keeping stale data)", err)
		return false
	}

	c.mu.Lock()
	c.solanaLedger = resp
	c.mu.Unlock()

	log.Printf("Solana ledger cache refreshed in %v", time.Since(start))
	return true
}

func (c *StatusCache) refreshValidatorPerf() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchValidatorPerfData(ctx)
	if err != nil {
		log.Printf("Validator perf cache refresh error: %v (keeping stale data)", err)
		return false
	}

	c.mu.Lock()
	c.validatorPerf = resp
	c.mu.Unlock()

	log.Printf("Validator perf cache refreshed in %v", time.Since(start))
	return true
}

func (c *StatusCache) refreshStakeOverview() bool {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchStakeOverviewData(ctx)
	if err != nil {
		log.Printf("Stake overview cache refresh error: %v (keeping stale data)", err)
		return false
	}

	c.mu.Lock()
	c.stakeOverview = resp
	c.mu.Unlock()

	log.Printf("Stake overview cache refreshed in %v", time.Since(start))
	return true
}

func linkHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}

func deviceHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}

// Global cache instance
var statusCache *StatusCache

// InitStatusCache initializes the global status cache.
// Should be called once during server startup.
func InitStatusCache() {
	statusCache = NewStatusCache(
		30*time.Second,  // Status refresh every 30s
		60*time.Second,  // Link history refresh every 60s
		30*time.Second,  // Timeline refresh every 30s
		60*time.Second,  // Incidents refresh every 60s
		120*time.Second, // Performance (latency comparison, metro path latency) refresh every 120s
		60*time.Second,  // Ledger (DZ/Solana ledger, validator perf, stake overview) refresh every 60s
	)
	statusCache.Start()
}

// StopStatusCache stops the global status cache.
// Should be called during server shutdown.
func StopStatusCache() {
	if statusCache != nil {
		statusCache.Stop()
	}
}

// IsStatusCacheReady returns true if the status cache is initialized and populated.
func IsStatusCacheReady() bool {
	return statusCache != nil && statusCache.IsReady()
}
