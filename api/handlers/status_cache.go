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
// = 20 connections, leaving 10 of the 30-connection pool for API/agent requests.
const maxConcurrentRefreshes = 2

// refreshCheckInterval is how often the refresh loop checks for due refreshes.
const refreshCheckInterval = 5 * time.Second

// StatusCache provides periodic background caching for status endpoints.
// This ensures fast initial page loads by pre-computing expensive queries.
type StatusCache struct {
	mu sync.RWMutex

	// Cached responses
	status            *StatusResponse
	linkHistory       map[string]*LinkHistoryResponse      // keyed by "range:buckets" e.g. "24h:72"
	deviceHistory     map[string]*DeviceHistoryResponse    // keyed by "range:buckets" e.g. "24h:72"
	timeline          *TimelineResponse                    // default 24h timeline
	outages           *LinkOutagesResponse                 // default 24h outages
	latencyComparison *LatencyComparisonResponse           // DZ vs Internet latency comparison
	metroPathLatency  map[string]*MetroPathLatencyResponse // keyed by optimize strategy (hops, latency, bandwidth)

	// Refresh intervals
	statusInterval      time.Duration
	linkHistoryInterval time.Duration
	timelineInterval    time.Duration
	outagesInterval     time.Duration
	performanceInterval time.Duration // for latency comparison and metro path latency

	// Last refresh times (for observability)
	statusLastRefresh            time.Time
	linkHistoryLastRefresh       time.Time
	deviceHistoryLastRefresh     time.Time
	timelineLastRefresh          time.Time
	outagesLastRefresh           time.Time
	latencyComparisonLastRefresh time.Time
	metroPathLatencyLastRefresh  time.Time

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
	fn       func()
}

// NewStatusCache creates a new cache with the specified refresh intervals.
func NewStatusCache(statusInterval, linkHistoryInterval, timelineInterval, outagesInterval, performanceInterval time.Duration) *StatusCache {
	ctx, cancel := context.WithCancel(context.Background())
	return &StatusCache{
		linkHistory:         make(map[string]*LinkHistoryResponse),
		deviceHistory:       make(map[string]*DeviceHistoryResponse),
		metroPathLatency:    make(map[string]*MetroPathLatencyResponse),
		statusInterval:      statusInterval,
		linkHistoryInterval: linkHistoryInterval,
		timelineInterval:    timelineInterval,
		outagesInterval:     outagesInterval,
		performanceInterval: performanceInterval,
		ctx:                 ctx,
		cancel:              cancel,
	}
}

// Start begins the background refresh loop.
// It performs an initial refresh synchronously to ensure cache is warm before returning.
func (c *StatusCache) Start() {
	log.Printf("Starting status cache with intervals: status=%v, linkHistory=%v, timeline=%v, outages=%v, performance=%v",
		c.statusInterval, c.linkHistoryInterval, c.timelineInterval, c.outagesInterval, c.performanceInterval)

	// Initial refresh (synchronous to ensure cache is warm)
	c.refreshStatus()
	c.refreshLinkHistory()
	c.refreshDeviceHistory()
	c.refreshTimeline()
	c.refreshOutages()
	c.refreshLatencyComparison()
	c.refreshMetroPathLatency()

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
		{"outages", c.outagesInterval, c.refreshOutages},
		{"link history", c.linkHistoryInterval, c.refreshLinkHistory},
		{"device history", c.linkHistoryInterval, c.refreshDeviceHistory},
		{"latency comparison", c.performanceInterval, c.refreshLatencyComparison},
		{"metro path latency", c.performanceInterval, c.refreshMetroPathLatency},
	}

	// Track when each refresh last ran. Initialized to now since Start()
	// already completed the initial synchronous refresh.
	lastRefresh := make([]time.Time, len(entries))
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
				if now.Sub(lastRefresh[i]) < entry.interval {
					continue
				}
				i, entry := i, entry
				g.Go(func() error {
					if c.ctx.Err() != nil {
						return nil
					}
					entry.fn()
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
	return c.status != nil && c.timeline != nil && c.outages != nil
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

// GetOutages returns the cached default outages response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *StatusCache) GetOutages() *LinkOutagesResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.outages
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
func (c *StatusCache) refreshStatus() {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp := fetchStatusData(ctx)

	if resp.Error != "" {
		log.Printf("Status cache refresh error: %v (keeping stale data)", resp.Error)
		return
	}

	c.mu.Lock()
	c.status = resp
	c.statusLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Status cache refreshed in %v", time.Since(start))
}

// refreshLinkHistory fetches fresh link history data for all configured ranges.
func (c *StatusCache) refreshLinkHistory() {
	start := time.Now()

	// Refresh all common configurations
	for _, cfg := range linkHistoryConfigs {
		ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
		resp, err := fetchLinkHistoryData(ctx, cfg.timeRange, cfg.buckets)
		cancel()

		if err != nil {
			log.Printf("Link history cache refresh error (range=%s, buckets=%d): %v", cfg.timeRange, cfg.buckets, err)
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
}

// refreshDeviceHistory fetches fresh device history data for all configured ranges.
func (c *StatusCache) refreshDeviceHistory() {
	start := time.Now()

	// Refresh all common configurations
	for _, cfg := range deviceHistoryConfigs {
		ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
		resp, err := fetchDeviceHistoryData(ctx, cfg.timeRange, cfg.buckets)
		cancel()

		if err != nil {
			log.Printf("Device history cache refresh error (range=%s, buckets=%d): %v", cfg.timeRange, cfg.buckets, err)
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
}

// refreshTimeline fetches fresh timeline data for the default 24h view.
func (c *StatusCache) refreshTimeline() {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultTimelineData(ctx)

	if ctx.Err() != nil {
		log.Printf("Timeline cache refresh error: %v (keeping stale data)", ctx.Err())
		return
	}

	c.mu.Lock()
	c.timeline = resp
	c.timelineLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Timeline cache refreshed in %v (%d events)", time.Since(start), len(resp.Events))
}

// refreshOutages fetches fresh outages data for the default 24h view.
func (c *StatusCache) refreshOutages() {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultOutagesData(ctx)

	if ctx.Err() != nil {
		log.Printf("Outages cache refresh error: %v (keeping stale data)", ctx.Err())
		return
	}

	c.mu.Lock()
	c.outages = resp
	c.outagesLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Outages cache refreshed in %v (%d outages)", time.Since(start), len(resp.Outages))
}

// refreshLatencyComparison fetches fresh DZ vs Internet latency comparison data.
func (c *StatusCache) refreshLatencyComparison() {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp, err := fetchLatencyComparisonData(ctx)
	if err != nil {
		log.Printf("Latency comparison cache refresh error: %v", err)
		return
	}

	c.mu.Lock()
	c.latencyComparison = resp
	c.latencyComparisonLastRefresh = time.Now()
	c.mu.Unlock()

	log.Printf("Latency comparison cache refreshed in %v (%d comparisons)", time.Since(start), len(resp.Comparisons))
}

// refreshMetroPathLatency fetches fresh metro path latency data for all optimization strategies.
func (c *StatusCache) refreshMetroPathLatency() {
	start := time.Now()

	// Cache all three optimization strategies
	strategies := []string{"latency", "hops", "bandwidth"}
	for _, strategy := range strategies {
		ctx, cancel := context.WithTimeout(c.ctx, 45*time.Second)
		resp, err := fetchMetroPathLatencyData(ctx, strategy)
		cancel()

		if err != nil {
			log.Printf("Metro path latency cache refresh error (optimize=%s): %v", strategy, err)
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
		60*time.Second,  // Outages refresh every 60s
		120*time.Second, // Performance (latency comparison, metro path latency) refresh every 120s
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
