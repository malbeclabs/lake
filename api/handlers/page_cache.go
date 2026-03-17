package handlers

import (
	"context"
	"log/slog"
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

// PageCache provides periodic background caching for page endpoints.
// This ensures fast initial page loads by pre-computing expensive queries.
type PageCache struct {
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
	publisherCheck    *PublisherCheckResponse // default publisher check (no filter, epochs=2)

	// Refresh intervals
	statusInterval         time.Duration
	linkHistoryInterval    time.Duration
	timelineInterval       time.Duration
	incidentsInterval      time.Duration
	performanceInterval    time.Duration // for latency comparison and metro path latency
	ledgerInterval         time.Duration // for ledger, validator perf, and stake overview
	publisherCheckInterval time.Duration

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
	publisherCheckLastRefresh    time.Time

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
	fn       func() (bool, string) // returns (success, errorDetail)
}

// NewPageCache creates a new cache with the specified refresh intervals.
func NewPageCache(statusInterval, linkHistoryInterval, timelineInterval, incidentsInterval, performanceInterval, ledgerInterval, publisherCheckInterval time.Duration) *PageCache {
	ctx, cancel := context.WithCancel(context.Background())
	return &PageCache{
		linkHistory:            make(map[string]*LinkHistoryResponse),
		deviceHistory:          make(map[string]*DeviceHistoryResponse),
		metroPathLatency:       make(map[string]*MetroPathLatencyResponse),
		statusInterval:         statusInterval,
		linkHistoryInterval:    linkHistoryInterval,
		timelineInterval:       timelineInterval,
		incidentsInterval:      incidentsInterval,
		performanceInterval:    performanceInterval,
		ledgerInterval:         ledgerInterval,
		publisherCheckInterval: publisherCheckInterval,
		ctx:                    ctx,
		cancel:                 cancel,
	}
}

// Start begins the background refresh loop.
// It performs an initial refresh synchronously to ensure cache is warm before returning.
func (c *PageCache) Start() {
	slog.Info("starting page cache", "status_interval", c.statusInterval, "link_history_interval", c.linkHistoryInterval, "timeline_interval", c.timelineInterval, "incidents_interval", c.incidentsInterval, "performance_interval", c.performanceInterval, "ledger_interval", c.ledgerInterval, "publisher_check_interval", c.publisherCheckInterval)

	// Initial refresh (concurrent to reduce startup time, but cache is warm before returning)
	start := time.Now()
	g, _ := errgroup.WithContext(c.ctx)
	// Use higher concurrency at startup since no API requests are competing
	// for the connection pool yet. Keep it moderate to avoid saturating
	// ClickHouse CPU/IO which causes query timeouts.
	g.SetLimit(4)
	for _, fn := range []func() (bool, string){
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
		c.refreshPublisherCheck,
	} {
		g.Go(func() error {
			fn()
			return nil
		})
	}
	_ = g.Wait()
	slog.Info("all caches warmed", "duration", time.Since(start).Round(time.Millisecond))

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
func (c *PageCache) refreshLoop() {
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
		{"publisher check", c.publisherCheckInterval, c.refreshPublisherCheck},
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
					ok, errDetail := entry.fn()
					if ok {
						if consecutiveFailures[i] > 1 {
							slog.Info("cache recovered", "cache", entry.name, "after_failures", consecutiveFailures[i])
						}
						consecutiveFailures[i] = 0
					} else {
						consecutiveFailures[i]++
						switch consecutiveFailures[i] {
						case 1:
							slog.Info("cache refresh unsuccessful, will retry next cycle", "cache", entry.name, "detail", errDetail)
						case 3:
							slog.Warn("cache refresh failing persistently, backing off", "cache", entry.name, "failures", 3, "detail", errDetail)
						default:
							slog.Info("cache refresh unsuccessful", "cache", entry.name, "failures", consecutiveFailures[i], "detail", errDetail)
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
func (c *PageCache) Stop() {
	slog.Info("stopping page cache")
	c.cancel()

	// Wait for goroutines to exit with a timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("page cache stopped")
	case <-time.After(cacheStopTimeout):
		slog.Warn("page cache stop timed out, continuing shutdown")
	}
}

// IsReady returns true if the cache has been populated with initial data.
func (c *PageCache) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status != nil && c.timeline != nil && c.incidents != nil
}

// GetStatus returns the cached status response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *PageCache) GetStatus() *StatusResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// GetLinkHistory returns the cached link history response for the given parameters.
// Returns nil if the specific configuration is not cached.
func (c *PageCache) GetLinkHistory(timeRange string, buckets int) *LinkHistoryResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := linkHistoryCacheKey(timeRange, buckets)
	return c.linkHistory[key]
}

// GetDeviceHistory returns the cached device history response for the given parameters.
// Returns nil if the specific configuration is not cached.
func (c *PageCache) GetDeviceHistory(timeRange string, buckets int) *DeviceHistoryResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := deviceHistoryCacheKey(timeRange, buckets)
	return c.deviceHistory[key]
}

// GetTimeline returns the cached default timeline response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *PageCache) GetTimeline() *TimelineResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timeline
}

// GetIncidents returns the cached default incidents response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *PageCache) GetIncidents() *LinkIncidentsResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.incidents
}

// GetDeviceIncidents returns the cached default device incidents response.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *PageCache) GetDeviceIncidents() *DeviceIncidentsResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.deviceIncidents
}

// GetLatencyComparison returns the cached DZ vs Internet latency comparison.
// Returns nil if cache is empty (should not happen after Start() completes).
func (c *PageCache) GetLatencyComparison() *LatencyComparisonResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latencyComparison
}

// GetMetroPathLatency returns the cached metro path latency for the given optimize strategy.
// Returns nil if the specific strategy is not cached.
func (c *PageCache) GetMetroPathLatency(optimize string) *MetroPathLatencyResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metroPathLatency[optimize]
}

// refreshStatus fetches fresh status data and updates the cache.
func (c *PageCache) refreshStatus() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp := fetchStatusData(ctx)

	if resp.Error != "" {
		return false, resp.Error
	}

	c.mu.Lock()
	c.status = resp
	c.statusLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("status cache refreshed", "duration", time.Since(start))
	return true, ""
}

// refreshLinkHistory fetches fresh link history data for all configured ranges.
func (c *PageCache) refreshLinkHistory() (bool, string) {
	start := time.Now()
	var lastErr string

	// Refresh all common configurations
	for _, cfg := range linkHistoryConfigs {
		ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
		resp, err := fetchLinkHistoryData(ctx, cfg.timeRange, cfg.buckets)
		cancel()

		if err != nil {
			lastErr = err.Error()
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

	if lastErr != "" {
		return false, lastErr
	}
	slog.Debug("link history cache refreshed", "duration", time.Since(start), "configs", len(linkHistoryConfigs))
	return true, ""
}

// refreshDeviceHistory fetches fresh device history data for all configured ranges.
func (c *PageCache) refreshDeviceHistory() (bool, string) {
	start := time.Now()
	var lastErr string

	// Refresh all common configurations
	for _, cfg := range deviceHistoryConfigs {
		ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
		resp, err := fetchDeviceHistoryData(ctx, cfg.timeRange, cfg.buckets)
		cancel()

		if err != nil {
			lastErr = err.Error()
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

	if lastErr != "" {
		return false, lastErr
	}
	slog.Debug("device history cache refreshed", "duration", time.Since(start), "configs", len(deviceHistoryConfigs))
	return true, ""
}

// refreshTimeline fetches fresh timeline data for the default 24h view.
func (c *PageCache) refreshTimeline() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultTimelineData(ctx)

	if ctx.Err() != nil {
		return false, ctx.Err().Error()
	}

	c.mu.Lock()
	c.timeline = resp
	c.timelineLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("timeline cache refreshed", "duration", time.Since(start), "events", len(resp.Events))
	return true, ""
}

// refreshIncidents fetches fresh incidents data for the default 24h view.
func (c *PageCache) refreshIncidents() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultIncidentsData(ctx)

	if ctx.Err() != nil {
		return false, ctx.Err().Error()
	}

	if resp == nil {
		return false, "nil response"
	}

	c.mu.Lock()
	c.incidents = resp
	c.incidentsLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("incidents cache refreshed", "duration", time.Since(start), "active", len(resp.Active), "drained", len(resp.Drained))
	return true, ""
}

// refreshDeviceIncidents fetches fresh device incidents data for the default 24h view.
func (c *PageCache) refreshDeviceIncidents() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp := fetchDefaultDeviceIncidentsData(ctx)

	if ctx.Err() != nil {
		return false, ctx.Err().Error()
	}

	if resp == nil {
		return false, "nil response"
	}

	c.mu.Lock()
	c.deviceIncidents = resp
	c.deviceIncidentsLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("device incidents cache refreshed", "duration", time.Since(start), "active", len(resp.Active), "drained", len(resp.Drained))
	return true, ""
}

// refreshLatencyComparison fetches fresh DZ vs Internet latency comparison data.
func (c *PageCache) refreshLatencyComparison() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	resp, err := fetchLatencyComparisonData(ctx)
	if err != nil {
		return false, err.Error()
	}

	c.mu.Lock()
	c.latencyComparison = resp
	c.latencyComparisonLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("latency comparison cache refreshed", "duration", time.Since(start), "comparisons", len(resp.Comparisons))
	return true, ""
}

// refreshMetroPathLatency fetches fresh metro path latency data for all optimization strategies.
func (c *PageCache) refreshMetroPathLatency() (bool, string) {
	start := time.Now()
	var lastErr string

	// Cache all three optimization strategies
	strategies := []string{"latency", "hops", "bandwidth"}
	for _, strategy := range strategies {
		ctx, cancel := context.WithTimeout(c.ctx, 45*time.Second)
		resp, err := fetchMetroPathLatencyData(ctx, strategy)
		cancel()

		if err != nil {
			lastErr = err.Error()
			continue
		}

		c.mu.Lock()
		c.metroPathLatency[strategy] = resp
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.metroPathLatencyLastRefresh = time.Now()
	c.mu.Unlock()

	if lastErr != "" {
		return false, lastErr
	}
	slog.Debug("metro path latency cache refreshed", "duration", time.Since(start), "strategies", len(strategies))
	return true, ""
}

// GetDZLedger returns the cached DZ ledger response.
func (c *PageCache) GetDZLedger() *LedgerResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dzLedger
}

// GetSolanaLedger returns the cached Solana ledger response.
func (c *PageCache) GetSolanaLedger() *LedgerResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.solanaLedger
}

// GetValidatorPerf returns the cached validator performance response.
func (c *PageCache) GetValidatorPerf() *ValidatorPerfResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.validatorPerf
}

// GetStakeOverview returns the cached stake overview response.
func (c *PageCache) GetStakeOverview() *StakeOverview {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stakeOverview
}

func (c *PageCache) refreshDZLedger() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchLedgerData(ctx, getDZLedgerRPCURL())
	if err != nil {
		return false, err.Error()
	}

	c.mu.Lock()
	c.dzLedger = resp
	c.ledgerLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("dz ledger cache refreshed", "duration", time.Since(start))
	return true, ""
}

func (c *PageCache) refreshSolanaLedger() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchLedgerData(ctx, getSolanaRPCURL())
	if err != nil {
		return false, err.Error()
	}

	c.mu.Lock()
	c.solanaLedger = resp
	c.mu.Unlock()

	slog.Debug("solana ledger cache refreshed", "duration", time.Since(start))
	return true, ""
}

func (c *PageCache) refreshValidatorPerf() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchValidatorPerfData(ctx)
	if err != nil {
		return false, err.Error()
	}

	c.mu.Lock()
	c.validatorPerf = resp
	c.mu.Unlock()

	slog.Debug("validator perf cache refreshed", "duration", time.Since(start))
	return true, ""
}

func (c *PageCache) refreshStakeOverview() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	resp, err := fetchStakeOverviewData(ctx)
	if err != nil {
		return false, err.Error()
	}

	c.mu.Lock()
	c.stakeOverview = resp
	c.mu.Unlock()

	slog.Debug("stake overview cache refreshed", "duration", time.Since(start))
	return true, ""
}

// GetPublisherCheck returns the cached default publisher check response.
func (c *PageCache) GetPublisherCheck() *PublisherCheckResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.publisherCheck
}

func (c *PageCache) refreshPublisherCheck() (bool, string) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
	defer cancel()

	resp, err := fetchPublisherCheckData(ctx, "", 2, 0)
	if err != nil {
		return false, err.Error()
	}

	c.mu.Lock()
	c.publisherCheck = resp
	c.publisherCheckLastRefresh = time.Now()
	c.mu.Unlock()

	slog.Debug("publisher check cache refreshed", "duration", time.Since(start), "publishers", len(resp.Publishers))
	return true, ""
}

func linkHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}

func deviceHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}

// Global cache instance
var pageCache *PageCache

// InitPageCache initializes the global page cache.
// Should be called once during server startup.
func InitPageCache() {
	pageCache = NewPageCache(
		30*time.Second,  // Status refresh every 30s
		60*time.Second,  // Link history refresh every 60s
		30*time.Second,  // Timeline refresh every 30s
		60*time.Second,  // Incidents refresh every 60s
		120*time.Second, // Performance (latency comparison, metro path latency) refresh every 120s
		60*time.Second,  // Ledger (DZ/Solana ledger, validator perf, stake overview) refresh every 60s
		30*time.Second,  // Publisher check refresh every 30s
	)
	pageCache.Start()
}

// StopPageCache stops the global page cache.
// Should be called during server shutdown.
func StopPageCache() {
	if pageCache != nil {
		pageCache.Stop()
	}
}

// IsPageCacheReady returns true if the page cache is initialized and populated.
func IsPageCacheReady() bool {
	return pageCache != nil && pageCache.IsReady()
}
