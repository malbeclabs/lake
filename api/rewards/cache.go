package rewards

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	epochPollInterval = 1 * time.Hour
	refreshTimeout    = 10 * time.Minute
	cacheStopTimeout  = 5 * time.Second
)

// RewardsCache computes Shapley values in the background, triggered by epoch changes.
type RewardsCache struct {
	mu          sync.RWMutex
	results     []OperatorValue
	totalValue  float64
	liveNetwork *LiveNetworkResponse
	computedAt  time.Time
	epoch       int64 // epoch of the cached results
	lastEpoch   int64 // last observed epoch from DB
	ready       bool

	db     driver.Conn
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRewardsCache creates a new background rewards cache.
func NewRewardsCache(db driver.Conn) *RewardsCache {
	ctx, cancel := context.WithCancel(context.Background())
	return &RewardsCache{
		db:     db,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start kicks off the background epoch-polling goroutine.
// Does NOT block — the first computation runs asynchronously.
func (c *RewardsCache) Start() {
	c.wg.Add(1)
	go c.loop()
	log.Println("Rewards cache: started (polling every", epochPollInterval, ")")
}

// Stop cancels the background goroutine and waits for it to exit.
func (c *RewardsCache) Stop() {
	log.Println("Rewards cache: stopping...")
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Rewards cache: stopped")
	case <-time.After(cacheStopTimeout):
		log.Println("Rewards cache: stop timed out, continuing shutdown")
	}
}

// IsReady returns true once the first computation has completed.
func (c *RewardsCache) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// GetSimulation returns cached Shapley results, total value, computation time, and epoch.
func (c *RewardsCache) GetSimulation() ([]OperatorValue, float64, time.Time, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.results, c.totalValue, c.computedAt, c.epoch
}

// GetLiveNetwork returns the cached network topology.
func (c *RewardsCache) GetLiveNetwork() *LiveNetworkResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.liveNetwork
}

func (c *RewardsCache) loop() {
	defer c.wg.Done()

	// Run immediately on startup
	c.checkAndRefresh()

	ticker := time.NewTicker(epochPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkAndRefresh()
		case <-c.ctx.Done():
			return
		}
	}
}

// checkAndRefresh queries the current epoch and recomputes if it changed.
func (c *RewardsCache) checkAndRefresh() {
	epoch, err := c.currentEpoch()
	if err != nil {
		log.Printf("Rewards cache: failed to query epoch: %v", err)
		// On first run with no cached data, still try to compute
		if !c.IsReady() {
			log.Println("Rewards cache: no cached data yet, computing anyway...")
			c.refresh(0)
		}
		return
	}

	c.mu.RLock()
	lastEpoch := c.lastEpoch
	ready := c.ready
	c.mu.RUnlock()

	if !ready || epoch > lastEpoch {
		log.Printf("Rewards cache: epoch %d detected (last=%d), computing...", epoch, lastEpoch)
		c.refresh(epoch)
	}
}

func (c *RewardsCache) currentEpoch() (int64, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	var epoch int64
	err := c.db.QueryRow(ctx, "SELECT max(epoch) FROM dim_solana_leader_schedule_current").Scan(&epoch)
	return epoch, err
}

func (c *RewardsCache) refresh(epoch int64) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(c.ctx, refreshTimeout)
	defer cancel()

	liveNet, err := FetchLiveNetwork(ctx, c.db)
	if err != nil {
		log.Printf("Rewards cache: fetch live network failed: %v", err)
		return
	}

	// Collapse small operators to keep coalition count tractable (2^n).
	const collapseThreshold = 5
	network := CollapseSmallOperators(liveNet.Network, collapseThreshold)

	results, err := Simulate(ctx, network)
	if err != nil {
		log.Printf("Rewards cache: simulation failed: %v", err)
		return
	}

	var total float64
	for _, r := range results {
		total += r.Value
	}

	c.mu.Lock()
	c.results = results
	c.totalValue = total
	c.liveNetwork = liveNet
	c.computedAt = time.Now()
	c.epoch = epoch
	c.lastEpoch = epoch
	c.ready = true
	c.mu.Unlock()

	log.Printf("Rewards cache: refreshed in %v (epoch=%d, operators=%d, total=%.4f)",
		time.Since(start), epoch, len(results), total)
}
