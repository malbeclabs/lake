package dztelemusage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// InfluxDBClient is an interface for querying InfluxDB interface counter data.
type InfluxDBClient interface {
	// QueryIntfCounters fetches interface counter rows for [start, end).
	// Returned rows contain: time (RFC3339Nano string), dzd_pubkey, host, intf,
	// model_name, serial_number, and all counter field names
	// (e.g. "in-octets", "out-octets", "in-errors", etc.)
	QueryIntfCounters(ctx context.Context, start, end time.Time) ([]map[string]any, error)
	// QueryBaselineCounter fetches the last non-null value of field for each
	// (dzd_pubkey, intf) pair in the window [lookbackStart, windowStart).
	// Returned rows contain: dzd_pubkey, intf, value.
	QueryBaselineCounter(ctx context.Context, field string, lookbackStart, windowStart time.Time) ([]map[string]any, error)
	// Close closes the client and releases resources.
	Close() error
}

// SDKInfluxDBClient implements InfluxDBClient using the official InfluxDB 3 Go SDK (Flight SQL).
// It is kept for compatibility; prefer FluxInfluxDBClient for production use.
type SDKInfluxDBClient struct {
	client *influxdb3.Client
}

// NewSDKInfluxDBClient creates a new SDK-based InfluxDB client using Flight SQL.
func NewSDKInfluxDBClient(host, token, database string) (*SDKInfluxDBClient, error) {
	client, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     host,
		Token:    token,
		Database: database,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create InfluxDB client: %w", err)
	}
	return &SDKInfluxDBClient{client: client}, nil
}

func (c *SDKInfluxDBClient) QueryIntfCounters(ctx context.Context, start, end time.Time) ([]map[string]any, error) {
	sqlQuery := fmt.Sprintf(`
		SELECT
			time,
			dzd_pubkey,
			host,
			intf,
			model_name,
			serial_number,
			"carrier-transitions",
			"in-broadcast-pkts",
			"in-discards",
			"in-errors",
			"in-fcs-errors",
			"in-multicast-pkts",
			"in-octets",
			"in-pkts",
			"in-unicast-pkts",
			"out-broadcast-pkts",
			"out-discards",
			"out-errors",
			"out-multicast-pkts",
			"out-octets",
			"out-pkts",
			"out-unicast-pkts"
		FROM "intfCounters"
		WHERE time >= '%s' AND time < '%s'
	`, start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
	return c.querySQL(ctx, sqlQuery)
}

func (c *SDKInfluxDBClient) QueryBaselineCounter(ctx context.Context, field string, lookbackStart, windowStart time.Time) ([]map[string]any, error) {
	sqlQuery := fmt.Sprintf(`
		SELECT
			dzd_pubkey,
			intf,
			"%s" as value
		FROM (
			SELECT
				dzd_pubkey,
				intf,
				"%s",
				ROW_NUMBER() OVER (PARTITION BY dzd_pubkey, intf ORDER BY time DESC) as rn
			FROM "intfCounters"
			WHERE time >= '%s' AND time < '%s' AND "%s" IS NOT NULL
		) ranked
		WHERE rn = 1
	`, field, field, lookbackStart.Format(time.RFC3339Nano), windowStart.Format(time.RFC3339Nano), field)
	return c.querySQL(ctx, sqlQuery)
}

func (c *SDKInfluxDBClient) querySQL(ctx context.Context, sqlQuery string) ([]map[string]any, error) {
	iterator, err := c.client.Query(ctx, sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var results []map[string]any
	for iterator.Next() {
		value := iterator.Value()
		row := make(map[string]any)
		for k, v := range value {
			row[k] = v
		}
		results = append(results, row)
	}

	if err := iterator.Err(); err != nil {
		return nil, fmt.Errorf("error iterating results: %w", err)
	}

	return results, nil
}

func (c *SDKInfluxDBClient) Close() error {
	if c.client != nil {
		err := c.client.Close()
		if err != nil {
			if isExpectedCloseError(err) {
				return nil
			}
		}
		return err
	}
	return nil
}

func isExpectedCloseError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "connection is closing") ||
		strings.Contains(errStr, "code = Canceled") ||
		strings.Contains(errStr, "grpc: the client connection is closing")
}

type ViewConfig struct {
	Logger          *slog.Logger
	Clock           clockwork.Clock
	InfluxDB        InfluxDBClient
	Bucket          string
	ClickHouse      clickhouse.Client
	RefreshInterval time.Duration
	QueryWindow     time.Duration // How far back to query from InfluxDB
	DZEnv           string        // DZ network environment (e.g. "mainnet-beta", "testnet", "devnet")
}

func (cfg *ViewConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	if cfg.InfluxDB == nil {
		return errors.New("influxdb client is required")
	}
	if cfg.Bucket == "" {
		return errors.New("influxdb bucket is required")
	}
	if cfg.RefreshInterval <= 0 {
		return errors.New("refresh interval must be greater than 0")
	}
	if cfg.QueryWindow <= 0 {
		cfg.QueryWindow = 1 * time.Hour // Default to 1 hour window
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	return nil
}

// baselineCacheTTL is the staleness threshold for the baseline cache.
// During normal operation the cache is updated after every refresh cycle (~60 s),
// so this threshold is only hit on startup or after the indexer has been paused
// longer than 5 minutes, triggering a ClickHouse re-query.
const baselineCacheTTL = 5 * time.Minute

type View struct {
	log       *slog.Logger
	cfg       ViewConfig
	store     *Store
	readyOnce sync.Once
	readyCh   chan struct{}
	refreshMu sync.Mutex // prevents concurrent refreshes

	// baselineCache caches the result of queryBaselineCountersFromClickHouse.
	// refreshMu already serialises refreshes, so no additional lock is needed.
	baselineCache     *CounterBaselines
	baselineCacheTime time.Time
}

func NewView(cfg ViewConfig) (*View, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	store, err := NewStore(StoreConfig{
		Logger:     cfg.Logger,
		ClickHouse: cfg.ClickHouse,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	v := &View{
		log:     cfg.Logger,
		cfg:     cfg,
		store:   store,
		readyCh: make(chan struct{}),
	}

	return v, nil
}

func (v *View) Start(ctx context.Context) {
	go func() {
		v.log.Info("telemetry/usage: starting refresh loop", "interval", v.cfg.RefreshInterval)

		v.safeRefresh(ctx)

		ticker := v.cfg.Clock.NewTicker(v.cfg.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				v.safeRefresh(ctx)
			}
		}
	}()
}

// safeRefresh wraps Refresh with panic recovery to prevent the refresh loop from dying
func (v *View) safeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			v.log.Error("telemetry/usage: refresh panicked", "panic", r)
			metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "panic").Inc()
		}
	}()

	if _, err := v.Refresh(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.log.Error("telemetry/usage: refresh failed", "error", err)
	}
}

func (v *View) Refresh(ctx context.Context) (ingestionlog.RefreshResult, error) {
	var result ingestionlog.RefreshResult

	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	refreshStart := time.Now()
	v.log.Debug("telemetry/usage: refresh started", "start_time", refreshStart)
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("telemetry/usage: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("telemetry-usage").Observe(duration.Seconds())
	}()

	maxTime, err := v.store.GetMaxTimestamp(ctx)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "error").Inc()
		return result, fmt.Errorf("failed to get max timestamp: %w", err)
	}
	if maxTime != nil {
		v.log.Debug("telemetry/usage: found max timestamp", "max_time", maxTime.UTC())
	} else {
		v.log.Debug("telemetry/usage: no existing data, performing initial refresh")
	}

	now := v.cfg.Clock.Now()
	queryWindowStart := now.Add(-v.cfg.QueryWindow)
	var queryStart time.Time

	if maxTime != nil {
		if maxTime.After(queryWindowStart) {
			// Include a small overlap (5 minutes) to catch late-arriving data with past timestamps
			overlap := 5 * time.Minute
			queryStart = maxTime.Add(-overlap)
			newDataWindow := now.Sub(*maxTime)
			totalQueryWindow := now.Sub(queryStart)
			v.log.Debug("telemetry/usage: incremental refresh (data within query window)",
				"maxTime", maxTime.UTC(),
				"queryStart", queryStart.UTC(),
				"now", now.UTC(),
				"newDataWindow", newDataWindow,
				"totalQueryWindow", totalQueryWindow,
				"overlap", overlap)
		} else {
			queryStart = queryWindowStart
			age := now.Sub(*maxTime)
			v.log.Debug("telemetry/usage: data exists but too old, starting from query window",
				"maxTime", maxTime.UTC(),
				"queryStart", queryStart.UTC(),
				"now", now.UTC(),
				"dataAge", age)
		}
	} else {
		queryStart = queryWindowStart
		v.log.Debug("telemetry/usage: initial full refresh", "from", queryStart, "to", now)
	}

	// Always try ClickHouse first; only query InfluxDB if ClickHouse returns 0 baselines
	var baselines *CounterBaselines
	v.log.Debug("telemetry/usage: querying baselines from clickhouse")
	chStart := time.Now()
	chBaselines, err := v.queryBaselineCountersFromClickHouse(ctx, queryStart)
	chDuration := time.Since(chStart)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to query baseline counters from clickhouse", "error", err, "duration", chDuration.String())
		return result, fmt.Errorf("failed to query baseline counters from clickhouse: %w", err)
	} else {
		totalKeys := v.countUniqueBaselineKeys(chBaselines)
		if totalKeys > 0 {
			// ClickHouse has baseline data, use it
			v.log.Info("telemetry/usage: queried baselines from clickhouse", "unique_keys", totalKeys, "duration", chDuration.String())
			baselines = chBaselines
		} else {
			v.log.Warn("telemetry/usage: no baseline data in clickhouse (0 rows), will query influxdb — this triggers expensive 1-year scans", "duration", chDuration.String())
		}
	}

	if baselines == nil {
		metrics.InfluxBaselineFallbackTotal.WithLabelValues(v.cfg.DZEnv).Inc()
		v.log.Warn("telemetry/usage: querying baselines from influxdb (clickhouse returned 0 baselines)")
		baselineCtx, baselineCancel := context.WithTimeout(ctx, 120*time.Second)
		defer baselineCancel()

		influxStart := time.Now()
		baselines, err = v.queryBaselineCounters(baselineCtx, queryStart)
		influxDuration := time.Since(influxStart)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return result, err
			}
			if errors.Is(err, context.DeadlineExceeded) {
				v.log.Warn("telemetry/usage: baseline query timed out, proceeding without baselines", "duration", influxDuration.String())
			} else {
				v.log.Warn("telemetry/usage: failed to query baseline counters from InfluxDB, proceeding without baselines", "error", err, "duration", influxDuration.String())
			}
			baselines = &CounterBaselines{
				InDiscards:  make(map[string]*int64),
				InErrors:    make(map[string]*int64),
				InFCSErrors: make(map[string]*int64),
				OutDiscards: make(map[string]*int64),
				OutErrors:   make(map[string]*int64),
			}
		} else {
			totalKeys := v.countUniqueBaselineKeys(baselines)
			v.log.Info("telemetry/usage: queried baselines from influxdb", "unique_keys", totalKeys, "duration", influxDuration.String())
		}
	}

	if baselines == nil {
		baselines = &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}
	}

	// Query max timestamps per device/interface to skip already-written rows
	// This is needed because we use an overlap window to catch late-arriving data,
	// but we don't want to re-insert rows that were already written
	alreadyWrittenStart := time.Now()
	alreadyWritten, err := v.store.GetMaxTimestampsByKey(ctx, queryStart)
	alreadyWrittenDuration := time.Since(alreadyWrittenStart)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to query already-written timestamps, proceeding without dedup",
			"error", err, "duration", alreadyWrittenDuration.String())
		alreadyWritten = nil
	} else {
		v.log.Debug("telemetry/usage: queried already-written timestamps",
			"keys", len(alreadyWritten), "duration", alreadyWrittenDuration.String())
	}

	// Query InfluxDB for interface usage data
	// Convert times to UTC for InfluxDB query (InfluxDB stores times in UTC)
	queryStartUTC := queryStart.UTC()
	nowUTC := now.UTC()
	usage, endBaselines, err := v.queryInfluxDB(ctx, queryStartUTC, nowUTC, baselines, alreadyWritten)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return result, err
		}
		metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "error").Inc()
		return result, fmt.Errorf("failed to query influxdb: %w", err)
	}
	// Update the baseline cache with the end-of-window values so the next cycle
	// uses what was actually processed rather than re-querying ClickHouse.
	if endBaselines != nil {
		v.baselineCache = endBaselines
		v.baselineCacheTime = now
	}

	v.log.Info("telemetry/usage: queried influxdb", "rows", len(usage), "from", queryStart, "to", now)

	if len(usage) == 0 {
		v.log.Warn("telemetry/usage: no data returned from influxdb query", "from", queryStart, "to", now)
		v.readyOnce.Do(func() {
			close(v.readyCh)
			v.log.Info("telemetry/usage: view is now ready (no data)")
		})
		metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "success").Inc()
		return result, nil
	}

	insertStart := time.Now()
	if err := v.store.InsertInterfaceUsage(ctx, usage); err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "error").Inc()
		return result, fmt.Errorf("failed to insert interface usage data to clickhouse: %w", err)
	}
	insertDuration := time.Since(insertStart)
	v.log.Info("telemetry/usage: inserted data to clickhouse", "rows", len(usage), "duration", insertDuration.String())

	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("telemetry/usage: view is now ready")
	})

	now2 := v.cfg.Clock.Now()
	result.RowsAffected = int64(len(usage))
	result.SourceMaxEventTS = &now2

	metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "success").Inc()
	return result, nil
}

// LinkInfo holds link information for a device/interface
type LinkInfo struct {
	LinkPK   string
	LinkSide string // "A" or "Z"
}

// CounterBaselines holds the last known counter values before the query window
// Key format: "device_pk:intf"
// Only sparse counters (errors/discards) need baselines; non-sparse counters use the first row as baseline
type CounterBaselines struct {
	InDiscards  map[string]*int64
	InErrors    map[string]*int64
	InFCSErrors map[string]*int64
	OutDiscards map[string]*int64
	OutErrors   map[string]*int64
}

func (v *View) queryInfluxDB(ctx context.Context, startTime, endTime time.Time, baselines *CounterBaselines, alreadyWritten MaxTimestampsByKey) ([]InterfaceUsage, *CounterBaselines, error) {
	// InfluxDB uses dzd_pubkey as a tag, which we extract and map to device_pk.
	v.log.Debug("telemetry/usage: executing main influxdb query", "from", startTime.UTC(), "to", endTime.UTC())
	queryStart := time.Now()

	rows, err := v.cfg.InfluxDB.QueryIntfCounters(ctx, startTime, endTime)
	queryDuration := time.Since(queryStart)
	metrics.RecordInfluxQuery(v.cfg.DZEnv, "interface_usage", queryDuration, len(rows), err)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("failed to execute SQL query: %w", err)
	}
	v.log.Info("telemetry/usage: main influxdb query completed", "rows", len(rows), "duration", queryDuration.String())

	// Baselines are already provided from Refresh() - use them as-is

	// Sort rows by time to ensure proper forward-fill
	sortStart := time.Now()
	sort.Slice(rows, func(i, j int) bool {
		timeI := extractStringFromRow(rows[i], "time")
		timeJ := extractStringFromRow(rows[j], "time")
		if timeI == nil || timeJ == nil {
			return false
		}
		ti, errI := time.Parse(time.RFC3339Nano, *timeI)
		if errI != nil {
			ti, _ = time.Parse(time.RFC3339, *timeI)
		}
		tj, errJ := time.Parse(time.RFC3339Nano, *timeJ)
		if errJ != nil {
			tj, _ = time.Parse(time.RFC3339, *timeJ)
		}
		return ti.Before(tj)
	})
	sortDuration := time.Since(sortStart)
	v.log.Debug("telemetry/usage: sorted rows", "rows", len(rows), "duration", sortDuration.String())

	// Build link lookup map from dz_links_current table
	linkLookup, err := v.buildLinkLookup(ctx)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to build link lookup map, proceeding without link information", "error", err)
		linkLookup = make(map[string]LinkInfo)
	} else {
		v.log.Debug("telemetry/usage: built link lookup map", "links", len(linkLookup))
	}

	// Convert rows to InterfaceUsage, tracking last known values per device/interface
	// We need to process in time order to properly forward-fill nulls
	convertStart := time.Now()
	usage, endBaselines, err := v.convertRowsToUsage(rows, baselines, linkLookup, alreadyWritten)
	convertDuration := time.Since(convertStart)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert rows: %w", err)
	}
	v.log.Debug("telemetry/usage: converted rows to usage data", "usage_records", len(usage), "duration", convertDuration.String())

	return usage, endBaselines, nil
}

// buildLinkLookup builds a map from "device_pk:intf" to LinkInfo by querying the dz_links_history table
func (v *View) buildLinkLookup(ctx context.Context) (map[string]LinkInfo, error) {
	lookup := make(map[string]LinkInfo)

	conn, err := v.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	// Query current links from history table using ROW_NUMBER for latest row per entity
	query := `
		WITH ranked AS (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
			FROM dim_dz_links_history
		)
		SELECT
			pk,
			side_a_pk,
			side_a_iface_name,
			side_z_pk,
			side_z_iface_name
		FROM ranked
		WHERE rn = 1 AND is_deleted = 0`
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query links: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var linkPK, sideAPK, sideAIface, sideZPK, sideZIface *string
		if err := rows.Scan(&linkPK, &sideAPK, &sideAIface, &sideZPK, &sideZIface); err != nil {
			return nil, fmt.Errorf("failed to scan link row: %w", err)
		}

		// Add side A mapping
		if sideAPK != nil && sideAIface != nil && *sideAPK != "" && *sideAIface != "" {
			key := fmt.Sprintf("%s:%s", *sideAPK, *sideAIface)
			linkPKVal := ""
			if linkPK != nil {
				linkPKVal = *linkPK
			}
			lookup[key] = LinkInfo{LinkPK: linkPKVal, LinkSide: "A"}
		}

		// Add side Z mapping
		if sideZPK != nil && sideZIface != nil && *sideZPK != "" && *sideZIface != "" {
			key := fmt.Sprintf("%s:%s", *sideZPK, *sideZIface)
			linkPKVal := ""
			if linkPK != nil {
				linkPKVal = *linkPK
			}
			lookup[key] = LinkInfo{LinkPK: linkPKVal, LinkSide: "Z"}
		}
	}

	return lookup, nil
}

// convertRowsToUsage converts rows to InterfaceUsage, using baselines only for the first null
// and forward-filling with the last known value for subsequent nulls.
// For non-sparse counters, the first row per device/interface is used as baseline and not stored.
// The second return value is the end-of-window sparse counter baselines (last seen values of
// in_discards, in_errors, in_fcs_errors, out_discards, out_errors per device/intf key).
// The caller should store these as the baseline for the next refresh cycle.
// If alreadyWritten is provided, rows with timestamps <= the max already written for that key are skipped
func (v *View) convertRowsToUsage(rows []map[string]any, baselines *CounterBaselines, linkLookup map[string]LinkInfo, alreadyWritten MaxTimestampsByKey) ([]InterfaceUsage, *CounterBaselines, error) {
	// Track last known values per device/interface for each counter
	// Key: "device_pk:intf", Value: map of counter name to last value
	lastKnownValues := make(map[string]map[string]*int64)
	// Track whether we've seen the first row for each device/interface
	// For non-sparse counters, we skip storing the first row and use it as baseline
	firstRowSeen := make(map[string]bool)
	// Track last time per device/interface for computing delta_duration
	lastTime := make(map[string]time.Time)

	// All counter field names for updating lastKnownValues on skipped rows
	counterFieldNames := []string{
		"carrier-transitions", "in-broadcast-pkts", "in-discards", "in-errors",
		"in-fcs-errors", "in-multicast-pkts", "in-octets", "in-pkts", "in-unicast-pkts",
		"out-broadcast-pkts", "out-discards", "out-errors", "out-multicast-pkts",
		"out-octets", "out-pkts", "out-unicast-pkts",
	}

	var usage []InterfaceUsage
	totalRows := len(rows)
	logInterval := totalRows / 10 // Log every 10% progress
	if logInterval < 100 {
		logInterval = 100 // But at least every 100 rows
	}

	for i, row := range rows {
		// Log progress periodically
		if i > 0 && i%logInterval == 0 {
			v.log.Debug("telemetry/usage: converting rows", "progress", fmt.Sprintf("%d/%d (%.1f%%)", i, totalRows, float64(i)/float64(totalRows)*100))
		}
		u := &InterfaceUsage{}

		// Extract time (required)
		timeStr := extractStringFromRow(row, "time")
		if timeStr == nil {
			continue // Skip rows without time
		}

		// Try multiple time formats that InfluxDB might return
		// InfluxDB SDK returns time in format: "2006-01-02 15:04:05.999999999 +0000 UTC"
		var t time.Time
		var err error
		timeFormats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999 -0700 UTC", // InfluxDB format with timezone
			"2006-01-02 15:04:05.999999999 +0700 UTC",
			"2006-01-02 15:04:05.999999999 +0000 UTC",
			"2006-01-02 15:04:05.999999 -0700 UTC",
			"2006-01-02 15:04:05.999999 +0700 UTC",
			"2006-01-02 15:04:05.999999 +0000 UTC",
			"2006-01-02 15:04:05.999 -0700 UTC",
			"2006-01-02 15:04:05.999 +0700 UTC",
			"2006-01-02 15:04:05.999 +0000 UTC",
			"2006-01-02 15:04:05 -0700 UTC",
			"2006-01-02 15:04:05 +0700 UTC",
			"2006-01-02 15:04:05 +0000 UTC",
		}

		parsed := false
		for _, format := range timeFormats {
			t, err = time.Parse(format, *timeStr)
			if err == nil {
				parsed = true
				break
			}
		}

		if !parsed {
			continue // Skip rows with invalid time
		}
		u.Time = t

		// Extract string fields
		u.DevicePK = extractStringFromRow(row, "dzd_pubkey")
		u.Host = extractStringFromRow(row, "host")
		u.Intf = extractStringFromRow(row, "intf")
		u.ModelName = extractStringFromRow(row, "model_name")
		u.SerialNumber = extractStringFromRow(row, "serial_number")

		// Extract tunnel ID from interface name if it starts with "Tunnel"
		if u.Intf != nil {
			u.UserTunnelID = extractTunnelIDFromInterface(*u.Intf)
		}

		// Build key for tracking
		var key string
		if u.DevicePK != nil && u.Intf != nil {
			key = fmt.Sprintf("%s:%s", *u.DevicePK, *u.Intf)
		} else {
			// Can't track without key, just extract what we can
			key = ""
		}

		// Initialize lastKnownValues and pre-populate sparse counter baselines.
		// This must happen before the alreadyWritten skip below, otherwise
		// baselines for sparse counters (errors/discards) are never loaded
		// and those counters stay NULL in all subsequent rows.
		if key != "" && lastKnownValues[key] == nil {
			lastKnownValues[key] = make(map[string]*int64)
			if baselines != nil {
				if val := baselines.InDiscards[key]; val != nil {
					lastKnownValues[key]["in-discards"] = val
				}
				if val := baselines.InErrors[key]; val != nil {
					lastKnownValues[key]["in-errors"] = val
				}
				if val := baselines.InFCSErrors[key]; val != nil {
					lastKnownValues[key]["in-fcs-errors"] = val
				}
				if val := baselines.OutDiscards[key]; val != nil {
					lastKnownValues[key]["out-discards"] = val
				}
				if val := baselines.OutErrors[key]; val != nil {
					lastKnownValues[key]["out-errors"] = val
				}
			}
		}

		// Skip rows that have already been written to avoid duplicates
		// This is important because we use an overlap window when refreshing
		if key != "" && alreadyWritten != nil {
			if maxTS, exists := alreadyWritten[key]; exists {
				if !t.After(maxTS) {
					// This row has already been written, skip it
					// But still update lastKnownValues for delta calculations of subsequent rows
					for _, field := range counterFieldNames {
						value := extractInt64FromRow(row, field)
						if value != nil {
							lastKnownValues[key][field] = value
						}
					}
					lastTime[key] = t
					firstRowSeen[key] = true
					continue
				}
			}
		}

		if key != "" {
			if linkInfo, ok := linkLookup[key]; ok {
				u.LinkPK = &linkInfo.LinkPK
				u.LinkSide = &linkInfo.LinkSide
			}
		}

		isFirstRow := key != "" && !firstRowSeen[key]

		// For all counter fields: use value if present, otherwise forward-fill with last known
		// Sparse counters (errors/discards) have baselines from 1-year query
		// Non-sparse counters: first row is used as baseline, not stored.
		// isRate marks counters whose deltas are divided by delta_duration to
		// produce per-second rates (bps, pps). Only rows carrying at least one
		// real isRate value should advance lastTime (see #388).
		allCounterFields := []struct {
			field     string
			dest      **int64
			deltaDest **int64
			baseline  map[string]*int64
			isSparse  bool
			isRate    bool
		}{
			{"carrier-transitions", &u.CarrierTransitions, &u.CarrierTransitionsDelta, nil, false, false},
			{"in-broadcast-pkts", &u.InBroadcastPkts, &u.InBroadcastPktsDelta, nil, false, true},
			{"in-discards", &u.InDiscards, &u.InDiscardsDelta, baselines.InDiscards, true, false},
			{"in-errors", &u.InErrors, &u.InErrorsDelta, baselines.InErrors, true, false},
			{"in-fcs-errors", &u.InFCSErrors, &u.InFCSErrorsDelta, baselines.InFCSErrors, true, false},
			{"in-multicast-pkts", &u.InMulticastPkts, &u.InMulticastPktsDelta, nil, false, true},
			{"in-octets", &u.InOctets, &u.InOctetsDelta, nil, false, true},
			{"in-pkts", &u.InPkts, &u.InPktsDelta, nil, false, true},
			{"in-unicast-pkts", &u.InUnicastPkts, &u.InUnicastPktsDelta, nil, false, true},
			{"out-broadcast-pkts", &u.OutBroadcastPkts, &u.OutBroadcastPktsDelta, nil, false, true},
			{"out-discards", &u.OutDiscards, &u.OutDiscardsDelta, baselines.OutDiscards, true, false},
			{"out-errors", &u.OutErrors, &u.OutErrorsDelta, baselines.OutErrors, true, false},
			{"out-multicast-pkts", &u.OutMulticastPkts, &u.OutMulticastPktsDelta, nil, false, true},
			{"out-octets", &u.OutOctets, &u.OutOctetsDelta, nil, false, true},
			{"out-pkts", &u.OutPkts, &u.OutPktsDelta, nil, false, true},
			{"out-unicast-pkts", &u.OutUnicastPkts, &u.OutUnicastPktsDelta, nil, false, true},
		}

		// For non-sparse counters on first row: extract values and use as baseline, skip storing the row
		// For sparse counters, we still process and store the first row (they have baselines from 1-year query)
		if isFirstRow {
			// Check if we have any non-sparse counter values
			hasNonSparseValues := false
			for _, cf := range allCounterFields {
				if !cf.isSparse {
					value := extractInt64FromRow(row, cf.field)
					if value != nil {
						hasNonSparseValues = true
						break
					}
				}
			}

			if hasNonSparseValues {
				// Extract all counter values and store as baselines
				for _, cf := range allCounterFields {
					value := extractInt64FromRow(row, cf.field)
					if value != nil && key != "" {
						lastKnownValues[key][cf.field] = value
					}
				}
				lastTime[key] = t
				firstRowSeen[key] = true
				continue
			}
			// If no non-sparse values, continue processing normally (sparse counters will be stored)
			firstRowSeen[key] = true
		}

		// Process all counters. Track whether any non-sparse counter had a
		// real (non-forward-filled) value in this row so we know whether to
		// advance lastTime below.
		hasRateCounter := false
		for _, cf := range allCounterFields {
			var currentValue *int64
			value := extractInt64FromRow(row, cf.field)
			if value != nil {
				currentValue = value
				if cf.isRate {
					hasRateCounter = true
				}
			} else if key != "" {
				// Forward-fill with last known value (includes pre-populated baselines)
				if lastKnown, ok := lastKnownValues[key][cf.field]; ok && lastKnown != nil {
					currentValue = lastKnown
				}
			}

			*cf.dest = currentValue

			// Compute delta against last-known value
			if currentValue != nil && key != "" {
				var previousValue *int64
				if lastKnown, ok := lastKnownValues[key][cf.field]; ok && lastKnown != nil {
					previousValue = lastKnown
				}

				if previousValue != nil {
					delta := *currentValue - *previousValue
					*cf.deltaDest = &delta
				}

				// For rate (monotonic) counters, only advance the baseline when the
				// counter moves forward. If the source sends a stale or replayed
				// reading (counter regresses), keep the previous high-water mark so
				// the next row's delta is computed against the last valid value
				// rather than the stale one — preventing inflated bps spikes.
				if !cf.isRate || previousValue == nil || *currentValue >= *previousValue {
					lastKnownValues[key][cf.field] = currentValue
				}
			}
		}

		// Compute delta_duration: time difference from previous measurement.
		// Only advance lastTime when the row carried real non-sparse counter
		// values (octets, pkts, etc.). Rows that only contain sparse counters
		// (e.g. a carrier-transition event) still get a delta_duration from the
		// previous row, but must not advance the clock — otherwise the next
		// real-counter row inherits a tiny duration for a full counter delta,
		// producing wildly inflated rates (see #388).
		if key != "" {
			if lastT, ok := lastTime[key]; ok {
				duration := t.Sub(lastT).Seconds()
				u.DeltaDuration = &duration
			}
			if hasRateCounter {
				lastTime[key] = t
			}
		}

		usage = append(usage, *u)
	}

	// Build end-of-window sparse baselines from lastKnownValues so the caller can
	// carry them forward as the baseline for the next cycle, avoiding a ClickHouse re-query.
	endBaselines := &CounterBaselines{
		InDiscards:  make(map[string]*int64),
		InErrors:    make(map[string]*int64),
		InFCSErrors: make(map[string]*int64),
		OutDiscards: make(map[string]*int64),
		OutErrors:   make(map[string]*int64),
	}
	for key, fields := range lastKnownValues {
		if v := fields["in-discards"]; v != nil {
			endBaselines.InDiscards[key] = v
		}
		if v := fields["in-errors"]; v != nil {
			endBaselines.InErrors[key] = v
		}
		if v := fields["in-fcs-errors"]; v != nil {
			endBaselines.InFCSErrors[key] = v
		}
		if v := fields["out-discards"]; v != nil {
			endBaselines.OutDiscards[key] = v
		}
		if v := fields["out-errors"]; v != nil {
			endBaselines.OutErrors[key] = v
		}
	}

	return usage, endBaselines, nil
}

// queryBaselineCountersFromClickHouse queries ClickHouse for the last non-null counter values before the window start
// for each device/interface combination. Returns error if ClickHouse doesn't have data or query fails.
//
// During steady-state operation the cache is populated after each successful refresh cycle with the
// end-of-window values from convertRowsToUsage, so this query runs only on startup or after a gap
// longer than baselineCacheTTL (5 minutes). The backfill path calls this with historical
// windowStart values and bypasses the cache entirely.
func (v *View) queryBaselineCountersFromClickHouse(ctx context.Context, windowStart time.Time) (*CounterBaselines, error) {
	// Use the cache only for near-real-time refreshes (windowStart within 2× baselineCacheTTL of now).
	// Backfill calls with historical windowStart values bypass the cache.
	now := v.cfg.Clock.Now()
	isRealtime := now.Sub(windowStart) < 2*baselineCacheTTL
	if isRealtime && v.baselineCache != nil && now.Before(v.baselineCacheTime.Add(baselineCacheTTL)) {
		v.log.Debug("telemetry/usage: using cached baselines", "age", now.Sub(v.baselineCacheTime).Round(time.Second))
		return v.baselineCache, nil
	}

	// Query recent data before the window start to find the last non-null values.
	// Use a 7-day lookback — the indexer writes every few minutes, so the last
	// non-null value for any sparse counter should be well within this window.
	// A shorter window is critical because the table has billions of rows and the
	// global max_execution_time (60s) can cause longer lookbacks to time out,
	// leaving baselines empty and breaking forward-fill.
	lookbackStart := windowStart.Add(-7 * 24 * time.Hour)

	baselines := &CounterBaselines{
		InDiscards:  make(map[string]*int64),
		InErrors:    make(map[string]*int64),
		InFCSErrors: make(map[string]*int64),
		OutDiscards: make(map[string]*int64),
		OutErrors:   make(map[string]*int64),
	}

	conn, err := v.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	// Use a single query to fetch all sparse counter baselines at once.
	// This is faster than 5 separate queries and avoids hitting the global
	// max_execution_time limit.
	sqlQuery := `
		SELECT
			device_pk,
			intf,
			argMaxIf(in_discards, event_ts, in_discards IS NOT NULL) as in_discards_val,
			argMaxIf(in_errors, event_ts, in_errors IS NOT NULL) as in_errors_val,
			argMaxIf(in_fcs_errors, event_ts, in_fcs_errors IS NOT NULL) as in_fcs_errors_val,
			argMaxIf(out_discards, event_ts, out_discards IS NOT NULL) as out_discards_val,
			argMaxIf(out_errors, event_ts, out_errors IS NOT NULL) as out_errors_val
		FROM fact_dz_device_interface_counters
		WHERE event_ts >= ? AND event_ts < ?
			AND (in_discards IS NOT NULL OR in_errors IS NOT NULL OR in_fcs_errors IS NOT NULL
				OR out_discards IS NOT NULL OR out_errors IS NOT NULL)
		GROUP BY device_pk, intf
	`

	rows, err := conn.Query(ctx, sqlQuery, lookbackStart, windowStart)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to query baselines from clickhouse", "error", err)
		return baselines, nil
	}
	defer rows.Close()

	for rows.Next() {
		var devicePK, intf *string
		var inDiscards, inErrors, inFCSErrors, outDiscards, outErrors *int64
		if err := rows.Scan(&devicePK, &intf, &inDiscards, &inErrors, &inFCSErrors, &outDiscards, &outErrors); err != nil {
			v.log.Warn("telemetry/usage: failed to scan baseline row", "error", err)
			continue
		}

		if devicePK == nil || intf == nil {
			continue
		}

		key := fmt.Sprintf("%s:%s", *devicePK, *intf)
		if inDiscards != nil {
			baselines.InDiscards[key] = inDiscards
		}
		if inErrors != nil {
			baselines.InErrors[key] = inErrors
		}
		if inFCSErrors != nil {
			baselines.InFCSErrors[key] = inFCSErrors
		}
		if outDiscards != nil {
			baselines.OutDiscards[key] = outDiscards
		}
		if outErrors != nil {
			baselines.OutErrors[key] = outErrors
		}
	}

	if err := rows.Err(); err != nil {
		v.log.Warn("telemetry/usage: error iterating baseline rows", "error", err)
	}

	return baselines, nil
}

// queryBaselineCounters queries InfluxDB for the last non-null counter values before the window start
// for sparse counters (errors/discards) using a 1-year lookback window.
func (v *View) queryBaselineCounters(ctx context.Context, windowStart time.Time) (*CounterBaselines, error) {
	baselines := &CounterBaselines{
		InDiscards:  make(map[string]*int64),
		InErrors:    make(map[string]*int64),
		InFCSErrors: make(map[string]*int64),
		OutDiscards: make(map[string]*int64),
		OutErrors:   make(map[string]*int64),
	}

	// Only query baselines for sparse counters (errors/discards)
	// For non-sparse counters, we use the first row as baseline and don't store it
	counterFields := []struct {
		field    string
		baseline map[string]*int64
	}{
		{"in-discards", baselines.InDiscards},
		{"in-errors", baselines.InErrors},
		{"in-fcs-errors", baselines.InFCSErrors},
		{"out-discards", baselines.OutDiscards},
		{"out-errors", baselines.OutErrors},
	}

	// For sparse counters, use a 1-year window directly (they're sparse, so rows are rare).
	// NOTE: These queries are expensive on InfluxDB — run sequentially to avoid saturating
	// the InfluxDB query budget (25m total in 30s). This path only runs when ClickHouse
	// has no baseline data, which should be rare in steady state.
	lookbackStart := windowStart.Add(-365 * 24 * time.Hour)
	v.log.Warn("telemetry/usage: querying baseline counters from influxdb (sequential to avoid rate limits)",
		"counters", len(counterFields),
		"from", lookbackStart.UTC(),
		"to", windowStart.UTC(),
		"lookback", "1y",
	)

	hasErrors := false
	for _, cf := range counterFields {
		counterStart := time.Now()

		v.log.Info("telemetry/usage: executing influxdb baseline counter query", "counter", cf.field, "from", lookbackStart.UTC(), "to", windowStart.UTC())
		rows, err := v.cfg.InfluxDB.QueryBaselineCounter(ctx, cf.field, lookbackStart, windowStart)
		counterDuration := time.Since(counterStart)
		queryType := "baseline_" + strings.ReplaceAll(cf.field, "-", "_")
		metrics.RecordInfluxQuery(v.cfg.DZEnv, queryType, counterDuration, len(rows), err)
		if err != nil {
			v.log.Warn("telemetry/usage: failed to query baseline counter", "counter", cf.field, "error", err, "duration", counterDuration.String())
			hasErrors = true
			continue
		}

		baselineCount := 0
		for _, row := range rows {
			devicePK := extractStringFromRow(row, "dzd_pubkey")
			intf := extractStringFromRow(row, "intf")
			if devicePK == nil || intf == nil {
				continue
			}
			key := fmt.Sprintf("%s:%s", *devicePK, *intf)
			value := extractInt64FromRow(row, "value")
			if value != nil {
				cf.baseline[key] = value
				baselineCount++
			}
		}
		v.log.Info("telemetry/usage: completed baseline counter query", "counter", cf.field, "baselines", baselineCount, "duration", counterDuration.String())
	}

	if hasErrors {
		// Return partial baselines even if some queries failed
		totalKeys := v.countUniqueBaselineKeys(baselines)
		v.log.Warn("telemetry/usage: some baseline counter queries failed, returning partial baselines", "unique_keys", totalKeys)
	} else {
		totalKeys := v.countUniqueBaselineKeys(baselines)
		v.log.Debug("telemetry/usage: completed all baseline counter queries", "unique_keys", totalKeys)
	}

	return baselines, nil
}

// countUniqueBaselineKeys counts the number of unique device/interface keys across all baseline maps
func (v *View) countUniqueBaselineKeys(baselines *CounterBaselines) int {
	keys := make(map[string]bool)
	for k := range baselines.InDiscards {
		keys[k] = true
	}
	for k := range baselines.InErrors {
		keys[k] = true
	}
	for k := range baselines.InFCSErrors {
		keys[k] = true
	}
	for k := range baselines.OutDiscards {
		keys[k] = true
	}
	for k := range baselines.OutErrors {
		keys[k] = true
	}
	return len(keys)
}

func extractStringFromRow(row map[string]any, key string) *string {
	val, ok := row[key]
	if !ok || val == nil {
		return nil
	}
	switch v := val.(type) {
	case string:
		return &v
	default:
		s := fmt.Sprintf("%v", v)
		return &s
	}
}

func extractInt64FromRow(row map[string]any, key string) *int64 {
	val, ok := row[key]
	if !ok || val == nil {
		return nil
	}
	switch v := val.(type) {
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return &i
		}
		return nil
	case int64:
		return &v
	case uint64:
		i := int64(v)
		return &i
	case int:
		i := int64(v)
		return &i
	case float64:
		i := int64(v)
		return &i
	default:
		return nil
	}
}

// extractTunnelIDFromInterface extracts the tunnel ID from an interface name.
// For interfaces with "Tunnel" prefix (e.g., "Tunnel501"), it returns the numeric part (501).
// Returns nil if the interface name doesn't match the pattern.
func extractTunnelIDFromInterface(intfName string) *int64 {
	if !strings.HasPrefix(intfName, "Tunnel") {
		return nil
	}
	// Extract the numeric part after "Tunnel"
	suffix := intfName[len("Tunnel"):]
	if suffix == "" {
		return nil
	}
	// Parse as int64
	if id, err := strconv.ParseInt(suffix, 10, 64); err == nil {
		return &id
	}
	return nil
}

// Ready returns true if the view has completed at least one successful refresh
func (v *View) Ready() bool {
	select {
	case <-v.readyCh:
		return true
	default:
		return false
	}
}

// WaitReady waits for the view to be ready (has completed at least one successful refresh)
// It returns immediately if already ready, or blocks until ready or context is cancelled.
func (v *View) WaitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while waiting for telemetry-usage view: %w", ctx.Err())
	}
}

// Store returns the underlying store
func (v *View) Store() *Store {
	return v.store
}
