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
	"github.com/malbeclabs/lake/utils/pkg/logger"
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
	QueryWindow     time.Duration // How far back the initial refresh (empty table) and a beyond-horizon skip (see maxCatchupHorizon) start from
	QueryChunk      time.Duration // Max time span of a single InfluxDB query; larger windows are split into chunks
	DZEnv           string        // DZ network environment (e.g. "mainnet-beta", "testnet", "devnet")
}

// defaultQueryChunk is the QueryChunk a View uses when the config leaves it
// unset. Nothing outside this package sets QueryChunk, so this is the value that
// ships and the one WorstCaseRefreshFluxBudget is computed at.
const defaultQueryChunk = 5 * time.Minute

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
	if cfg.QueryChunk <= 0 {
		// Bound each InfluxDB query to a small span. In steady state the query
		// window is only a few minutes (one chunk), but when the high-water mark
		// falls behind the window grows to QueryWindow; chunking keeps each
		// server-side pivot small enough to stay under InfluxDB Cloud's heap limit.
		cfg.QueryChunk = defaultQueryChunk
	}
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	// The baseline cache only hits while windowStart stays within
	// baselineCacheMaxLag of the watermark; steady-state windowStart lags by
	// refreshOverlap plus up to one capped catch-up step. A QueryChunk large
	// enough to break this would silently regress every refresh to a full
	// baseline re-scan, so reject it at startup rather than rely on the metric.
	if steadyStateLag := refreshOverlap + cfg.QueryChunk*maxCatchupChunks; steadyStateLag >= baselineCacheMaxLag {
		return fmt.Errorf("query chunk %s is too large for the baseline cache: refreshOverlap (%s) + QueryChunk×maxCatchupChunks (%s) must stay below baselineCacheMaxLag (%s)",
			cfg.QueryChunk, refreshOverlap, cfg.QueryChunk*maxCatchupChunks, baselineCacheMaxLag)
	}
	// The beyond-horizon skip lands at now−QueryWindow (see maxCatchupHorizon);
	// a window at or past the horizon would make the "skip" jump backwards
	// behind the watermark. The empty-span age-out needs the full margin: it
	// fires at watermark age > QueryWindow + one capped span (queryEnd must age
	// out of the window), and the horizon skip intercepts at age >
	// maxCatchupHorizon first — without the capped-span headroom a genuine
	// source gap could never age out and would pin ingest at the horizon,
	// re-paging forever.
	if ageOutLag := cfg.QueryWindow + cfg.QueryChunk*maxCatchupChunks; ageOutLag >= maxCatchupHorizon {
		return fmt.Errorf("query window %s plus one capped span (%s) must stay below maxCatchupHorizon (%s)",
			cfg.QueryWindow, cfg.QueryChunk*maxCatchupChunks, maxCatchupHorizon)
	}
	// The adaptive catch-up span (see adjustCatchupSpan) can only shrink while its
	// ceiling sits above the floor. A QueryChunk small enough to put the ceiling
	// at or below minCatchupSpan would silently disable the shrink entirely and
	// restore the #740 identical-window livelock, so reject it at startup.
	if ceiling := cfg.QueryChunk * maxCatchupChunks; ceiling <= minCatchupSpan {
		return fmt.Errorf("query chunk %s is too small: the catch-up span ceiling (QueryChunk×maxCatchupChunks = %s) must exceed minCatchupSpan (%s) or the adaptive shrink cannot fire",
			cfg.QueryChunk, ceiling, minCatchupSpan)
	}
	return nil
}

// baselineLookback bounds the raw ClickHouse baseline scan. The indexer writes
// interface counters every few minutes, so the last non-null sparse-counter
// value is almost always within a couple of days. 2 days keeps the scan far
// under the global max_execution_time (60s) that a 7-day scan repeatedly hit
// during the June backfill. An interface silent longer than this loses its
// baseline (one nil delta on its next report) unless the in-memory cache below
// still carries it forward.
const baselineLookback = 2 * 24 * time.Hour

// refreshOverlap is how far a refresh re-reads behind maxTime to catch
// late-arriving data with past timestamps. It also serves as the forward slack
// in the baseline cache guard: within a single writer ClickHouse's max event_ts
// never exceeds the watermark, so any excursion past watermark+refreshOverlap
// proves another writer filled the region and forces a re-scan.
const refreshOverlap = 5 * time.Minute

// baselineFallbackTimeout bounds the InfluxDB baseline scan taken when the
// in-memory cache misses AND ClickHouse returns zero baselines. It is InfluxDB
// time spent on the same activity deadline as the chunked read, so
// WorstCaseRefreshFluxBudget counts it.
const baselineFallbackTimeout = 120 * time.Second

// baselineCacheMaxLag is how far windowStart may sit behind the cached watermark
// and still reuse the cache. The cache holds sparse-counter state as of the last
// data watermark this process processed, so its validity is judged against that
// watermark, not against wall-clock time. Steady-state incremental refreshes
// query [maxTime-5m, ...], so windowStart lands ~5m below the watermark; the
// margin also covers capped catch-up steps.
//
// It must exceed refreshOverlap plus one capped catch-up step (QueryChunk ×
// maxCatchupChunks, 10m); 20m leaves headroom. ViewConfig.Validate enforces
// this against the operator-settable QueryChunk so a larger chunk can't
// silently regress every refresh to a full re-scan; the runtime signal is
// doublezero_data_indexer_clickhouse_baseline_query_total.
//
// This bounds only the backward direction (windowStart below the watermark).
// The forward direction is bounded separately, by comparing ClickHouse's max
// event_ts against the watermark (see queryBaselineCountersFromClickHouse).
const baselineCacheMaxLag = 20 * time.Minute

// maxCatchupChunks bounds how much NEW data (past maxTime) a single refresh
// ingests: maxCatchupChunks × QueryChunk. After downtime maxTime can fall
// behind the query window, making one refresh span the entire window at once;
// capping it spreads the catch-up over successive refreshes (maxTime advances
// after each) so peak memory stays near steady state.
//
// The cap is anchored at maxTime, not queryStart (see Refresh). queryStart
// always sits refreshOverlap behind maxTime — the overlap re-read is
// load-bearing, not just late-arrival capture. Non-sparse counters (in-octets,
// in-pkts, …) have no ClickHouse baseline; re-reading each key's
// recently-written rows seeds lastKnownValues/firstRowSeen via the dedup path
// in convertRowsToUsage, so the first NEW row per key emits a correct delta.
// Without the re-read, every key whose latest written row sits behind the
// global maxTime — the norm for an unsynchronized multi-device fleet — would
// have its first in-window row consumed as a baseline and silently dropped,
// undercounting its traffic. Catch-up throughput must therefore come from this
// forward cap, never from skipping the overlap.
//
// Two chunks, not one, so catch-up converges (#713): the measured prod refresh
// cadence is ~5m per cycle, and a 1-chunk (5m) cap gained ~1.0× wall clock per
// cycle — a stale watermark never caught up. Two chunks pay lag down ~5m per
// cycle. The cost, paid only while the cap binds (maxTime more than 2 chunks
// behind now): a capped refresh spans overlap + 2 chunks (~15m, 3 Flux
// queries) versus steady state's ≤ overlap + lag (~10m, ≤2 Flux queries) —
// ~1.5× steady-state peak memory and one extra Flux query.
//
// The cap must keep each refresh inside the dzingest activity's
// StartToCloseTimeout; WorstCaseRefreshFluxBudget computes the InfluxDB half of
// that worst case, and dzingest asserts the deadline exceeds it.
// RefreshTelemetryUsage runs under a dedicated 15m StartToCloseTimeout (see
// dzingest/workflow.go) that bounds it plus the ClickHouse
// dedup/baseline/insert work, so ctx is not already expired when
// InsertInterfaceUsage runs and maxTime advances every cycle. (History: a 15m
// span under the old 5m deadline overran on both mainnet-beta and testnet —
// ctx expired before the insert, every catch-up refresh failed, and the window
// stayed pinned at the cap in an unrecoverable loop. The deadline, not the
// span, was the bug.)
//
// This product is now only the CEILING of the span. Because no fixed pair of
// (span, deadline) values holds on every environment — #740 froze staging for
// ~22.6h with the two chosen independently in #711 and #714 — the span shrinks
// on a failed capped cycle and recovers on success (see View.adjustCatchupSpan).
const maxCatchupChunks = 2

// minCatchupSpan is the floor the adaptive catch-up span shrinks to (see
// View.adjustCatchupSpan) and, reused, the step it recovers by on success.
//
// Shrinking below a minute buys nothing: refreshOverlap (5m) is always re-read
// and is load-bearing (it seeds non-sparse counter baselines), so the effective
// window can never drop below ~5m no matter how small the span. At the floor a
// capped window is ~6m — roughly 2-2.5 min of Flux on the staging numbers from
// #740, comfortably inside the activity budget.
//
// The floor does NOT guarantee convergence. If a ~6m window still exceeds the
// budget, the watermark advances at most minCatchupSpan per cycle — slower than
// real time — so lag keeps growing. That is strictly better than #740's
// zero-ingest freeze, and the escalation path is unchanged: the divergence WARN
// below, the watermark-lag gauge, and eventually the maxCatchupHorizon ERROR
// with the exact dropped span.
const minCatchupSpan = 1 * time.Minute

// sameWindowWarnAfter is how many consecutive identical catch-up windows the
// capping log tolerates at INFO before escalating to WARN. With the adaptive
// span this can only trip once the span has bottomed out at minCatchupSpan —
// every shrink changes the window — so it is the "shrinking has run out of
// room" signal, not a general freeze detector.
const sameWindowWarnAfter = 3

// WorstCaseRefreshFluxBudget is how long one capped catch-up refresh can spend
// in InfluxDB alone at the shipping QueryChunk: each chunk is a separate Flux
// query bounded only by the client's per-request HTTP timeout
// (defaultFluxHTTPTimeout) and does not abort on the activity context deadline,
// plus the baseline fallback's own window when the cache misses and ClickHouse
// returns zero baselines.
//
// The dzingest activity's StartToCloseTimeout must exceed this plus the
// ClickHouse dedup/baseline/insert work, or a capped cycle dies on the deadline
// before the insert and the watermark never advances (#740). That relationship
// lived only in prose, which is how #711's 10m deadline and #714's 15m of data
// drifted into incoherence; dzingest asserts it against this helper.
func WorstCaseRefreshFluxBudget() time.Duration {
	span := refreshOverlap + defaultQueryChunk*maxCatchupChunks
	subQueries := int((span + defaultQueryChunk - 1) / defaultQueryChunk) // ceil
	return time.Duration(subQueries)*defaultFluxHTTPTimeout + baselineFallbackTimeout
}

// maxCatchupHorizon is the only remaining hard skip-ahead: a watermark older
// than this jumps to now−QueryWindow and the intervening span is PERMANENTLY
// LOST, so the skip logs at ERROR (pages on-call, once per distinct stall)
// with the exact span — recover it with the admin backfill tooling. Within the
// horizon the refresh always catches up from the watermark instead (paced one
// capped span per cycle, see maxCatchupChunks), so an ingest outage shorter
// than this is recovered rather than dropped.
//
// Convergence: a capped cycle advances the watermark by at most QueryChunk ×
// maxCatchupChunks (10m) while wall clock advances by the cycle time, so lag
// closes at (10m − cycle_time) per cycle — ~5m per cycle at the measured ~5m
// prod cadence, i.e. ~1× real time: a near-24h gap takes on the order of a
// day to clear on its own; run the admin backfill to clear a large gap
// faster. If a cycle sustainedly exceeds 10m (the budgeted worst case under a
// slow InfluxDB is ~12m+, see the timeout note on maxCatchupChunks), lag
// GROWS toward the horizon instead of closing; Refresh logs a WARN when lag
// fails to decrease across consecutive capped cycles and reports the real max
// ingested event_ts as SourceMaxEventTS, so freshness monitoring shows the
// divergence long before the horizon discards anything. The old jump at
// QueryWindow (1h) silently discarded every span that fell behind it (#718).
const maxCatchupHorizon = 24 * time.Hour

type View struct {
	log       *slog.Logger
	cfg       ViewConfig
	store     *Store
	readyOnce sync.Once
	readyCh   chan struct{}
	refreshMu sync.Mutex // prevents concurrent refreshes

	// baselineCache caches the result of queryBaselineCountersFromClickHouse.
	// Both Refresh and BackfillForTimeRange read and write it under refreshMu, so
	// no additional lock is needed.
	// baselineCacheWatermark is the data end (queryEnd of the refresh, or endTime
	// of the backfill chunk) that the cached baselines represent.
	baselineCache          *CounterBaselines
	baselineCacheWatermark time.Time

	// sourceEmptyThrough is the end of the latest capped catch-up span that
	// returned zero ingestible rows AND had already aged out of QueryWindow:
	// the source held nothing there for the full late-arrival window, so the
	// next refresh anchors past it instead of re-querying the same span
	// forever. Without it, removing the old jump-to-now−QueryWindow (#718)
	// would let a genuine source data gap longer than one capped span pin
	// catch-up permanently (maxTime only advances on insert). The age-out
	// condition preserves the pre-#718 late-replay tolerance: a span that is
	// empty merely *right now* (source stall) is re-read every cycle until it
	// is QueryWindow old, so a writer that replays buffered data with past
	// timestamps within QueryWindow loses nothing; gap traversal instead
	// trails now−QueryWindow. Guarded by refreshMu. In-memory only: a restart
	// re-traverses the already-aged gap with cheap zero-row queries, one
	// capped span per cycle.
	sourceEmptyThrough time.Time

	// lastHorizonSkipFrom is the watermark of the last beyond-horizon skip that
	// logged at ERROR. The skip branch re-executes every refresh while the
	// watermark stays put (a source dead longer than the horizon cannot
	// self-clear), and re-paging each cycle for the same ongoing stall adds
	// nothing — repeats log at WARN until the watermark moves and falls behind
	// again (a distinct loss event). Guarded by refreshMu.
	lastHorizonSkipFrom time.Time

	// lastCatchupLag is the watermark lag observed on the previous refresh IF
	// that refresh was capped, anchored at the watermark, AND ingested rows
	// (zero otherwise). Catch-up converges only while a cycle completes faster
	// than the capped span (see maxCatchupHorizon); when lag fails to decrease
	// across consecutive such cycles the refresh WARNs so on-call sees the
	// divergence long before the horizon ERROR. Empty cycles reset it: their
	// lag growth means the source has no rows (bounded separately by the
	// sourceEmptyThrough age-out), not that cycles are slow. Guarded by
	// refreshMu.
	lastCatchupLag time.Duration

	// catchupSpan is how much NEW data (past the watermark) a capped refresh
	// ingests. It starts at, and is ceilinged by, QueryChunk × maxCatchupChunks
	// and self-tunes from each capped cycle's outcome (see adjustCatchupSpan).
	//
	// Without this the window depended only on the watermark, so a capped cycle
	// that failed after the window was computed re-queried the IDENTICAL window
	// next cycle — forever, since maxTime only advances on a successful insert.
	// On staging that pinned the watermark for ~22.6h across 76 cycles (#740).
	// Guarded by refreshMu. In-memory only: a restart pays one full-cost cycle
	// again, same as sourceEmptyThrough and the baseline cache.
	catchupSpan time.Duration

	// lastWindowStart/lastWindowEnd and sameWindowCycles count consecutive CAPPED
	// refreshes that queried the same window, so the capping log can escalate to
	// WARN when the span has bottomed out and shrinking has stopped helping. An
	// uncapped cycle resets them: its window is not pinned. Guarded by refreshMu.
	lastWindowStart  time.Time
	lastWindowEnd    time.Time
	sameWindowCycles int

	// esc escalates consecutive refresh failures from WARN to ERROR so a
	// single blip doesn't page on-call (see logger.Escalator).
	esc logger.Escalator
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
		// Start at the ceiling: a healthy environment behaves exactly as before
		// and only shrinks once a capped cycle actually fails.
		catchupSpan: cfg.QueryChunk * maxCatchupChunks,
		// This view refreshes every ~5 minutes and is the only signal for the
		// InfluxDB dependency, so the default transient threshold (10) would
		// take ~50 minutes to page. Escalate transient causes at the strict
		// threshold (~15 minutes) instead.
		esc: logger.Escalator{TransientErrorAfter: logger.DefaultErrorAfter},
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

	_, err := v.Refresh(ctx)
	if err != nil && errors.Is(err, context.Canceled) {
		return
	}
	v.esc.Observe(v.log, "refresh", "telemetry/usage: refresh failed", err)
}

func (v *View) Refresh(ctx context.Context) (result ingestionlog.RefreshResult, err error) {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	// capped records whether the catch-up span bound this cycle's window. Only a
	// capped cycle can repeat an identical window, so only a capped cycle's
	// outcome adjusts the span. Declared up here so the deferred adjustment can
	// read it; deferred calls run LIFO, so it runs before refreshMu is released
	// (still serialized) and covers every one of Refresh's error returns without
	// touching each site.
	capped := false
	defer func() {
		// A panic unwinds with the named err still nil, which would score the
		// cycle as a success and GROW the span. safeRefresh treats panics as a
		// real mode (it has its own metric label), so re-raise after scoring it
		// as the failure it is.
		if r := recover(); r != nil {
			v.adjustCatchupSpan(capped, fmt.Errorf("refresh panicked: %v", r))
			panic(r)
		}
		v.adjustCatchupSpan(capped, err)
	}()

	refreshStart := time.Now()
	v.log.Debug("telemetry/usage: refresh started", "start_time", refreshStart)
	defer func() {
		duration := time.Since(refreshStart)
		v.log.Info("telemetry/usage: refresh completed", "duration", duration.String())
		metrics.ViewRefreshDuration.WithLabelValues("telemetry-usage").Observe(duration.Seconds())
	}()

	// Note: `:=` here assigns the NAMED err (same scope, only maxTime is new), so
	// the adjustCatchupSpan defer observes real error values. The same holds for
	// every later `x, err := ...` in this function body; inner `if err := ...`
	// shadows are returned explicitly and so are observed too.
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

	// Publish the watermark before any expensive work, so a cycle that later dies
	// on the activity deadline still reports it. Its age (`time() - <gauge>`) then
	// climbs from the first failing cycle instead of a frozen watermark going
	// unnoticed until the ~24h horizon ERROR (#740).
	if maxTime != nil {
		metrics.TelemetryUsageWatermarkTimestampSeconds.WithLabelValues(v.cfg.DZEnv).Set(float64(maxTime.UnixNano()) / 1e9)
	}

	// Effective watermark: ClickHouse's max event_ts, advanced past any span a
	// prior capped refresh queried and proved empty (see sourceEmptyThrough).
	watermark := maxTime
	if watermark != nil && v.sourceEmptyThrough.After(*watermark) {
		emptyThrough := v.sourceEmptyThrough
		watermark = &emptyThrough
	}

	var queryStart time.Time
	if watermark != nil {
		if age := now.Sub(*watermark); age > maxCatchupHorizon {
			queryStart = queryWindowStart
			// chMaxTime is the raw ClickHouse watermark: the effective watermark
			// may sit past it (sourceEmptyThrough), and this log line is the
			// operator's backfill instruction, so report both ends honestly.
			chMaxTime := "none"
			if maxTime != nil {
				chMaxTime = maxTime.UTC().String()
			}
			// Page once per distinct stall (see lastHorizonSkipFrom): first
			// sight of this watermark beyond the horizon logs ERROR, repeats
			// while it stays put log WARN.
			logSkip := v.log.Warn
			if !watermark.Equal(v.lastHorizonSkipFrom) {
				logSkip = v.log.Error
				v.lastHorizonSkipFrom = *watermark
			}
			logSkip("telemetry/usage: watermark is beyond the catch-up horizon; skipping ahead — the intervening span will not be ingested once this refresh succeeds; recover it with the admin backfill",
				"skippedFrom", watermark.UTC(),
				"skippedTo", queryStart.UTC(),
				"skippedSpan", queryStart.Sub(*watermark).String(),
				"chMaxTime", chMaxTime,
				"watermarkAge", age.String(),
				"horizon", maxCatchupHorizon.String())
		} else {
			// Catch up from the watermark, however far behind now it sits
			// (bounded by maxCatchupHorizon above). The forward cap below paces
			// the catch-up one capped span per refresh, so a watermark that
			// fell behind — even past QueryWindow — is recovered instead of
			// skipped (#718; the old jump to now−QueryWindow silently dropped
			// the intervening span).
			//
			// Include a small overlap to catch late-arriving data with past
			// timestamps. The overlap is kept even while far behind (catch-up):
			// beyond late arrivals, the re-read of already-written rows seeds
			// non-sparse counter baselines via the dedup path — without it,
			// every key whose latest row sits behind the global maxTime would
			// have its first new row swallowed as a baseline (see
			// maxCatchupChunks). Catch-up speed comes from the forward cap
			// instead.
			queryStart = watermark.Add(-refreshOverlap)
			v.log.Debug("telemetry/usage: incremental refresh from watermark",
				"watermark", watermark.UTC(),
				"queryStart", queryStart.UTC(),
				"now", now.UTC(),
				"lag", age,
				"overlap", refreshOverlap)
		}
	} else {
		queryStart = queryWindowStart
		v.log.Debug("telemetry/usage: initial full refresh", "from", queryStart, "to", now)
	}

	// Always try ClickHouse first; only query InfluxDB if ClickHouse returns 0 baselines
	var baselines *CounterBaselines
	v.log.Debug("telemetry/usage: querying baselines from clickhouse")
	chStart := time.Now()
	chBaselines, err := v.queryBaselineCountersFromClickHouse(ctx, queryStart, maxTime)
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
		baselineCtx, baselineCancel := context.WithTimeout(ctx, baselineFallbackTimeout)
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
	// Proceeding without dedup was tolerable when baselines were re-scanned from
	// ClickHouse every refresh (re-emitted overlap rows recomputed the same
	// deltas). With the baseline cache holding END-of-window values, re-emitting
	// overlap rows would delta them against those newer values, writing negative
	// sparse deltas that ReplacingMergeTree keeps (fresh event_ts) — so a failed
	// dedup query must fail the refresh instead; the next cycle retries.
	alreadyWrittenStart := time.Now()
	alreadyWritten, err := v.store.GetMaxTimestampsByKey(ctx, queryStart)
	alreadyWrittenDuration := time.Since(alreadyWrittenStart)
	if err != nil {
		metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "error").Inc()
		return result, fmt.Errorf("failed to query already-written timestamps: %w", err)
	}
	v.log.Debug("telemetry/usage: queried already-written timestamps",
		"keys", len(alreadyWritten), "duration", alreadyWrittenDuration.String())

	// Query InfluxDB for interface usage data
	// Convert times to UTC for InfluxDB query (InfluxDB stores times in UTC)
	// Bound how much a single refresh ingests. After downtime the watermark can
	// fall hours behind, making [queryStart, now) span the whole backlog.
	// queryIntfCountersChunked bounds InfluxDB's server-side memory per chunk,
	// but the indexer still materializes every chunk's rows, the converted usage
	// slice, and the ClickHouse batch for the entire span at once — which
	// OOM-crashlooped the pod: it died before InsertInterfaceUsage, so maxTime
	// never advanced and every restart re-ran the same giant query. Capping the
	// span lets a large catch-up proceed across successive refreshes (maxTime
	// advances after each), keeping peak memory near steady state.
	//
	// Anchor the cap at the start of NEW data (the watermark), not queryStart.
	// queryStart sits refreshOverlap (5m) behind the watermark to re-read late
	// arrivals; anchoring the cap there made queryEnd = queryStart + 5m =
	// maxTime, so an incremental refresh could never query past maxTime — it
	// only re-read rows that dedup out and ingest stalled ~1h behind (#708).
	// Anchoring at the watermark makes the steady-state span overlap + one
	// chunk (~10m, 2 Flux queries) so maxTime advances every cycle. When the
	// watermark is nil or older than queryStart (initial refresh / horizon
	// skip), newDataStart falls back to queryStart and the cap behaves as
	// before — one capped span from the window start, preserving the
	// post-downtime memory bound.
	newDataStart := queryStart
	if watermark != nil && watermark.After(newDataStart) {
		newDataStart = *watermark
	}
	queryEnd := now
	if maxCatchup := v.catchupSpan; maxCatchup > 0 && queryEnd.Sub(newDataStart) > maxCatchup {
		queryEnd = newDataStart.Add(maxCatchup)
		capped = true
	}
	if capped {
		// Count consecutive capped refreshes that queried the same window. An
		// uncapped cycle breaks the streak (below): its window is not pinned, so
		// the two are not the same situation.
		if queryStart.Equal(v.lastWindowStart) && queryEnd.Equal(v.lastWindowEnd) {
			v.sameWindowCycles++
		} else {
			v.sameWindowCycles = 1
			v.lastWindowStart, v.lastWindowEnd = queryStart, queryEnd
		}
		// Escalate to WARN only once the window has repeated AND the span is
		// pinned at the floor: shrinking has run out of room, so a human is
		// needed. Both conditions are load-bearing. A repeat alone is benign and
		// expected — a capped span the source has no rows in SUCCEEDS, leaving the
		// span at its ceiling and the watermark where it is, so the same window is
		// re-read every cycle until it ages out of QueryWindow (see the empty-span
		// branch below, which is what bounds that state). WARN, not ERROR — the
		// underlying failure already escalates through the refresh escalator, and
		// one failure yields at most one alert-bearing line.
		logCap := v.log.Info
		if v.sameWindowCycles > sameWindowWarnAfter && v.catchupSpan <= minCatchupSpan {
			logCap = v.log.Warn
		}
		logCap("telemetry/usage: capping catch-up window to bound memory",
			"queryStart", queryStart.UTC(), "queryEnd", queryEnd.UTC(), "target", now.UTC(),
			"cappedSpan", v.catchupSpan.String(), "sameWindowCycles", v.sameWindowCycles)
	} else {
		v.sameWindowCycles = 0
		v.lastWindowStart, v.lastWindowEnd = time.Time{}, time.Time{}
	}
	queryStartUTC := queryStart.UTC()
	queryEndUTC := queryEnd.UTC()
	usage, endBaselines, err := v.queryInfluxDB(ctx, queryStartUTC, queryEndUTC, baselines, alreadyWritten)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return result, err
		}
		metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "error").Inc()
		return result, fmt.Errorf("failed to query influxdb: %w", err)
	}
	v.log.Info("telemetry/usage: queried influxdb", "rows", len(usage), "from", queryStart, "to", queryEnd)

	if len(usage) == 0 {
		v.log.Warn("telemetry/usage: no data returned from influxdb query", "from", queryStart, "to", now)
		// Nothing to insert, so no persistence to wait on. Merge end-of-window
		// baselines (a no-op when the window was genuinely empty) and advance the
		// baseline-cache watermark: nothing changed through queryEnd, so the
		// cached last-known values still hold there and the next refresh can
		// cache-hit.
		//
		// maxTime doesn't advance (nothing was written), so a source data gap
		// longer than the capped span would pin the incremental window at the
		// same [queryStart, queryEnd) forever. A capped span that returned zero
		// ingestible rows AND has aged out of QueryWindow held nothing for the
		// full late-arrival window — advance sourceEmptyThrough so the next
		// refresh anchors past it, traversing the gap one capped span per cycle
		// (trailing now−QueryWindow). A younger empty span is re-read every
		// cycle instead: it may only be empty *yet* (source stall), and a
		// writer replaying buffered data with past timestamps within
		// QueryWindow must lose nothing. Uncapped (steady-state) empty windows
		// keep the watermark where it is: the window isn't pinned (it grows
		// with now), and the wide re-read maximizes late-arrival capture.
		if capped && !queryEnd.After(queryWindowStart) && queryEnd.After(v.sourceEmptyThrough) {
			v.sourceEmptyThrough = queryEnd
			v.log.Info("telemetry/usage: no ingestible rows in aged-out capped span; advancing catch-up anchor past it",
				"emptyFrom", newDataStart.UTC(), "emptyThrough", queryEnd.UTC())
		}
		// Nothing changed through queryEnd, so the start-of-window baselines
		// are also the end-of-window state: merge them too, unless they already
		// ARE the cache (cache hit) — then the merge is a pointless O(keys)
		// self-walk and advancing the watermark below suffices. Merging only
		// `endBaselines` (empty when zero rows came back) would seed a
		// restarted process's nil cache with empty maps, and every later
		// refresh would "hit" a 0-key cache and fall back to the expensive
		// 1-year InfluxDB baseline scan for the rest of the gap traversal.
		if baselines != v.baselineCache {
			v.updateBaselineCache(baselines, queryEnd)
		}
		v.updateBaselineCache(endBaselines, queryEnd)
		// An empty cycle says nothing about cycle-time throughput — lag grows
		// here because the source has no rows, a state the sourceEmptyThrough
		// age-out bounds separately — so it must not feed the divergence
		// comparison below.
		v.lastCatchupLag = 0
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

	// Catch-up only converges while a cycle completes faster than the capped
	// span (see maxCatchupHorizon); surface divergence before the horizon
	// cliff. Compared only across capped cycles that actually ingested rows
	// with the cap anchored at the watermark (newDataStart advanced past
	// queryStart): an initial refresh or horizon skip anchors at the window
	// start, and an empty cycle grows lag because the source has no rows, not
	// because cycles are slow — both reset the comparison instead.
	if capped && !newDataStart.Equal(queryStart) {
		lag := now.Sub(newDataStart)
		if v.lastCatchupLag > 0 && lag >= v.lastCatchupLag {
			v.log.Warn("telemetry/usage: catch-up lag is not decreasing — refresh cycle time is at or above the capped span; ingest is diverging toward the catch-up horizon",
				"lag", lag.String(),
				"previousLag", v.lastCatchupLag.String(),
				"cappedSpan", v.catchupSpan.String(),
				"horizon", maxCatchupHorizon.String())
		}
		v.lastCatchupLag = lag
	} else {
		v.lastCatchupLag = 0
	}

	// Update the baseline cache only after the rows are durably in ClickHouse. A
	// failed insert must not leave the cache holding unpersisted end-of-window
	// values: the next refresh's overlap re-reads those same rows and would
	// compute their first sparse delta against a poisoned baseline. Merge (not
	// replace) so interfaces that reported no rows this window keep their carried
	// baseline instead of being dropped and losing forward-fill.
	v.updateBaselineCache(endBaselines, queryEnd)

	v.readyOnce.Do(func() {
		close(v.readyCh)
		v.log.Info("telemetry/usage: view is now ready")
	})

	// Report the real max ingested event_ts, not wall-clock now: this is what
	// ingestion-log freshness monitoring reads, and during catch-up it must
	// show the actual watermark lag (a diverging catch-up otherwise looks
	// healthy right up to the horizon cliff — see maxCatchupHorizon).
	maxIngested := usage[0].Time
	for _, u := range usage[1:] {
		if u.Time.After(maxIngested) {
			maxIngested = u.Time
		}
	}
	result.RowsAffected = int64(len(usage))
	result.SourceMaxEventTS = &maxIngested

	metrics.ViewRefreshTotal.WithLabelValues("telemetry-usage", "success").Inc()
	return result, nil
}

// adjustCatchupSpan self-tunes the catch-up span from a capped cycle's outcome,
// AIMD-style (the TCP congestion-control shape). It is the fix for #740: a
// capped cycle that failed WILL re-query the same window next cycle, so the
// retry must be made strictly cheaper or the loop repeats identical work
// forever.
//
//   - Failure → multiplicative decrease, halve down to minCatchupSpan. Halving
//     the new-data span shrinks the window and with it the row count, the Flux
//     sub-query count, and the ClickHouse batch.
//   - Success → additive increase, +minCatchupSpan up to the ceiling. Probing up
//     one step at a time is the point: the safe operating point is unknown and
//     drifts with source row density, so jumping back to the ceiling would
//     re-trigger the failure that caused the shrink and turn the loop into a
//     fail/succeed oscillation that pays a full failed cycle every other cycle.
//     Cost: floor→ceiling takes 9 successful cycles, and while the span is below
//     the cycle time lag still grows. Bounded and self-correcting.
//
// The trigger is ANY failure, not just deadline-class ones: #740's cycles failed
// as both a wrapped context.DeadlineExceeded (batch insert) and a bare Flux
// iterator error string, so narrow detection would have missed the case that
// actually froze staging. The property that matters is cause-independent.
// Shrinking on an unrelated failure (ClickHouse down) costs only catch-up
// throughput and unwinds additively once the dependency recovers.
//
// Callers must hold refreshMu.
func (v *View) adjustCatchupSpan(capped bool, err error) {
	// An uncapped cycle's window ends at now, so it is never pinned; a transient
	// blip in steady state must not perturb the catch-up span.
	if !capped {
		return
	}
	// The ceiling is the pre-#740 fixed cap; NewView starts the span there.
	ceiling := v.cfg.QueryChunk * maxCatchupChunks
	switch {
	case err == nil:
		if v.catchupSpan >= ceiling {
			return
		}
		v.catchupSpan = min(v.catchupSpan+minCatchupSpan, ceiling)
	case errors.Is(err, context.Canceled):
		// Pod shutdown says nothing about the cost of the window.
	case v.catchupSpan <= minCatchupSpan:
		// Already at the floor; the repeated-window WARN owns this state.
	default:
		prev := v.catchupSpan
		v.catchupSpan = max(prev/2, minCatchupSpan)
		// WARN, not INFO: this is the only per-cycle line that names an
		// in-progress walk-down, and on an environment whose metrics are not
		// scraped it is what an operator greps for. It stays below ERROR because
		// the underlying failure already escalates through the refresh escalator.
		v.log.Warn("telemetry/usage: capped catch-up cycle failed; halving the catch-up span so the retry cannot repeat the same work",
			"previousSpan", prev.String(),
			"newSpan", v.catchupSpan.String(),
			"floor", minCatchupSpan.String(),
			"ceiling", ceiling.String(),
			"error", err)
	}
}

// updateBaselineCache merges the end-of-window sparse baselines into the cache
// and advances the watermark to the data end just processed. Merging (rather
// than replacing the pointer) preserves baselines for interfaces that reported
// no rows in this window: convertRowsToUsage only emits keys it saw rows for, so
// a wholesale replace would drop a silent interface's baseline and — since the
// cache now actually hits and never expires — never recover it. A key that did
// report keeps its latest value (src overwrites dst). Callers must hold the same
// serialization as the rest of the refresh (refreshMu).
func (v *View) updateBaselineCache(endBaselines *CounterBaselines, watermark time.Time) {
	if endBaselines == nil {
		return
	}
	if v.baselineCache == nil {
		v.baselineCache = &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}
	}
	mergeBaselineMap(v.baselineCache.InDiscards, endBaselines.InDiscards)
	mergeBaselineMap(v.baselineCache.InErrors, endBaselines.InErrors)
	mergeBaselineMap(v.baselineCache.InFCSErrors, endBaselines.InFCSErrors)
	mergeBaselineMap(v.baselineCache.OutDiscards, endBaselines.OutDiscards)
	mergeBaselineMap(v.baselineCache.OutErrors, endBaselines.OutErrors)
	v.baselineCacheWatermark = watermark
}

// mergeBaselineMap upserts each non-nil src entry into dst. It never deletes:
// a decommissioned device/interface keeps its entry for the life of the process.
// This is intentional, not an oversight — the map is bounded by real
// device×interface cardinality (small), and evicting would require per-key
// timestamps for a leak that a restart clears anyway. Consequence: an interface
// silent longer than baselineLookback still deltas against its carried value
// rather than getting the one nil delta described in baselineLookback's doc;
// that doc's "unless the in-memory cache still carries it forward" caveat is
// this code path.
func mergeBaselineMap(dst, src map[string]*int64) {
	for k, val := range src {
		if val != nil {
			dst[k] = val
		}
	}
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

// queryIntfCountersChunked fetches interface counters over [start, end) by splitting the
// range into contiguous half-open sub-windows of at most chunk duration and concatenating
// the rows. A single unbounded query over a multi-hour window pivots millions of rows
// server-side and exhausts InfluxDB Cloud's heap ("deduplicate batches: Heap exhausted");
// chunking bounds each query's memory regardless of how far behind the high-water mark has
// fallen. The sub-windows exactly partition [start, end), so the concatenated result matches
// a single query and downstream time-sorting/forward-fill is unaffected.
func queryIntfCountersChunked(ctx context.Context, client InfluxDBClient, start, end time.Time, chunk time.Duration) ([]map[string]any, error) {
	if chunk <= 0 || !end.After(start) {
		return client.QueryIntfCounters(ctx, start, end)
	}

	var all []map[string]any
	for s := start; s.Before(end); s = s.Add(chunk) {
		e := s.Add(chunk)
		if e.After(end) {
			e = end
		}
		rows, err := client.QueryIntfCounters(ctx, s, e)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
	}
	return all, nil
}

func (v *View) queryInfluxDB(ctx context.Context, startTime, endTime time.Time, baselines *CounterBaselines, alreadyWritten MaxTimestampsByKey) ([]InterfaceUsage, *CounterBaselines, error) {
	// InfluxDB uses dzd_pubkey as a tag, which we extract and map to device_pk.
	v.log.Debug("telemetry/usage: executing main influxdb query", "from", startTime.UTC(), "to", endTime.UTC())
	queryStart := time.Now()

	rows, err := queryIntfCountersChunked(ctx, v.cfg.InfluxDB, startTime, endTime, v.cfg.QueryChunk)
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
// chMaxTime is ClickHouse's current max event_ts (nil when the table is empty or
// the caller has no fresher signal, e.g. backfill); it gates the forward side of
// the cache guard below.
//
// During steady-state operation the cache is populated after each successful refresh cycle
// (and after each backfill chunk) with the end-of-window values from convertRowsToUsage, so
// this query runs only on startup per env or when windowStart moves outside the cached
// watermark's lag window (backfill of an old region).
func (v *View) queryBaselineCountersFromClickHouse(ctx context.Context, windowStart time.Time, chMaxTime *time.Time) (*CounterBaselines, error) {
	// Reuse the cached baselines when windowStart is at or near the watermark they
	// represent. The cache holds end-of-window sparse-counter values from the last
	// refresh/backfill chunk this process ran. A windowStart slightly below the
	// watermark (the normal refreshOverlap) is safe: overlap rows are re-read and
	// their values overwrite lastKnownValues before any new row is processed.
	// Historical windowStart values (backfill of an old region) fall far below the
	// watermark and bypass the cache, re-querying ClickHouse as before.
	//
	// A windowStart above the watermark is safe only while this process is the sole
	// writer of [watermark, now): then ClickHouse holds nothing past the watermark
	// and the cached state still describes it (a stalled source that resumes must
	// still cache-hit). But a second writer — concretely, the admin backfill's
	// "continue from where we left off" mode — can fill that region while this
	// process holds a frozen watermark; serving the stale cache then computes the
	// first post-gap sparse deltas against pre-gap values, silently corrupting
	// them (fresh event_ts, so ReplacingMergeTree never repairs the rows). The
	// chMaxTime gate detects exactly that: within one writer max event_ts never
	// exceeds the watermark, so an excursion past watermark+refreshOverlap proves
	// foreign writes and forces a re-scan (only a baselineLookback-bounded query).
	//
	// The gate cannot detect re-backfills of an already-cached HISTORICAL range
	// (they don't advance max event_ts past the watermark): after correcting data
	// under a running live indexer, restart it so the cache is rebuilt.
	if v.baselineCache != nil {
		foreignWrites := chMaxTime != nil && chMaxTime.After(v.baselineCacheWatermark.Add(refreshOverlap))
		if !foreignWrites && !windowStart.Before(v.baselineCacheWatermark.Add(-baselineCacheMaxLag)) {
			v.log.Debug("telemetry/usage: using cached baselines",
				"windowStart", windowStart.UTC(), "watermark", v.baselineCacheWatermark.UTC())
			return v.baselineCache, nil
		}
		if foreignWrites {
			// Foreign writes invalidate the whole cache, not just this lookup:
			// the post-refresh updateBaselineCache MERGES, so keys silent through
			// the foreign-filled gap would otherwise keep their pre-gap values and
			// poison their next delta. Dropping the cache means such keys get one
			// nil delta on their next report (the documented lookback semantics).
			v.log.Warn("telemetry/usage: clickhouse max event_ts moved past the baseline watermark (another writer filled the gap); discarding cached baselines",
				"chMaxTime", chMaxTime.UTC(), "watermark", v.baselineCacheWatermark.UTC())
			v.baselineCache = nil
			v.baselineCacheWatermark = time.Time{}
		}
	}

	metrics.ClickHouseBaselineQueryTotal.WithLabelValues(v.cfg.DZEnv).Inc()

	// Query recent data before the window start to find the last non-null values.
	// baselineLookback bounds the scan; see its doc for why 7 days was cut to 2.
	lookbackStart := windowStart.Add(-baselineLookback)

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
