package dztelemlatency

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// prevRTTLookback bounds the previous-RTT seed scan for circuits missing from
// the in-memory carry-forward cache. Samples are written every refresh cycle
// (minutes), so an active circuit's latest RTT is almost always within hours;
// 2 days matches the usage view's baselineLookback margin. The unbounded
// full-history scan this replaces was the top lake CPU consumer on the shared
// cluster (#720). Circuits with no row inside the window (new, or quiet longer
// than the lookback) fall back to an unbounded query restricted to just those
// circuits, so IPDV semantics are unchanged.
const prevRTTLookback = 2 * 24 * time.Hour

// maxSampleIndexLookback bounds the max-sample-index scans to enable partition
// pruning. Callers only fetch the current and previous epoch; Solana epochs
// are ~2-3 days, so 4 days covers the previous epoch's boundary row. A key
// dropped by the bound makes the caller pass existingMaxIdx=-1 and refetch the
// epoch's full sample tail from the chain; ReplacingMergeTree dedups the
// re-insert, so the worst case is a redundant RPC fetch, not data loss.
const maxSampleIndexLookback = 4 * 24 * time.Hour

type StoreConfig struct {
	Logger     *slog.Logger
	ClickHouse clickhouse.Client
	// DZEnv labels the store's Prometheus metrics; it does not affect behavior.
	DZEnv string
}

func (cfg *StoreConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	return nil
}

type Store struct {
	log *slog.Logger
	cfg StoreConfig

	// prevRTTMu guards the previous-RTT carry-forward caches below. They hold
	// the last non-zero RTT this process wrote (or seeded from ClickHouse) per
	// circuit, so steady-state appends compute IPDV without any ClickHouse
	// read. The indexer is the only live writer per env and its appends are
	// serialized by the view's refreshMu; the mutex is cheap insurance since
	// Store is an exported type. Entries are only merged in after a successful
	// WriteBatch so a failed write's retry re-seeds from the pre-batch state.
	prevRTTMu             sync.Mutex
	prevDeviceLinkRTTs    map[string]uint32 // "origin:target:link" -> last non-zero RTT
	prevInternetMetroRTTs map[string]uint32 // "origin:target:provider" -> last non-zero RTT
}

func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		log:                   cfg.Logger,
		cfg:                   cfg,
		prevDeviceLinkRTTs:    make(map[string]uint32),
		prevInternetMetroRTTs: make(map[string]uint32),
	}, nil
}

type deviceLinkCircuit struct {
	originDevicePK, targetDevicePK, linkPK string
}

type internetMetroCircuit struct {
	originMetroPK, targetMetroPK, dataProvider string
}

// getPreviousDeviceLinkRTTs resolves the last non-zero RTT per circuit,
// preferring the in-memory carry-forward cache. Cache misses are seeded from
// ClickHouse with a prevRTTLookback-bounded scan; circuits with no row inside
// the window (new or quiet) fall back to an unbounded scan restricted to just
// those circuits.
func (s *Store) getPreviousDeviceLinkRTTs(ctx context.Context, circuits []deviceLinkCircuit) (map[string]uint32, error) {
	result := make(map[string]uint32, len(circuits))
	missing := make([]deviceLinkCircuit, 0)
	s.prevRTTMu.Lock()
	for _, c := range circuits {
		key := fmt.Sprintf("%s:%s:%s", c.originDevicePK, c.targetDevicePK, c.linkPK)
		if rtt, ok := s.prevDeviceLinkRTTs[key]; ok {
			result[key] = rtt
		} else {
			missing = append(missing, c)
		}
	}
	s.prevRTTMu.Unlock()
	if len(missing) == 0 {
		return result, nil
	}

	since := time.Now().UTC().Add(-prevRTTLookback)
	metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(s.cfg.DZEnv, "device_link", "bounded").Inc()
	bounded, err := s.queryPreviousDeviceLinkRTTBatch(ctx, missing, &since)
	if err != nil {
		return nil, err
	}
	unresolved := make([]deviceLinkCircuit, 0)
	for _, c := range missing {
		key := fmt.Sprintf("%s:%s:%s", c.originDevicePK, c.targetDevicePK, c.linkPK)
		if rtt, ok := bounded[key]; ok {
			result[key] = rtt
		} else {
			unresolved = append(unresolved, c)
		}
	}
	if len(unresolved) == 0 {
		return result, nil
	}

	metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(s.cfg.DZEnv, "device_link", "fallback").Inc()
	metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(s.cfg.DZEnv, "device_link").Add(float64(len(unresolved)))
	fallback, err := s.queryPreviousDeviceLinkRTTBatch(ctx, unresolved, nil)
	if err != nil {
		return nil, err
	}
	for key, rtt := range fallback {
		result[key] = rtt
	}
	return result, nil
}

// queryPreviousDeviceLinkRTTBatch gets the most recent RTT for multiple device
// link circuits in one query. A non-nil since adds an event_ts lower bound so
// ClickHouse can prune partitions and granules; within a circuit the argMax
// ordering key (epoch, sample_index, event_ts) is monotone with event_ts, so a
// bounded result equals the unbounded one whenever the circuit has any row
// inside the window.
func (s *Store) queryPreviousDeviceLinkRTTBatch(ctx context.Context, circuits []deviceLinkCircuit, since *time.Time) (map[string]uint32, error) {
	if len(circuits) == 0 {
		return make(map[string]uint32), nil
	}

	// Build query with IN clauses for each circuit
	// Use argMax to get the latest RTT for each circuit
	conditions := make([]string, 0, len(circuits))
	args := make([]any, 0, len(circuits)*3+1)
	for _, circuit := range circuits {
		conditions = append(conditions, "(origin_device_pk = ? AND target_device_pk = ? AND link_pk = ?)")
		args = append(args, circuit.originDevicePK, circuit.targetDevicePK, circuit.linkPK)
	}

	timeFilter := ""
	if since != nil {
		timeFilter = "AND event_ts >= ?"
		args = append(args, *since)
	}

	query := fmt.Sprintf(`SELECT
		origin_device_pk,
		target_device_pk,
		link_pk,
		argMax(rtt_us, (epoch, sample_index, event_ts)) as rtt_us
		FROM (
			SELECT origin_device_pk, target_device_pk, link_pk, rtt_us, epoch, sample_index, event_ts
			FROM fact_dz_device_link_latency
			WHERE rtt_us > 0
			AND (%s)
			%s
		)
		GROUP BY origin_device_pk, target_device_pk, link_pk`,
		strings.Join(conditions, " OR "), timeFilter)

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query previous RTTs: %w", err)
	}
	defer rows.Close()

	result := make(map[string]uint32)
	for rows.Next() {
		var originDevicePK, targetDevicePK, linkPK string
		var rtt *int64
		if err := rows.Scan(&originDevicePK, &targetDevicePK, &linkPK, &rtt); err != nil {
			return nil, fmt.Errorf("failed to scan previous RTT: %w", err)
		}
		if rtt != nil && *rtt > 0 {
			key := fmt.Sprintf("%s:%s:%s", originDevicePK, targetDevicePK, linkPK)
			result[key] = uint32(*rtt)
		}
	}

	return result, nil
}

func (s *Store) AppendDeviceLinkLatencySamples(ctx context.Context, samples []DeviceLinkLatencySample) error {
	if len(samples) == 0 {
		return nil
	}

	// Sort samples by circuit, then by epoch, then by sample_index
	sortedSamples := make([]DeviceLinkLatencySample, len(samples))
	copy(sortedSamples, samples)
	sort.Slice(sortedSamples, func(i, j int) bool {
		keyI := fmt.Sprintf("%s:%s:%s", sortedSamples[i].OriginDevicePK, sortedSamples[i].TargetDevicePK, sortedSamples[i].LinkPK)
		keyJ := fmt.Sprintf("%s:%s:%s", sortedSamples[j].OriginDevicePK, sortedSamples[j].TargetDevicePK, sortedSamples[j].LinkPK)
		if keyI != keyJ {
			return keyI < keyJ
		}
		if sortedSamples[i].Epoch != sortedSamples[j].Epoch {
			return sortedSamples[i].Epoch < sortedSamples[j].Epoch
		}
		return sortedSamples[i].SampleIndex < sortedSamples[j].SampleIndex
	})

	// Collect unique circuits from the batch
	circuitSet := make(map[string]deviceLinkCircuit)
	for _, sample := range sortedSamples {
		key := fmt.Sprintf("%s:%s:%s", sample.OriginDevicePK, sample.TargetDevicePK, sample.LinkPK)
		if _, ok := circuitSet[key]; !ok {
			circuitSet[key] = deviceLinkCircuit{sample.OriginDevicePK, sample.TargetDevicePK, sample.LinkPK}
		}
	}

	// Resolve previous RTTs for all circuits (cache first, ClickHouse for misses)
	circuits := make([]deviceLinkCircuit, 0, len(circuitSet))
	for _, circuit := range circuitSet {
		circuits = append(circuits, circuit)
	}

	// Track previous RTT for each circuit (seeded + updated within batch)
	prevRTTs, err := s.getPreviousDeviceLinkRTTs(ctx, circuits)
	if err != nil {
		// Query errors should fail - no data is handled by empty result
		return fmt.Errorf("failed to query previous RTTs: %w", err)
	}

	ds, err := NewDeviceLinkLatencyDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	// Write to ClickHouse
	ingestedAt := time.Now().UTC()
	if err := ds.WriteBatch(ctx, conn, len(sortedSamples), func(idx int) ([]any, error) {
		sample := sortedSamples[idx]
		loss := sample.RTTMicroseconds == 0

		// Calculate IPDV: absolute difference from previous RTT
		var ipdv *int64
		key := fmt.Sprintf("%s:%s:%s", sample.OriginDevicePK, sample.TargetDevicePK, sample.LinkPK)
		if sample.RTTMicroseconds > 0 {
			if prevRTT, ok := prevRTTs[key]; ok && prevRTT > 0 {
				var ipdvVal uint32
				if sample.RTTMicroseconds > prevRTT {
					ipdvVal = sample.RTTMicroseconds - prevRTT
				} else {
					ipdvVal = prevRTT - sample.RTTMicroseconds
				}
				ipdvValInt64 := int64(ipdvVal)
				ipdv = &ipdvValInt64
			}
			prevRTTs[key] = sample.RTTMicroseconds
		}

		return []any{
			sample.Time.UTC(), // event_ts
			ingestedAt,        // ingested_at
			sample.Epoch,
			sample.SampleIndex,
			sample.OriginDevicePK,
			sample.TargetDevicePK,
			sample.LinkPK,
			sample.RTTMicroseconds,
			loss,
			ipdv,
		}, nil
	}); err != nil {
		return fmt.Errorf("failed to write device link latency samples to ClickHouse: %w", err)
	}

	// Merge only after a successful write: a failed batch must not advance the
	// carried-forward IPDV baseline the retry will re-seed from.
	s.prevRTTMu.Lock()
	for key, rtt := range prevRTTs {
		s.prevDeviceLinkRTTs[key] = rtt
	}
	s.prevRTTMu.Unlock()

	return nil
}

// getPreviousInternetMetroRTTs resolves the last non-zero RTT per circuit,
// preferring the in-memory carry-forward cache; see getPreviousDeviceLinkRTTs.
func (s *Store) getPreviousInternetMetroRTTs(ctx context.Context, circuits []internetMetroCircuit) (map[string]uint32, error) {
	result := make(map[string]uint32, len(circuits))
	missing := make([]internetMetroCircuit, 0)
	s.prevRTTMu.Lock()
	for _, c := range circuits {
		key := fmt.Sprintf("%s:%s:%s", c.originMetroPK, c.targetMetroPK, c.dataProvider)
		if rtt, ok := s.prevInternetMetroRTTs[key]; ok {
			result[key] = rtt
		} else {
			missing = append(missing, c)
		}
	}
	s.prevRTTMu.Unlock()
	if len(missing) == 0 {
		return result, nil
	}

	since := time.Now().UTC().Add(-prevRTTLookback)
	metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(s.cfg.DZEnv, "internet_metro", "bounded").Inc()
	bounded, err := s.queryPreviousInternetMetroRTTBatch(ctx, missing, &since)
	if err != nil {
		return nil, err
	}
	unresolved := make([]internetMetroCircuit, 0)
	for _, c := range missing {
		key := fmt.Sprintf("%s:%s:%s", c.originMetroPK, c.targetMetroPK, c.dataProvider)
		if rtt, ok := bounded[key]; ok {
			result[key] = rtt
		} else {
			unresolved = append(unresolved, c)
		}
	}
	if len(unresolved) == 0 {
		return result, nil
	}

	metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(s.cfg.DZEnv, "internet_metro", "fallback").Inc()
	metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(s.cfg.DZEnv, "internet_metro").Add(float64(len(unresolved)))
	fallback, err := s.queryPreviousInternetMetroRTTBatch(ctx, unresolved, nil)
	if err != nil {
		return nil, err
	}
	for key, rtt := range fallback {
		result[key] = rtt
	}
	return result, nil
}

// queryPreviousInternetMetroRTTBatch gets the most recent RTT for multiple
// internet metro circuits in one query. A non-nil since adds an event_ts lower
// bound; see queryPreviousDeviceLinkRTTBatch for why bounded == unbounded when
// the circuit has any row inside the window.
func (s *Store) queryPreviousInternetMetroRTTBatch(ctx context.Context, circuits []internetMetroCircuit, since *time.Time) (map[string]uint32, error) {
	if len(circuits) == 0 {
		return make(map[string]uint32), nil
	}

	// Build query with IN clauses for each circuit
	conditions := make([]string, 0, len(circuits))
	args := make([]any, 0, len(circuits)*3+1)
	for _, circuit := range circuits {
		conditions = append(conditions, "(origin_metro_pk = ? AND target_metro_pk = ? AND data_provider = ?)")
		args = append(args, circuit.originMetroPK, circuit.targetMetroPK, circuit.dataProvider)
	}

	timeFilter := ""
	if since != nil {
		timeFilter = "AND event_ts >= ?"
		args = append(args, *since)
	}

	query := fmt.Sprintf(`SELECT
		origin_metro_pk,
		target_metro_pk,
		data_provider,
		argMax(rtt_us, (epoch, sample_index, event_ts)) as rtt_us
		FROM (
			SELECT origin_metro_pk, target_metro_pk, data_provider, rtt_us, epoch, sample_index, event_ts
			FROM fact_dz_internet_metro_latency
			WHERE rtt_us > 0
			AND (%s)
			%s
		)
		GROUP BY origin_metro_pk, target_metro_pk, data_provider`,
		strings.Join(conditions, " OR "), timeFilter)

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query previous RTTs: %w", err)
	}
	defer rows.Close()

	result := make(map[string]uint32)
	for rows.Next() {
		var originMetroPK, targetMetroPK, dataProvider string
		var rtt *int64
		if err := rows.Scan(&originMetroPK, &targetMetroPK, &dataProvider, &rtt); err != nil {
			return nil, fmt.Errorf("failed to scan previous RTT: %w", err)
		}
		if rtt != nil && *rtt > 0 {
			key := fmt.Sprintf("%s:%s:%s", originMetroPK, targetMetroPK, dataProvider)
			result[key] = uint32(*rtt)
		}
	}

	return result, nil
}

func (s *Store) AppendInternetMetroLatencySamples(ctx context.Context, samples []InternetMetroLatencySample) error {
	if len(samples) == 0 {
		return nil
	}

	// Sort samples by circuit, then by epoch, then by sample_index
	sortedSamples := make([]InternetMetroLatencySample, len(samples))
	copy(sortedSamples, samples)
	sort.Slice(sortedSamples, func(i, j int) bool {
		keyI := fmt.Sprintf("%s:%s:%s", sortedSamples[i].OriginMetroPK, sortedSamples[i].TargetMetroPK, sortedSamples[i].DataProvider)
		keyJ := fmt.Sprintf("%s:%s:%s", sortedSamples[j].OriginMetroPK, sortedSamples[j].TargetMetroPK, sortedSamples[j].DataProvider)
		if keyI != keyJ {
			return keyI < keyJ
		}
		if sortedSamples[i].Epoch != sortedSamples[j].Epoch {
			return sortedSamples[i].Epoch < sortedSamples[j].Epoch
		}
		return sortedSamples[i].SampleIndex < sortedSamples[j].SampleIndex
	})

	// Collect unique circuits from the batch
	circuitSet := make(map[string]internetMetroCircuit)
	for _, sample := range sortedSamples {
		key := fmt.Sprintf("%s:%s:%s", sample.OriginMetroPK, sample.TargetMetroPK, sample.DataProvider)
		if _, ok := circuitSet[key]; !ok {
			circuitSet[key] = internetMetroCircuit{sample.OriginMetroPK, sample.TargetMetroPK, sample.DataProvider}
		}
	}

	// Resolve previous RTTs for all circuits (cache first, ClickHouse for misses)
	circuits := make([]internetMetroCircuit, 0, len(circuitSet))
	for _, circuit := range circuitSet {
		circuits = append(circuits, circuit)
	}

	// Track previous RTT for each circuit (seeded + updated within batch)
	prevRTTs, err := s.getPreviousInternetMetroRTTs(ctx, circuits)
	if err != nil {
		// Query errors should fail - no data is handled by empty result
		return fmt.Errorf("failed to query previous RTTs: %w", err)
	}

	ds, err := NewInternetMetroLatencyDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	// Write to ClickHouse
	ingestedAt := time.Now().UTC()
	if err := ds.WriteBatch(ctx, conn, len(sortedSamples), func(idx int) ([]any, error) {
		sample := sortedSamples[idx]

		// Calculate IPDV: absolute difference from previous RTT
		var ipdv *int64
		key := fmt.Sprintf("%s:%s:%s", sample.OriginMetroPK, sample.TargetMetroPK, sample.DataProvider)
		if sample.RTTMicroseconds > 0 {
			if prevRTT, ok := prevRTTs[key]; ok && prevRTT > 0 {
				var ipdvVal uint32
				if sample.RTTMicroseconds > prevRTT {
					ipdvVal = sample.RTTMicroseconds - prevRTT
				} else {
					ipdvVal = prevRTT - sample.RTTMicroseconds
				}
				ipdvValInt64 := int64(ipdvVal)
				ipdv = &ipdvValInt64
			}
			prevRTTs[key] = sample.RTTMicroseconds
		}

		return []any{
			sample.Time.UTC(), // event_ts
			ingestedAt,        // ingested_at
			sample.Epoch,
			sample.SampleIndex,
			sample.OriginMetroPK,
			sample.TargetMetroPK,
			sample.DataProvider,
			sample.RTTMicroseconds,
			ipdv,
		}, nil
	}); err != nil {
		return fmt.Errorf("failed to write internet metro latency samples to ClickHouse: %w", err)
	}

	// Merge only after a successful write: a failed batch must not advance the
	// carried-forward IPDV baseline the retry will re-seed from.
	s.prevRTTMu.Lock()
	for key, rtt := range prevRTTs {
		s.prevInternetMetroRTTs[key] = rtt
	}
	s.prevRTTMu.Unlock()

	return nil
}

func (s *Store) GetExistingMaxSampleIndices() (map[string]int, error) {
	ctx := context.Background()
	// maxSampleIndexLookback (4 days) covers the current and previous epoch,
	// which is all the callers fetch; see the constant's doc for the safety of
	// dropping older keys. Within an epoch a higher sample_index always has a
	// later event_ts, so the bound can only drop whole keys, never return a
	// lower max.
	query := `SELECT origin_device_pk, target_device_pk, link_pk, epoch, max(sample_index) as max_idx
	          FROM fact_dz_device_link_latency
	          WHERE event_ts >= ?
	          GROUP BY origin_device_pk, target_device_pk, link_pk, epoch`
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	rows, err := conn.Query(ctx, query, time.Now().UTC().Add(-maxSampleIndexLookback))
	if err != nil {
		return nil, fmt.Errorf("failed to query existing max indices: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var originDevicePK, targetDevicePK, linkPK string
		var epoch int64
		var maxIdx *int32
		if err := rows.Scan(&originDevicePK, &targetDevicePK, &linkPK, &epoch, &maxIdx); err != nil {
			return nil, fmt.Errorf("failed to scan max index: %w", err)
		}
		if maxIdx != nil {
			key := fmt.Sprintf("%s:%s:%s:%d", originDevicePK, targetDevicePK, linkPK, epoch)
			result[key] = int(*maxIdx)
		}
	}
	return result, nil
}

func (s *Store) GetExistingInternetMaxSampleIndices() (map[string]int, error) {
	ctx := context.Background()
	// Query recent samples grouped by circuit and epoch to determine what's already been inserted.
	// This enables incremental appends by only inserting new samples (sample_index > existing max).
	// maxSampleIndexLookback bounds the scan; see GetExistingMaxSampleIndices.
	query := `SELECT origin_metro_pk, target_metro_pk, data_provider, epoch, max(sample_index) as max_idx
	          FROM fact_dz_internet_metro_latency
	          WHERE event_ts >= ?
	          GROUP BY origin_metro_pk, target_metro_pk, data_provider, epoch`
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	rows, err := conn.Query(ctx, query, time.Now().UTC().Add(-maxSampleIndexLookback))
	if err != nil {
		return nil, fmt.Errorf("failed to query existing max indices: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var originMetroPK, targetMetroPK, dataProvider string
		var epoch int64
		var maxIdx *int32
		if err := rows.Scan(&originMetroPK, &targetMetroPK, &dataProvider, &epoch, &maxIdx); err != nil {
			return nil, fmt.Errorf("failed to scan max index: %w", err)
		}
		if maxIdx != nil {
			// Convert int64 epoch to uint64 for key consistency (epoch is stored as int64 in DB but used as uint64 elsewhere)
			key := fmt.Sprintf("%s:%s:%s:%d", originMetroPK, targetMetroPK, dataProvider, uint64(epoch))
			result[key] = int(*maxIdx)
		}
	}
	return result, nil
}

// DataBoundaries contains min/max timestamps and epochs for a fact table
type DataBoundaries struct {
	MinTime  *time.Time
	MaxTime  *time.Time
	MinEpoch *int64
	MaxEpoch *int64
	RowCount uint64
}

// GetDeviceLinkLatencyBoundaries returns the data boundaries for the device link latency fact table
func (s *Store) GetDeviceLinkLatencyBoundaries(ctx context.Context) (*DataBoundaries, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	query := `SELECT
		min(event_ts) as min_ts,
		max(event_ts) as max_ts,
		min(epoch) as min_epoch,
		max(epoch) as max_epoch,
		count() as row_count
	FROM fact_dz_device_link_latency`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query data boundaries: %w", err)
	}
	defer rows.Close()

	bounds := &DataBoundaries{}
	if rows.Next() {
		var minTS, maxTS time.Time
		var minEpoch, maxEpoch int64
		var rowCount uint64
		if err := rows.Scan(&minTS, &maxTS, &minEpoch, &maxEpoch, &rowCount); err != nil {
			return nil, fmt.Errorf("failed to scan data boundaries: %w", err)
		}
		bounds.RowCount = rowCount
		// ClickHouse returns zero time for empty tables
		zeroTime := time.Unix(0, 0).UTC()
		if minTS.After(zeroTime) {
			bounds.MinTime = &minTS
			bounds.MaxTime = &maxTS
			bounds.MinEpoch = &minEpoch
			bounds.MaxEpoch = &maxEpoch
		}
	}

	return bounds, nil
}

// GetInternetMetroLatencyBoundaries returns the data boundaries for the internet metro latency fact table
func (s *Store) GetInternetMetroLatencyBoundaries(ctx context.Context) (*DataBoundaries, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	query := `SELECT
		min(event_ts) as min_ts,
		max(event_ts) as max_ts,
		min(epoch) as min_epoch,
		max(epoch) as max_epoch,
		count() as row_count
	FROM fact_dz_internet_metro_latency`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query data boundaries: %w", err)
	}
	defer rows.Close()

	bounds := &DataBoundaries{}
	if rows.Next() {
		var minTS, maxTS time.Time
		var minEpoch, maxEpoch int64
		var rowCount uint64
		if err := rows.Scan(&minTS, &maxTS, &minEpoch, &maxEpoch, &rowCount); err != nil {
			return nil, fmt.Errorf("failed to scan data boundaries: %w", err)
		}
		bounds.RowCount = rowCount
		// ClickHouse returns zero time for empty tables
		zeroTime := time.Unix(0, 0).UTC()
		if minTS.After(zeroTime) {
			bounds.MinTime = &minTS
			bounds.MaxTime = &maxTS
			bounds.MinEpoch = &minEpoch
			bounds.MaxEpoch = &maxEpoch
		}
	}

	return bounds, nil
}
