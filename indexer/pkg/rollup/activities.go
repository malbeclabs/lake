package rollup

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"go.temporal.io/sdk/activity"
)

// Activities holds dependencies for rollup activities.
type Activities struct {
	ClickHouse   driver.Conn
	Log          *slog.Logger
	IngestionLog *ingestionlog.Writer
	Network      string
	// TelemetryDatabase is the gNMI interface_state DB (e.g. telemetry_mainnet_beta).
	// When set, the device-interface rollup prefers it per device and falls back to the
	// InfluxDB fact table for devices not present there. Empty = fact-only (current behavior).
	TelemetryDatabase  string
	CompetitorDatabase string
}

// tableRef returns a qualified table reference. If sourceDB is non-empty, the
// table is prefixed with the database name (e.g. "lake.fact_dz_device_link_latency").
// This allows backfill activities to read from remote proxy tables.
func tableRef(sourceDB, table string) string {
	if sourceDB != "" {
		return fmt.Sprintf("`%s`.`%s`", sourceDB, table)
	}
	return table
}

// bucketKey identifies a (link/device, 5-minute bucket) pair for state resolution.
type bucketKey struct {
	linkPK string
	bucket time.Time
}

// sortedTimes returns the keys of a time set sorted ascending.
func sortedTimes(m map[time.Time]struct{}) []time.Time {
	times := make([]time.Time, 0, len(m))
	for t := range m {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	return times
}

// logRollup logs a rollup computation result, including the source database if set.
func (a *Activities) logRollup(msg string, count int, input BackfillChunkInput, duration time.Duration) {
	attrs := []any{
		"count", count,
		"window", fmt.Sprintf("[%s, %s)", input.WindowStart.Format(time.RFC3339), input.WindowEnd.Format(time.RFC3339)),
		"duration", duration,
	}
	if input.SourceDatabase != "" {
		attrs = append(attrs, "source_database", input.SourceDatabase)
	}
	a.Log.Info(msg, attrs...)
}

// safeHeartbeat records a heartbeat if running inside a Temporal activity context.
func safeHeartbeat(ctx context.Context, details ...any) {
	defer func() { recover() }() //nolint:errcheck
	activity.RecordHeartbeat(ctx, details...)
}

// provisioningSentinel is the committed_rtt_ns value that indicates a link is provisioning.
const provisioningSentinel int64 = 1_000_000_000

// tunnelKey identifies a user tunnel on a specific device.
type tunnelKey struct {
	devicePK string
	tunnelID int64
}

// ComputeLinkRollup queries probe telemetry for the given time window and returns
// link rollup buckets with per-direction latency percentiles and loss.
// Entity state (status, provisioning, ISIS down) is resolved from history tables.
func (a *Activities) ComputeLinkRollup(ctx context.Context, input BackfillChunkInput) ([]LinkBucket, error) {
	safeHeartbeat(ctx, "computing link rollup")
	start := time.Now()

	// Link latency event_ts is interpolated from on-chain sample headers, which can
	// be inaccurate when the on-chain writer has gaps. For live rollup (recent data
	// near the epoch head), we use a display timestamp that picks ingested_at for
	// samples near latest_sample_index, falling back to event_ts for older data.
	//
	// For backfill, we use event_ts directly for both filtering and bucketing.
	// The displayTs formula depends on latest_sample_index which grows over time,
	// making it non-deterministic — re-running a backfill would produce different
	// results and overwrite correct data via ReplacingMergeTree. Using event_ts
	// is deterministic and correct for historical data.
	const displayTs = "if(h.sampling_interval_us > 0 AND f.sample_index >= h.latest_sample_index - 1000, f.ingested_at, f.event_ts)"

	isLiveWindow := input.WindowEnd.Sub(input.WindowStart) <= 10*time.Minute

	// For live windows, use displayTs (ingested_at for recent samples) for bucketing
	// and ingested_at for filtering. For backfill, use event_ts for both.
	bucketTs := "f.event_ts"
	filterCol := "f.event_ts"
	headerJoin := ""
	srcDB := input.SourceDatabase
	factLatency := tableRef(srcDB, "fact_dz_device_link_latency")
	factLatencyHeader := tableRef(srcDB, "fact_dz_device_link_latency_sample_header")
	linksCurrent := tableRef(srcDB, "dz_links_current")

	if isLiveWindow {
		bucketTs = displayTs
		filterCol = "f.ingested_at"
		headerJoin = `
			LEFT JOIN (
				SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
					   max(latest_sample_index) AS latest_sample_index,
					   any(sampling_interval_us) AS sampling_interval_us
				FROM ` + factLatencyHeader + ` FINAL
				GROUP BY origin_device_pk, target_device_pk, link_pk, epoch
			) h ON f.origin_device_pk = h.origin_device_pk
				AND f.target_device_pk = h.target_device_pk
				AND f.link_pk = h._hdr_link_pk
				AND f.epoch = h.epoch`
	}

	latencyQuery := `
		WITH loss_sub AS (
			SELECT
				f.link_pk,
				toStartOfFiveMinutes(` + bucketTs + `) as bucket,
				if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') as direction,
				countIf(f.loss OR f.rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM ` + factLatency + ` f FINAL
			JOIN ` + linksCurrent + ` l ON f.link_pk = l.pk` + headerJoin + `
			WHERE ` + filterCol + ` >= $1 AND ` + filterCol + ` < $2
			GROUP BY f.link_pk, bucket, direction
		)
		SELECT
			f.link_pk,
			toStartOfFiveMinutes(` + bucketTs + `) as bucket,
			if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') as direction,
			avg(f.rtt_us) as avg_rtt,
			toFloat64(min(f.rtt_us)) as min_rtt,
			quantile(0.50)(f.rtt_us) as p50_rtt,
			quantile(0.90)(f.rtt_us) as p90_rtt,
			quantile(0.95)(f.rtt_us) as p95_rtt,
			quantile(0.99)(f.rtt_us) as p99_rtt,
			toFloat64(max(f.rtt_us)) as max_rtt,
			max(ls.loss_pct) as loss_pct,
			toUInt32(count(*)) as samples,
			avg(abs(f.ipdv_us)) as avg_jitter,
			toFloat64(min(abs(f.ipdv_us))) as min_jitter,
			quantile(0.50)(abs(f.ipdv_us)) as p50_jitter,
			quantile(0.90)(abs(f.ipdv_us)) as p90_jitter,
			quantile(0.95)(abs(f.ipdv_us)) as p95_jitter,
			quantile(0.99)(abs(f.ipdv_us)) as p99_jitter,
			toFloat64(max(abs(f.ipdv_us))) as max_jitter
		FROM ` + factLatency + ` f FINAL
		JOIN ` + linksCurrent + ` l ON f.link_pk = l.pk` + headerJoin + `
		LEFT JOIN loss_sub ls ON f.link_pk = ls.link_pk
			AND toStartOfFiveMinutes(` + bucketTs + `) = ls.bucket
			AND if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') = ls.direction
		WHERE ` + filterCol + ` >= $1 AND ` + filterCol + ` < $2
		GROUP BY f.link_pk, bucket, direction
		ORDER BY f.link_pk, bucket, direction
	`

	rows, err := a.ClickHouse.Query(ctx, latencyQuery, input.WindowStart, input.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("link latency query: %w", err)
	}
	defer rows.Close()

	type bucketData struct {
		a *LinkLatencyStats
		z *LinkLatencyStats
	}
	bucketMap := make(map[bucketKey]*bucketData)

	for rows.Next() {
		var linkPK, direction string
		var bucket time.Time
		var d LinkLatencyStats
		if err := rows.Scan(&linkPK, &bucket, &direction,
			&d.AvgRttUs, &d.MinRttUs, &d.P50RttUs, &d.P90RttUs, &d.P95RttUs, &d.P99RttUs, &d.MaxRttUs,
			&d.LossPct, &d.Samples,
			&d.AvgJitterUs, &d.MinJitterUs, &d.P50JitterUs, &d.P90JitterUs, &d.P95JitterUs, &d.P99JitterUs, &d.MaxJitterUs); err != nil {
			return nil, fmt.Errorf("link latency scan: %w", err)
		}
		bk := bucketKey{linkPK: linkPK, bucket: bucket.UTC()}
		if bucketMap[bk] == nil {
			bucketMap[bk] = &bucketData{}
		}
		ds := d
		if direction == "A" {
			bucketMap[bk].a = &ds
		} else {
			bucketMap[bk].z = &ds
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("link latency rows: %w", err)
	}

	if len(bucketMap) == 0 {
		a.logRollup("computed link rollup buckets", 0, input, time.Since(start))
		return nil, nil
	}

	// Collect unique link PKs for state resolution
	linkPKs := make(map[string]struct{})
	for bk := range bucketMap {
		linkPKs[bk.linkPK] = struct{}{}
	}

	// Collect unique bucket timestamps
	bucketTimes := make(map[time.Time]struct{})
	for bk := range bucketMap {
		bucketTimes[bk.bucket] = struct{}{}
	}

	// Resolve link status and provisioning per (link, bucket) from dim_dz_links_history
	linkState, err := a.resolveLinkState(ctx, linkPKs, bucketTimes, input.WindowStart, input.WindowEnd, srcDB)
	if err != nil {
		return nil, fmt.Errorf("resolve link state: %w", err)
	}

	// Resolve ISIS adjacency status per (link, bucket) from dim_isis_adjacencies_history
	isisDown, err := a.resolveISISAdjacency(ctx, linkPKs, bucketTimes, input.WindowStart, input.WindowEnd, srcDB)
	if err != nil {
		return nil, fmt.Errorf("resolve ISIS adjacency: %w", err)
	}

	// For live windows (ingested_at filtering), only emit the bucket that
	// contains WindowEnd — the in-progress bucket that hasn't been backfilled
	// yet. Older buckets get partial data because the ingested_at filter
	// doesn't see probes ingested before the window, producing incomplete rows
	// that would overwrite correct backfill results via ReplacingMergeTree.
	// The workflow's WindowEnd is truncated to 5 minutes, so the target bucket
	// is the one starting at WindowEnd - 5min.
	liveBucket := time.Time{}
	if isLiveWindow {
		liveBucket = input.WindowEnd.Truncate(5 * time.Minute).Add(-5 * time.Minute)
	}

	now := time.Now()
	var buckets []LinkBucket

	for bk, bd := range bucketMap {
		if isLiveWindow && !bk.bucket.Equal(liveBucket) {
			continue
		}

		b := LinkBucket{
			BucketTS:   bk.bucket,
			LinkPK:     bk.linkPK,
			IngestedAt: now,
		}
		if bd.a != nil {
			b.A = *bd.a
		}
		if bd.z != nil {
			b.Z = *bd.z
		}

		// Apply per-bucket state
		if ls, ok := linkState[bk]; ok {
			b.Status = ls.status
			b.Provisioning = ls.provisioning
		}
		if down, ok := isisDown[bk]; ok {
			b.ISISDown = down
		}

		buckets = append(buckets, b)
	}

	a.logRollup("computed link rollup buckets", len(buckets), input, time.Since(start))

	return buckets, nil
}

// linkStateInfo holds resolved link status and provisioning flag.
type linkStateInfo struct {
	status       string
	provisioning bool
}

// resolveLinkState queries dim_dz_links_history to get the status and provisioning
// flag for each (link, bucket) pair. For each bucket, it finds the latest state
// as of the bucket's end time (bucket + 5min).
func (a *Activities) resolveLinkState(ctx context.Context, linkPKs map[string]struct{}, bucketTimes map[time.Time]struct{}, _, windowEnd time.Time, sourceDB string) (map[bucketKey]linkStateInfo, error) {
	// Query all state entries up to windowEnd (covers baseline + in-window changes)
	query := `
		SELECT pk, snapshot_ts, status, committed_rtt_ns
		FROM ` + tableRef(sourceDB, "dim_dz_links_history") + `
		WHERE snapshot_ts <= $1
		ORDER BY pk, snapshot_ts
	`
	rows, err := a.ClickHouse.Query(ctx, query, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("link state query: %w", err)
	}
	defer rows.Close()

	type entry struct {
		ts    time.Time
		state linkStateInfo
	}
	entriesByLink := make(map[string][]entry)
	for rows.Next() {
		var pk, status string
		var ts time.Time
		var committedRttNs int64
		if err := rows.Scan(&pk, &ts, &status, &committedRttNs); err != nil {
			return nil, fmt.Errorf("link state scan: %w", err)
		}
		if _, ok := linkPKs[pk]; ok {
			entriesByLink[pk] = append(entriesByLink[pk], entry{
				ts: ts.UTC(),
				state: linkStateInfo{
					status:       status,
					provisioning: committedRttNs == provisioningSentinel,
				},
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("link state rows: %w", err)
	}

	// For each (link, bucket), find the latest entry with ts <= bucket end (bucket + 5m)
	const bucketWidth = 5 * time.Minute
	result := make(map[bucketKey]linkStateInfo)
	sortedBuckets := sortedTimes(bucketTimes)
	for pk := range linkPKs {
		entries := entriesByLink[pk]
		if len(entries) == 0 {
			continue
		}
		entryIdx := 0
		var current linkStateInfo
		for _, bt := range sortedBuckets {
			bucketEnd := bt.Add(bucketWidth)
			for entryIdx < len(entries) && !entries[entryIdx].ts.After(bucketEnd) {
				current = entries[entryIdx].state
				entryIdx++
			}
			if current.status != "" {
				result[bucketKey{linkPK: pk, bucket: bt}] = current
			}
		}
	}
	return result, nil
}

// resolveISISAdjacency queries dim_isis_adjacencies_history to determine if each
// (link, bucket) pair has an ISIS adjacency. For each bucket, it finds the latest
// adjacency state as of the bucket's end time (bucket + 5min).
//
// A link is UP only when both sides independently confirm the adjacency is healthy
// (activeCount >= 2). This guards against false positives from hostname renames,
// where the old entity gets is_deleted=1 alongside a new is_deleted=0 entity for
// the same link_pk at the same snapshot_ts. Because ClickHouse row ordering for
// same-timestamp entries is nondeterministic, a single carry-forward variable could
// flip to "deleted" and never recover. Per-entity tracking and evaluating state
// after each timestamp group (not after each individual row) avoids this.
func (a *Activities) resolveISISAdjacency(ctx context.Context, linkPKs map[string]struct{}, bucketTimes map[time.Time]struct{}, _, windowEnd time.Time, sourceDB string) (map[bucketKey]bool, error) {
	// Include entity_id so we can carry forward state per entity independently.
	query := `
		SELECT link_pk, entity_id, snapshot_ts, is_deleted
		FROM ` + tableRef(sourceDB, "dim_isis_adjacencies_history") + `
		WHERE snapshot_ts <= $1 AND link_pk != ''
		ORDER BY link_pk, snapshot_ts
	`
	rows, err := a.ClickHouse.Query(ctx, query, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("ISIS adjacency query: %w", err)
	}
	defer rows.Close()

	type entry struct {
		entityID  string
		ts        time.Time
		isDeleted bool
	}
	entriesByLink := make(map[string][]entry)
	for rows.Next() {
		var linkPK, entityID string
		var ts time.Time
		var isDeleted uint8
		if err := rows.Scan(&linkPK, &entityID, &ts, &isDeleted); err != nil {
			return nil, fmt.Errorf("ISIS adjacency scan: %w", err)
		}
		if _, ok := linkPKs[linkPK]; ok {
			entriesByLink[linkPK] = append(entriesByLink[linkPK], entry{
				entityID:  entityID,
				ts:        ts.UTC(),
				isDeleted: isDeleted == 1,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ISIS adjacency rows: %w", err)
	}

	// consumeGroup advances entryIdx past all entries sharing the same timestamp,
	// applying each to entityState. Returns true if any entries were consumed.
	activeCount := func(entityState map[string]bool) int {
		n := 0
		for _, deleted := range entityState {
			if !deleted {
				n++
			}
		}
		return n
	}
	consumeGroup := func(entries []entry, entryIdx *int, entityState map[string]bool, until func(time.Time) bool) bool {
		if *entryIdx >= len(entries) || !until(entries[*entryIdx].ts) {
			return false
		}
		consumed := false
		for *entryIdx < len(entries) && until(entries[*entryIdx].ts) {
			ts := entries[*entryIdx].ts
			for *entryIdx < len(entries) && entries[*entryIdx].ts.Equal(ts) {
				entityState[entries[*entryIdx].entityID] = entries[*entryIdx].isDeleted
				*entryIdx++
			}
			consumed = true
		}
		return consumed
	}

	// For each (link, bucket), determine isis_down using per-entity carry-forward.
	// Each entity (one per side of the link) tracks its own is_deleted independently.
	// A link is UP only when at least 2 entities are active (both sides confirm UP).
	// State is evaluated after each full same-timestamp group so that a rename's
	// simultaneous delete+create doesn't cause a momentary false down.
	const bucketWidth = 5 * time.Minute
	result := make(map[bucketKey]bool)
	sortedBuckets := sortedTimes(bucketTimes)
	for pk := range linkPKs {
		entries := entriesByLink[pk]
		if len(entries) == 0 {
			continue // not an ISIS link
		}
		entityState := make(map[string]bool) // entity_id → isDeleted
		entryIdx := 0
		seenEntry := false

		for _, bt := range sortedBuckets {
			bucketEnd := bt.Add(bucketWidth)
			// Consume timestamp groups before this bucket to update carry-forward.
			if consumeGroup(entries, &entryIdx, entityState, func(ts time.Time) bool { return ts.Before(bt) }) {
				seenEntry = true
			}
			// Initial wasDown from carry-forward. If no entities seen yet, default UP.
			wasDown := len(entityState) > 0 && activeCount(entityState) < 2
			// Consume timestamp groups within [bt, bucketEnd], checking after each group.
			for entryIdx < len(entries) && !entries[entryIdx].ts.After(bucketEnd) {
				ts := entries[entryIdx].ts
				for entryIdx < len(entries) && entries[entryIdx].ts.Equal(ts) {
					entityState[entries[entryIdx].entityID] = entries[entryIdx].isDeleted
					entryIdx++
				}
				if activeCount(entityState) < 2 {
					wasDown = true
				}
				seenEntry = true
			}
			if seenEntry {
				result[bucketKey{linkPK: pk, bucket: bt}] = wasDown
			}
		}
	}
	return result, nil
}

// ComputeDeviceInterfaceRollup queries interface counters and traffic rates for
// the given time window and returns per-device, per-interface rollup buckets.
// Link context (link_pk, link_side), user context, and device state are resolved.
func (a *Activities) ComputeDeviceInterfaceRollup(ctx context.Context, input BackfillChunkInput) ([]DeviceInterfaceBucket, error) {
	safeHeartbeat(ctx, "computing device interface rollup")
	start := time.Now()

	srcDB := input.SourceDatabase
	factCounters := tableRef(srcDB, "fact_dz_device_interface_counters")

	query := `
		SELECT
			device_pk,
			intf,
			toStartOfFiveMinutes(event_ts) as bucket,
			-- Link context: take the most common link_pk/link_side in the bucket
			anyIf(link_pk, link_pk != '') as link_pk,
			anyIf(link_side, link_side != '') as link_side,
			-- User context: take any non-null tunnel ID
			anyIf(user_tunnel_id, user_tunnel_id IS NOT NULL) as user_tunnel_id,
			-- Error/discard counters
			toUInt64(SUM(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, carrier_transitions_delta))) as carrier_transitions,
			-- In BPS percentiles
			avgIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as avg_in_bps,
			minIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as min_in_bps,
			quantileIf(0.50)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p50_in_bps,
			quantileIf(0.90)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p90_in_bps,
			quantileIf(0.95)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p95_in_bps,
			quantileIf(0.99)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p99_in_bps,
			maxIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as max_in_bps,
			-- Out BPS percentiles
			avgIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as avg_out_bps,
			minIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as min_out_bps,
			quantileIf(0.50)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p50_out_bps,
			quantileIf(0.90)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p90_out_bps,
			quantileIf(0.95)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p95_out_bps,
			quantileIf(0.99)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p99_out_bps,
			maxIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as max_out_bps,
			-- In PPS percentiles
			avgIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as avg_in_pps,
			minIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as min_in_pps,
			quantileIf(0.50)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p50_in_pps,
			quantileIf(0.90)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p90_in_pps,
			quantileIf(0.95)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p95_in_pps,
			quantileIf(0.99)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p99_in_pps,
			maxIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as max_in_pps,
			-- Out PPS percentiles
			avgIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as avg_out_pps,
			minIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as min_out_pps,
			quantileIf(0.50)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p50_out_pps,
			quantileIf(0.90)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p90_out_pps,
			quantileIf(0.95)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p95_out_pps,
			quantileIf(0.99)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p99_out_pps,
			maxIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as max_out_pps,
			-- In Multicast PPS percentiles
			avgIf(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as avg_in_mcast_pps,
			minIf(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as min_in_mcast_pps,
			quantileIf(0.50)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p50_in_mcast_pps,
			quantileIf(0.90)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p90_in_mcast_pps,
			quantileIf(0.95)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p95_in_mcast_pps,
			quantileIf(0.99)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p99_in_mcast_pps,
			maxIf(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as max_in_mcast_pps,
			-- Out Multicast PPS percentiles
			avgIf(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as avg_out_mcast_pps,
			minIf(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as min_out_mcast_pps,
			quantileIf(0.50)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p50_out_mcast_pps,
			quantileIf(0.90)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p90_out_mcast_pps,
			quantileIf(0.95)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p95_out_mcast_pps,
			quantileIf(0.99)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p99_out_mcast_pps,
			maxIf(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as max_out_mcast_pps
		FROM ` + factCounters + ` FINAL
		WHERE event_ts >= $1 AND event_ts < $2
		GROUP BY device_pk, intf, bucket
		ORDER BY device_pk, intf, bucket
	`

	rows, err := a.ClickHouse.Query(ctx, query, input.WindowStart, input.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("device interface query: %w", err)
	}
	defer rows.Close()

	now := time.Now()
	var buckets []DeviceInterfaceBucket

	for rows.Next() {
		var b DeviceInterfaceBucket
		b.IngestedAt = now
		if err := rows.Scan(
			&b.DevicePK, &b.Intf, &b.BucketTS,
			&b.LinkPK, &b.LinkSide,
			&b.UserTunnelID,
			&b.InErrors, &b.OutErrors, &b.InFcsErrors, &b.InDiscards, &b.OutDiscards, &b.CarrierTransitions,
			&b.InBps.Avg, &b.InBps.Min, &b.InBps.P50, &b.InBps.P90, &b.InBps.P95, &b.InBps.P99, &b.InBps.Max,
			&b.OutBps.Avg, &b.OutBps.Min, &b.OutBps.P50, &b.OutBps.P90, &b.OutBps.P95, &b.OutBps.P99, &b.OutBps.Max,
			&b.InPps.Avg, &b.InPps.Min, &b.InPps.P50, &b.InPps.P90, &b.InPps.P95, &b.InPps.P99, &b.InPps.Max,
			&b.OutPps.Avg, &b.OutPps.Min, &b.OutPps.P50, &b.OutPps.P90, &b.OutPps.P95, &b.OutPps.P99, &b.OutPps.Max,
			&b.InMcastPps.Avg, &b.InMcastPps.Min, &b.InMcastPps.P50, &b.InMcastPps.P90, &b.InMcastPps.P95, &b.InMcastPps.P99, &b.InMcastPps.Max,
			&b.OutMcastPps.Avg, &b.OutMcastPps.Min, &b.OutMcastPps.P50, &b.OutMcastPps.P90, &b.OutMcastPps.P95, &b.OutMcastPps.P99, &b.OutMcastPps.Max,
		); err != nil {
			return nil, fmt.Errorf("device interface scan: %w", err)
		}
		b.BucketTS = b.BucketTS.UTC()
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("device interface rows: %w", err)
	}

	if len(buckets) == 0 {
		a.logRollup("computed device interface rollup buckets", 0, input, time.Since(start))
		return nil, nil
	}

	if err := a.resolveDeviceInterfaceState(ctx, buckets, input.WindowEnd, srcDB); err != nil {
		return nil, err
	}

	a.logRollup("computed device interface rollup buckets", len(buckets), input, time.Since(start))

	return buckets, nil
}

// ComputeDeviceInterfaceRollupFromGNMI computes the device-interface rollup from the
// gNMI interface_state table (raw cumulative counters) rather than the InfluxDB-derived
// fact table. interface_state has no deltas, so we derive per-sample deltas with window
// functions (per device+interface ordered by timestamp), cast to Int64 so counter resets
// read negative and are dropped, and produce the SAME Nullable(Int64)/Nullable(Float64)
// delta columns the fact table carries. The outer aggregation is then identical to
// ComputeDeviceInterfaceRollup, so both sources roll up the same way; only the input
// sampling cadence differs. Link/tunnel enrichment and the de-junk filter are added in a
// later step; this step emits empty link context.
func (a *Activities) ComputeDeviceInterfaceRollupFromGNMI(ctx context.Context, input BackfillChunkInput) ([]DeviceInterfaceBucket, error) {
	safeHeartbeat(ctx, "computing device interface rollup from gnmi")
	start := time.Now()

	interfaceState := tableRef(input.TelemetryDatabase, "interface_state")
	linksHistory := tableRef(input.SourceDatabase, "dim_dz_links_history")

	query := `
		WITH links AS (
			SELECT pk, side_a_pk, side_a_iface_name, side_z_pk, side_z_iface_name
			FROM (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM ` + linksHistory + `
			)
			WHERE rn = 1 AND is_deleted = 0
		),
		link_map AS (
			-- One row per (device_pk, intf): an interface maps to at most one link side, so
			-- collapse any duplicates (e.g. a re-created link sharing an interface) to avoid
			-- fanning out the per-sample delta rows in the JOIN below (double-counting SUMs).
			SELECT device_pk, intf, any(link_pk) AS link_pk, any(link_side) AS link_side
			FROM (
				SELECT side_a_pk AS device_pk, side_a_iface_name AS intf, pk AS link_pk, 'A' AS link_side FROM links WHERE side_a_pk != '' AND side_a_iface_name != ''
				UNION ALL
				SELECT side_z_pk AS device_pk, side_z_iface_name AS intf, pk AS link_pk, 'Z' AS link_side FROM links WHERE side_z_pk != '' AND side_z_iface_name != ''
			)
			GROUP BY device_pk, intf
		)
		SELECT
			d.device_pk,
			d.intf,
			d.bucket,
			anyIf(lm.link_pk, lm.link_pk != '') as link_pk,
			anyIf(lm.link_side, lm.link_side != '') as link_side,
			if(startsWith(d.intf, 'Tunnel'), toInt64OrNull(substring(d.intf, 7)), NULL) as user_tunnel_id,
			-- Error/discard counters
			toUInt64(SUM(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, carrier_transitions_delta))) as carrier_transitions,
			-- In BPS percentiles
			avgIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as avg_in_bps,
			minIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as min_in_bps,
			quantileIf(0.50)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p50_in_bps,
			quantileIf(0.90)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p90_in_bps,
			quantileIf(0.95)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p95_in_bps,
			quantileIf(0.99)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p99_in_bps,
			maxIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as max_in_bps,
			-- Out BPS percentiles
			avgIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as avg_out_bps,
			minIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as min_out_bps,
			quantileIf(0.50)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p50_out_bps,
			quantileIf(0.90)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p90_out_bps,
			quantileIf(0.95)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p95_out_bps,
			quantileIf(0.99)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p99_out_bps,
			maxIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as max_out_bps,
			-- In PPS percentiles
			avgIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as avg_in_pps,
			minIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as min_in_pps,
			quantileIf(0.50)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p50_in_pps,
			quantileIf(0.90)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p90_in_pps,
			quantileIf(0.95)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p95_in_pps,
			quantileIf(0.99)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p99_in_pps,
			maxIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as max_in_pps,
			-- Out PPS percentiles
			avgIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as avg_out_pps,
			minIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as min_out_pps,
			quantileIf(0.50)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p50_out_pps,
			quantileIf(0.90)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p90_out_pps,
			quantileIf(0.95)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p95_out_pps,
			quantileIf(0.99)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p99_out_pps,
			maxIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as max_out_pps,
			-- In Multicast PPS percentiles
			avgIf(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as avg_in_mcast_pps,
			minIf(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as min_in_mcast_pps,
			quantileIf(0.50)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p50_in_mcast_pps,
			quantileIf(0.90)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p90_in_mcast_pps,
			quantileIf(0.95)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p95_in_mcast_pps,
			quantileIf(0.99)(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as p99_in_mcast_pps,
			maxIf(in_multicast_pkts_delta / delta_duration, delta_duration > 0 AND in_multicast_pkts_delta >= 0) as max_in_mcast_pps,
			-- Out Multicast PPS percentiles
			avgIf(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as avg_out_mcast_pps,
			minIf(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as min_out_mcast_pps,
			quantileIf(0.50)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p50_out_mcast_pps,
			quantileIf(0.90)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p90_out_mcast_pps,
			quantileIf(0.95)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p95_out_mcast_pps,
			quantileIf(0.99)(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as p99_out_mcast_pps,
			maxIf(out_multicast_pkts_delta / delta_duration, delta_duration > 0 AND out_multicast_pkts_delta >= 0) as max_out_mcast_pps
		FROM (
			SELECT
				device_pubkey as device_pk,
				interface_name as intf,
				toStartOfFiveMinutes(timestamp) as bucket,
				-- Per-sample deltas; NULL for the first sample of each partition (no baseline).
				-- Cast to Int64 so counter resets produce negatives (dropped downstream).
				if(rn > 1, toInt64(in_errors) - toInt64(prev_in_errors), NULL) as in_errors_delta,
				if(rn > 1, toInt64(out_errors) - toInt64(prev_out_errors), NULL) as out_errors_delta,
				if(rn > 1, toInt64(in_fcs_errors) - toInt64(prev_in_fcs_errors), NULL) as in_fcs_errors_delta,
				if(rn > 1, toInt64(in_discards) - toInt64(prev_in_discards), NULL) as in_discards_delta,
				if(rn > 1, toInt64(out_discards) - toInt64(prev_out_discards), NULL) as out_discards_delta,
				if(rn > 1, toInt64(carrier_transitions) - toInt64(prev_carrier_transitions), NULL) as carrier_transitions_delta,
				if(rn > 1, toInt64(in_octets) - toInt64(prev_in_octets), NULL) as in_octets_delta,
				if(rn > 1, toInt64(out_octets) - toInt64(prev_out_octets), NULL) as out_octets_delta,
				if(rn > 1, toInt64(in_pkts) - toInt64(prev_in_pkts), NULL) as in_pkts_delta,
				if(rn > 1, toInt64(out_pkts) - toInt64(prev_out_pkts), NULL) as out_pkts_delta,
				if(rn > 1, toInt64(in_multicast_pkts) - toInt64(prev_in_multicast_pkts), NULL) as in_multicast_pkts_delta,
				if(rn > 1, toInt64(out_multicast_pkts) - toInt64(prev_out_multicast_pkts), NULL) as out_multicast_pkts_delta,
				if(rn > 1, (toUnixTimestamp64Milli(timestamp) - toUnixTimestamp64Milli(prev_ts)) / 1000.0, NULL) as delta_duration
			FROM (
				SELECT
					device_pubkey, interface_name, timestamp,
					in_errors, out_errors, in_fcs_errors, in_discards, out_discards, carrier_transitions,
					in_octets, out_octets, in_pkts, out_pkts, in_multicast_pkts, out_multicast_pkts,
					row_number() OVER w as rn,
					lagInFrame(timestamp) OVER w as prev_ts,
					lagInFrame(in_errors) OVER w as prev_in_errors,
					lagInFrame(out_errors) OVER w as prev_out_errors,
					lagInFrame(in_fcs_errors) OVER w as prev_in_fcs_errors,
					lagInFrame(in_discards) OVER w as prev_in_discards,
					lagInFrame(out_discards) OVER w as prev_out_discards,
					lagInFrame(carrier_transitions) OVER w as prev_carrier_transitions,
					lagInFrame(in_octets) OVER w as prev_in_octets,
					lagInFrame(out_octets) OVER w as prev_out_octets,
					lagInFrame(in_pkts) OVER w as prev_in_pkts,
					lagInFrame(out_pkts) OVER w as prev_out_pkts,
					lagInFrame(in_multicast_pkts) OVER w as prev_in_multicast_pkts,
					lagInFrame(out_multicast_pkts) OVER w as prev_out_multicast_pkts
				FROM ` + interfaceState + `
				WHERE timestamp >= $1 AND timestamp < $3
				WINDOW w AS (PARTITION BY device_pubkey, interface_name ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
			)
		) d
		LEFT JOIN link_map lm ON d.device_pk = lm.device_pk AND d.intf = lm.intf
		WHERE d.bucket >= toStartOfFiveMinutes($2) AND d.bucket < $3
		GROUP BY d.device_pk, d.intf, d.bucket
		ORDER BY d.device_pk, d.intf, d.bucket
	`

	// Baseline lookback: scan samples up to 10m before WindowStart so the first in-window
	// sample of each interface diffs against a real prior (mirrors the InfluxDB baseline);
	// the outer query keeps only buckets in [WindowStart, WindowEnd) so lookback rows are dropped.
	lookbackStart := input.WindowStart.Add(-10 * time.Minute)
	rows, err := a.ClickHouse.Query(ctx, query, lookbackStart, input.WindowStart, input.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("device interface gnmi query: %w", err)
	}
	defer rows.Close()

	now := time.Now()
	var buckets []DeviceInterfaceBucket

	for rows.Next() {
		var b DeviceInterfaceBucket
		b.IngestedAt = now
		if err := rows.Scan(
			&b.DevicePK, &b.Intf, &b.BucketTS,
			&b.LinkPK, &b.LinkSide,
			&b.UserTunnelID,
			&b.InErrors, &b.OutErrors, &b.InFcsErrors, &b.InDiscards, &b.OutDiscards, &b.CarrierTransitions,
			&b.InBps.Avg, &b.InBps.Min, &b.InBps.P50, &b.InBps.P90, &b.InBps.P95, &b.InBps.P99, &b.InBps.Max,
			&b.OutBps.Avg, &b.OutBps.Min, &b.OutBps.P50, &b.OutBps.P90, &b.OutBps.P95, &b.OutBps.P99, &b.OutBps.Max,
			&b.InPps.Avg, &b.InPps.Min, &b.InPps.P50, &b.InPps.P90, &b.InPps.P95, &b.InPps.P99, &b.InPps.Max,
			&b.OutPps.Avg, &b.OutPps.Min, &b.OutPps.P50, &b.OutPps.P90, &b.OutPps.P95, &b.OutPps.P99, &b.OutPps.Max,
			&b.InMcastPps.Avg, &b.InMcastPps.Min, &b.InMcastPps.P50, &b.InMcastPps.P90, &b.InMcastPps.P95, &b.InMcastPps.P99, &b.InMcastPps.Max,
			&b.OutMcastPps.Avg, &b.OutMcastPps.Min, &b.OutMcastPps.P50, &b.OutMcastPps.P90, &b.OutMcastPps.P95, &b.OutMcastPps.P99, &b.OutMcastPps.Max,
		); err != nil {
			return nil, fmt.Errorf("device interface gnmi scan: %w", err)
		}
		b.BucketTS = b.BucketTS.UTC()
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("device interface gnmi rows: %w", err)
	}

	if err := a.resolveDeviceInterfaceState(ctx, buckets, input.WindowEnd, input.SourceDatabase); err != nil {
		return nil, err
	}

	a.logRollup("computed device interface rollup buckets from gnmi", len(buckets), input, time.Since(start))
	return buckets, nil
}

// coalesceDeviceInterfaceBuckets merges gNMI-sourced and fact-table-sourced rollup buckets
// per (device, interface, bucket) key — the destination table's ReplacingMergeTree grain.
// For any key present in the gNMI buckets, the gNMI bucket wins; every other key falls back
// to its fact-table bucket. Keying at the row grain (not per device) keeps fact coverage for
// buckets/interfaces gNMI hasn't produced (mid-window gNMI start, stalls, partial-interface
// coverage), and because every live key is rewritten each cycle it supersedes stale rows.
// This lets monitoring shift to gNMI device-by-device as contributors upgrade, with no flag day.
func coalesceDeviceInterfaceBuckets(gnmiBuckets, factBuckets []DeviceInterfaceBucket) []DeviceInterfaceBucket {
	// Key on the destination table's replacement grain: (bucket_ts, device_pk, intf).
	type diKey struct {
		device string
		intf   string
		bucket int64
	}
	seen := make(map[diKey]struct{}, len(gnmiBuckets))
	out := make([]DeviceInterfaceBucket, 0, len(gnmiBuckets)+len(factBuckets))
	for _, b := range gnmiBuckets { // gNMI wins per key
		seen[diKey{b.DevicePK, b.Intf, b.BucketTS.UTC().UnixNano()}] = struct{}{}
		out = append(out, b)
	}
	for _, b := range factBuckets { // fact fills every key gNMI did not produce
		if _, ok := seen[diKey{b.DevicePK, b.Intf, b.BucketTS.UTC().UnixNano()}]; !ok {
			out = append(out, b)
		}
	}
	return out
}

// resolveDeviceInterfaceState resolves device Status, ISIS state, and user PK onto the
// given buckets. Shared by the InfluxDB (ComputeDeviceInterfaceRollup) and gNMI
// (ComputeDeviceInterfaceRollupFromGNMI) device-interface rollups so both produce
// fully-resolved buckets.
func (a *Activities) resolveDeviceInterfaceState(ctx context.Context, buckets []DeviceInterfaceBucket, asOf time.Time, srcDB string) error {
	if len(buckets) == 0 {
		return nil
	}

	devicePKs := make(map[string]struct{})
	tunnelKeys := make(map[tunnelKey]struct{})
	for i := range buckets {
		devicePKs[buckets[i].DevicePK] = struct{}{}
		if buckets[i].UserTunnelID != nil {
			tunnelKeys[tunnelKey{buckets[i].DevicePK, *buckets[i].UserTunnelID}] = struct{}{}
		}
	}

	deviceStatus, err := a.resolveDeviceStatus(ctx, devicePKs, asOf, srcDB)
	if err != nil {
		return fmt.Errorf("resolve device status: %w", err)
	}
	isisDeviceState, err := a.resolveISISDeviceState(ctx, devicePKs, asOf, srcDB)
	if err != nil {
		return fmt.Errorf("resolve ISIS device state: %w", err)
	}
	userPKs, err := a.resolveUserPKs(ctx, tunnelKeys, asOf, srcDB)
	if err != nil {
		return fmt.Errorf("resolve user PKs: %w", err)
	}

	for i := range buckets {
		b := &buckets[i]
		if status, ok := deviceStatus[b.DevicePK]; ok {
			b.Status = status
		}
		if state, ok := isisDeviceState[b.DevicePK]; ok {
			b.ISISOverload = state.overload
			b.ISISUnreachable = state.unreachable
		}
		if b.UserTunnelID != nil {
			tk := tunnelKey{b.DevicePK, *b.UserTunnelID}
			if pk, ok := userPKs[tk]; ok {
				b.UserPK = pk
			}
		}
	}
	return nil
}

// resolveDeviceStatus queries dim_dz_devices_history to get the most recent status
// for each device as of the given time.
func (a *Activities) resolveDeviceStatus(ctx context.Context, devicePKs map[string]struct{}, asOf time.Time, sourceDB string) (map[string]string, error) {
	query := `
		SELECT pk,
			argMax(status, snapshot_ts) as status
		FROM ` + tableRef(sourceDB, "dim_dz_devices_history") + `
		WHERE snapshot_ts <= $1
		GROUP BY pk
	`
	rows, err := a.ClickHouse.Query(ctx, query, asOf)
	if err != nil {
		return nil, fmt.Errorf("device status query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var pk, status string
		if err := rows.Scan(&pk, &status); err != nil {
			return nil, fmt.Errorf("device status scan: %w", err)
		}
		if _, ok := devicePKs[pk]; ok {
			result[pk] = status
		}
	}
	return result, rows.Err()
}

// isisDeviceStateInfo holds resolved ISIS device state.
type isisDeviceStateInfo struct {
	overload    bool
	unreachable bool
}

// resolveISISDeviceState queries dim_isis_devices_history to get ISIS overload and
// unreachable flags for each device as of the given time.
func (a *Activities) resolveISISDeviceState(ctx context.Context, devicePKs map[string]struct{}, asOf time.Time, sourceDB string) (map[string]isisDeviceStateInfo, error) {
	query := `
		SELECT device_pk,
			argMax(overload, snapshot_ts) as overload,
			argMax(node_unreachable, snapshot_ts) as node_unreachable,
			argMax(is_deleted, snapshot_ts) as is_deleted
		FROM ` + tableRef(sourceDB, "dim_isis_devices_history") + `
		WHERE snapshot_ts <= $1 AND device_pk != ''
		GROUP BY device_pk
	`
	rows, err := a.ClickHouse.Query(ctx, query, asOf)
	if err != nil {
		return nil, fmt.Errorf("ISIS device state query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]isisDeviceStateInfo)
	for rows.Next() {
		var devicePK string
		var overload, unreachable, isDeleted uint8
		if err := rows.Scan(&devicePK, &overload, &unreachable, &isDeleted); err != nil {
			return nil, fmt.Errorf("ISIS device state scan: %w", err)
		}
		if _, ok := devicePKs[devicePK]; ok && isDeleted == 0 {
			result[devicePK] = isisDeviceStateInfo{
				overload:    overload == 1,
				unreachable: unreachable == 1,
			}
		}
	}
	return result, rows.Err()
}

// resolveUserPKs queries dim_dz_users_history to map (device_pk, tunnel_id) pairs
// to user PKs as of the given time.
func (a *Activities) resolveUserPKs(ctx context.Context, tunnelKeys map[tunnelKey]struct{}, asOf time.Time, sourceDB string) (map[tunnelKey]string, error) {
	if len(tunnelKeys) == 0 {
		return nil, nil
	}

	query := `
		SELECT device_pk, tunnel_id,
			argMax(pk, snapshot_ts) as pk,
			argMax(is_deleted, snapshot_ts) as is_deleted
		FROM ` + tableRef(sourceDB, "dim_dz_users_history") + `
		WHERE snapshot_ts <= $1
		GROUP BY device_pk, tunnel_id
	`
	rows, err := a.ClickHouse.Query(ctx, query, asOf)
	if err != nil {
		return nil, fmt.Errorf("user PK query: %w", err)
	}
	defer rows.Close()

	result := make(map[tunnelKey]string)
	for rows.Next() {
		var devicePK, pk string
		var tunnelID int32
		var isDeleted uint8
		if err := rows.Scan(&devicePK, &tunnelID, &pk, &isDeleted); err != nil {
			return nil, fmt.Errorf("user PK scan: %w", err)
		}
		tk := tunnelKey{devicePK, int64(tunnelID)}
		if _, ok := tunnelKeys[tk]; ok && isDeleted == 0 {
			result[tk] = pk
		}
	}
	return result, rows.Err()
}

// WriteLinkBuckets batch inserts link rollup buckets into ClickHouse.
func (a *Activities) WriteLinkBuckets(ctx context.Context, buckets []LinkBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	safeHeartbeat(ctx, fmt.Sprintf("writing %d link buckets", len(buckets)))

	batch, err := a.ClickHouse.PrepareBatch(ctx, `INSERT INTO link_rollup_5m (
		bucket_ts, link_pk, ingested_at,
		a_avg_rtt_us, a_min_rtt_us, a_p50_rtt_us, a_p90_rtt_us, a_p95_rtt_us, a_p99_rtt_us, a_max_rtt_us, a_loss_pct, a_samples,
		a_avg_jitter_us, a_min_jitter_us, a_p50_jitter_us, a_p90_jitter_us, a_p95_jitter_us, a_p99_jitter_us, a_max_jitter_us,
		z_avg_rtt_us, z_min_rtt_us, z_p50_rtt_us, z_p90_rtt_us, z_p95_rtt_us, z_p99_rtt_us, z_max_rtt_us, z_loss_pct, z_samples,
		z_avg_jitter_us, z_min_jitter_us, z_p50_jitter_us, z_p90_jitter_us, z_p95_jitter_us, z_p99_jitter_us, z_max_jitter_us,
		status, provisioning, isis_down
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, b := range buckets {
		if err := batch.Append(
			b.BucketTS, b.LinkPK, b.IngestedAt,
			b.A.AvgRttUs, b.A.MinRttUs, b.A.P50RttUs, b.A.P90RttUs, b.A.P95RttUs, b.A.P99RttUs, b.A.MaxRttUs, b.A.LossPct, b.A.Samples,
			b.A.AvgJitterUs, b.A.MinJitterUs, b.A.P50JitterUs, b.A.P90JitterUs, b.A.P95JitterUs, b.A.P99JitterUs, b.A.MaxJitterUs,
			b.Z.AvgRttUs, b.Z.MinRttUs, b.Z.P50RttUs, b.Z.P90RttUs, b.Z.P95RttUs, b.Z.P99RttUs, b.Z.MaxRttUs, b.Z.LossPct, b.Z.Samples,
			b.Z.AvgJitterUs, b.Z.MinJitterUs, b.Z.P50JitterUs, b.Z.P90JitterUs, b.Z.P95JitterUs, b.Z.P99JitterUs, b.Z.MaxJitterUs,
			b.Status, b.Provisioning, b.ISISDown,
		); err != nil {
			return fmt.Errorf("append batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	a.Log.Info("wrote link rollup buckets", "count", len(buckets))
	return nil
}

// WriteDeviceInterfaceBuckets batch inserts device interface rollup buckets into ClickHouse.
func (a *Activities) WriteDeviceInterfaceBuckets(ctx context.Context, buckets []DeviceInterfaceBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	safeHeartbeat(ctx, fmt.Sprintf("writing %d device interface buckets", len(buckets)))

	batch, err := a.ClickHouse.PrepareBatch(ctx, `INSERT INTO device_interface_rollup_5m (
		bucket_ts, device_pk, intf, ingested_at,
		link_pk, link_side, user_tunnel_id, user_pk,
		in_errors, out_errors, in_fcs_errors, in_discards, out_discards, carrier_transitions,
		avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps,
		avg_in_mcast_pps, min_in_mcast_pps, p50_in_mcast_pps, p90_in_mcast_pps, p95_in_mcast_pps, p99_in_mcast_pps, max_in_mcast_pps,
		avg_out_mcast_pps, min_out_mcast_pps, p50_out_mcast_pps, p90_out_mcast_pps, p95_out_mcast_pps, p99_out_mcast_pps, max_out_mcast_pps,
		status, isis_overload, isis_unreachable
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, b := range buckets {
		if err := batch.Append(
			b.BucketTS, b.DevicePK, b.Intf, b.IngestedAt,
			b.LinkPK, b.LinkSide, b.UserTunnelID, b.UserPK,
			b.InErrors, b.OutErrors, b.InFcsErrors, b.InDiscards, b.OutDiscards, b.CarrierTransitions,
			b.InBps.Avg, b.InBps.Min, b.InBps.P50, b.InBps.P90, b.InBps.P95, b.InBps.P99, b.InBps.Max,
			b.OutBps.Avg, b.OutBps.Min, b.OutBps.P50, b.OutBps.P90, b.OutBps.P95, b.OutBps.P99, b.OutBps.Max,
			b.InPps.Avg, b.InPps.Min, b.InPps.P50, b.InPps.P90, b.InPps.P95, b.InPps.P99, b.InPps.Max,
			b.OutPps.Avg, b.OutPps.Min, b.OutPps.P50, b.OutPps.P90, b.OutPps.P95, b.OutPps.P99, b.OutPps.Max,
			b.InMcastPps.Avg, b.InMcastPps.Min, b.InMcastPps.P50, b.InMcastPps.P90, b.InMcastPps.P95, b.InMcastPps.P99, b.InMcastPps.Max,
			b.OutMcastPps.Avg, b.OutMcastPps.Min, b.OutMcastPps.P50, b.OutMcastPps.P90, b.OutMcastPps.P95, b.OutMcastPps.P99, b.OutMcastPps.Max,
			b.Status, b.ISISOverload, b.ISISUnreachable,
		); err != nil {
			return fmt.Errorf("append batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	a.Log.Info("wrote device interface rollup buckets", "count", len(buckets))
	return nil
}

// RollupLinks computes link rollup buckets and writes them to ClickHouse in a
// single activity, avoiding large payloads flowing through Temporal.
func (a *Activities) RollupLinks(ctx context.Context, input BackfillChunkInput) (int, error) {
	var count int
	err := a.IngestionLog.Wrap(ctx, "rollup", "RollupLinks", a.Network, func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		buckets, err := a.ComputeLinkRollup(ctx, input)
		if err != nil {
			return result, err
		}
		if err := a.WriteLinkBuckets(ctx, buckets); err != nil {
			return result, err
		}
		count = len(buckets)
		result.RowsAffected = int64(count)
		return result, nil
	})
	return count, err
}

// RollupDeviceInterfaces computes device interface rollup buckets and writes
// them to ClickHouse in a single activity.
func (a *Activities) RollupDeviceInterfaces(ctx context.Context, input BackfillChunkInput) (int, error) {
	input.TelemetryDatabase = a.TelemetryDatabase
	var count int
	err := a.IngestionLog.Wrap(ctx, "rollup", "RollupDeviceInterfaces", a.Network, func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		factBuckets, err := a.ComputeDeviceInterfaceRollup(ctx, input)
		if err != nil {
			return result, err
		}
		// gNMI is additive: if it fails (e.g. misconfigured or unavailable), fall back to
		// fact-only so the rollup is never worse than the InfluxDB-only behavior.
		gnmiBuckets, gerr := a.ComputeDeviceInterfaceRollupFromGNMI(ctx, input)
		if gerr != nil {
			a.Log.Warn("gnmi device interface rollup failed; falling back to fact-only", "error", gerr)
			gnmiBuckets = nil
		}
		buckets := coalesceDeviceInterfaceBuckets(gnmiBuckets, factBuckets)
		if err := a.WriteDeviceInterfaceBuckets(ctx, buckets); err != nil {
			return result, err
		}
		count = len(buckets)
		result.RowsAffected = int64(count)
		return result, nil
	})
	return count, err
}
