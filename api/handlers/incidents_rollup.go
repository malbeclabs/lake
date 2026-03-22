package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// incidentWindow captures the time range and entity for a counter incident.
type incidentWindow struct {
	entityPK     string
	incidentType string
	startedAt    time.Time
	endedAt      time.Time // for ongoing, use now
}

// incidentQueryParams holds all parameters for rollup-based incident queries.
type incidentQueryParams struct {
	Duration          time.Duration
	BucketInterval    time.Duration
	LossThreshold     float64
	ErrorsThreshold   int64
	FCSThreshold      int64
	DiscardsThreshold int64
	CarrierThreshold  int64
	MinDurationMin    int64
	CoalesceGapMin    int64
	TypeFilter        string // "all", "packet_loss", "errors", etc.
	Filters           []IncidentFilter
	IncludeLinkIntfs  bool // for device incidents: include link-associated interfaces
	UseRaw            bool // query raw fact tables instead of rollup tables
}

// rollupBucketMinutes returns bucket size in minutes based on duration.
func rollupBucketMinutes(d time.Duration) int {
	if d > 3*24*time.Hour {
		return 15
	}
	return 5
}

// isDefaultLinkThresholds returns true if the params use default thresholds that match
// the link_incidents_v view (loss ≥ 10%, counters ≥ 1, coalesce gap 180 min, bucket 5 min).
func isDefaultLinkThresholds(p incidentQueryParams) bool {
	return p.LossThreshold == 10 &&
		p.ErrorsThreshold == 1 &&
		p.FCSThreshold == 1 &&
		p.DiscardsThreshold == 1 &&
		p.CarrierThreshold == 1 &&
		p.CoalesceGapMin == 180 &&
		rollupBucketMinutes(p.Duration) == 5
}

// buildLinkIncidentsViewQuery builds a simple query against the link_incidents_v view.
func buildLinkIncidentsViewQuery(p incidentQueryParams) (string, []any) {
	startSecs := int64(p.Duration.Seconds())
	var args []any
	argIdx := 1

	addArg := func(v any) string {
		args = append(args, v)
		s := fmt.Sprintf("$%d", argIdx)
		argIdx++
		return s
	}

	startArg := addArg(startSecs)

	var typeClause string
	if p.TypeFilter != "all" {
		typeArg := addArg(p.TypeFilter)
		typeClause = fmt.Sprintf("AND incident_type = %s", typeArg)
	}

	filterClauses, filterArgs := buildLinkFilterClauses(p.Filters, argIdx)
	args = append(args, filterArgs...)

	query := fmt.Sprintf(`
SELECT
	entity_pk, incident_type, started_at, ended_at, is_ongoing,
	peak_value, total_buckets, duration_seconds,
	link_code, link_type, status, side_a_metro, side_z_metro, contributor_code
FROM link_incidents_v
WHERE (ended_at >= now() - INTERVAL %s SECOND OR is_ongoing)
  %s
  %s
ORDER BY started_at DESC`,
		startArg, typeClause, filterClauses)

	return query, args
}

// buildLinkIncidentsQuery builds a single SQL query that detects all link incident types
// using UNION ALL across rollup tables, with gap-and-island detection, coalescing,
// min-duration filtering, and metadata JOINs all in SQL.
func buildLinkIncidentsQuery(p incidentQueryParams) (string, []any) {
	// Use the view for default thresholds (not in raw mode)
	if isDefaultLinkThresholds(p) && !p.UseRaw {
		return buildLinkIncidentsViewQuery(p)
	}

	bucketMin := rollupBucketMinutes(p.Duration)
	startSecs := int64(p.Duration.Seconds())
	// Add 1 day lookback so incidents starting before the window get their true start
	lookbackSecs := int64((p.Duration + 24*time.Hour).Seconds())

	var aboveParts []string
	var args []any
	argIdx := 1

	// Helper to add a numbered arg
	addArg := func(v any) string {
		args = append(args, v)
		s := fmt.Sprintf("$%d", argIdx)
		argIdx++
		return s
	}

	// Re-bucket expression for coarser intervals
	bucketExpr := func(ts string) string {
		if bucketMin == 5 {
			return ts
		}
		return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d MINUTE)", ts, bucketMin)
	}

	lookbackArg := addArg(lookbackSecs) // $1
	startArg := addArg(startSecs)       // $2

	// Source tables: rollup or raw fact tables
	linkSource := "link_rollup_5m FINAL"
	intfSource := "device_interface_rollup_5m FINAL"
	if p.UseRaw {
		// Raw link source: compute per-direction loss from latency probes,
		// resolve ISIS and provisioning state per-bucket from history tables.
		linkSource = `(
			SELECT
				lb.bucket_ts AS bucket_ts,
				lb.link_pk AS link_pk,
				lb.a_loss_pct AS a_loss_pct,
				lb.z_loss_pct AS z_loss_pct,
				CASE WHEN ia.link_pk IS NULL THEN false ELSE ia.is_deleted = 1 END AS isis_down,
				COALESCE(lh.committed_rtt_ns, lc.committed_rtt_ns, 0) = 500000 AS provisioning
			FROM (
				SELECT
					toStartOfFiveMinutes(f.event_ts) AS bucket_ts,
					f.link_pk AS link_pk,
					countIf(f.origin_device_pk = l.side_a_pk AND (f.loss = true OR f.rtt_us = 0)) * 100.0
						/ greatest(countIf(f.origin_device_pk = l.side_a_pk), 1) AS a_loss_pct,
					countIf(f.origin_device_pk != l.side_a_pk AND (f.loss = true OR f.rtt_us = 0)) * 100.0
						/ greatest(countIf(f.origin_device_pk != l.side_a_pk), 1) AS z_loss_pct
				FROM fact_dz_device_link_latency f
				JOIN dz_links_current l ON f.link_pk = l.pk
				GROUP BY bucket_ts, f.link_pk
			) lb
			LEFT JOIN (
				SELECT pk, toStartOfFiveMinutes(snapshot_ts) AS bucket_ts,
					argMax(committed_rtt_ns, snapshot_ts) AS committed_rtt_ns
				FROM dim_dz_links_history
				GROUP BY pk, bucket_ts
			) lh ON lb.link_pk = lh.pk AND lb.bucket_ts = lh.bucket_ts
			LEFT JOIN dz_links_current lc ON lb.link_pk = lc.pk
			LEFT JOIN (
				SELECT link_pk, argMax(is_deleted, snapshot_ts) AS is_deleted
				FROM dim_isis_adjacencies_history WHERE link_pk != ''
				GROUP BY link_pk
			) ia ON lb.link_pk = ia.link_pk
		)`
		// Raw interface source: compute counter sums from raw deltas
		intfSource = `(
			SELECT
				toStartOfFiveMinutes(ic.event_ts) AS bucket_ts,
				anyIf(ic.link_pk, ic.link_pk != '') AS link_pk,
				toUInt64(SUM(greatest(0, ic.in_errors_delta))) AS in_errors,
				toUInt64(SUM(greatest(0, ic.out_errors_delta))) AS out_errors,
				toUInt64(SUM(greatest(0, ic.in_fcs_errors_delta))) AS in_fcs_errors,
				toUInt64(SUM(greatest(0, ic.in_discards_delta))) AS in_discards,
				toUInt64(SUM(greatest(0, ic.out_discards_delta))) AS out_discards,
				toUInt64(SUM(greatest(0, ic.carrier_transitions_delta))) AS carrier_transitions
			FROM fact_dz_device_interface_counters ic
			WHERE ic.link_pk != ''
			GROUP BY bucket_ts, ic.device_pk, ic.intf
		)`
	}

	// Packet loss
	if p.TypeFilter == "all" || p.TypeFilter == "packet_loss" {
		lossThreshArg := addArg(p.LossThreshold)
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT link_pk AS entity_pk, %s AS bucket_ts,
			greatest(a_loss_pct, z_loss_pct) AS metric_value,
			'packet_loss' AS incident_type
		FROM %s
		WHERE bucket_ts >= now() - INTERVAL %s SECOND
		  AND provisioning = false
		  AND greatest(a_loss_pct, z_loss_pct) >= %s`,
			bucketExpr("bucket_ts"), linkSource, lookbackArg, lossThreshArg))
	}

	// ISIS down
	if p.TypeFilter == "all" || p.TypeFilter == "isis_down" {
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT link_pk AS entity_pk, %s AS bucket_ts,
			toFloat64(1) AS metric_value,
			'isis_down' AS incident_type
		FROM %s
		WHERE bucket_ts >= now() - INTERVAL %s SECOND
		  AND isis_down = true
		  AND provisioning = false`,
			bucketExpr("bucket_ts"), linkSource, lookbackArg))
	}

	// No data: missing rows
	if p.TypeFilter == "all" || p.TypeFilter == "no_data" {
		noDataSource := "link_rollup_5m FINAL"
		if p.UseRaw {
			noDataSource = linkSource
		}
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT al.link_pk AS entity_pk, e.bucket_ts AS bucket_ts,
			toFloat64(1) AS metric_value,
			'no_data' AS incident_type
		FROM (
			SELECT DISTINCT link_pk FROM %s
			WHERE bucket_ts >= now() - INTERVAL %s SECOND - INTERVAL 1 HOUR
			  AND provisioning = false
		) al
		CROSS JOIN (
			SELECT toDateTime(toStartOfFiveMinutes(now() - INTERVAL %s SECOND)) + number * 300 AS bucket_ts
			FROM numbers(toUInt64(dateDiff('second', now() - INTERVAL %s SECOND, now()) / 300))
		) e
		LEFT JOIN (
			SELECT link_pk, bucket_ts FROM %s
			WHERE bucket_ts >= now() - INTERVAL %s SECOND
		) a ON al.link_pk = a.link_pk AND e.bucket_ts = a.bucket_ts
		WHERE a.bucket_ts IS NULL`,
			noDataSource, lookbackArg, lookbackArg, lookbackArg, noDataSource, lookbackArg))
	}

	// Counter types
	counterTypes := []struct {
		name      string
		expr      string
		threshold int64
	}{
		{"errors", "sum(in_errors + out_errors)", p.ErrorsThreshold},
		{"fcs", "sum(in_fcs_errors)", p.FCSThreshold},
		{"discards", "sum(in_discards + out_discards)", p.DiscardsThreshold},
		{"carrier", "sum(carrier_transitions)", p.CarrierThreshold},
	}

	for _, ct := range counterTypes {
		if p.TypeFilter != "all" && p.TypeFilter != ct.name {
			continue
		}
		threshArg := addArg(ct.threshold)
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT link_pk AS entity_pk, %s AS bucket_ts,
			toFloat64(%s) AS metric_value,
			'%s' AS incident_type
		FROM %s
		WHERE bucket_ts >= now() - INTERVAL %s SECOND
		  AND link_pk != ''
		GROUP BY link_pk, bucket_ts
		HAVING metric_value >= %s`,
			bucketExpr("bucket_ts"), ct.expr, ct.name, intfSource, lookbackArg, threshArg))
	}

	if len(aboveParts) == 0 {
		return "", nil
	}

	aboveCTE := strings.Join(aboveParts, "\n\n\tUNION ALL\n")

	// Build filter clauses for metadata
	filterClauses, filterArgs := buildLinkFilterClauses(p.Filters, argIdx)
	args = append(args, filterArgs...)
	argIdx += len(filterArgs)

	coalesceGapArg := addArg(p.CoalesceGapMin)
	bucketMinArg := addArg(bucketMin)

	query := fmt.Sprintf(`
WITH
above AS (%s
),

-- Gap-and-island: group consecutive above-threshold buckets
islands AS (
	SELECT entity_pk, incident_type, bucket_ts, metric_value,
		bucket_ts - toIntervalSecond(row_number() OVER (
			PARTITION BY entity_pk, incident_type ORDER BY bucket_ts
		) * %s * 60) AS island_grp
	FROM above
),

-- Aggregate each island
raw_incidents AS (
	SELECT entity_pk, incident_type, island_grp,
		min(bucket_ts) AS started_at,
		max(bucket_ts) + toIntervalSecond(%s * 60) AS ended_at,
		max(metric_value) AS peak_value,
		count() AS bucket_count
	FROM islands
	GROUP BY entity_pk, incident_type, island_grp
),

-- Coalesce nearby incidents
numbered AS (
	SELECT *,
		lagInFrame(ended_at) OVER (
			PARTITION BY entity_pk, incident_type ORDER BY started_at
		) AS prev_ended_at
	FROM raw_incidents
),
coalesce_groups AS (
	SELECT *,
		sum(if(prev_ended_at IS NULL
			OR dateDiff('minute', prev_ended_at, started_at) >= %s, 1, 0))
			OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS coalesce_grp
	FROM numbered
),
coalesced AS (
	SELECT entity_pk, incident_type,
		min(started_at) AS started_at,
		max(ended_at) AS ended_at,
		max(peak_value) AS peak_value,
		sum(bucket_count) AS total_buckets
	FROM coalesce_groups
	GROUP BY entity_pk, incident_type, coalesce_grp
)

SELECT
	c.entity_pk,
	c.incident_type,
	c.started_at,
	if(c.ended_at >= now() - toIntervalMinute(15), toDateTime('1970-01-01 00:00:00'), c.ended_at) AS ended_at,
	c.ended_at >= now() - toIntervalMinute(15) AS is_ongoing,
	c.peak_value,
	c.total_buckets,
	dateDiff('second', c.started_at,
		if(c.ended_at >= now() - toIntervalMinute(15), now(), c.ended_at)) AS duration_seconds,
	COALESCE(l.code, '') AS link_code,
	COALESCE(l.link_type, '') AS link_type,
	COALESCE(l.status, '') AS status,
	COALESCE(ma.code, '') AS side_a_metro,
	COALESCE(mz.code, '') AS side_z_metro,
	COALESCE(cc.code, '') AS contributor_code
FROM coalesced c
LEFT JOIN dz_links_current l ON c.entity_pk = l.pk
LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
LEFT JOIN dz_contributors_current cc ON l.contributor_pk = cc.pk
WHERE c.ended_at >= now() - INTERVAL %s SECOND
  %s
ORDER BY c.started_at DESC`,
		aboveCTE,
		bucketMinArg,   // island row_number interval
		bucketMinArg,   // ended_at offset
		coalesceGapArg, // coalesce gap threshold
		startArg,       // show incidents that overlap with the display window
		filterClauses,
	)

	return query, args
}

// buildLinkFilterClauses builds WHERE clauses for link incident metadata filtering.
func buildLinkFilterClauses(filters []IncidentFilter, startArgIdx int) (string, []any) {
	var clauses []string
	var args []any
	argIdx := startArgIdx

	for _, f := range filters {
		switch f.Type {
		case "metro":
			clauses = append(clauses, fmt.Sprintf("AND (ma.code = $%d OR mz.code = $%d)", argIdx, argIdx))
			args = append(args, f.Value)
			argIdx++
		case "link":
			clauses = append(clauses, fmt.Sprintf("AND l.code = $%d", argIdx))
			args = append(args, f.Value)
			argIdx++
		case "contributor":
			clauses = append(clauses, fmt.Sprintf("AND cc.code = $%d", argIdx))
			args = append(args, f.Value)
			argIdx++
		case "device":
			clauses = append(clauses, fmt.Sprintf("AND (da.code = $%d OR dz.code = $%d)", argIdx, argIdx))
			args = append(args, f.Value)
			argIdx++
		}
	}

	return strings.Join(clauses, "\n  "), args
}

// fetchLinkIncidentsFromRollup executes the single rollup query and scans into LinkIncident structs.
func fetchLinkIncidentsFromRollup(ctx context.Context, conn driver.Conn, p incidentQueryParams) ([]LinkIncident, error) {
	query, args := buildLinkIncidentsQuery(p)
	if query == "" {
		return nil, nil
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("link incidents rollup query failed: %w", err)
	}
	defer rows.Close()

	var incidents []LinkIncident
	idCounter := 0

	for rows.Next() {
		var (
			entityPK        string
			incidentType    string
			startedAt       time.Time
			endedAt         time.Time
			isOngoing       bool
			peakValue       float64
			totalBuckets    uint64
			durationSeconds int64
			linkCode        string
			linkType        string
			status          string
			sideAMetro      string
			sideZMetro      string
			contributorCode string
		)

		if err := rows.Scan(
			&entityPK, &incidentType, &startedAt, &endedAt, &isOngoing,
			&peakValue, &totalBuckets, &durationSeconds,
			&linkCode, &linkType, &status, &sideAMetro, &sideZMetro, &contributorCode,
		); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		idCounter++
		isDrained := status == "soft-drained" || status == "hard-drained"

		inc := LinkIncident{
			ID:              fmt.Sprintf("%s-%d", incidentType, idCounter),
			LinkPK:          entityPK,
			LinkCode:        linkCode,
			LinkType:        linkType,
			SideAMetro:      sideAMetro,
			SideZMetro:      sideZMetro,
			ContributorCode: contributorCode,
			IncidentType:    incidentType,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       isOngoing,
			IsDrained:       isDrained,
			Severity:        incidentSeverity(incidentType, peakValue, int64(peakValue)),
		}

		if !isOngoing {
			endStr := endedAt.UTC().Format(time.RFC3339)
			inc.EndedAt = &endStr
		}
		inc.DurationSeconds = &durationSeconds

		// Populate type-specific fields
		switch incidentType {
		case "packet_loss":
			threshold := p.LossThreshold
			inc.ThresholdPct = &threshold
			inc.PeakLossPct = &peakValue
		case "errors", "fcs", "discards", "carrier":
			var threshold int64
			switch incidentType {
			case "errors":
				threshold = p.ErrorsThreshold
			case "fcs":
				threshold = p.FCSThreshold
			case "discards":
				threshold = p.DiscardsThreshold
			case "carrier":
				threshold = p.CarrierThreshold
			}
			peak := int64(peakValue)
			inc.ThresholdCount = &threshold
			inc.PeakCount = &peak
		}

		// Set Confirmed based on min duration
		minDurationSecs := p.MinDurationMin * 60
		if isOngoing {
			inc.Confirmed = time.Since(startedAt) >= time.Duration(p.MinDurationMin)*time.Minute
		} else {
			inc.Confirmed = durationSeconds >= minDurationSecs
		}

		incidents = append(incidents, inc)
	}

	return incidents, nil
}

// isDefaultDeviceThresholds returns true if the params use default thresholds that match
// the device_incidents_v view (counters ≥ 1, coalesce gap 180 min, bucket 5 min, no link intfs).
func isDefaultDeviceThresholds(p incidentQueryParams) bool {
	return p.ErrorsThreshold == 1 &&
		p.FCSThreshold == 1 &&
		p.DiscardsThreshold == 1 &&
		p.CarrierThreshold == 1 &&
		p.CoalesceGapMin == 180 &&
		rollupBucketMinutes(p.Duration) == 5 &&
		!p.IncludeLinkIntfs
}

// buildDeviceIncidentsViewQuery builds a simple query against the device_incidents_v view.
func buildDeviceIncidentsViewQuery(p incidentQueryParams) (string, []any) {
	startSecs := int64(p.Duration.Seconds())
	var args []any
	argIdx := 1

	addArg := func(v any) string {
		args = append(args, v)
		s := fmt.Sprintf("$%d", argIdx)
		argIdx++
		return s
	}

	startArg := addArg(startSecs)

	var typeClause string
	if p.TypeFilter != "all" {
		typeArg := addArg(p.TypeFilter)
		typeClause = fmt.Sprintf("AND incident_type = %s", typeArg)
	}

	filterClauses, filterArgs := buildDeviceFilterClauses(p.Filters, argIdx)
	args = append(args, filterArgs...)

	query := fmt.Sprintf(`
SELECT
	entity_pk, incident_type, started_at, ended_at, is_ongoing,
	peak_value, total_buckets, duration_seconds,
	device_code, device_type, status, metro, contributor_code
FROM device_incidents_v
WHERE (ended_at >= now() - INTERVAL %s SECOND OR is_ongoing)
  %s
  %s
ORDER BY started_at DESC`,
		startArg, typeClause, filterClauses)

	return query, args
}

// buildDeviceIncidentsQuery builds a single SQL query that detects all device incident types.
func buildDeviceIncidentsQuery(p incidentQueryParams) (string, []any) {
	// Use the view for default thresholds
	if isDefaultDeviceThresholds(p) {
		return buildDeviceIncidentsViewQuery(p)
	}

	bucketMin := rollupBucketMinutes(p.Duration)
	startSecs := int64(p.Duration.Seconds())
	lookbackSecs := int64((p.Duration + 24*time.Hour).Seconds())

	var aboveParts []string
	var args []any
	argIdx := 1

	addArg := func(v any) string {
		args = append(args, v)
		s := fmt.Sprintf("$%d", argIdx)
		argIdx++
		return s
	}

	bucketExpr := func(ts string) string {
		if bucketMin == 5 {
			return ts
		}
		return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d MINUTE)", ts, bucketMin)
	}

	lookbackArg := addArg(lookbackSecs) // $1
	startArg := addArg(startSecs)       // $2

	// Link interface filter: by default device incidents exclude link-associated interfaces
	linkFilter := "AND link_pk = ''"
	if p.IncludeLinkIntfs {
		linkFilter = ""
	}

	// Counter types from device_interface_rollup_5m grouped by device_pk
	counterTypes := []struct {
		name      string
		expr      string
		threshold int64
	}{
		{"errors", "sum(in_errors + out_errors)", p.ErrorsThreshold},
		{"fcs", "sum(in_fcs_errors)", p.FCSThreshold},
		{"discards", "sum(in_discards + out_discards)", p.DiscardsThreshold},
		{"carrier", "sum(carrier_transitions)", p.CarrierThreshold},
	}

	for _, ct := range counterTypes {
		if p.TypeFilter != "all" && p.TypeFilter != ct.name {
			continue
		}
		threshArg := addArg(ct.threshold)
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT device_pk AS entity_pk, %s AS bucket_ts,
			toFloat64(%s) AS metric_value,
			'%s' AS incident_type
		FROM device_interface_rollup_5m FINAL
		WHERE bucket_ts >= now() - INTERVAL %s SECOND
		  %s
		GROUP BY device_pk, bucket_ts
		HAVING metric_value >= %s`,
			bucketExpr("bucket_ts"), ct.expr, ct.name, lookbackArg, linkFilter, threshArg))
	}

	// No data for devices: missing rollup rows
	if p.TypeFilter == "all" || p.TypeFilter == "no_data" {
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT ad.device_pk AS entity_pk, e.bucket_ts AS bucket_ts,
			toFloat64(1) AS metric_value,
			'no_data' AS incident_type
		FROM (
			SELECT DISTINCT device_pk FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= now() - INTERVAL %s SECOND - INTERVAL 1 HOUR
			  %s
		) ad
		CROSS JOIN (
			SELECT toDateTime(toStartOfFiveMinutes(now() - INTERVAL %s SECOND)) + number * 300 AS bucket_ts
			FROM numbers(toUInt64(dateDiff('second', now() - INTERVAL %s SECOND, now()) / 300))
		) e
		LEFT JOIN (
			SELECT device_pk, bucket_ts FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= now() - INTERVAL %s SECOND
			  %s
			GROUP BY device_pk, bucket_ts
		) a ON ad.device_pk = a.device_pk AND e.bucket_ts = a.bucket_ts
		WHERE a.bucket_ts IS NULL`,
			lookbackArg, linkFilter, lookbackArg, lookbackArg, lookbackArg, linkFilter))
	}

	// ISIS overload from device_interface_rollup_5m
	if p.TypeFilter == "all" || p.TypeFilter == "isis_overload" {
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT device_pk AS entity_pk, %s AS bucket_ts,
			toFloat64(1) AS metric_value,
			'isis_overload' AS incident_type
		FROM device_interface_rollup_5m FINAL
		WHERE bucket_ts >= now() - INTERVAL %s SECOND
		  AND isis_overload = true
		  %s
		GROUP BY device_pk, bucket_ts`,
			bucketExpr("bucket_ts"), lookbackArg, linkFilter))
	}

	// ISIS unreachable from device_interface_rollup_5m
	if p.TypeFilter == "all" || p.TypeFilter == "isis_unreachable" {
		aboveParts = append(aboveParts, fmt.Sprintf(`
		SELECT device_pk AS entity_pk, %s AS bucket_ts,
			toFloat64(1) AS metric_value,
			'isis_unreachable' AS incident_type
		FROM device_interface_rollup_5m FINAL
		WHERE bucket_ts >= now() - INTERVAL %s SECOND
		  AND isis_unreachable = true
		  %s
		GROUP BY device_pk, bucket_ts`,
			bucketExpr("bucket_ts"), lookbackArg, linkFilter))
	}

	if len(aboveParts) == 0 {
		return "", nil
	}

	aboveCTE := strings.Join(aboveParts, "\n\n\tUNION ALL\n")

	// Build filter clauses for device metadata
	filterClauses, filterArgs := buildDeviceFilterClauses(p.Filters, argIdx)
	args = append(args, filterArgs...)
	argIdx += len(filterArgs)

	coalesceGapArg := addArg(p.CoalesceGapMin)
	bucketMinArg := addArg(bucketMin)

	query := fmt.Sprintf(`
WITH
above AS (%s
),

islands AS (
	SELECT entity_pk, incident_type, bucket_ts, metric_value,
		bucket_ts - toIntervalSecond(row_number() OVER (
			PARTITION BY entity_pk, incident_type ORDER BY bucket_ts
		) * %s * 60) AS island_grp
	FROM above
),

raw_incidents AS (
	SELECT entity_pk, incident_type, island_grp,
		min(bucket_ts) AS started_at,
		max(bucket_ts) + toIntervalSecond(%s * 60) AS ended_at,
		max(metric_value) AS peak_value,
		count() AS bucket_count
	FROM islands
	GROUP BY entity_pk, incident_type, island_grp
),

numbered AS (
	SELECT *,
		lagInFrame(ended_at) OVER (
			PARTITION BY entity_pk, incident_type ORDER BY started_at
		) AS prev_ended_at
	FROM raw_incidents
),
coalesce_groups AS (
	SELECT *,
		sum(if(prev_ended_at IS NULL
			OR dateDiff('minute', prev_ended_at, started_at) >= %s, 1, 0))
			OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS coalesce_grp
	FROM numbered
),
coalesced AS (
	SELECT entity_pk, incident_type,
		min(started_at) AS started_at,
		max(ended_at) AS ended_at,
		max(peak_value) AS peak_value,
		sum(bucket_count) AS total_buckets
	FROM coalesce_groups
	GROUP BY entity_pk, incident_type, coalesce_grp
)

SELECT
	c.entity_pk,
	c.incident_type,
	c.started_at,
	if(c.ended_at >= now() - toIntervalMinute(15), toDateTime('1970-01-01 00:00:00'), c.ended_at) AS ended_at,
	c.ended_at >= now() - toIntervalMinute(15) AS is_ongoing,
	c.peak_value,
	c.total_buckets,
	dateDiff('second', c.started_at,
		if(c.ended_at >= now() - toIntervalMinute(15), now(), c.ended_at)) AS duration_seconds,
	COALESCE(d.code, '') AS device_code,
	COALESCE(d.device_type, '') AS device_type,
	COALESCE(d.status, '') AS status,
	COALESCE(m.code, '') AS metro,
	COALESCE(cc.code, '') AS contributor_code
FROM coalesced c
LEFT JOIN dz_devices_current d ON c.entity_pk = d.pk
LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT JOIN dz_contributors_current cc ON d.contributor_pk = cc.pk
WHERE c.ended_at >= now() - INTERVAL %s SECOND
  %s
ORDER BY c.started_at DESC`,
		aboveCTE,
		bucketMinArg,
		bucketMinArg,
		coalesceGapArg,
		startArg,
		filterClauses,
	)

	return query, args
}

// buildDeviceFilterClauses builds WHERE clauses for device incident metadata filtering.
func buildDeviceFilterClauses(filters []IncidentFilter, startArgIdx int) (string, []any) {
	var clauses []string
	var args []any
	argIdx := startArgIdx

	for _, f := range filters {
		switch f.Type {
		case "metro":
			clauses = append(clauses, fmt.Sprintf("AND m.code = $%d", argIdx))
			args = append(args, f.Value)
			argIdx++
		case "device":
			clauses = append(clauses, fmt.Sprintf("AND d.code = $%d", argIdx))
			args = append(args, f.Value)
			argIdx++
		case "contributor":
			clauses = append(clauses, fmt.Sprintf("AND cc.code = $%d", argIdx))
			args = append(args, f.Value)
			argIdx++
		}
	}

	return strings.Join(clauses, "\n  "), args
}

// fetchDeviceIncidentsFromRollup executes the single rollup query and scans into DeviceIncident structs.
func fetchDeviceIncidentsFromRollup(ctx context.Context, conn driver.Conn, p incidentQueryParams) ([]DeviceIncident, error) {
	query, args := buildDeviceIncidentsQuery(p)
	if query == "" {
		return nil, nil
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("device incidents rollup query failed: %w", err)
	}
	defer rows.Close()

	var incidents []DeviceIncident
	idCounter := 0

	for rows.Next() {
		var (
			entityPK        string
			incidentType    string
			startedAt       time.Time
			endedAt         time.Time
			isOngoing       bool
			peakValue       float64
			totalBuckets    uint64
			durationSeconds int64
			deviceCode      string
			deviceType      string
			status          string
			metro           string
			contributorCode string
		)

		if err := rows.Scan(
			&entityPK, &incidentType, &startedAt, &endedAt, &isOngoing,
			&peakValue, &totalBuckets, &durationSeconds,
			&deviceCode, &deviceType, &status, &metro, &contributorCode,
		); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		idCounter++

		inc := DeviceIncident{
			ID:              fmt.Sprintf("dev-%s-%d", incidentType, idCounter),
			DevicePK:        entityPK,
			DeviceCode:      deviceCode,
			DeviceType:      deviceType,
			Metro:           metro,
			ContributorCode: contributorCode,
			IncidentType:    incidentType,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       isOngoing,
			IsDrained:       isDeviceDrained(status),
			Severity:        incidentSeverity(incidentType, peakValue, int64(peakValue)),
		}

		if !isOngoing {
			endStr := endedAt.UTC().Format(time.RFC3339)
			inc.EndedAt = &endStr
		}
		inc.DurationSeconds = &durationSeconds

		// Populate type-specific fields
		switch incidentType {
		case "errors", "fcs", "discards", "carrier":
			var threshold int64
			switch incidentType {
			case "errors":
				threshold = p.ErrorsThreshold
			case "fcs":
				threshold = p.FCSThreshold
			case "discards":
				threshold = p.DiscardsThreshold
			case "carrier":
				threshold = p.CarrierThreshold
			}
			peak := int64(peakValue)
			inc.ThresholdCount = &threshold
			inc.PeakCount = &peak
		}

		// Set Confirmed based on min duration
		minDurationSecs := p.MinDurationMin * 60
		if isOngoing {
			inc.Confirmed = time.Since(startedAt) >= time.Duration(p.MinDurationMin)*time.Minute
		} else {
			inc.Confirmed = durationSeconds >= minDurationSecs
		}

		incidents = append(incidents, inc)
	}

	return incidents, nil
}

// enrichLinkIncidentsWithInterfacesRollup queries the rollup table for affected interfaces
// on counter-type incidents and populates the AffectedInterfaces field.
func enrichLinkIncidentsWithInterfacesRollup(ctx context.Context, conn driver.Conn, incidents []LinkIncident) {
	if len(incidents) == 0 {
		return
	}

	type key struct {
		linkPK       string
		incidentType string
	}
	windows := make(map[key]incidentWindow)
	for _, inc := range incidents {
		if inc.IncidentType == "packet_loss" || inc.IncidentType == "no_data" || inc.IncidentType == "isis_down" {
			continue
		}
		start, _ := time.Parse(time.RFC3339, inc.StartedAt)
		end := time.Now()
		if inc.EndedAt != nil {
			end, _ = time.Parse(time.RFC3339, *inc.EndedAt)
		}
		k := key{inc.LinkPK, inc.IncidentType}
		if existing, ok := windows[k]; ok {
			if start.Before(existing.startedAt) {
				existing.startedAt = start
			}
			if end.After(existing.endedAt) {
				existing.endedAt = end
			}
			windows[k] = existing
		} else {
			windows[k] = incidentWindow{
				entityPK:     inc.LinkPK,
				incidentType: inc.IncidentType,
				startedAt:    start,
				endedAt:      end,
			}
		}
	}

	if len(windows) == 0 {
		return
	}

	interfacesByKey := make(map[key][]string)

	byType := make(map[string][]incidentWindow)
	for k, w := range windows {
		byType[k.incidentType] = append(byType[k.incidentType], w)
	}

	for incType, typeWindows := range byType {
		filter := rollupCounterFilter(incType)

		var linkPKs []string
		earliest := typeWindows[0].startedAt
		latest := typeWindows[0].endedAt
		for _, w := range typeWindows {
			linkPKs = append(linkPKs, w.entityPK)
			if w.startedAt.Before(earliest) {
				earliest = w.startedAt
			}
			if w.endedAt.After(latest) {
				latest = w.endedAt
			}
		}

		query := fmt.Sprintf(`
			SELECT link_pk, intf
			FROM device_interface_rollup_5m FINAL
			WHERE link_pk IN ($1)
			  AND bucket_ts >= $2
			  AND bucket_ts <= $3
			  AND %s
			GROUP BY link_pk, intf
			ORDER BY link_pk, intf
		`, filter)

		rows, err := conn.Query(ctx, query, linkPKs, earliest, latest)
		if err != nil {
			slog.Warn("rollup interface enrichment query failed", "type", incType, "error", err)
			continue
		}

		for rows.Next() {
			var linkPK, intf string
			if err := rows.Scan(&linkPK, &intf); err != nil {
				continue
			}
			k := key{linkPK, incType}
			interfacesByKey[k] = append(interfacesByKey[k], intf)
		}
		rows.Close()
	}

	for i := range incidents {
		inc := &incidents[i]
		if inc.IncidentType == "packet_loss" || inc.IncidentType == "no_data" || inc.IncidentType == "isis_down" {
			continue
		}
		k := key{inc.LinkPK, inc.IncidentType}
		if intfs, ok := interfacesByKey[k]; ok {
			inc.AffectedInterfaces = intfs
		}
	}
}

// enrichDeviceIncidentsWithInterfacesRollup queries the rollup table for affected interfaces.
func enrichDeviceIncidentsWithInterfacesRollup(ctx context.Context, conn driver.Conn, incidents []DeviceIncident) {
	if len(incidents) == 0 {
		return
	}

	type key struct {
		devicePK     string
		incidentType string
	}
	windows := make(map[key]incidentWindow)
	for _, inc := range incidents {
		if inc.IncidentType == "no_data" || inc.IncidentType == "isis_overload" || inc.IncidentType == "isis_unreachable" {
			continue
		}
		start, _ := time.Parse(time.RFC3339, inc.StartedAt)
		end := time.Now()
		if inc.EndedAt != nil {
			end, _ = time.Parse(time.RFC3339, *inc.EndedAt)
		}
		k := key{inc.DevicePK, inc.IncidentType}
		if existing, ok := windows[k]; ok {
			if start.Before(existing.startedAt) {
				existing.startedAt = start
			}
			if end.After(existing.endedAt) {
				existing.endedAt = end
			}
			windows[k] = existing
		} else {
			windows[k] = incidentWindow{
				entityPK:     inc.DevicePK,
				incidentType: inc.IncidentType,
				startedAt:    start,
				endedAt:      end,
			}
		}
	}

	if len(windows) == 0 {
		return
	}

	interfacesByKey := make(map[key][]string)

	byType := make(map[string][]incidentWindow)
	for k, w := range windows {
		byType[k.incidentType] = append(byType[k.incidentType], w)
	}

	for incType, typeWindows := range byType {
		filter := rollupCounterFilter(incType)

		var devicePKs []string
		earliest := typeWindows[0].startedAt
		latest := typeWindows[0].endedAt
		for _, w := range typeWindows {
			devicePKs = append(devicePKs, w.entityPK)
			if w.startedAt.Before(earliest) {
				earliest = w.startedAt
			}
			if w.endedAt.After(latest) {
				latest = w.endedAt
			}
		}

		query := fmt.Sprintf(`
			SELECT device_pk, intf
			FROM device_interface_rollup_5m FINAL
			WHERE device_pk IN ($1)
			  AND bucket_ts >= $2
			  AND bucket_ts <= $3
			  AND %s
			GROUP BY device_pk, intf
			ORDER BY device_pk, intf
		`, filter)

		rows, err := conn.Query(ctx, query, devicePKs, earliest, latest)
		if err != nil {
			slog.Warn("rollup device interface enrichment query failed", "type", incType, "error", err)
			continue
		}

		for rows.Next() {
			var devicePK, intf string
			if err := rows.Scan(&devicePK, &intf); err != nil {
				continue
			}
			k := key{devicePK, incType}
			interfacesByKey[k] = append(interfacesByKey[k], intf)
		}
		rows.Close()
	}

	for i := range incidents {
		inc := &incidents[i]
		if inc.IncidentType == "no_data" || inc.IncidentType == "isis_overload" || inc.IncidentType == "isis_unreachable" {
			continue
		}
		k := key{inc.DevicePK, inc.IncidentType}
		if intfs, ok := interfacesByKey[k]; ok {
			inc.AffectedInterfaces = intfs
		}
	}
}

// rollupCounterFilter returns the SQL condition for non-zero counter activity in rollup tables.
func rollupCounterFilter(incidentType string) string {
	switch incidentType {
	case "errors":
		return "(in_errors > 0 OR out_errors > 0)"
	case "fcs":
		return "(in_fcs_errors > 0)"
	case "discards":
		return "(in_discards > 0 OR out_discards > 0)"
	case "carrier":
		return "(carrier_transitions > 0)"
	default:
		return "1=0"
	}
}
