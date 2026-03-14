package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
)

// DeviceIncident represents a discrete incident event on a device
type DeviceIncident struct {
	ID                 string   `json:"id"`
	DevicePK           string   `json:"device_pk"`
	DeviceCode         string   `json:"device_code"`
	DeviceType         string   `json:"device_type"`
	Metro              string   `json:"metro"`
	ContributorCode    string   `json:"contributor_code"`
	IncidentType       string   `json:"incident_type"` // errors, discards, carrier, no_data
	ThresholdCount     *int64   `json:"threshold_count,omitempty"`
	PeakCount          *int64   `json:"peak_count,omitempty"`
	StartedAt          string   `json:"started_at"`
	EndedAt            *string  `json:"ended_at,omitempty"`
	DurationSeconds    *int64   `json:"duration_seconds,omitempty"`
	IsOngoing          bool     `json:"is_ongoing"`
	Confirmed          bool     `json:"confirmed"`
	IsDrained          bool     `json:"is_drained"`
	Severity           string   `json:"severity"` // "degraded" or "incident"
	AffectedInterfaces []string `json:"affected_interfaces,omitempty"`
}

// DrainedDeviceInfo represents a drained device with its incident status
type DrainedDeviceInfo struct {
	DevicePK        string           `json:"device_pk"`
	DeviceCode      string           `json:"device_code"`
	DeviceType      string           `json:"device_type"`
	Metro           string           `json:"metro"`
	ContributorCode string           `json:"contributor_code"`
	DrainStatus     string           `json:"drain_status"`
	DrainedSince    string           `json:"drained_since"`
	ActiveIncidents []DeviceIncident `json:"active_incidents"`
	RecentIncidents []DeviceIncident `json:"recent_incidents"`
	LastIncidentEnd *string          `json:"last_incident_end,omitempty"`
	ClearForSeconds *int64           `json:"clear_for_seconds,omitempty"`
	Readiness       string           `json:"readiness"` // "red", "yellow", "green", "gray"
}

// DeviceIncidentsSummary contains aggregate counts for active device incidents
type DeviceIncidentsSummary struct {
	Total   int            `json:"total"`
	Ongoing int            `json:"ongoing"`
	ByType  map[string]int `json:"by_type"`
}

// DeviceIncidentsResponse is the API response for device incidents
type DeviceIncidentsResponse struct {
	Active         []DeviceIncident       `json:"active"`
	Drained        []DrainedDeviceInfo    `json:"drained"`
	ActiveSummary  DeviceIncidentsSummary `json:"active_summary"`
	DrainedSummary DrainedSummary         `json:"drained_summary"`
}

// deviceMetadata contains device info for enriching incidents
type deviceMetadata struct {
	DevicePK        string
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
}

// deviceCounterBucket represents a 5-minute aggregation of counter metrics per device
type deviceCounterBucket struct {
	DevicePK string
	Bucket   time.Time
	Value    int64
}

func isDeviceDrained(status string) bool {
	return status == "soft-drained" || status == "hard-drained" || status == "suspended"
}

func fetchDeviceMetadata(ctx context.Context, conn driver.Conn, filters []IncidentFilter) (map[string]deviceMetadata, error) {
	var qb strings.Builder
	qb.WriteString(`
		SELECT
			d.pk,
			d.code,
			d.device_type,
			COALESCE(m.code, '') AS metro,
			COALESCE(c.code, '') AS contributor_code,
			d.status
		FROM dz_devices_current d
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		WHERE 1=1
	`)

	var args []any
	argIdx := 1

	for _, f := range filters {
		switch f.Type {
		case "metro":
			fmt.Fprintf(&qb, " AND m.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "device":
			fmt.Fprintf(&qb, " AND d.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "contributor":
			fmt.Fprintf(&qb, " AND c.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		}
	}

	rows, err := conn.Query(ctx, qb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]deviceMetadata)
	for rows.Next() {
		var dm deviceMetadata
		if err := rows.Scan(&dm.DevicePK, &dm.DeviceCode, &dm.DeviceType, &dm.Metro, &dm.ContributorCode, &dm.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[dm.DevicePK] = dm
	}

	return result, nil
}

// fetchDeviceCounterIncidents detects counter-based incidents per device
func fetchDeviceCounterIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, threshold int64, metricExpr string, incidentType string, deviceMeta map[string]deviceMetadata, dp incidentDetectionParams, linkFilter string) ([]DeviceIncident, error) {
	currentIncidents, err := fetchCurrentHighCounterDevices(ctx, conn, threshold, metricExpr, incidentType, deviceMeta, dp, linkFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current %s devices: %w", incidentType, err)
	}

	ongoingDevices := make(map[string]bool)
	for _, inc := range currentIncidents {
		ongoingDevices[inc.DeviceCode] = true
	}

	devicePKs := make([]string, 0, len(deviceMeta))
	for pk := range deviceMeta {
		devicePKs = append(devicePKs, pk)
	}
	if len(devicePKs) == 0 {
		return currentIncidents, nil
	}

	query := fmt.Sprintf(`
		SELECT
			ic.device_pk,
			toStartOfInterval(ic.event_ts, INTERVAL %s) as bucket,
			%s as metric_value
		FROM fact_dz_device_interface_counters ic
		WHERE ic.event_ts >= now() - INTERVAL $1 SECOND
		  AND ic.device_pk IN ($2)
		  %s
		GROUP BY ic.device_pk, bucket
		ORDER BY ic.device_pk, bucket
	`, sqlBucketInterval(dp.bucketSize()), metricExpr, linkFilter)

	lookbackSecs := int64((duration + 24*time.Hour).Seconds())
	rows, err := conn.Query(ctx, query, lookbackSecs, devicePKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var buckets []deviceCounterBucket
	for rows.Next() {
		var cb deviceCounterBucket
		if err := rows.Scan(&cb.DevicePK, &cb.Bucket, &cb.Value); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		buckets = append(buckets, cb)
	}

	completedIncidents := pairDeviceCounterIncidentsCompleted(buckets, deviceMeta, threshold, incidentType, ongoingDevices, dp)

	allIncidents := append(currentIncidents, completedIncidents...)
	allIncidents = coalesceDeviceIncidents(allIncidents, dp.CoalesceGap)
	return allIncidents, nil
}

// fetchCurrentHighCounterDevices finds devices currently experiencing counter metrics above threshold
func fetchCurrentHighCounterDevices(ctx context.Context, conn driver.Conn, threshold int64, metricExpr string, incidentType string, deviceMeta map[string]deviceMetadata, dp incidentDetectionParams, linkFilter string) ([]DeviceIncident, error) {
	devicePKs := make([]string, 0, len(deviceMeta))
	for pk := range deviceMeta {
		devicePKs = append(devicePKs, pk)
	}
	if len(devicePKs) == 0 {
		return nil, nil
	}

	lookbackSecs := int64(dp.CoalesceGap.Seconds())
	query := fmt.Sprintf(`
		WITH recent_buckets AS (
			SELECT
				ic.device_pk,
				toStartOfInterval(ic.event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters ic
			WHERE ic.event_ts >= now() - INTERVAL $1 SECOND
			  AND ic.device_pk IN ($2)
			  %s
			GROUP BY ic.device_pk, bucket
		),
		ranked AS (
			SELECT
				device_pk,
				bucket,
				metric_value,
				ROW_NUMBER() OVER (PARTITION BY device_pk ORDER BY bucket DESC) AS rn
			FROM recent_buckets
		)
		SELECT device_pk, bucket, metric_value, rn
		FROM ranked
		WHERE rn <= 3
		ORDER BY device_pk, rn
	`, metricExpr, linkFilter)

	rows, err := conn.Query(ctx, query, lookbackSecs, devicePKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type recentBucket struct {
		Bucket time.Time
		Value  int64
		Rank   uint64
	}
	deviceRecentBuckets := make(map[string][]recentBucket)

	for rows.Next() {
		var devicePK string
		var bucket time.Time
		var value int64
		var rn uint64

		if err := rows.Scan(&devicePK, &bucket, &value, &rn); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		deviceRecentBuckets[devicePK] = append(deviceRecentBuckets[devicePK], recentBucket{
			Bucket: bucket, Value: value, Rank: rn,
		})
	}

	var incidents []DeviceIncident
	idCounter := 0

	for devicePK, buckets := range deviceRecentBuckets {
		meta, ok := deviceMeta[devicePK]
		if !ok {
			continue
		}

		// Require the most recent bucket to be above threshold to trigger detecting state.
		// A single bucket is enough — the confirmed flag (based on min_duration) handles
		// whether the incident is promoted from "detecting" to "confirmed".
		if len(buckets) == 0 || buckets[0].Value < threshold {
			continue
		}

		startedAt, peakValue, err := findDeviceCounterIncidentStart(ctx, conn, devicePK, threshold, metricExpr, linkFilter)
		if err != nil {
			startedAt = buckets[0].Bucket.Add(-10 * time.Minute)
			peakValue = buckets[0].Value
		}

		idCounter++
		thresholdCount := threshold

		incidents = append(incidents, DeviceIncident{
			ID:              fmt.Sprintf("dev-%s-%d", incidentType, idCounter),
			DevicePK:        devicePK,
			DeviceCode:      meta.DeviceCode,
			DeviceType:      meta.DeviceType,
			Metro:           meta.Metro,
			ContributorCode: meta.ContributorCode,
			IncidentType:    incidentType,
			ThresholdCount:  &thresholdCount,
			PeakCount:       &peakValue,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       true,
			IsDrained:       isDeviceDrained(meta.Status),
			Severity:        incidentSeverity(incidentType, 0, peakValue),
		})
	}

	return incidents, nil
}

// findDeviceCounterIncidentStart looks back to find when a device counter incident started
func findDeviceCounterIncidentStart(ctx context.Context, conn driver.Conn, devicePK string, threshold int64, metricExpr string, linkFilter string) (time.Time, int64, error) {
	query := fmt.Sprintf(`
		WITH buckets AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE device_pk = $1
			  AND event_ts >= now() - INTERVAL 7 DAY
			  %s
			GROUP BY bucket
			ORDER BY bucket DESC
		),
		above AS (
			SELECT
				bucket,
				metric_value,
				lagInFrame(bucket, 1) OVER (ORDER BY bucket ASC) as prev_bucket
			FROM buckets
			WHERE metric_value >= $2
		)
		SELECT bucket, metric_value
		FROM above
		WHERE prev_bucket IS NULL OR dateDiff('minute', prev_bucket, bucket) > 15
		ORDER BY bucket DESC
		LIMIT 1
	`, metricExpr, linkFilter)

	var startBucket time.Time
	var value int64

	err := conn.QueryRow(ctx, query, devicePK, threshold).Scan(&startBucket, &value)
	if err != nil {
		return time.Time{}, 0, err
	}

	peakQuery := fmt.Sprintf(`
		SELECT max(metric_value) FROM (
			SELECT %s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE device_pk = $1
			  AND event_ts >= $2
			  %s
			GROUP BY toStartOfInterval(event_ts, INTERVAL 5 MINUTE)
		)
	`, metricExpr, linkFilter)

	var peak int64
	err = conn.QueryRow(ctx, peakQuery, devicePK, startBucket).Scan(&peak)
	if err == nil && peak > value {
		value = peak
	}

	return startBucket, value, nil
}

// pairDeviceCounterIncidentsCompleted finds completed counter incidents from historical buckets
func pairDeviceCounterIncidentsCompleted(buckets []deviceCounterBucket, deviceMeta map[string]deviceMetadata, threshold int64, incidentType string, excludeDevices map[string]bool, dp incidentDetectionParams) []DeviceIncident {
	var incidents []DeviceIncident
	idCounter := 1000

	byDevice := make(map[string][]deviceCounterBucket)
	for _, b := range buckets {
		byDevice[b.DevicePK] = append(byDevice[b.DevicePK], b)
	}

	for devicePK, deviceBuckets := range byDevice {
		meta, ok := deviceMeta[devicePK]
		if !ok {
			continue
		}
		if excludeDevices[meta.DeviceCode] {
			continue
		}

		sort.Slice(deviceBuckets, func(i, j int) bool {
			return deviceBuckets[i].Bucket.Before(deviceBuckets[j].Bucket)
		})

		var activeIncident *DeviceIncident
		var peakValue int64
		var consecutiveBuckets int

		for i, b := range deviceBuckets {
			aboveThreshold := b.Value >= threshold

			if i == 0 {
				if aboveThreshold {
					idCounter++
					thresholdCount := threshold
					activeIncident = &DeviceIncident{
						ID:              fmt.Sprintf("dev-%s-%d", incidentType, idCounter),
						DevicePK:        devicePK,
						DeviceCode:      meta.DeviceCode,
						DeviceType:      meta.DeviceType,
						Metro:           meta.Metro,
						ContributorCode: meta.ContributorCode,
						IncidentType:    incidentType,
						ThresholdCount:  &thresholdCount,
						StartedAt:       b.Bucket.UTC().Format(time.RFC3339),
						IsOngoing:       false,
						IsDrained:       isDeviceDrained(meta.Status),
					}
					peakValue = b.Value
					consecutiveBuckets = 1
				}
				continue
			}

			prevAbove := deviceBuckets[i-1].Value >= threshold

			if aboveThreshold && !prevAbove {
				idCounter++
				thresholdCount := threshold
				activeIncident = &DeviceIncident{
					ID:              fmt.Sprintf("dev-%s-%d", incidentType, idCounter),
					DevicePK:        devicePK,
					DeviceCode:      meta.DeviceCode,
					DeviceType:      meta.DeviceType,
					Metro:           meta.Metro,
					ContributorCode: meta.ContributorCode,
					IncidentType:    incidentType,
					ThresholdCount:  &thresholdCount,
					StartedAt:       b.Bucket.UTC().Format(time.RFC3339),
					IsOngoing:       false,
					IsDrained:       isDeviceDrained(meta.Status),
				}
				peakValue = b.Value
				consecutiveBuckets = 1
			} else if !aboveThreshold && prevAbove && activeIncident != nil {
				if consecutiveBuckets >= dp.minBuckets() {
					prevBucket := deviceBuckets[i-1]
					endedAt := prevBucket.Bucket.Add(dp.bucketSize()).UTC().Format(time.RFC3339)
					activeIncident.EndedAt = &endedAt
					peak := peakValue
					activeIncident.PeakCount = &peak
					activeIncident.Severity = incidentSeverity(incidentType, 0, peakValue)

					startTime, _ := time.Parse(time.RFC3339, activeIncident.StartedAt)
					endTime := prevBucket.Bucket.Add(dp.bucketSize())
					durationSecs := int64(endTime.Sub(startTime).Seconds())
					activeIncident.DurationSeconds = &durationSecs

					incidents = append(incidents, *activeIncident)
				}
				activeIncident = nil
				peakValue = 0
				consecutiveBuckets = 0
			} else if aboveThreshold && activeIncident != nil {
				consecutiveBuckets++
				if b.Value > peakValue {
					peakValue = b.Value
				}
			}
		}

		// Handle incident active at end of window
		if activeIncident != nil && consecutiveBuckets >= dp.minBuckets() {
			lastBucket := deviceBuckets[len(deviceBuckets)-1]
			peak := peakValue
			activeIncident.PeakCount = &peak
			activeIncident.Severity = incidentSeverity(incidentType, 0, peakValue)

			// If the last bucket is recent (within 3 bucket intervals of now), the incident
			// is likely still ongoing but wasn't caught by the current detection.
			if time.Since(lastBucket.Bucket) <= 3*dp.bucketSize() {
				activeIncident.IsOngoing = true
			} else {
				endedAt := lastBucket.Bucket.Add(dp.bucketSize()).UTC().Format(time.RFC3339)
				activeIncident.EndedAt = &endedAt

				startTime, _ := time.Parse(time.RFC3339, activeIncident.StartedAt)
				endTime := lastBucket.Bucket.Add(dp.bucketSize())
				durationSecs := int64(endTime.Sub(startTime).Seconds())
				activeIncident.DurationSeconds = &durationSecs
			}

			incidents = append(incidents, *activeIncident)
		}
	}

	return incidents
}

// coalesceDeviceIncidents merges nearby device incidents of the same type on the same device
func coalesceDeviceIncidents(incidents []DeviceIncident, coalesceGap time.Duration) []DeviceIncident {
	if len(incidents) <= 1 || coalesceGap <= 0 {
		return incidents
	}

	type groupKey struct {
		DeviceCode   string
		IncidentType string
	}

	byGroup := make(map[groupKey][]DeviceIncident)
	for _, inc := range incidents {
		key := groupKey{DeviceCode: inc.DeviceCode, IncidentType: inc.IncidentType}
		byGroup[key] = append(byGroup[key], inc)
	}

	var result []DeviceIncident
	for _, group := range byGroup {
		if len(group) <= 1 {
			result = append(result, group...)
			continue
		}

		sort.Slice(group, func(i, j int) bool {
			return group[i].StartedAt < group[j].StartedAt
		})

		merged := group[0]
		for i := 1; i < len(group); i++ {
			curr := group[i]

			if merged.IsOngoing {
				if curr.PeakCount != nil && (merged.PeakCount == nil || *curr.PeakCount > *merged.PeakCount) {
					merged.PeakCount = curr.PeakCount
				}
				continue
			}

			mergedEnd, _ := time.Parse(time.RFC3339, strVal(merged.EndedAt))
			currStart, _ := time.Parse(time.RFC3339, curr.StartedAt)
			gap := currStart.Sub(mergedEnd)

			if gap < coalesceGap {
				if curr.IsOngoing {
					merged.EndedAt = nil
					merged.DurationSeconds = nil
					merged.IsOngoing = true
				} else {
					merged.EndedAt = curr.EndedAt
					startTime, _ := time.Parse(time.RFC3339, merged.StartedAt)
					endTime, _ := time.Parse(time.RFC3339, strVal(curr.EndedAt))
					durationSecs := int64(endTime.Sub(startTime).Seconds())
					merged.DurationSeconds = &durationSecs
				}
				if curr.PeakCount != nil && (merged.PeakCount == nil || *curr.PeakCount > *merged.PeakCount) {
					merged.PeakCount = curr.PeakCount
				}
			} else {
				result = append(result, merged)
				merged = curr
			}
		}
		result = append(result, merged)
	}

	return result
}

// fetchDeviceNoDataIncidents detects no-data incidents per device
func fetchDeviceNoDataIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, deviceMeta map[string]deviceMetadata) ([]DeviceIncident, error) {
	currentNoData, err := fetchCurrentNoDataDevices(ctx, conn, deviceMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current no-data devices: %w", err)
	}

	ongoingDevices := make(map[string]bool)
	for _, inc := range currentNoData {
		ongoingDevices[inc.DeviceCode] = true
	}

	completedNoData, err := findCompletedDeviceNoDataEvents(ctx, conn, duration, deviceMeta, ongoingDevices)
	if err != nil {
		return nil, fmt.Errorf("failed to find completed no-data device incidents: %w", err)
	}

	return append(currentNoData, completedNoData...), nil
}

// fetchCurrentNoDataDevices finds devices not reporting any telemetry
func fetchCurrentNoDataDevices(ctx context.Context, conn driver.Conn, deviceMeta map[string]deviceMetadata) ([]DeviceIncident, error) {
	devicePKs := make([]string, 0, len(deviceMeta))
	for pk := range deviceMeta {
		devicePKs = append(devicePKs, pk)
	}
	if len(devicePKs) == 0 {
		return nil, nil
	}

	// Only check origin_device_pk — if the device itself stopped sending
	// probes it's no_data, even if other devices can still reach it as a target.
	query := `
		SELECT
			origin_device_pk as device_pk,
			max(event_ts) as last_seen
		FROM fact_dz_device_link_latency
		WHERE event_ts >= now() - INTERVAL 30 DAY
		  AND origin_device_pk IN ($1)
		GROUP BY origin_device_pk
		HAVING last_seen < now() - INTERVAL 15 MINUTE
	`

	rows, err := conn.Query(ctx, query, devicePKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var incidents []DeviceIncident
	idCounter := 0

	for rows.Next() {
		var devicePK string
		var lastSeen time.Time

		if err := rows.Scan(&devicePK, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		meta, ok := deviceMeta[devicePK]
		if !ok {
			continue
		}

		idCounter++
		startedAt := lastSeen.Add(5 * time.Minute)

		incidents = append(incidents, DeviceIncident{
			ID:              fmt.Sprintf("dev-nodata-%d", idCounter),
			DevicePK:        devicePK,
			DeviceCode:      meta.DeviceCode,
			DeviceType:      meta.DeviceType,
			Metro:           meta.Metro,
			ContributorCode: meta.ContributorCode,
			IncidentType:    "no_data",
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       true,
			IsDrained:       isDeviceDrained(meta.Status),
			Severity:        "incident",
		})
	}

	return incidents, nil
}

// findCompletedDeviceNoDataEvents finds gaps in device telemetry that later resumed
func findCompletedDeviceNoDataEvents(ctx context.Context, conn driver.Conn, duration time.Duration, deviceMeta map[string]deviceMetadata, excludeDevices map[string]bool) ([]DeviceIncident, error) {
	drainedPeriods, err := fetchDeviceDrainedPeriods(ctx, conn, duration)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch device drained periods: %w", err)
	}

	devicePKs := make([]string, 0, len(deviceMeta))
	for pk := range deviceMeta {
		devicePKs = append(devicePKs, pk)
	}
	if len(devicePKs) == 0 {
		return nil, nil
	}

	// Only check origin — if the device stopped sending probes, that's a gap,
	// even if other devices can still reach it as a target.
	query := `
		SELECT origin_device_pk as device_pk, toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket
		FROM fact_dz_device_link_latency
		WHERE event_ts >= now() - INTERVAL $1 SECOND
		  AND origin_device_pk IN ($2)
		GROUP BY device_pk, bucket
		ORDER BY device_pk, bucket
	`

	rows, err := conn.Query(ctx, query, int64(duration.Seconds()), devicePKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type bucket struct {
		DevicePK string
		Bucket   time.Time
	}
	var buckets []bucket
	for rows.Next() {
		var b bucket
		if err := rows.Scan(&b.DevicePK, &b.Bucket); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		buckets = append(buckets, b)
	}

	byDevice := make(map[string][]time.Time)
	for _, b := range buckets {
		byDevice[b.DevicePK] = append(byDevice[b.DevicePK], b.Bucket)
	}

	var incidents []DeviceIncident
	idCounter := 1000

	for devicePK, deviceBuckets := range byDevice {
		meta, ok := deviceMeta[devicePK]
		if !ok {
			continue
		}
		if excludeDevices[meta.DeviceCode] {
			continue
		}

		sort.Slice(deviceBuckets, func(i, j int) bool {
			return deviceBuckets[i].Before(deviceBuckets[j])
		})

		for i := 1; i < len(deviceBuckets); i++ {
			gap := deviceBuckets[i].Sub(deviceBuckets[i-1])
			if gap >= noDataGapThreshold {
				gapStart := deviceBuckets[i-1].Add(5 * time.Minute)
				gapEnd := deviceBuckets[i]

				if periods, hasPeriods := drainedPeriods[devicePK]; hasPeriods {
					if gapOverlapsDrainedPeriod(gapStart, gapEnd, periods) {
						continue
					}
				}

				idCounter++
				gapDuration := int64(gapEnd.Sub(gapStart).Seconds())
				endedAt := gapEnd.UTC().Format(time.RFC3339)

				incidents = append(incidents, DeviceIncident{
					ID:              fmt.Sprintf("dev-nodata-%d", idCounter),
					DevicePK:        devicePK,
					DeviceCode:      meta.DeviceCode,
					DeviceType:      meta.DeviceType,
					Metro:           meta.Metro,
					ContributorCode: meta.ContributorCode,
					IncidentType:    "no_data",
					StartedAt:       gapStart.UTC().Format(time.RFC3339),
					EndedAt:         &endedAt,
					DurationSeconds: &gapDuration,
					IsOngoing:       false,
					IsDrained:       isDeviceDrained(meta.Status),
					Severity:        "incident",
				})
			}
		}
	}

	return incidents, nil
}

// fetchDeviceDrainedPeriods reconstructs drain periods from dim_dz_devices_history snapshots
func fetchDeviceDrainedPeriods(ctx context.Context, conn driver.Conn, duration time.Duration) (map[string][]drainedPeriod, error) {
	query := `
		SELECT pk as device_pk, snapshot_ts, status
		FROM dim_dz_devices_history
		WHERE snapshot_ts >= now() - INTERVAL $1 SECOND
		ORDER BY device_pk, snapshot_ts
	`

	rows, err := conn.Query(ctx, query, int64(duration.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type snapshot struct {
		DevicePK   string
		SnapshotTS time.Time
		Status     string
	}

	var snapshots []snapshot
	for rows.Next() {
		var s snapshot
		if err := rows.Scan(&s.DevicePK, &s.SnapshotTS, &s.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		snapshots = append(snapshots, s)
	}

	byDevice := make(map[string][]snapshot)
	for _, s := range snapshots {
		byDevice[s.DevicePK] = append(byDevice[s.DevicePK], s)
	}

	result := make(map[string][]drainedPeriod)
	for devicePK, deviceSnapshots := range byDevice {
		var periods []drainedPeriod
		var activeDrain *drainedPeriod

		for _, s := range deviceSnapshots {
			isDrained := isDeviceDrained(s.Status)

			if isDrained && activeDrain == nil {
				activeDrain = &drainedPeriod{Start: s.SnapshotTS}
			} else if !isDrained && activeDrain != nil {
				endTime := s.SnapshotTS
				activeDrain.End = &endTime
				periods = append(periods, *activeDrain)
				activeDrain = nil
			}
		}

		if activeDrain != nil {
			periods = append(periods, *activeDrain)
		}

		if len(periods) > 0 {
			result[devicePK] = periods
		}
	}

	return result, nil
}

// fetchDeviceDrainedSince finds when each drained device entered its current drain state
func fetchDeviceDrainedSince(ctx context.Context, conn driver.Conn, deviceMeta map[string]deviceMetadata) map[string]time.Time {
	drainedPKs := make([]string, 0)
	for pk, meta := range deviceMeta {
		if isDeviceDrained(meta.Status) {
			drainedPKs = append(drainedPKs, pk)
		}
	}
	if len(drainedPKs) == 0 {
		return nil
	}

	// Find the most recent snapshot where the device transitioned into a drained state
	query := `
		WITH ordered AS (
			SELECT
				pk as device_pk,
				snapshot_ts,
				status,
				lagInFrame(status, 1) OVER (PARTITION BY pk ORDER BY snapshot_ts ASC) as prev_status
			FROM dim_dz_devices_history
			WHERE pk IN ($1)
			ORDER BY pk, snapshot_ts
		)
		SELECT device_pk, max(snapshot_ts) as drained_at
		FROM ordered
		WHERE status IN ('soft-drained', 'hard-drained', 'suspended')
		  AND (prev_status IS NULL OR prev_status NOT IN ('soft-drained', 'hard-drained', 'suspended'))
		GROUP BY device_pk
	`

	rows, err := conn.Query(ctx, query, drainedPKs)
	if err != nil {
		return nil
	}
	defer rows.Close()

	result := make(map[string]time.Time)
	for rows.Next() {
		var devicePK string
		var drainedAt time.Time
		if err := rows.Scan(&devicePK, &drainedAt); err != nil {
			continue
		}
		result[devicePK] = drainedAt
	}
	return result
}

// buildDrainedDevicesInfo builds the drained view from device metadata and incidents
func buildDrainedDevicesInfo(deviceMeta map[string]deviceMetadata, incidentsByDevice map[string][]DeviceIncident, drainedSince map[string]time.Time) []DrainedDeviceInfo {
	var drainedDevices []DrainedDeviceInfo

	for devicePK, meta := range deviceMeta {
		if !isDeviceDrained(meta.Status) {
			continue
		}

		incidents := incidentsByDevice[devicePK]

		var activeIncidents []DeviceIncident
		var lastEndedAt *time.Time

		for _, inc := range incidents {
			if inc.IsOngoing {
				activeIncidents = append(activeIncidents, inc)
			} else if inc.EndedAt != nil {
				endTime, err := time.Parse(time.RFC3339, *inc.EndedAt)
				if err == nil && (lastEndedAt == nil || endTime.After(*lastEndedAt)) {
					lastEndedAt = &endTime
				}
			}
		}

		var recentIncidents []DeviceIncident
		for _, inc := range incidents {
			if !inc.IsOngoing {
				recentIncidents = append(recentIncidents, inc)
			}
		}
		sort.Slice(recentIncidents, func(i, j int) bool {
			return recentIncidents[i].StartedAt > recentIncidents[j].StartedAt
		})

		drainedSinceStr := ""
		if t, ok := drainedSince[devicePK]; ok {
			drainedSinceStr = t.UTC().Format(time.RFC3339)
		}

		dd := DrainedDeviceInfo{
			DevicePK:        devicePK,
			DeviceCode:      meta.DeviceCode,
			DeviceType:      meta.DeviceType,
			Metro:           meta.Metro,
			ContributorCode: meta.ContributorCode,
			DrainStatus:     meta.Status,
			DrainedSince:    drainedSinceStr,
			ActiveIncidents: activeIncidents,
			RecentIncidents: recentIncidents,
		}

		if dd.ActiveIncidents == nil {
			dd.ActiveIncidents = []DeviceIncident{}
		}
		if dd.RecentIncidents == nil {
			dd.RecentIncidents = []DeviceIncident{}
		}

		if len(activeIncidents) > 0 {
			dd.Readiness = "red"
		} else if lastEndedAt != nil {
			clearFor := int64(time.Since(*lastEndedAt).Seconds())
			dd.ClearForSeconds = &clearFor
			lastEnd := lastEndedAt.UTC().Format(time.RFC3339)
			dd.LastIncidentEnd = &lastEnd
			if clearFor >= 1800 {
				dd.Readiness = "green"
			} else {
				dd.Readiness = "yellow"
			}
		} else {
			dd.Readiness = "gray"
		}

		drainedDevices = append(drainedDevices, dd)
	}

	readinessOrder := map[string]int{"red": 0, "yellow": 1, "green": 2, "gray": 3}
	sort.Slice(drainedDevices, func(i, j int) bool {
		oi, oj := readinessOrder[drainedDevices[i].Readiness], readinessOrder[drainedDevices[j].Readiness]
		if oi != oj {
			return oi < oj
		}
		return drainedDevices[i].DeviceCode < drainedDevices[j].DeviceCode
	})

	return drainedDevices
}

func isDefaultDeviceIncidentsRequest(r *http.Request) bool {
	q := r.URL.Query()

	timeRange := q.Get("range")
	if timeRange != "" && timeRange != "24h" {
		return false
	}

	incidentType := q.Get("type")
	if incidentType != "" && incidentType != "all" {
		return false
	}

	if q.Get("filter") != "" {
		return false
	}

	errorsThreshold := q.Get("errors_threshold")
	if errorsThreshold != "" && errorsThreshold != "1" {
		return false
	}

	discardsThreshold := q.Get("discards_threshold")
	if discardsThreshold != "" && discardsThreshold != "1" {
		return false
	}

	carrierThreshold := q.Get("carrier_threshold")
	if carrierThreshold != "" && carrierThreshold != "1" {
		return false
	}

	minDuration := q.Get("min_duration")
	if minDuration != "" && minDuration != "30" {
		return false
	}

	coalesceGap := q.Get("coalesce_gap")
	if coalesceGap != "" && coalesceGap != "180" {
		return false
	}

	linkInterfaces := q.Get("link_interfaces")
	if linkInterfaces != "" && linkInterfaces != "false" {
		return false
	}

	return true
}

// GetDeviceIncidents returns incidents for devices with active and drained views
func GetDeviceIncidents(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) && isDefaultDeviceIncidentsRequest(r) && statusCache != nil {
		if cached := statusCache.GetDeviceIncidents(); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(cached); err != nil {
				log.Printf("Error encoding cached device incidents response: %v", err)
			}
			return
		}
	}

	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	duration := parseTimeRange(timeRange)

	errorsThreshold := parseIntParam(r.URL.Query().Get("errors_threshold"), 1)
	fcsThreshold := parseIntParam(r.URL.Query().Get("fcs_threshold"), 1)
	discardsThreshold := parseIntParam(r.URL.Query().Get("discards_threshold"), 1)
	carrierThreshold := parseIntParam(r.URL.Query().Get("carrier_threshold"), 1)

	minDurationMin := parseIntParam(r.URL.Query().Get("min_duration"), 30)
	if minDurationMin < 5 {
		minDurationMin = 5
	}
	coalesceGapMin := parseIntParam(r.URL.Query().Get("coalesce_gap"), 180)
	if coalesceGapMin < 0 {
		coalesceGapMin = 0
	}
	detectParams := incidentDetectionParams{
		MinDuration:    time.Duration(minDurationMin) * time.Minute,
		CoalesceGap:    time.Duration(coalesceGapMin) * time.Minute,
		BucketInterval: bucketIntervalForDuration(duration),
	}

	incidentType := r.URL.Query().Get("type")
	if incidentType == "" {
		incidentType = "all"
	}

	filterStr := r.URL.Query().Get("filter")
	filters := parseIncidentFilters(filterStr)

	linkFilter := "AND ic.link_pk = ''"
	if r.URL.Query().Get("link_interfaces") == "true" {
		linkFilter = ""
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	deviceMeta, err := fetchDeviceMetadata(ctx, envDB(ctx), filters)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch device metadata: %v", err), http.StatusInternalServerError)
		return
	}

	var allIncidents []DeviceIncident
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)

	if incidentType == "all" || incidentType == "errors" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, errorsThreshold,
				"sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))", "errors", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return fmt.Errorf("errors: %w", err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "fcs" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, fcsThreshold,
				"sum(greatest(0, coalesce(in_fcs_errors_delta, 0)))", "fcs", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return fmt.Errorf("fcs: %w", err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "discards" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, discardsThreshold,
				"sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))", "discards", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return fmt.Errorf("discards: %w", err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "carrier" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, carrierThreshold,
				"sum(greatest(0, coalesce(carrier_transitions_delta, 0)))", "carrier", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return fmt.Errorf("carrier: %w", err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "no_data" {
		g.Go(func() error {
			incidents, err := fetchDeviceNoDataIncidents(gCtx, envDB(gCtx), duration, deviceMeta)
			if err != nil {
				return fmt.Errorf("no_data: %w", err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Printf("Error fetching device incidents: %v", err)
		http.Error(w, fmt.Sprintf("Failed to fetch device incidents: %v", err), http.StatusInternalServerError)
		return
	}

	// Set Confirmed flag based on min duration
	minDurationSecs := int64(detectParams.MinDuration.Seconds())
	for i := range allIncidents {
		inc := &allIncidents[i]
		if inc.IsOngoing {
			startTime, _ := time.Parse(time.RFC3339, inc.StartedAt)
			inc.Confirmed = time.Since(startTime) >= detectParams.MinDuration
		} else {
			inc.Confirmed = inc.DurationSeconds == nil || *inc.DurationSeconds >= minDurationSecs
		}
	}

	// Enrich counter incidents with affected interface names
	enrichDeviceIncidentsWithInterfaces(ctx, envDB(ctx), allIncidents)

	// Split into active (non-drained) and drained
	var activeIncidents []DeviceIncident
	drainedIncidentsByDevice := make(map[string][]DeviceIncident)

	for _, inc := range allIncidents {
		if !inc.IsDrained {
			activeIncidents = append(activeIncidents, inc)
		}
		if inc.IsDrained {
			drainedIncidentsByDevice[inc.DevicePK] = append(drainedIncidentsByDevice[inc.DevicePK], inc)
		}
	}

	sort.Slice(activeIncidents, func(i, j int) bool {
		return activeIncidents[i].StartedAt > activeIncidents[j].StartedAt
	})

	drainedSince := fetchDeviceDrainedSince(ctx, envDB(ctx), deviceMeta)
	drainedDevices := buildDrainedDevicesInfo(deviceMeta, drainedIncidentsByDevice, drainedSince)

	activeSummary := DeviceIncidentsSummary{
		Total:   len(activeIncidents),
		Ongoing: 0,
		ByType:  map[string]int{"errors": 0, "discards": 0, "carrier": 0, "no_data": 0},
	}
	for _, inc := range activeIncidents {
		if inc.IsOngoing {
			activeSummary.Ongoing++
		}
		activeSummary.ByType[inc.IncidentType]++
	}

	drainedSummary := DrainedSummary{
		Total: len(drainedDevices),
	}
	for _, dd := range drainedDevices {
		if len(dd.ActiveIncidents) > 0 {
			drainedSummary.WithIncidents++
			drainedSummary.NotReady++
		} else if dd.Readiness == "green" {
			drainedSummary.Ready++
		} else {
			drainedSummary.NotReady++
		}
	}

	if activeIncidents == nil {
		activeIncidents = []DeviceIncident{}
	}
	if drainedDevices == nil {
		drainedDevices = []DrainedDeviceInfo{}
	}

	response := DeviceIncidentsResponse{
		Active:         activeIncidents,
		Drained:        drainedDevices,
		ActiveSummary:  activeSummary,
		DrainedSummary: drainedSummary,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// GetDeviceIncidentsCSV returns device incidents as a CSV download
func GetDeviceIncidentsCSV(w http.ResponseWriter, r *http.Request) {
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	duration := parseTimeRange(timeRange)

	errorsThreshold := parseIntParam(r.URL.Query().Get("errors_threshold"), 1)
	fcsThreshold := parseIntParam(r.URL.Query().Get("fcs_threshold"), 1)
	discardsThreshold := parseIntParam(r.URL.Query().Get("discards_threshold"), 1)
	carrierThreshold := parseIntParam(r.URL.Query().Get("carrier_threshold"), 1)

	minDurationMin := parseIntParam(r.URL.Query().Get("min_duration"), 30)
	if minDurationMin < 5 {
		minDurationMin = 5
	}
	coalesceGapMin := parseIntParam(r.URL.Query().Get("coalesce_gap"), 180)
	if coalesceGapMin < 0 {
		coalesceGapMin = 0
	}
	detectParams := incidentDetectionParams{
		MinDuration:    time.Duration(minDurationMin) * time.Minute,
		CoalesceGap:    time.Duration(coalesceGapMin) * time.Minute,
		BucketInterval: bucketIntervalForDuration(duration),
	}

	incidentType := r.URL.Query().Get("type")
	if incidentType == "" {
		incidentType = "all"
	}

	filterStr := r.URL.Query().Get("filter")
	filters := parseIncidentFilters(filterStr)

	linkFilter := "AND ic.link_pk = ''"
	if r.URL.Query().Get("link_interfaces") == "true" {
		linkFilter = ""
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	deviceMeta, err := fetchDeviceMetadata(ctx, envDB(ctx), filters)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch device metadata: %v", err), http.StatusInternalServerError)
		return
	}

	var allIncidents []DeviceIncident
	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)

	if incidentType == "all" || incidentType == "errors" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, errorsThreshold,
				"sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))", "errors", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return err
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "fcs" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, fcsThreshold,
				"sum(greatest(0, coalesce(in_fcs_errors_delta, 0)))", "fcs", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return err
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "discards" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, discardsThreshold,
				"sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))", "discards", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return err
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "carrier" {
		g.Go(func() error {
			incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, carrierThreshold,
				"sum(greatest(0, coalesce(carrier_transitions_delta, 0)))", "carrier", deviceMeta, detectParams, linkFilter)
			if err != nil {
				return err
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "no_data" {
		g.Go(func() error {
			incidents, err := fetchDeviceNoDataIncidents(gCtx, envDB(gCtx), duration, deviceMeta)
			if err != nil {
				return err
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch device incidents: %v", err), http.StatusInternalServerError)
		return
	}

	// Set Confirmed flag based on min duration
	minDurationSecs := int64(detectParams.MinDuration.Seconds())
	for i := range allIncidents {
		inc := &allIncidents[i]
		if inc.IsOngoing {
			startTime, _ := time.Parse(time.RFC3339, inc.StartedAt)
			inc.Confirmed = time.Since(startTime) >= detectParams.MinDuration
		} else {
			inc.Confirmed = inc.DurationSeconds == nil || *inc.DurationSeconds >= minDurationSecs
		}
	}

	enrichDeviceIncidentsWithInterfaces(ctx, envDB(ctx), allIncidents)

	sort.Slice(allIncidents, func(i, j int) bool {
		return allIncidents[i].StartedAt > allIncidents[j].StartedAt
	})

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"device-incidents-%s.csv\"", timeRange))

	_, _ = w.Write([]byte("id,device_code,device_type,metro,contributor,incident_type,severity,is_drained,details,affected_interfaces,started_at,ended_at,duration_seconds,is_ongoing\n"))

	for _, inc := range allIncidents {
		details := ""
		if inc.PeakCount != nil && inc.ThresholdCount != nil {
			details = fmt.Sprintf("peak %d (threshold %d)", *inc.PeakCount, *inc.ThresholdCount)
		} else if inc.IncidentType == "no_data" {
			details = "telemetry stopped"
		}

		endedAt := ""
		if inc.EndedAt != nil {
			endedAt = *inc.EndedAt
		}
		durationSecs := ""
		if inc.DurationSeconds != nil {
			durationSecs = strconv.FormatInt(*inc.DurationSeconds, 10)
		}

		interfaces := strings.Join(inc.AffectedInterfaces, "; ")

		line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%t,\"%s\",\"%s\",%s,%s,%s,%t\n",
			inc.ID, inc.DeviceCode, inc.DeviceType, inc.Metro,
			inc.ContributorCode, inc.IncidentType, inc.Severity, inc.IsDrained,
			details, interfaces, inc.StartedAt, endedAt, durationSecs, inc.IsOngoing)
		_, _ = w.Write([]byte(line))
	}
}

// fetchDefaultDeviceIncidentsData fetches device incidents data with default parameters for caching.
func fetchDefaultDeviceIncidentsData(ctx context.Context) *DeviceIncidentsResponse {
	duration := 24 * time.Hour
	var filters []IncidentFilter

	detectParams := incidentDetectionParams{
		MinDuration:    30 * time.Minute,
		CoalesceGap:    180 * time.Minute,
		BucketInterval: bucketIntervalForDuration(duration),
	}

	deviceMeta, err := fetchDeviceMetadata(ctx, envDB(ctx), filters)
	if err != nil {
		log.Printf("Cache: Failed to fetch device metadata for incidents: %v", err)
		return nil
	}

	var allIncidents []DeviceIncident
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)

	defaultLinkFilter := "AND ic.link_pk = ''"

	g.Go(func() error {
		incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))", "errors", deviceMeta, detectParams, defaultLinkFilter)
		if err != nil {
			return fmt.Errorf("errors: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))", "discards", deviceMeta, detectParams, defaultLinkFilter)
		if err != nil {
			return fmt.Errorf("discards: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchDeviceCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(carrier_transitions_delta, 0)))", "carrier", deviceMeta, detectParams, defaultLinkFilter)
		if err != nil {
			return fmt.Errorf("carrier: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchDeviceNoDataIncidents(gCtx, envDB(gCtx), duration, deviceMeta)
		if err != nil {
			return fmt.Errorf("no_data: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Printf("Cache: Failed to fetch device incidents: %v", err)
		return nil
	}

	minDurationSecs := int64(detectParams.MinDuration.Seconds())
	for i := range allIncidents {
		inc := &allIncidents[i]
		if inc.IsOngoing {
			startTime, _ := time.Parse(time.RFC3339, inc.StartedAt)
			inc.Confirmed = time.Since(startTime) >= detectParams.MinDuration
		} else {
			inc.Confirmed = inc.DurationSeconds == nil || *inc.DurationSeconds >= minDurationSecs
		}
	}

	enrichDeviceIncidentsWithInterfaces(ctx, envDB(ctx), allIncidents)

	var activeIncidents []DeviceIncident
	drainedIncidentsByDevice := make(map[string][]DeviceIncident)

	for _, inc := range allIncidents {
		if !inc.IsDrained {
			activeIncidents = append(activeIncidents, inc)
		}
		if inc.IsDrained {
			drainedIncidentsByDevice[inc.DevicePK] = append(drainedIncidentsByDevice[inc.DevicePK], inc)
		}
	}

	sort.Slice(activeIncidents, func(i, j int) bool {
		return activeIncidents[i].StartedAt > activeIncidents[j].StartedAt
	})

	drainedSince := fetchDeviceDrainedSince(ctx, envDB(ctx), deviceMeta)
	drainedDevices := buildDrainedDevicesInfo(deviceMeta, drainedIncidentsByDevice, drainedSince)

	activeSummary := DeviceIncidentsSummary{
		Total:   len(activeIncidents),
		Ongoing: 0,
		ByType:  map[string]int{"errors": 0, "discards": 0, "carrier": 0, "no_data": 0},
	}
	for _, inc := range activeIncidents {
		if inc.IsOngoing {
			activeSummary.Ongoing++
		}
		activeSummary.ByType[inc.IncidentType]++
	}

	drainedSummary := DrainedSummary{
		Total: len(drainedDevices),
	}
	for _, dd := range drainedDevices {
		if len(dd.ActiveIncidents) > 0 {
			drainedSummary.WithIncidents++
			drainedSummary.NotReady++
		} else if dd.Readiness == "green" {
			drainedSummary.Ready++
		} else {
			drainedSummary.NotReady++
		}
	}

	if activeIncidents == nil {
		activeIncidents = []DeviceIncident{}
	}
	if drainedDevices == nil {
		drainedDevices = []DrainedDeviceInfo{}
	}

	return &DeviceIncidentsResponse{
		Active:         activeIncidents,
		Drained:        drainedDevices,
		ActiveSummary:  activeSummary,
		DrainedSummary: drainedSummary,
	}
}
