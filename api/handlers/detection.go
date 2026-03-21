package handlers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DetectedEvent represents a discrete detected event on a link (packet loss, no data, etc.)
type DetectedEvent struct {
	ID              string   `json:"id"`
	LinkPK          string   `json:"link_pk"`
	LinkCode        string   `json:"link_code"`
	LinkType        string   `json:"link_type"`
	SideAMetro      string   `json:"side_a_metro"`
	SideZMetro      string   `json:"side_z_metro"`
	ContributorCode string   `json:"contributor_code"`
	EventType       string   `json:"outage_type"`            // "status", "packet_loss", or "no_data"
	EventTypes      []string `json:"outage_types,omitempty"` // populated when multiple ongoing events are merged for the same link
	PreviousStatus  *string  `json:"previous_status,omitempty"`
	NewStatus       *string  `json:"new_status,omitempty"`
	ThresholdPct    *float64 `json:"threshold_pct,omitempty"`
	PeakLossPct     *float64 `json:"peak_loss_pct,omitempty"`
	StartedAt       string   `json:"started_at"`
	EndedAt         *string  `json:"ended_at,omitempty"`
	DurationSeconds *int64   `json:"duration_seconds,omitempty"`
	IsOngoing       bool     `json:"is_ongoing"`
	Severity        string   `json:"severity"` // "degraded" or "outage"
}

// packetLossSeverity returns "degraded" for peak loss < 10%, "outage" for >= 10%
func packetLossSeverity(peakLossPct float64) string {
	if peakLossPct >= 10.0 {
		return "outage"
	}
	return "degraded"
}

// parseTimeRange converts a time range string to a duration
func parseTimeRange(rangeStr string) time.Duration {
	switch rangeStr {
	case "3h":
		return 3 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "24h":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "7d":
		return 7 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// parseThreshold returns the packet loss threshold percentage
func parseThreshold(thresholdStr string) float64 {
	switch thresholdStr {
	case "1":
		return 1.0
	case "10":
		return 10.0
	default:
		return 10.0
	}
}

// IncidentFilter represents a filter for incidents (e.g., metro:SAO, link:WAN-LAX-01)
type IncidentFilter struct {
	Type  string // device, link, metro, contributor
	Value string
}

// parseIncidentFilters parses a comma-separated filter string into IncidentFilter structs
func parseIncidentFilters(filterStr string) []IncidentFilter {
	if filterStr == "" {
		return nil
	}
	var filters []IncidentFilter
	for _, f := range strings.Split(filterStr, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		parts := strings.SplitN(f, ":", 2)
		if len(parts) == 2 {
			filters = append(filters, IncidentFilter{Type: parts[0], Value: parts[1]})
		}
	}
	return filters
}

type lossBucket struct {
	LinkPK      string
	Bucket      time.Time
	LossPct     float64
	SampleCount uint64
}

func coalescePacketLossEvents(events []DetectedEvent, coalesceGap time.Duration) []DetectedEvent {
	if len(events) <= 1 {
		return events
	}

	// Group by link
	byLink := make(map[string][]DetectedEvent)
	for _, o := range events {
		byLink[o.LinkCode] = append(byLink[o.LinkCode], o)
	}

	var result []DetectedEvent
	for _, linkEvents := range byLink {
		if len(linkEvents) <= 1 {
			result = append(result, linkEvents...)
			continue
		}

		// Sort by start time
		sort.Slice(linkEvents, func(i, j int) bool {
			return linkEvents[i].StartedAt < linkEvents[j].StartedAt
		})

		merged := linkEvents[0]
		for i := 1; i < len(linkEvents); i++ {
			curr := linkEvents[i]

			// If the merged event is ongoing, absorb everything after it
			if merged.IsOngoing {
				if floatVal(curr.PeakLossPct) > floatVal(merged.PeakLossPct) {
					peak := floatVal(curr.PeakLossPct)
					merged.PeakLossPct = &peak
				}
				continue
			}

			// Calculate gap between end of merged and start of current
			mergedEnd, _ := time.Parse(time.RFC3339, strVal(merged.EndedAt))
			currStart, _ := time.Parse(time.RFC3339, curr.StartedAt)
			gap := currStart.Sub(mergedEnd)

			if gap < coalesceGap {
				// Merge: extend end time, take max peak loss
				if curr.IsOngoing {
					merged.EndedAt = nil
					merged.DurationSeconds = nil
					merged.IsOngoing = true
				} else {
					merged.EndedAt = curr.EndedAt
					merged.DurationSeconds = curr.DurationSeconds
					// Recalculate duration from merged start to new end
					startTime, _ := time.Parse(time.RFC3339, merged.StartedAt)
					endTime, _ := time.Parse(time.RFC3339, strVal(curr.EndedAt))
					durationSecs := int64(endTime.Sub(startTime).Seconds())
					merged.DurationSeconds = &durationSecs
				}
				if floatVal(curr.PeakLossPct) > floatVal(merged.PeakLossPct) {
					peak := floatVal(curr.PeakLossPct)
					merged.PeakLossPct = &peak
				}
			} else {
				// Gap is large enough — emit merged and start fresh
				result = append(result, merged)
				merged = curr
			}
		}
		result = append(result, merged)
	}

	return result
}

func fetchCurrentHighLossLinks(ctx context.Context, conn driver.Conn, threshold float64, linkMeta map[string]linkMetadata) ([]DetectedEvent, error) {
	// Collect link PKs to scope the query
	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return nil, nil
	}

	// Get the most recent 5-min buckets to check for above-threshold loss
	// Use ingested_at for recent epochs (handles on-chain writer gaps), fall back to event_ts for backfills.
	displayTs := "if(h.sampling_interval_us > 0 AND lat.sample_index >= h.latest_sample_index - 1000, lat.ingested_at, lat.event_ts)"
	query := fmt.Sprintf(`
		WITH recent_buckets AS (
			SELECT
				lat.link_pk,
				toStartOfInterval(%s, INTERVAL 5 MINUTE) as bucket,
				countIf(lat.loss = true OR lat.rtt_us = 0) * 100.0 / count(*) as loss_pct,
				count(*) as sample_count
			FROM fact_dz_device_link_latency lat
			LEFT JOIN (
				SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
					   latest_sample_index, sampling_interval_us
				FROM fact_dz_device_link_latency_sample_header
			) h ON lat.origin_device_pk = h.origin_device_pk
				AND lat.target_device_pk = h.target_device_pk
				AND lat.link_pk = h._hdr_link_pk
				AND lat.epoch = h.epoch
			WHERE lat.ingested_at >= now() - INTERVAL 15 MINUTE
			  AND lat.link_pk IN ($2)
			GROUP BY lat.link_pk, bucket
			HAVING count(*) >= 3
		),
		ranked AS (
			SELECT
				link_pk,
				bucket,
				loss_pct,
				ROW_NUMBER() OVER (PARTITION BY link_pk ORDER BY bucket DESC) AS rn
			FROM recent_buckets
		)
		SELECT link_pk, bucket, loss_pct, rn
		FROM ranked
		WHERE rn <= 3
		ORDER BY link_pk, rn
	`, displayTs)

	rows, err := conn.Query(ctx, query, threshold, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Collect recent buckets per link
	type recentBucket struct {
		Bucket  time.Time
		LossPct float64
		Rank    uint64
	}
	linkRecentBuckets := make(map[string][]recentBucket)

	for rows.Next() {
		var linkPK string
		var bucket time.Time
		var lossPct float64
		var rn uint64

		if err := rows.Scan(&linkPK, &bucket, &lossPct, &rn); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		linkRecentBuckets[linkPK] = append(linkRecentBuckets[linkPK], recentBucket{
			Bucket: bucket, LossPct: lossPct, Rank: rn,
		})
	}

	var events []DetectedEvent
	eventIDCounter := 0

	for linkPK, buckets := range linkRecentBuckets {
		meta, hasMeta := linkMeta[linkPK]
		if !hasMeta {
			continue
		}

		// Require the most recent bucket to be above threshold to trigger detecting state.
		// A single bucket is enough — the confirmed flag (based on min_duration) handles
		// whether the incident is promoted from "detecting" to "confirmed".
		if len(buckets) == 0 || buckets[0].LossPct < threshold {
			continue
		}

		// Find when this event started by looking back in history
		lastSeen := buckets[0].Bucket
		startedAt, peakLoss, err := findPacketLossEventStart(ctx, conn, linkPK, threshold)
		if err != nil {
			startedAt = lastSeen.Add(-10 * time.Minute)
			peakLoss = buckets[0].LossPct
		}

		eventIDCounter++
		thresholdPct := threshold

		events = append(events, DetectedEvent{
			ID:              fmt.Sprintf("loss-%d", eventIDCounter),
			LinkPK:          linkPK,
			LinkCode:        meta.LinkCode,
			LinkType:        meta.LinkType,
			SideAMetro:      meta.SideAMetro,
			SideZMetro:      meta.SideZMetro,
			ContributorCode: meta.ContributorCode,
			EventType:       "packet_loss",
			ThresholdPct:    &thresholdPct,
			PeakLossPct:     &peakLoss,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       true,
			Severity:        packetLossSeverity(peakLoss),
		})
	}

	return events, nil
}

// findPacketLossEventStart looks back in history to find when the current event started
func findPacketLossEventStart(ctx context.Context, conn driver.Conn, linkPK string, threshold float64) (time.Time, float64, error) {
	eventDisplayTs := "if(h.sampling_interval_us > 0 AND f.sample_index >= h.latest_sample_index - 1000, f.ingested_at, f.event_ts)"
	eventHeaderJoin := `LEFT JOIN (
				SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
					   latest_sample_index, sampling_interval_us
				FROM fact_dz_device_link_latency_sample_header
			) h ON f.origin_device_pk = h.origin_device_pk
				AND f.target_device_pk = h.target_device_pk
				AND f.link_pk = h._hdr_link_pk
				AND f.epoch = h.epoch`
	query := fmt.Sprintf(`
		WITH buckets AS (
			SELECT
				toStartOfInterval(%s, INTERVAL 5 MINUTE) as bucket,
				countIf(f.loss = true OR f.rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency f
			%s
			WHERE f.link_pk = $1
			  AND f.ingested_at >= now() - INTERVAL 365 DAY
			GROUP BY bucket
			HAVING count(*) >= 3
			ORDER BY bucket DESC
		),
		above AS (
			SELECT
				bucket,
				loss_pct,
				lagInFrame(bucket, 1) OVER (ORDER BY bucket ASC) as prev_bucket
			FROM buckets
			WHERE loss_pct >= $2
		)
		SELECT bucket, loss_pct
		FROM above
		WHERE prev_bucket IS NULL OR dateDiff('minute', prev_bucket, bucket) > 15
		ORDER BY bucket DESC
		LIMIT 1
	`, eventDisplayTs, eventHeaderJoin)

	var startBucket time.Time
	var peakLoss float64

	err := conn.QueryRow(ctx, query, linkPK, threshold).Scan(&startBucket, &peakLoss)
	if err != nil {
		return time.Time{}, 0, err
	}

	// Now find the peak loss during this event
	peakQuery := fmt.Sprintf(`
		SELECT max(loss_pct) as peak_loss FROM (
			SELECT countIf(f.loss = true OR f.rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency f
			%s
			WHERE f.link_pk = $1
			  AND f.ingested_at >= $2
			GROUP BY toStartOfInterval(%s, INTERVAL 5 MINUTE)
			HAVING count(*) >= 3
		)
	`, eventHeaderJoin, eventDisplayTs)

	var peak float64
	err = conn.QueryRow(ctx, peakQuery, linkPK, startBucket).Scan(&peak)
	if err == nil && peak > peakLoss {
		peakLoss = peak
	}

	return startBucket, peakLoss, nil
}

// linkMetadata contains link info for enriching detected events
type linkMetadata struct {
	LinkPK          string
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
}

// pairPacketLossEventsCompleted finds completed packet loss events within the time window
// Links with ongoing events are excluded (they're handled separately)
func pairPacketLossEventsCompleted(buckets []lossBucket, linkMeta map[string]linkMetadata, threshold float64, excludeLinks map[string]bool, bucketInterval time.Duration) []DetectedEvent {
	var events []DetectedEvent
	eventIDCounter := 1000 // Start high to avoid collision with ongoing IDs

	// Group by link
	byLink := make(map[string][]lossBucket)
	for _, b := range buckets {
		byLink[b.LinkPK] = append(byLink[b.LinkPK], b)
	}

	for linkPK, linkBuckets := range byLink {
		meta, hasMeta := linkMeta[linkPK]
		if !hasMeta {
			continue
		}

		// Skip links with ongoing events (handled separately)
		if excludeLinks[meta.LinkCode] {
			continue
		}

		// Sort by time
		sort.Slice(linkBuckets, func(i, j int) bool {
			return linkBuckets[i].Bucket.Before(linkBuckets[j].Bucket)
		})

		var activeEvent *DetectedEvent
		var peakLoss float64
		var consecutiveBuckets int

		for i, b := range linkBuckets {
			aboveThreshold := b.LossPct >= threshold

			if i == 0 {
				if aboveThreshold {
					eventIDCounter++
					thresholdPct := threshold
					activeEvent = &DetectedEvent{
						ID:              fmt.Sprintf("loss-%d", eventIDCounter),
						LinkPK:          linkPK,
						LinkCode:        meta.LinkCode,
						LinkType:        meta.LinkType,
						SideAMetro:      meta.SideAMetro,
						SideZMetro:      meta.SideZMetro,
						ContributorCode: meta.ContributorCode,
						EventType:       "packet_loss",
						ThresholdPct:    &thresholdPct,
						StartedAt:       b.Bucket.UTC().Format(time.RFC3339),
						IsOngoing:       false,
					}
					peakLoss = b.LossPct
					consecutiveBuckets = 1
				}
				continue
			}

			prevLoss := linkBuckets[i-1].LossPct
			wasAbove := prevLoss >= threshold

			if aboveThreshold && !wasAbove {
				if activeEvent != nil {
					activeEvent = nil
				}
				eventIDCounter++
				thresholdPct := threshold
				activeEvent = &DetectedEvent{
					ID:              fmt.Sprintf("loss-%d", eventIDCounter),
					LinkPK:          linkPK,
					LinkCode:        meta.LinkCode,
					LinkType:        meta.LinkType,
					SideAMetro:      meta.SideAMetro,
					SideZMetro:      meta.SideZMetro,
					ContributorCode: meta.ContributorCode,
					EventType:       "packet_loss",
					ThresholdPct:    &thresholdPct,
					StartedAt:       b.Bucket.UTC().Format(time.RFC3339),
					IsOngoing:       false,
				}
				peakLoss = b.LossPct
				consecutiveBuckets = 1
			} else if !aboveThreshold && wasAbove && activeEvent != nil {
				if consecutiveBuckets >= 2 {
					prevBucket := linkBuckets[i-1]
					endedAt := prevBucket.Bucket.Add(bucketInterval).UTC().Format(time.RFC3339)
					activeEvent.EndedAt = &endedAt
					activeEvent.IsOngoing = false
					peak := peakLoss
					activeEvent.PeakLossPct = &peak
					activeEvent.Severity = packetLossSeverity(peakLoss)

					startTime, _ := time.Parse(time.RFC3339, activeEvent.StartedAt)
					endTime := prevBucket.Bucket.Add(bucketInterval)
					durationSecs := int64(endTime.Sub(startTime).Seconds())
					activeEvent.DurationSeconds = &durationSecs

					events = append(events, *activeEvent)
				}
				activeEvent = nil
				peakLoss = 0
				consecutiveBuckets = 0
			} else if aboveThreshold && activeEvent != nil {
				consecutiveBuckets++
				if b.LossPct > peakLoss {
					peakLoss = b.LossPct
				}
			}
		}

		// Handle event that was active at end of time window
		if activeEvent != nil && len(linkBuckets) > 0 && consecutiveBuckets >= 2 {
			lastBucket := linkBuckets[len(linkBuckets)-1]
			peak := peakLoss
			activeEvent.PeakLossPct = &peak
			activeEvent.Severity = packetLossSeverity(peakLoss)

			// If the last bucket is recent (within 3 bucket intervals of now), the event
			// is likely still ongoing but wasn't caught by the current detection
			// (e.g., the most recent bucket doesn't have enough samples yet).
			if time.Since(lastBucket.Bucket) <= 3*bucketInterval {
				activeEvent.IsOngoing = true
			} else {
				endedAt := lastBucket.Bucket.Add(bucketInterval).UTC().Format(time.RFC3339)
				activeEvent.EndedAt = &endedAt
				activeEvent.IsOngoing = false

				startTime, _ := time.Parse(time.RFC3339, activeEvent.StartedAt)
				endTime := lastBucket.Bucket.Add(bucketInterval)
				durationSecs := int64(endTime.Sub(startTime).Seconds())
				activeEvent.DurationSeconds = &durationSecs
			}

			events = append(events, *activeEvent)
		}
	}

	return events
}

// noDataGapThreshold is the minimum gap duration to consider as a "no data" event
const noDataGapThreshold = 15 * time.Minute

// drainedPeriod represents a time period when a link was in drained state
type drainedPeriod struct {
	Start time.Time
	End   *time.Time // nil if still drained
}

// fetchDrainedPeriods gets all periods when links were drained within the time range
func fetchDrainedPeriods(ctx context.Context, conn driver.Conn, duration time.Duration) (map[string][]drainedPeriod, error) {
	query := `
		SELECT
			link_pk,
			previous_status,
			new_status,
			changed_ts
		FROM dz_link_status_changes
		WHERE changed_ts >= now() - INTERVAL $1 SECOND
		ORDER BY link_pk, changed_ts
	`

	rows, err := conn.Query(ctx, query, int64(duration.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type statusChange struct {
		LinkPK     string
		PrevStatus string
		NewStatus  string
		ChangedTS  time.Time
	}

	var changes []statusChange
	for rows.Next() {
		var sc statusChange
		if err := rows.Scan(&sc.LinkPK, &sc.PrevStatus, &sc.NewStatus, &sc.ChangedTS); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		changes = append(changes, sc)
	}

	// Group by link and build drained periods
	byLink := make(map[string][]statusChange)
	for _, c := range changes {
		byLink[c.LinkPK] = append(byLink[c.LinkPK], c)
	}

	result := make(map[string][]drainedPeriod)
	for linkPK, linkChanges := range byLink {
		var periods []drainedPeriod
		var activeDrain *drainedPeriod

		for _, c := range linkChanges {
			isDrained := c.NewStatus == "soft-drained" || c.NewStatus == "hard-drained"
			isRecovery := c.NewStatus == "activated" && (c.PrevStatus == "soft-drained" || c.PrevStatus == "hard-drained")

			if isDrained && activeDrain == nil {
				activeDrain = &drainedPeriod{Start: c.ChangedTS}
			} else if isRecovery && activeDrain != nil {
				endTime := c.ChangedTS
				activeDrain.End = &endTime
				periods = append(periods, *activeDrain)
				activeDrain = nil
			}
		}

		// If still drained at end, add period with no end time
		if activeDrain != nil {
			periods = append(periods, *activeDrain)
		}

		if len(periods) > 0 {
			result[linkPK] = periods
		}
	}

	return result, nil
}

// gapOverlapsDrainedPeriod checks if a data gap overlaps with any drained period
func gapOverlapsDrainedPeriod(gapStart, gapEnd time.Time, periods []drainedPeriod) bool {
	for _, p := range periods {
		periodEnd := time.Now().Add(time.Hour) // Default to future if ongoing
		if p.End != nil {
			periodEnd = *p.End
		}

		if gapStart.Before(periodEnd) && gapEnd.After(p.Start) {
			return true
		}
	}
	return false
}

// findCompletedNoDataEvents finds gaps in data within the time range that later resumed
// Filters out gaps that occurred during drained periods
func findCompletedNoDataEvents(ctx context.Context, conn driver.Conn, duration time.Duration, linkMeta map[string]linkMetadata, excludeLinks map[string]bool) ([]DetectedEvent, error) {
	drainedPeriods, err := fetchDrainedPeriods(ctx, conn, duration)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch drained periods: %w", err)
	}

	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return nil, nil
	}

	noDataDisplayTs := "if(h.sampling_interval_us > 0 AND f.sample_index >= h.latest_sample_index - 1000, f.ingested_at, f.event_ts)"
	query := fmt.Sprintf(`
		SELECT
			f.link_pk,
			toStartOfInterval(%s, INTERVAL 5 MINUTE) as bucket
		FROM fact_dz_device_link_latency f
		LEFT JOIN (
			SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
				   latest_sample_index, sampling_interval_us
			FROM fact_dz_device_link_latency_sample_header
		) h ON f.origin_device_pk = h.origin_device_pk
			AND f.target_device_pk = h.target_device_pk
			AND f.link_pk = h._hdr_link_pk
			AND f.epoch = h.epoch
		WHERE f.ingested_at >= now() - INTERVAL $1 SECOND
		  AND f.link_pk IN ($2)
		GROUP BY f.link_pk, bucket
		ORDER BY f.link_pk, bucket
	`, noDataDisplayTs)

	rows, err := conn.Query(ctx, query, int64(duration.Seconds()), linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type bucket struct {
		LinkPK string
		Bucket time.Time
	}
	var buckets []bucket
	for rows.Next() {
		var b bucket
		if err := rows.Scan(&b.LinkPK, &b.Bucket); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		buckets = append(buckets, b)
	}

	byLink := make(map[string][]time.Time)
	for _, b := range buckets {
		byLink[b.LinkPK] = append(byLink[b.LinkPK], b.Bucket)
	}

	var events []DetectedEvent
	eventIDCounter := 1000

	for linkPK, linkBuckets := range byLink {
		meta, hasMeta := linkMeta[linkPK]
		if !hasMeta {
			continue
		}

		if excludeLinks[meta.LinkCode] {
			continue
		}

		sort.Slice(linkBuckets, func(i, j int) bool {
			return linkBuckets[i].Before(linkBuckets[j])
		})

		for i := 1; i < len(linkBuckets); i++ {
			gap := linkBuckets[i].Sub(linkBuckets[i-1])
			if gap >= noDataGapThreshold {
				gapStart := linkBuckets[i-1].Add(5 * time.Minute)
				gapEnd := linkBuckets[i]

				if periods, hasPeriods := drainedPeriods[linkPK]; hasPeriods {
					if gapOverlapsDrainedPeriod(gapStart, gapEnd, periods) {
						continue
					}
				}

				eventIDCounter++
				gapDuration := int64(gapEnd.Sub(gapStart).Seconds())
				endedAt := gapEnd.UTC().Format(time.RFC3339)

				events = append(events, DetectedEvent{
					ID:              fmt.Sprintf("nodata-%d", eventIDCounter),
					LinkPK:          linkPK,
					LinkCode:        meta.LinkCode,
					LinkType:        meta.LinkType,
					SideAMetro:      meta.SideAMetro,
					SideZMetro:      meta.SideZMetro,
					ContributorCode: meta.ContributorCode,
					EventType:       "no_data",
					StartedAt:       gapStart.UTC().Format(time.RFC3339),
					EndedAt:         &endedAt,
					DurationSeconds: &gapDuration,
					IsOngoing:       false,
					Severity:        "outage",
				})
			}
		}
	}

	return events, nil
}

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func floatVal(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}

// counterTypeFilter returns the SQL WHERE clause fragment that identifies
// non-zero counter activity for a given incident type.
func counterTypeFilter(incidentType string) string {
	switch incidentType {
	case "errors":
		return "(coalesce(in_errors_delta, 0) > 0 OR coalesce(out_errors_delta, 0) > 0)"
	case "fcs":
		return "(coalesce(in_fcs_errors_delta, 0) > 0)"
	case "discards":
		return "(coalesce(in_discards_delta, 0) > 0 OR coalesce(out_discards_delta, 0) > 0)"
	case "carrier":
		return "(coalesce(carrier_transitions_delta, 0) > 0)"
	default:
		return "1=0"
	}
}

// incidentWindow captures the time range and entity for a counter incident.
type incidentWindow struct {
	entityPK     string
	incidentType string
	startedAt    time.Time
	endedAt      time.Time // for ongoing, use now
}

// enrichLinkIncidentsWithInterfaces queries for affected interfaces on each counter incident
// and populates the AffectedInterfaces field.
func enrichLinkIncidentsWithInterfaces(ctx context.Context, conn driver.Conn, incidents []LinkIncident) {
	if len(incidents) == 0 {
		return
	}

	// Collect counter incidents that need enrichment
	type key struct {
		linkPK       string
		incidentType string
	}
	windows := make(map[key]incidentWindow)
	for _, inc := range incidents {
		if inc.IncidentType == "packet_loss" || inc.IncidentType == "no_data" {
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

	// Query affected interfaces per (link_pk, incident_type)
	// Build a UNION ALL query for each incident type group
	interfacesByKey := make(map[key][]string)

	byType := make(map[string][]incidentWindow)
	for k, w := range windows {
		byType[k.incidentType] = append(byType[k.incidentType], w)
	}

	for incType, typeWindows := range byType {
		filter := counterTypeFilter(incType)

		// Collect all link PKs and find the widest time range for this type
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
			FROM fact_dz_device_interface_counters
			WHERE link_pk IN ($1)
			  AND event_ts >= $2
			  AND event_ts <= $3
			  AND %s
			GROUP BY link_pk, intf
			ORDER BY link_pk, intf
		`, filter)

		rows, err := conn.Query(ctx, query, linkPKs, earliest, latest)
		if err != nil {
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

	// Assign interfaces to incidents
	for i := range incidents {
		inc := &incidents[i]
		if inc.IncidentType == "packet_loss" || inc.IncidentType == "no_data" {
			continue
		}
		k := key{inc.LinkPK, inc.IncidentType}
		if intfs, ok := interfacesByKey[k]; ok {
			inc.AffectedInterfaces = intfs
		}
	}
}

// enrichDeviceIncidentsWithInterfaces queries for affected interfaces on each counter incident
// and populates the AffectedInterfaces field.
func enrichDeviceIncidentsWithInterfaces(ctx context.Context, conn driver.Conn, incidents []DeviceIncident) {
	if len(incidents) == 0 {
		return
	}

	type key struct {
		devicePK     string
		incidentType string
	}
	windows := make(map[key]incidentWindow)
	for _, inc := range incidents {
		if inc.IncidentType == "no_data" {
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
		filter := counterTypeFilter(incType)

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
			FROM fact_dz_device_interface_counters
			WHERE device_pk IN ($1)
			  AND event_ts >= $2
			  AND event_ts <= $3
			  AND %s
			GROUP BY device_pk, intf
			ORDER BY device_pk, intf
		`, filter)

		rows, err := conn.Query(ctx, query, devicePKs, earliest, latest)
		if err != nil {
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
		if inc.IncidentType == "no_data" {
			continue
		}
		k := key{inc.DevicePK, inc.IncidentType}
		if intfs, ok := interfacesByKey[k]; ok {
			inc.AffectedInterfaces = intfs
		}
	}
}
