package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
)

// LinkIncident represents a discrete incident event on a link
type LinkIncident struct {
	ID                 string   `json:"id"`
	LinkPK             string   `json:"link_pk"`
	LinkCode           string   `json:"link_code"`
	LinkType           string   `json:"link_type"`
	SideAMetro         string   `json:"side_a_metro"`
	SideZMetro         string   `json:"side_z_metro"`
	ContributorCode    string   `json:"contributor_code"`
	IncidentType       string   `json:"incident_type"` // packet_loss, errors, discards, carrier, no_data
	ThresholdPct       *float64 `json:"threshold_pct,omitempty"`
	PeakLossPct        *float64 `json:"peak_loss_pct,omitempty"`
	ThresholdCount     *int64   `json:"threshold_count,omitempty"`
	PeakCount          *int64   `json:"peak_count,omitempty"`
	StartedAt          string   `json:"started_at"`
	EndedAt            *string  `json:"ended_at,omitempty"`
	DurationSeconds    *int64   `json:"duration_seconds,omitempty"`
	IsOngoing          bool     `json:"is_ongoing"`
	Confirmed          bool     `json:"confirmed"` // true if ongoing duration >= min_duration
	IsDrained          bool     `json:"is_drained"`
	Severity           string   `json:"severity"` // "degraded" or "incident"
	AffectedInterfaces []string `json:"affected_interfaces,omitempty"`
}

// DrainedLinkInfo represents a drained link with its incident status
type DrainedLinkInfo struct {
	LinkPK          string         `json:"link_pk"`
	LinkCode        string         `json:"link_code"`
	LinkType        string         `json:"link_type"`
	SideAMetro      string         `json:"side_a_metro"`
	SideZMetro      string         `json:"side_z_metro"`
	ContributorCode string         `json:"contributor_code"`
	DrainStatus     string         `json:"drain_status"`
	DrainedSince    string         `json:"drained_since"`
	ActiveIncidents []LinkIncident `json:"active_incidents"`
	RecentIncidents []LinkIncident `json:"recent_incidents"`
	LastIncidentEnd *string        `json:"last_incident_end,omitempty"`
	ClearForSeconds *int64         `json:"clear_for_seconds,omitempty"`
	Readiness       string         `json:"readiness"` // "red", "yellow", "green", "gray"
}

// LinkIncidentsSummary contains aggregate counts for active incidents
type LinkIncidentsSummary struct {
	Total   int            `json:"total"`
	Ongoing int            `json:"ongoing"`
	ByType  map[string]int `json:"by_type"`
}

// DrainedSummary contains aggregate counts for drained links
type DrainedSummary struct {
	Total         int `json:"total"`
	WithIncidents int `json:"with_incidents"`
	Ready         int `json:"ready"`
	NotReady      int `json:"not_ready"`
}

// LinkIncidentsResponse is the API response for link incidents
type LinkIncidentsResponse struct {
	Active         []LinkIncident       `json:"active"`
	Drained        []DrainedLinkInfo    `json:"drained"`
	ActiveSummary  LinkIncidentsSummary `json:"active_summary"`
	DrainedSummary DrainedSummary       `json:"drained_summary"`
}

// linkMetadataWithStatus extends linkMetadata with link status
type linkMetadataWithStatus struct {
	linkMetadata
	Status string
}

// incidentDetectionParams holds configurable detection parameters
type incidentDetectionParams struct {
	MinDuration    time.Duration // minimum consecutive duration above threshold (default 30m)
	CoalesceGap    time.Duration // gap between incidents to merge (default 180m/3h)
	BucketInterval time.Duration // aggregation bucket size (default 5m, larger for longer ranges)
}

const defaultBucketInterval = 5 * time.Minute

// bucketIntervalForDuration returns a coarser bucket interval for longer time ranges
// to reduce the amount of data scanned in ClickHouse.
func bucketIntervalForDuration(d time.Duration) time.Duration {
	switch {
	case d > 3*24*time.Hour: // 7d, 30d
		return 15 * time.Minute
	default: // 3h, 6h, 12h, 24h, 3d
		return defaultBucketInterval
	}
}

// sqlBucketInterval returns the SQL INTERVAL string for a bucket interval duration.
func sqlBucketInterval(d time.Duration) string {
	return fmt.Sprintf("%d MINUTE", int(d.Minutes()))
}

// bucketSize returns the configured bucket interval, defaulting to 5 minutes.
func (p incidentDetectionParams) bucketSize() time.Duration {
	if p.BucketInterval == 0 {
		return defaultBucketInterval
	}
	return p.BucketInterval
}

// minBuckets returns the minimum number of consecutive buckets for the configured duration
func (p incidentDetectionParams) minBuckets() int {
	n := int(p.MinDuration / p.bucketSize())
	if n < 1 {
		return 1
	}
	return n
}

// counterBucket represents a bucketed aggregation of counter metrics
type counterBucket struct {
	LinkPK string
	Bucket time.Time
	Value  int64
}

// incidentSeverity returns severity based on incident type and magnitude
func incidentSeverity(incidentType string, peakLossPct float64, _ int64) string {
	switch incidentType {
	case "packet_loss":
		if peakLossPct >= 10.0 {
			return "incident"
		}
		return "degraded"
	case "carrier", "fcs":
		return "incident"
	default:
		return "degraded"
	}
}

func fetchLinkMetadataWithStatus(ctx context.Context, conn driver.Conn, filters []IncidentFilter) (map[string]linkMetadataWithStatus, error) {
	var qb strings.Builder
	qb.WriteString(`
		SELECT
			l.pk,
			l.code,
			l.link_type,
			COALESCE(ma.code, '') AS side_a_metro,
			COALESCE(mz.code, '') AS side_z_metro,
			COALESCE(c.code, '') AS contributor_code,
			l.status
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
		WHERE l.committed_rtt_ns != $1
	`)

	var args []any
	args = append(args, committedRttProvisioningNs)
	argIdx := 2

	for _, f := range filters {
		switch f.Type {
		case "metro":
			fmt.Fprintf(&qb, " AND (ma.code = $%d OR mz.code = $%d)", argIdx, argIdx)
			args = append(args, f.Value)
			argIdx++
		case "link":
			fmt.Fprintf(&qb, " AND l.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "contributor":
			fmt.Fprintf(&qb, " AND c.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "device":
			fmt.Fprintf(&qb, " AND (da.code = $%d OR dz.code = $%d)", argIdx, argIdx)
			args = append(args, f.Value)
			argIdx++
		}
	}

	rows, err := conn.Query(ctx, qb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]linkMetadataWithStatus)
	for rows.Next() {
		var lm linkMetadataWithStatus
		if err := rows.Scan(&lm.LinkPK, &lm.LinkCode, &lm.LinkType, &lm.SideAMetro, &lm.SideZMetro, &lm.ContributorCode, &lm.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[lm.LinkPK] = lm
	}

	return result, nil
}

// fetchPacketLossIncidents detects packet loss incidents using the shared detection logic
func fetchPacketLossIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, threshold float64, linkMeta map[string]linkMetadataWithStatus, dp incidentDetectionParams) ([]LinkIncident, error) {
	// Convert to plain linkMetadata for reusing existing functions
	plainMeta := make(map[string]linkMetadata, len(linkMeta))
	for k, v := range linkMeta {
		plainMeta[k] = v.linkMetadata
	}

	// First, find links with current high packet loss (ongoing)
	currentEvents, err := fetchCurrentHighLossLinks(ctx, conn, threshold, plainMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current high loss links: %w", err)
	}

	ongoingLinks := make(map[string]bool)
	for _, o := range currentEvents {
		ongoingLinks[o.LinkCode] = true
	}

	// Query for historical buckets
	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return convertEventsToIncidents(currentEvents, linkMeta), nil
	}

	query := fmt.Sprintf(`
		WITH buckets AS (
			SELECT
				lat.link_pk,
				toStartOfInterval(lat.event_ts, INTERVAL %s) as bucket,
				countIf(lat.loss = true OR lat.rtt_us = 0) * 100.0 / count(*) as loss_pct,
				count(*) as sample_count
			FROM fact_dz_device_link_latency lat
			WHERE lat.event_ts >= now() - INTERVAL $1 SECOND
			  AND lat.link_pk IN ($2)
			GROUP BY lat.link_pk, bucket
			HAVING count(*) >= 3
		)
		SELECT b.link_pk, b.bucket, b.loss_pct, b.sample_count
		FROM buckets b
		ORDER BY b.link_pk, b.bucket
	`, sqlBucketInterval(dp.bucketSize()))

	// Add 1 day of lookback padding so incidents starting before the time range boundary get their true start time
	lookbackSecs := int64((duration + 24*time.Hour).Seconds())
	rows, err := conn.Query(ctx, query, lookbackSecs, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var buckets []lossBucket
	for rows.Next() {
		var lb lossBucket
		if err := rows.Scan(&lb.LinkPK, &lb.Bucket, &lb.LossPct, &lb.SampleCount); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		buckets = append(buckets, lb)
	}

	completedEvents := pairPacketLossEventsCompleted(buckets, plainMeta, threshold, ongoingLinks, dp.bucketSize())
	allEvents := append(currentEvents, completedEvents...)
	allEvents = coalescePacketLossEvents(allEvents, dp.CoalesceGap)

	return convertEventsToIncidents(allEvents, linkMeta), nil
}

// convertEventsToIncidents converts DetectedEvent slice to LinkIncident slice
func convertEventsToIncidents(detectedEvents []DetectedEvent, linkMeta map[string]linkMetadataWithStatus) []LinkIncident {
	incidents := make([]LinkIncident, 0, len(detectedEvents))
	for _, o := range detectedEvents {
		meta := linkMeta[o.LinkPK]
		isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"

		inc := LinkIncident{
			ID:              o.ID,
			LinkPK:          o.LinkPK,
			LinkCode:        o.LinkCode,
			LinkType:        o.LinkType,
			SideAMetro:      o.SideAMetro,
			SideZMetro:      o.SideZMetro,
			ContributorCode: o.ContributorCode,
			IncidentType:    "packet_loss",
			ThresholdPct:    o.ThresholdPct,
			PeakLossPct:     o.PeakLossPct,
			StartedAt:       o.StartedAt,
			EndedAt:         o.EndedAt,
			DurationSeconds: o.DurationSeconds,
			IsOngoing:       o.IsOngoing,
			IsDrained:       isDrained,
			Severity:        o.Severity,
		}
		// Remap severity from "outage" to "incident"
		if inc.Severity == "outage" {
			inc.Severity = "incident"
		}
		incidents = append(incidents, inc)
	}
	return incidents
}

// fetchNoDataIncidents detects no-data incidents
func fetchNoDataIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, linkMeta map[string]linkMetadataWithStatus) ([]LinkIncident, error) {
	plainMeta := make(map[string]linkMetadata, len(linkMeta))
	for k, v := range linkMeta {
		plainMeta[k] = v.linkMetadata
	}

	// For incidents page, we don't exclude drained links from no_data detection
	// (they show with is_drained=true instead)
	currentNoData, err := fetchCurrentNoDataLinksIncidents(ctx, conn, linkMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current no-data links: %w", err)
	}

	ongoingLinks := make(map[string]bool)
	for _, o := range currentNoData {
		ongoingLinks[o.LinkCode] = true
	}

	completedNoData, err := findCompletedNoDataEvents(ctx, conn, duration, plainMeta, ongoingLinks)
	if err != nil {
		return nil, fmt.Errorf("failed to find completed no-data incidents: %w", err)
	}

	// Convert completed events to incidents
	completedIncidents := convertNoDataEventsToIncidents(completedNoData, linkMeta)

	// Detect links with one direction missing (partial data loss)
	partialNoData, err := fetchPartialDataLinksIncidents(ctx, conn, linkMeta, ongoingLinks)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch partial no-data links: %w", err)
	}

	all := append(currentNoData, completedIncidents...)
	all = append(all, partialNoData...)
	return all, nil
}

// fetchCurrentNoDataLinksIncidents finds links currently not reporting data (for incidents page - includes drained)
func fetchCurrentNoDataLinksIncidents(ctx context.Context, conn driver.Conn, linkMeta map[string]linkMetadataWithStatus) ([]LinkIncident, error) {
	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return nil, nil
	}

	query := `
		WITH link_last_seen AS (
			SELECT
				link_pk,
				max(event_ts) as last_seen
			FROM fact_dz_device_link_latency
			WHERE event_ts >= now() - INTERVAL 30 DAY
			  AND link_pk IN ($1)
			GROUP BY link_pk
		)
		SELECT lls.link_pk, lls.last_seen
		FROM link_last_seen lls
		WHERE lls.last_seen < now() - INTERVAL 15 MINUTE
		  AND lls.last_seen >= now() - INTERVAL 30 DAY
	`

	rows, err := conn.Query(ctx, query, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var incidents []LinkIncident
	idCounter := 0

	for rows.Next() {
		var linkPK string
		var lastSeen time.Time

		if err := rows.Scan(&linkPK, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}

		idCounter++
		startedAt := lastSeen.Add(5 * time.Minute)
		isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"

		incidents = append(incidents, LinkIncident{
			ID:              fmt.Sprintf("nodata-%d", idCounter),
			LinkPK:          linkPK,
			LinkCode:        meta.LinkCode,
			LinkType:        meta.LinkType,
			SideAMetro:      meta.SideAMetro,
			SideZMetro:      meta.SideZMetro,
			ContributorCode: meta.ContributorCode,
			IncidentType:    "no_data",
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       true,
			IsDrained:       isDrained,
			Severity:        "incident",
		})
	}

	return incidents, nil
}

// fetchPartialDataLinksIncidents finds links where one direction has recent data
// but the other direction has gone silent — indicating a one-sided connectivity issue.
func fetchPartialDataLinksIncidents(ctx context.Context, conn driver.Conn, linkMeta map[string]linkMetadataWithStatus, excludeLinks map[string]bool) ([]LinkIncident, error) {
	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		if excludeLinks[linkMeta[pk].LinkCode] {
			continue // already flagged as fully no_data
		}
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return nil, nil
	}

	// For each link, get the last seen time per direction (origin_device_pk).
	// A link with only one direction reporting in the last 15 minutes is partial.
	query := `
		SELECT
			link_pk,
			origin_device_pk,
			max(event_ts) as last_seen
		FROM fact_dz_device_link_latency
		WHERE event_ts >= now() - INTERVAL 30 DAY
		  AND link_pk IN ($1)
		GROUP BY link_pk, origin_device_pk
	`

	rows, err := conn.Query(ctx, query, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("partial data query failed: %w", err)
	}
	defer rows.Close()

	type directionInfo struct {
		lastSeen time.Time
	}
	// linkPK -> map of origin_device_pk -> directionInfo
	linkDirections := make(map[string]map[string]directionInfo)

	for rows.Next() {
		var linkPK, originPK string
		var lastSeen time.Time
		if err := rows.Scan(&linkPK, &originPK, &lastSeen); err != nil {
			return nil, fmt.Errorf("partial data scan failed: %w", err)
		}
		if linkDirections[linkPK] == nil {
			linkDirections[linkPK] = make(map[string]directionInfo)
		}
		linkDirections[linkPK][originPK] = directionInfo{lastSeen: lastSeen}
	}

	threshold := time.Now().Add(-15 * time.Minute)
	var incidents []LinkIncident
	idCounter := 0

	for linkPK, directions := range linkDirections {
		if len(directions) < 2 {
			// Only one direction has ever reported — treat the missing
			// direction's start as the earliest seen time for the link.
			var hasRecent bool
			var earliestSeen time.Time
			for _, d := range directions {
				if d.lastSeen.After(threshold) {
					hasRecent = true
				}
				if earliestSeen.IsZero() || d.lastSeen.Before(earliestSeen) {
					earliestSeen = d.lastSeen
				}
			}
			if !hasRecent {
				continue // both sides stale — handled by full no_data detection
			}

			meta, ok := linkMeta[linkPK]
			if !ok {
				continue
			}
			idCounter++
			isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"
			incidents = append(incidents, LinkIncident{
				ID:              fmt.Sprintf("partialdata-%d", idCounter),
				LinkPK:          linkPK,
				LinkCode:        meta.LinkCode,
				LinkType:        meta.LinkType,
				SideAMetro:      meta.SideAMetro,
				SideZMetro:      meta.SideZMetro,
				ContributorCode: meta.ContributorCode,
				IncidentType:    "no_data",
				StartedAt:       earliestSeen.Add(5 * time.Minute).UTC().Format(time.RFC3339),
				IsOngoing:       true,
				IsDrained:       isDrained,
				Severity:        "incident",
			})
			continue
		}

		// Two directions exist — check if one is stale while the other is recent
		var hasRecent, hasStale bool
		var staleLastSeen time.Time
		for _, d := range directions {
			if d.lastSeen.After(threshold) {
				hasRecent = true
			} else {
				hasStale = true
				if staleLastSeen.IsZero() || d.lastSeen.After(staleLastSeen) {
					staleLastSeen = d.lastSeen
				}
			}
		}

		if !hasRecent || !hasStale {
			continue // both recent (healthy) or both stale (handled by full no_data)
		}

		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}

		idCounter++
		isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"
		incidents = append(incidents, LinkIncident{
			ID:              fmt.Sprintf("partialdata-%d", idCounter),
			LinkPK:          linkPK,
			LinkCode:        meta.LinkCode,
			LinkType:        meta.LinkType,
			SideAMetro:      meta.SideAMetro,
			SideZMetro:      meta.SideZMetro,
			ContributorCode: meta.ContributorCode,
			IncidentType:    "no_data",
			StartedAt:       staleLastSeen.Add(5 * time.Minute).UTC().Format(time.RFC3339),
			IsOngoing:       true,
			IsDrained:       isDrained,
			Severity:        "incident",
		})
	}

	return incidents, nil
}

// convertNoDataEventsToIncidents converts no-data DetectedEvents to LinkIncidents
func convertNoDataEventsToIncidents(detectedEvents []DetectedEvent, linkMeta map[string]linkMetadataWithStatus) []LinkIncident {
	incidents := make([]LinkIncident, 0, len(detectedEvents))
	for _, o := range detectedEvents {
		meta := linkMeta[o.LinkPK]
		isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"

		incidents = append(incidents, LinkIncident{
			ID:              o.ID,
			LinkPK:          o.LinkPK,
			LinkCode:        o.LinkCode,
			LinkType:        o.LinkType,
			SideAMetro:      o.SideAMetro,
			SideZMetro:      o.SideZMetro,
			ContributorCode: o.ContributorCode,
			IncidentType:    "no_data",
			StartedAt:       o.StartedAt,
			EndedAt:         o.EndedAt,
			DurationSeconds: o.DurationSeconds,
			IsOngoing:       o.IsOngoing,
			IsDrained:       isDrained,
			Severity:        "incident",
		})
	}
	return incidents
}

// fetchCounterIncidents is a generic function for detecting incidents based on interface counter metrics.
// metricExpr is the SQL expression for the metric (e.g., "sum(in_errors_delta) + sum(out_errors_delta)")
func fetchCounterIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, threshold int64, metricExpr string, incidentType string, linkMeta map[string]linkMetadataWithStatus, dp incidentDetectionParams) ([]LinkIncident, error) {
	// First, find currently active counter incidents
	currentIncidents, err := fetchCurrentHighCounterLinks(ctx, conn, threshold, metricExpr, incidentType, linkMeta, dp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current %s links: %w", incidentType, err)
	}

	ongoingLinks := make(map[string]bool)
	for _, inc := range currentIncidents {
		ongoingLinks[inc.LinkCode] = true
	}

	// Query historical buckets
	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return currentIncidents, nil
	}

	query := fmt.Sprintf(`
		SELECT
			ic.link_pk,
			toStartOfInterval(ic.event_ts, INTERVAL %s) as bucket,
			%s as metric_value
		FROM fact_dz_device_interface_counters ic
		WHERE ic.event_ts >= now() - INTERVAL $1 SECOND
		  AND ic.link_pk IN ($2)
		GROUP BY ic.link_pk, bucket
		ORDER BY ic.link_pk, bucket
	`, sqlBucketInterval(dp.bucketSize()), metricExpr)

	// Add 1 day of lookback padding so incidents starting before the time range boundary get their true start time
	lookbackSecs := int64((duration + 24*time.Hour).Seconds())
	rows, err := conn.Query(ctx, query, lookbackSecs, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var buckets []counterBucket
	for rows.Next() {
		var cb counterBucket
		if err := rows.Scan(&cb.LinkPK, &cb.Bucket, &cb.Value); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		buckets = append(buckets, cb)
	}

	// Detect completed incidents from historical buckets
	completedIncidents := pairCounterIncidentsCompleted(buckets, linkMeta, threshold, incidentType, ongoingLinks, dp)

	allIncidents := append(currentIncidents, completedIncidents...)
	allIncidents = coalesceIncidents(allIncidents, dp.CoalesceGap)
	return allIncidents, nil
}

// fetchCurrentHighCounterLinks finds links currently experiencing counter metrics above threshold
func fetchCurrentHighCounterLinks(ctx context.Context, conn driver.Conn, threshold int64, metricExpr string, incidentType string, linkMeta map[string]linkMetadataWithStatus, dp incidentDetectionParams) ([]LinkIncident, error) {
	linkPKs := make([]string, 0, len(linkMeta))
	for pk := range linkMeta {
		linkPKs = append(linkPKs, pk)
	}
	if len(linkPKs) == 0 {
		return nil, nil
	}

	lookbackSecs := int64(dp.CoalesceGap.Seconds())
	query := fmt.Sprintf(`
		WITH recent_buckets AS (
			SELECT
				ic.link_pk,
				toStartOfInterval(ic.event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters ic
			WHERE ic.event_ts >= now() - INTERVAL $1 SECOND
			  AND ic.link_pk IN ($2)
			GROUP BY ic.link_pk, bucket
		),
		ranked AS (
			SELECT
				link_pk,
				bucket,
				metric_value,
				ROW_NUMBER() OVER (PARTITION BY link_pk ORDER BY bucket DESC) AS rn
			FROM recent_buckets
		)
		SELECT link_pk, bucket, metric_value, rn
		FROM ranked
		WHERE rn <= 3
		ORDER BY link_pk, rn
	`, metricExpr)

	rows, err := conn.Query(ctx, query, lookbackSecs, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type recentBucket struct {
		Bucket time.Time
		Value  int64
		Rank   uint64
	}
	linkRecentBuckets := make(map[string][]recentBucket)

	for rows.Next() {
		var linkPK string
		var bucket time.Time
		var value int64
		var rn uint64

		if err := rows.Scan(&linkPK, &bucket, &value, &rn); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		linkRecentBuckets[linkPK] = append(linkRecentBuckets[linkPK], recentBucket{
			Bucket: bucket, Value: value, Rank: rn,
		})
	}

	var incidents []LinkIncident
	idCounter := 0

	for linkPK, buckets := range linkRecentBuckets {
		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}

		// Require the most recent bucket to be above threshold to trigger detecting state.
		// A single bucket is enough — the confirmed flag (based on min_duration) handles
		// whether the incident is promoted from "detecting" to "confirmed".
		if len(buckets) == 0 || buckets[0].Value < threshold {
			continue
		}

		// Find when this incident started
		startedAt, peakValue, err := findCounterIncidentStart(ctx, conn, linkPK, threshold, metricExpr)
		if err != nil {
			startedAt = buckets[0].Bucket.Add(-10 * time.Minute)
			peakValue = buckets[0].Value
		}

		idCounter++
		isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"
		thresholdCount := threshold

		incidents = append(incidents, LinkIncident{
			ID:              fmt.Sprintf("%s-%d", incidentType, idCounter),
			LinkPK:          linkPK,
			LinkCode:        meta.LinkCode,
			LinkType:        meta.LinkType,
			SideAMetro:      meta.SideAMetro,
			SideZMetro:      meta.SideZMetro,
			ContributorCode: meta.ContributorCode,
			IncidentType:    incidentType,
			ThresholdCount:  &thresholdCount,
			PeakCount:       &peakValue,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			IsOngoing:       true,
			IsDrained:       isDrained,
			Severity:        incidentSeverity(incidentType, 0, peakValue),
		})
	}

	return incidents, nil
}

// findCounterIncidentStart looks back in history to find when the current counter incident started
func findCounterIncidentStart(ctx context.Context, conn driver.Conn, linkPK string, threshold int64, metricExpr string) (time.Time, int64, error) {
	query := fmt.Sprintf(`
		WITH buckets AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE link_pk = $1
			  AND event_ts >= now() - INTERVAL 7 DAY
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
	`, metricExpr)

	var startBucket time.Time
	var value int64

	err := conn.QueryRow(ctx, query, linkPK, threshold).Scan(&startBucket, &value)
	if err != nil {
		return time.Time{}, 0, err
	}

	// Find peak value during this incident
	peakQuery := fmt.Sprintf(`
		SELECT max(metric_value) FROM (
			SELECT %s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE link_pk = $1
			  AND event_ts >= $2
			GROUP BY toStartOfInterval(event_ts, INTERVAL 5 MINUTE)
		)
	`, metricExpr)

	var peak int64
	err = conn.QueryRow(ctx, peakQuery, linkPK, startBucket).Scan(&peak)
	if err == nil && peak > value {
		value = peak
	}

	return startBucket, value, nil
}

// pairCounterIncidentsCompleted finds completed counter incidents from historical buckets
func pairCounterIncidentsCompleted(buckets []counterBucket, linkMeta map[string]linkMetadataWithStatus, threshold int64, incidentType string, excludeLinks map[string]bool, dp incidentDetectionParams) []LinkIncident {
	var incidents []LinkIncident
	idCounter := 1000

	byLink := make(map[string][]counterBucket)
	for _, b := range buckets {
		byLink[b.LinkPK] = append(byLink[b.LinkPK], b)
	}

	for linkPK, linkBuckets := range byLink {
		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}
		if excludeLinks[meta.LinkCode] {
			continue
		}

		sort.Slice(linkBuckets, func(i, j int) bool {
			return linkBuckets[i].Bucket.Before(linkBuckets[j].Bucket)
		})

		var activeIncident *LinkIncident
		var peakValue int64
		var consecutiveBuckets int
		isDrained := meta.Status == "soft-drained" || meta.Status == "hard-drained"

		for i, b := range linkBuckets {
			aboveThreshold := b.Value >= threshold

			if i == 0 {
				if aboveThreshold {
					idCounter++
					thresholdCount := threshold
					activeIncident = &LinkIncident{
						ID:              fmt.Sprintf("%s-%d", incidentType, idCounter),
						LinkPK:          linkPK,
						LinkCode:        meta.LinkCode,
						LinkType:        meta.LinkType,
						SideAMetro:      meta.SideAMetro,
						SideZMetro:      meta.SideZMetro,
						ContributorCode: meta.ContributorCode,
						IncidentType:    incidentType,
						ThresholdCount:  &thresholdCount,
						StartedAt:       b.Bucket.UTC().Format(time.RFC3339),
						IsOngoing:       false,
						IsDrained:       isDrained,
					}
					peakValue = b.Value
					consecutiveBuckets = 1
				}
				continue
			}

			prevAbove := linkBuckets[i-1].Value >= threshold

			if aboveThreshold && !prevAbove {
				if activeIncident != nil {
					activeIncident = nil
				}
				idCounter++
				thresholdCount := threshold
				activeIncident = &LinkIncident{
					ID:              fmt.Sprintf("%s-%d", incidentType, idCounter),
					LinkPK:          linkPK,
					LinkCode:        meta.LinkCode,
					LinkType:        meta.LinkType,
					SideAMetro:      meta.SideAMetro,
					SideZMetro:      meta.SideZMetro,
					ContributorCode: meta.ContributorCode,
					IncidentType:    incidentType,
					ThresholdCount:  &thresholdCount,
					StartedAt:       b.Bucket.UTC().Format(time.RFC3339),
					IsOngoing:       false,
					IsDrained:       isDrained,
				}
				peakValue = b.Value
				consecutiveBuckets = 1
			} else if !aboveThreshold && prevAbove && activeIncident != nil {
				prevBucket := linkBuckets[i-1]
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
		if activeIncident != nil {
			lastBucket := linkBuckets[len(linkBuckets)-1]
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

// coalesceIncidents merges incidents on the same link that are separated by less than the coalesce gap.
func coalesceIncidents(incidents []LinkIncident, coalesceGap time.Duration) []LinkIncident {
	if len(incidents) <= 1 {
		return incidents
	}

	// Group by link + incident type
	type key struct {
		LinkCode     string
		IncidentType string
	}
	byKey := make(map[key][]LinkIncident)
	for _, inc := range incidents {
		k := key{LinkCode: inc.LinkCode, IncidentType: inc.IncidentType}
		byKey[k] = append(byKey[k], inc)
	}

	var result []LinkIncident
	for _, group := range byKey {
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
				if merged.PeakCount != nil && curr.PeakCount != nil && *curr.PeakCount > *merged.PeakCount {
					peak := *curr.PeakCount
					merged.PeakCount = &peak
				}
				if merged.PeakLossPct != nil && curr.PeakLossPct != nil && *curr.PeakLossPct > *merged.PeakLossPct {
					peak := *curr.PeakLossPct
					merged.PeakLossPct = &peak
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
					peak := *curr.PeakCount
					merged.PeakCount = &peak
				}
				if curr.PeakLossPct != nil && (merged.PeakLossPct == nil || *curr.PeakLossPct > *merged.PeakLossPct) {
					peak := *curr.PeakLossPct
					merged.PeakLossPct = &peak
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

// parseIntParam parses an integer query parameter with a default value
func parseIntParam(value string, defaultVal int64) int64 {
	if value == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

// isDefaultIncidentsRequest checks if the request matches the default cached parameters
func isDefaultIncidentsRequest(r *http.Request) bool {
	q := r.URL.Query()

	rangeParam := q.Get("range")
	if rangeParam != "" && rangeParam != "24h" {
		return false
	}

	threshold := q.Get("threshold")
	if threshold != "" && threshold != "10" {
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

	fcsThreshold := q.Get("fcs_threshold")
	if fcsThreshold != "" && fcsThreshold != "1" {
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

	return true
}

// GetLinkIncidents returns incidents for links with active and drained views
func GetLinkIncidents(w http.ResponseWriter, r *http.Request) {
	// Check if this is a default request that can be served from cache
	if isMainnet(r.Context()) && isDefaultIncidentsRequest(r) && statusCache != nil {
		if cached := statusCache.GetIncidents(); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(cached); err != nil {
				slog.Error("failed to encode cached incidents response", "error", err)
			}
			return
		}
	}

	// Parse query parameters
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	duration := parseTimeRange(timeRange)

	thresholdStr := r.URL.Query().Get("threshold")
	threshold := parseThreshold(thresholdStr)

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

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Fetch link metadata with status
	linkMeta, err := fetchLinkMetadataWithStatus(ctx, envDB(ctx), filters)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch link metadata: %v", err), http.StatusInternalServerError)
		return
	}

	// Run all incident type queries in parallel
	var allIncidents []LinkIncident
	var mu = &sync.Mutex{}

	g, gCtx := errgroup.WithContext(ctx)

	if incidentType == "all" || incidentType == "packet_loss" {
		g.Go(func() error {
			incidents, err := fetchPacketLossIncidents(gCtx, envDB(gCtx), duration, threshold, linkMeta, detectParams)
			if err != nil {
				return fmt.Errorf("packet loss: %w", err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "errors" {
		g.Go(func() error {
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, errorsThreshold,
				"sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))", "errors", linkMeta, detectParams)
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
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, fcsThreshold,
				"sum(greatest(0, coalesce(in_fcs_errors_delta, 0)))", "fcs", linkMeta, detectParams)
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
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, discardsThreshold,
				"sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))", "discards", linkMeta, detectParams)
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
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, carrierThreshold,
				"sum(greatest(0, coalesce(carrier_transitions_delta, 0)))", "carrier", linkMeta, detectParams)
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
			incidents, err := fetchNoDataIncidents(gCtx, envDB(gCtx), duration, linkMeta)
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
		slog.Error("failed to fetch incidents", "error", err)
		http.Error(w, fmt.Sprintf("Failed to fetch incidents: %v", err), http.StatusInternalServerError)
		return
	}

	// Set Confirmed flag based on min duration. Ongoing incidents are confirmed once
	// they've been active >= min_duration. Completed incidents are confirmed if their
	// duration >= min_duration, otherwise they remain unconfirmed (shown via "Show detecting").
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
	enrichLinkIncidentsWithInterfaces(ctx, envDB(ctx), allIncidents)

	// Split into active (non-drained) and build drained view
	var activeIncidents []LinkIncident
	drainedIncidentsByLink := make(map[string][]LinkIncident)

	for _, inc := range allIncidents {
		if !inc.IsDrained {
			activeIncidents = append(activeIncidents, inc)
		}
		// All incidents (drained or not) get indexed by link for the drained view
		if inc.IsDrained {
			drainedIncidentsByLink[inc.LinkPK] = append(drainedIncidentsByLink[inc.LinkPK], inc)
		}
	}

	// Sort active incidents by start time (most recent first)
	sort.Slice(activeIncidents, func(i, j int) bool {
		return activeIncidents[i].StartedAt > activeIncidents[j].StartedAt
	})

	// Fetch when each drained link was drained
	drainedSince := fetchDrainedSince(ctx, envDB(ctx), linkMeta)

	// Build drained links info
	drainedLinks := buildDrainedLinksInfo(linkMeta, drainedIncidentsByLink, drainedSince)

	// Build summaries
	activeSummary := LinkIncidentsSummary{
		Total:   len(activeIncidents),
		Ongoing: 0,
		ByType:  map[string]int{"packet_loss": 0, "errors": 0, "fcs": 0, "discards": 0, "carrier": 0, "no_data": 0},
	}
	for _, inc := range activeIncidents {
		if inc.IsOngoing {
			activeSummary.Ongoing++
		}
		activeSummary.ByType[inc.IncidentType]++
	}

	drainedSummary := DrainedSummary{
		Total: len(drainedLinks),
	}
	for _, dl := range drainedLinks {
		if len(dl.ActiveIncidents) > 0 {
			drainedSummary.WithIncidents++
			drainedSummary.NotReady++
		} else if dl.Readiness == "green" {
			drainedSummary.Ready++
		} else {
			drainedSummary.NotReady++
		}
	}

	if activeIncidents == nil {
		activeIncidents = []LinkIncident{}
	}
	if drainedLinks == nil {
		drainedLinks = []DrainedLinkInfo{}
	}

	response := LinkIncidentsResponse{
		Active:         activeIncidents,
		Drained:        drainedLinks,
		ActiveSummary:  activeSummary,
		DrainedSummary: drainedSummary,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// GetLinkIncidentsCSV returns link incidents as a CSV download
func GetLinkIncidentsCSV(w http.ResponseWriter, r *http.Request) {
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	duration := parseTimeRange(timeRange)

	thresholdStr := r.URL.Query().Get("threshold")
	threshold := parseThreshold(thresholdStr)

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

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	linkMeta, err := fetchLinkMetadataWithStatus(ctx, envDB(ctx), filters)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch link metadata: %v", err), http.StatusInternalServerError)
		return
	}

	var allIncidents []LinkIncident
	var mu = &sync.Mutex{}
	g, gCtx := errgroup.WithContext(ctx)

	if incidentType == "all" || incidentType == "packet_loss" {
		g.Go(func() error {
			incidents, err := fetchPacketLossIncidents(gCtx, envDB(gCtx), duration, threshold, linkMeta, detectParams)
			if err != nil {
				return err
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	if incidentType == "all" || incidentType == "errors" {
		g.Go(func() error {
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, errorsThreshold,
				"sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))", "errors", linkMeta, detectParams)
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
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, fcsThreshold,
				"sum(greatest(0, coalesce(in_fcs_errors_delta, 0)))", "fcs", linkMeta, detectParams)
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
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, discardsThreshold,
				"sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))", "discards", linkMeta, detectParams)
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
			incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, carrierThreshold,
				"sum(greatest(0, coalesce(carrier_transitions_delta, 0)))", "carrier", linkMeta, detectParams)
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
			incidents, err := fetchNoDataIncidents(gCtx, envDB(gCtx), duration, linkMeta)
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
		http.Error(w, fmt.Sprintf("Failed to fetch incidents: %v", err), http.StatusInternalServerError)
		return
	}

	// Filter by min duration
	minDurationSecs := int64(detectParams.MinDuration.Seconds())
	filtered := allIncidents[:0]
	for _, inc := range allIncidents {
		if inc.IsOngoing || inc.DurationSeconds == nil || *inc.DurationSeconds >= minDurationSecs {
			filtered = append(filtered, inc)
		}
	}
	allIncidents = filtered

	enrichLinkIncidentsWithInterfaces(ctx, envDB(ctx), allIncidents)

	sort.Slice(allIncidents, func(i, j int) bool {
		return allIncidents[i].StartedAt > allIncidents[j].StartedAt
	})

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=link-incidents.csv")

	_, _ = w.Write([]byte("id,link_code,link_type,side_a_metro,side_z_metro,contributor,incident_type,severity,is_drained,details,affected_interfaces,started_at,ended_at,duration_seconds,is_ongoing\n"))

	for _, inc := range allIncidents {
		var details string
		switch inc.IncidentType {
		case "packet_loss":
			if inc.PeakLossPct != nil && inc.ThresholdPct != nil {
				details = fmt.Sprintf("peak %.1f%% (threshold %.0f%%)", *inc.PeakLossPct, *inc.ThresholdPct)
			}
		case "errors", "discards", "carrier":
			if inc.PeakCount != nil && inc.ThresholdCount != nil {
				details = fmt.Sprintf("peak %d (threshold %d)", *inc.PeakCount, *inc.ThresholdCount)
			}
		case "no_data":
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

		line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%t,\"%s\",\"%s\",%s,%s,%s,%t\n",
			inc.ID, inc.LinkCode, inc.LinkType, inc.SideAMetro, inc.SideZMetro,
			inc.ContributorCode, inc.IncidentType, inc.Severity, inc.IsDrained,
			details, interfaces, inc.StartedAt, endedAt, durationSecs, inc.IsOngoing)
		_, _ = w.Write([]byte(line))
	}
}

func drainedSinceStr(drainedSince map[string]time.Time, linkPK string) string {
	if t, ok := drainedSince[linkPK]; ok {
		return t.UTC().Format(time.RFC3339)
	}
	return ""
}

// fetchDrainedSince finds when each drained link entered its current drain state
func fetchDrainedSince(ctx context.Context, conn driver.Conn, linkMeta map[string]linkMetadataWithStatus) map[string]time.Time {
	drainedPKs := make([]string, 0)
	for pk, meta := range linkMeta {
		if meta.Status == "soft-drained" || meta.Status == "hard-drained" {
			drainedPKs = append(drainedPKs, pk)
		}
	}
	if len(drainedPKs) == 0 {
		return nil
	}

	query := `
		SELECT sc.link_pk, max(sc.changed_ts) as drained_at
		FROM dz_link_status_changes sc
		WHERE sc.link_pk IN ($1)
		  AND sc.new_status IN ('soft-drained', 'hard-drained')
		GROUP BY sc.link_pk
	`

	rows, err := conn.Query(ctx, query, drainedPKs)
	if err != nil {
		return nil
	}
	defer rows.Close()

	result := make(map[string]time.Time)
	for rows.Next() {
		var linkPK string
		var drainedAt time.Time
		if err := rows.Scan(&linkPK, &drainedAt); err != nil {
			continue
		}
		result[linkPK] = drainedAt
	}
	return result
}

// buildDrainedLinksInfo builds the drained view from link metadata and incidents
func buildDrainedLinksInfo(linkMeta map[string]linkMetadataWithStatus, incidentsByLink map[string][]LinkIncident, drainedSince map[string]time.Time) []DrainedLinkInfo {
	var drainedLinks []DrainedLinkInfo

	for linkPK, meta := range linkMeta {
		if meta.Status != "soft-drained" && meta.Status != "hard-drained" {
			continue
		}

		incidents := incidentsByLink[linkPK]

		// Find active (ongoing) incidents
		var activeIncidents []LinkIncident
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

		// Build recent (completed) incidents list — most recent first
		var recentIncidents []LinkIncident
		for _, inc := range incidents {
			if !inc.IsOngoing {
				recentIncidents = append(recentIncidents, inc)
			}
		}
		sort.Slice(recentIncidents, func(i, j int) bool {
			return recentIncidents[i].StartedAt > recentIncidents[j].StartedAt
		})

		dl := DrainedLinkInfo{
			LinkPK:          linkPK,
			LinkCode:        meta.LinkCode,
			LinkType:        meta.LinkType,
			SideAMetro:      meta.SideAMetro,
			SideZMetro:      meta.SideZMetro,
			ContributorCode: meta.ContributorCode,
			DrainStatus:     meta.Status,
			DrainedSince:    drainedSinceStr(drainedSince, linkPK),
			ActiveIncidents: activeIncidents,
			RecentIncidents: recentIncidents,
		}

		if dl.ActiveIncidents == nil {
			dl.ActiveIncidents = []LinkIncident{}
		}
		if dl.RecentIncidents == nil {
			dl.RecentIncidents = []LinkIncident{}
		}

		// Determine readiness
		if len(activeIncidents) > 0 {
			dl.Readiness = "red"
		} else if lastEndedAt != nil {
			clearFor := int64(time.Since(*lastEndedAt).Seconds())
			dl.ClearForSeconds = &clearFor
			lastEnd := lastEndedAt.UTC().Format(time.RFC3339)
			dl.LastIncidentEnd = &lastEnd
			if clearFor >= 1800 { // 30 minutes
				dl.Readiness = "green"
			} else {
				dl.Readiness = "yellow"
			}
		} else {
			dl.Readiness = "gray"
		}

		drainedLinks = append(drainedLinks, dl)
	}

	// Sort by readiness priority: red first, then yellow, green, gray
	readinessOrder := map[string]int{"red": 0, "yellow": 1, "green": 2, "gray": 3}
	sort.Slice(drainedLinks, func(i, j int) bool {
		oi, oj := readinessOrder[drainedLinks[i].Readiness], readinessOrder[drainedLinks[j].Readiness]
		if oi != oj {
			return oi < oj
		}
		return drainedLinks[i].LinkCode < drainedLinks[j].LinkCode
	})

	return drainedLinks
}

// fetchDefaultIncidentsData fetches incidents data with default parameters for caching.
func fetchDefaultIncidentsData(ctx context.Context) *LinkIncidentsResponse {
	duration := 24 * time.Hour
	threshold := 10.0
	var filters []IncidentFilter

	detectParams := incidentDetectionParams{
		MinDuration:    30 * time.Minute,
		CoalesceGap:    180 * time.Minute,
		BucketInterval: bucketIntervalForDuration(duration),
	}

	linkMeta, err := fetchLinkMetadataWithStatus(ctx, envDB(ctx), filters)
	if err != nil {
		slog.Info("cache: incidents link metadata fetch unsuccessful", "detail", err)
		return nil
	}

	var allIncidents []LinkIncident
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		incidents, err := fetchPacketLossIncidents(gCtx, envDB(gCtx), duration, threshold, linkMeta, detectParams)
		if err != nil {
			return fmt.Errorf("packet loss: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))", "errors", linkMeta, detectParams)
		if err != nil {
			return fmt.Errorf("errors: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))", "discards", linkMeta, detectParams)
		if err != nil {
			return fmt.Errorf("discards: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(in_fcs_errors_delta, 0)))", "fcs", linkMeta, detectParams)
		if err != nil {
			return fmt.Errorf("fcs: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchCounterIncidents(gCtx, envDB(gCtx), duration, 1,
			"sum(greatest(0, coalesce(carrier_transitions_delta, 0)))", "carrier", linkMeta, detectParams)
		if err != nil {
			return fmt.Errorf("carrier: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	g.Go(func() error {
		incidents, err := fetchNoDataIncidents(gCtx, envDB(gCtx), duration, linkMeta)
		if err != nil {
			return fmt.Errorf("no_data: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	if err := g.Wait(); err != nil {
		slog.Info("cache: incidents fetch unsuccessful", "detail", err)
		return nil
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
	enrichLinkIncidentsWithInterfaces(ctx, envDB(ctx), allIncidents)

	// Split into active and drained
	var activeIncidents []LinkIncident
	drainedIncidentsByLink := make(map[string][]LinkIncident)

	for _, inc := range allIncidents {
		if !inc.IsDrained {
			activeIncidents = append(activeIncidents, inc)
		}
		if inc.IsDrained {
			drainedIncidentsByLink[inc.LinkPK] = append(drainedIncidentsByLink[inc.LinkPK], inc)
		}
	}

	sort.Slice(activeIncidents, func(i, j int) bool {
		return activeIncidents[i].StartedAt > activeIncidents[j].StartedAt
	})

	drainedSince := fetchDrainedSince(ctx, envDB(ctx), linkMeta)
	drainedLinks := buildDrainedLinksInfo(linkMeta, drainedIncidentsByLink, drainedSince)

	activeSummary := LinkIncidentsSummary{
		Total:   len(activeIncidents),
		Ongoing: 0,
		ByType:  map[string]int{"packet_loss": 0, "errors": 0, "fcs": 0, "discards": 0, "carrier": 0, "no_data": 0},
	}
	for _, inc := range activeIncidents {
		if inc.IsOngoing {
			activeSummary.Ongoing++
		}
		activeSummary.ByType[inc.IncidentType]++
	}

	drainedSummary := DrainedSummary{
		Total: len(drainedLinks),
	}
	for _, dl := range drainedLinks {
		if len(dl.ActiveIncidents) > 0 {
			drainedSummary.WithIncidents++
			drainedSummary.NotReady++
		} else if dl.Readiness == "green" {
			drainedSummary.Ready++
		} else {
			drainedSummary.NotReady++
		}
	}

	if activeIncidents == nil {
		activeIncidents = []LinkIncident{}
	}
	if drainedLinks == nil {
		drainedLinks = []DrainedLinkInfo{}
	}

	return &LinkIncidentsResponse{
		Active:         activeIncidents,
		Drained:        drainedLinks,
		ActiveSummary:  activeSummary,
		DrainedSummary: drainedSummary,
	}
}
