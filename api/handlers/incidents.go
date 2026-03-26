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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

// linkMetadata contains link info for enriching incidents
type linkMetadata struct {
	LinkPK          string
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
}

// linkMetadataWithStatus extends linkMetadata with link status
type linkMetadataWithStatus struct {
	linkMetadata
	Status string
}

// bucketIntervalForDuration returns a coarser bucket interval for longer time ranges
// to reduce the amount of data scanned in ClickHouse.
func bucketIntervalForDuration(d time.Duration) time.Duration {
	switch {
	case d > 3*24*time.Hour: // 7d, 30d
		return 15 * time.Minute
	default: // 3h, 6h, 12h, 24h, 3d
		return 5 * time.Minute
	}
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

	if q.Get("source") != "" {
		return false
	}

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
	if isMainnet(r.Context()) && isDefaultIncidentsRequest(r) {
		if data, err := ReadPageCache(r.Context(), "incidents"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
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

	incidentType := r.URL.Query().Get("type")
	if incidentType == "" {
		incidentType = "all"
	}

	filterStr := r.URL.Query().Get("filter")
	filters := parseIncidentFilters(filterStr)

	ctx, cancel := statusContext(r, 30*time.Second)
	defer cancel()

	params := incidentQueryParams{
		Duration:          duration,
		BucketInterval:    bucketIntervalForDuration(duration),
		LossThreshold:     threshold,
		ErrorsThreshold:   errorsThreshold,
		FCSThreshold:      fcsThreshold,
		DiscardsThreshold: discardsThreshold,
		CarrierThreshold:  carrierThreshold,
		MinDurationMin:    minDurationMin,
		CoalesceGapMin:    coalesceGapMin,
		TypeFilter:        incidentType,
		Filters:           filters,
		UseRaw:            isRawSource(ctx),
	}

	allIncidents, err := fetchLinkIncidentsFromRollup(ctx, envDB(ctx), params)
	if err != nil {
		slog.Error("failed to fetch incidents", "error", err)
		http.Error(w, fmt.Sprintf("Failed to fetch incidents: %v", err), http.StatusInternalServerError)
		return
	}

	// Enrich counter incidents with affected interface names
	enrichLinkIncidentsWithInterfacesRollup(ctx, envDB(ctx), allIncidents)

	response := buildLinkIncidentsResponse(ctx, envDB(ctx), allIncidents, filters)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// buildLinkIncidentsResponse builds the full response from a flat list of incidents.
// It splits active/drained, fetches drained metadata, computes readiness, and builds summaries.
func buildLinkIncidentsResponse(ctx context.Context, conn driver.Conn, allIncidents []LinkIncident, filters []IncidentFilter) LinkIncidentsResponse {
	// Split into active (non-drained) and build drained view
	var activeIncidents []LinkIncident
	drainedIncidentsByLink := make(map[string][]LinkIncident)

	for _, inc := range allIncidents {
		if !inc.IsDrained {
			activeIncidents = append(activeIncidents, inc)
		} else {
			drainedIncidentsByLink[inc.LinkPK] = append(drainedIncidentsByLink[inc.LinkPK], inc)
		}
	}

	// Sort active incidents by start time (most recent first)
	sort.Slice(activeIncidents, func(i, j int) bool {
		return activeIncidents[i].StartedAt > activeIncidents[j].StartedAt
	})

	// Fetch link metadata for drained view (need all drained links, even those without incidents)
	linkMeta, err := fetchLinkMetadataWithStatus(ctx, conn, filters)
	if err != nil {
		slog.Warn("failed to fetch link metadata for drained view", "error", err)
		linkMeta = make(map[string]linkMetadataWithStatus)
	}

	drainedSince := fetchDrainedSince(ctx, conn, linkMeta)
	drainedLinks := buildDrainedLinksInfo(linkMeta, drainedIncidentsByLink, drainedSince)

	// Build summaries
	activeSummary := LinkIncidentsSummary{
		Total:   len(activeIncidents),
		Ongoing: 0,
		ByType:  map[string]int{"packet_loss": 0, "errors": 0, "fcs": 0, "discards": 0, "carrier": 0, "no_data": 0, "isis_down": 0},
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

	return LinkIncidentsResponse{
		Active:         activeIncidents,
		Drained:        drainedLinks,
		ActiveSummary:  activeSummary,
		DrainedSummary: drainedSummary,
	}
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

	incidentType := r.URL.Query().Get("type")
	if incidentType == "" {
		incidentType = "all"
	}

	filterStr := r.URL.Query().Get("filter")
	filters := parseIncidentFilters(filterStr)

	ctx, cancel := statusContext(r, 30*time.Second)
	defer cancel()

	params := incidentQueryParams{
		Duration:          duration,
		BucketInterval:    bucketIntervalForDuration(duration),
		LossThreshold:     threshold,
		ErrorsThreshold:   errorsThreshold,
		FCSThreshold:      fcsThreshold,
		DiscardsThreshold: discardsThreshold,
		CarrierThreshold:  carrierThreshold,
		MinDurationMin:    minDurationMin,
		CoalesceGapMin:    coalesceGapMin,
		TypeFilter:        incidentType,
		Filters:           filters,
		UseRaw:            isRawSource(ctx),
	}

	allIncidents, err := fetchLinkIncidentsFromRollup(ctx, envDB(ctx), params)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch incidents: %v", err), http.StatusInternalServerError)
		return
	}

	enrichLinkIncidentsWithInterfacesRollup(ctx, envDB(ctx), allIncidents)

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

// FetchDefaultIncidentsData fetches incidents data with default parameters for caching.
func FetchDefaultIncidentsData(ctx context.Context) *LinkIncidentsResponse {
	params := incidentQueryParams{
		Duration:          24 * time.Hour,
		BucketInterval:    bucketIntervalForDuration(24 * time.Hour),
		LossThreshold:     10.0,
		ErrorsThreshold:   1,
		FCSThreshold:      1,
		DiscardsThreshold: 1,
		CarrierThreshold:  1,
		MinDurationMin:    30,
		CoalesceGapMin:    180,
		TypeFilter:        "all",
	}

	allIncidents, err := fetchLinkIncidentsFromRollup(ctx, envDB(ctx), params)
	if err != nil {
		slog.Info("cache: incidents rollup fetch unsuccessful", "detail", err)
		return nil
	}

	enrichLinkIncidentsWithInterfacesRollup(ctx, envDB(ctx), allIncidents)

	resp := buildLinkIncidentsResponse(ctx, envDB(ctx), allIncidents, nil)
	return &resp
}
