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
	if isMainnet(r.Context()) && isDefaultDeviceIncidentsRequest(r) {
		if data, err := ReadPageCache(r.Context(), "device_incidents"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
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

	incidentType := r.URL.Query().Get("type")
	if incidentType == "" {
		incidentType = "all"
	}

	filterStr := r.URL.Query().Get("filter")
	filters := parseIncidentFilters(filterStr)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	params := incidentQueryParams{
		Duration:          duration,
		BucketInterval:    bucketIntervalForDuration(duration),
		ErrorsThreshold:   errorsThreshold,
		FCSThreshold:      fcsThreshold,
		DiscardsThreshold: discardsThreshold,
		CarrierThreshold:  carrierThreshold,
		MinDurationMin:    minDurationMin,
		CoalesceGapMin:    coalesceGapMin,
		TypeFilter:        incidentType,
		Filters:           filters,
		IncludeLinkIntfs:  r.URL.Query().Get("link_interfaces") == "true",
	}

	allIncidents, err := fetchDeviceIncidentsFromRollup(ctx, envDB(ctx), params)
	if err != nil {
		slog.Error("failed to fetch device incidents", "error", err)
		http.Error(w, fmt.Sprintf("Failed to fetch device incidents: %v", err), http.StatusInternalServerError)
		return
	}

	enrichDeviceIncidentsWithInterfacesRollup(ctx, envDB(ctx), allIncidents)

	response := buildDeviceIncidentsResponse(ctx, envDB(ctx), allIncidents, filters)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// buildDeviceIncidentsResponse builds the full response from a flat list of device incidents.
func buildDeviceIncidentsResponse(ctx context.Context, conn driver.Conn, allIncidents []DeviceIncident, filters []IncidentFilter) DeviceIncidentsResponse {
	var activeIncidents []DeviceIncident
	drainedIncidentsByDevice := make(map[string][]DeviceIncident)

	for _, inc := range allIncidents {
		if !inc.IsDrained {
			activeIncidents = append(activeIncidents, inc)
		} else {
			drainedIncidentsByDevice[inc.DevicePK] = append(drainedIncidentsByDevice[inc.DevicePK], inc)
		}
	}

	sort.Slice(activeIncidents, func(i, j int) bool {
		return activeIncidents[i].StartedAt > activeIncidents[j].StartedAt
	})

	// Fetch device metadata for drained view
	deviceMeta, err := fetchDeviceMetadata(ctx, conn, filters)
	if err != nil {
		slog.Warn("failed to fetch device metadata for drained view", "error", err)
		deviceMeta = make(map[string]deviceMetadata)
	}

	drainedSince := fetchDeviceDrainedSince(ctx, conn, deviceMeta)
	drainedDevices := buildDrainedDevicesInfo(deviceMeta, drainedIncidentsByDevice, drainedSince)

	activeSummary := DeviceIncidentsSummary{
		Total:   len(activeIncidents),
		Ongoing: 0,
		ByType:  map[string]int{"errors": 0, "fcs": 0, "discards": 0, "carrier": 0, "no_data": 0, "isis_overload": 0, "isis_unreachable": 0},
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

	return DeviceIncidentsResponse{
		Active:         activeIncidents,
		Drained:        drainedDevices,
		ActiveSummary:  activeSummary,
		DrainedSummary: drainedSummary,
	}
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
	incidentType := r.URL.Query().Get("type")
	if incidentType == "" {
		incidentType = "all"
	}

	filterStr := r.URL.Query().Get("filter")
	filters := parseIncidentFilters(filterStr)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	params := incidentQueryParams{
		Duration:          duration,
		BucketInterval:    bucketIntervalForDuration(duration),
		ErrorsThreshold:   errorsThreshold,
		FCSThreshold:      fcsThreshold,
		DiscardsThreshold: discardsThreshold,
		CarrierThreshold:  carrierThreshold,
		MinDurationMin:    minDurationMin,
		CoalesceGapMin:    coalesceGapMin,
		TypeFilter:        incidentType,
		Filters:           filters,
		IncludeLinkIntfs:  r.URL.Query().Get("link_interfaces") == "true",
	}

	allIncidents, err := fetchDeviceIncidentsFromRollup(ctx, envDB(ctx), params)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch device incidents: %v", err), http.StatusInternalServerError)
		return
	}

	enrichDeviceIncidentsWithInterfacesRollup(ctx, envDB(ctx), allIncidents)

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

// FetchDefaultDeviceIncidentsData fetches device incidents data with default parameters for caching.
func FetchDefaultDeviceIncidentsData(ctx context.Context) *DeviceIncidentsResponse {
	params := incidentQueryParams{
		Duration:          24 * time.Hour,
		BucketInterval:    bucketIntervalForDuration(24 * time.Hour),
		ErrorsThreshold:   1,
		FCSThreshold:      1,
		DiscardsThreshold: 1,
		CarrierThreshold:  1,
		MinDurationMin:    30,
		CoalesceGapMin:    180,
		TypeFilter:        "all",
	}

	allIncidents, err := fetchDeviceIncidentsFromRollup(ctx, envDB(ctx), params)
	if err != nil {
		slog.Info("cache: device incidents rollup fetch unsuccessful", "detail", err)
		return nil
	}

	enrichDeviceIncidentsWithInterfacesRollup(ctx, envDB(ctx), allIncidents)

	resp := buildDeviceIncidentsResponse(ctx, envDB(ctx), allIncidents, nil)
	return &resp
}
