package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// Severity values returned in optics responses. Ordered worst-first so the
// "overall" severity is easy to compute by taking the max rank.
const (
	OpticsSeverityUnknown  = "unknown"
	OpticsSeverityOK       = "ok"
	OpticsSeverityWarning  = "warning"
	OpticsSeverityCritical = "critical"
)

// DeviceOpticsResponse is the response for GET /api/devices/{pk}/optics.
type DeviceOpticsResponse struct {
	DevicePK   string        `json:"device_pk"`
	DeviceCode string        `json:"device_code"`
	Lanes      []OpticsLane  `json:"lanes"`
	Summary    OpticsSummary `json:"summary"`
	FetchedAt  string        `json:"fetched_at"`
}

// OpticsLane is one (interface, channel) sample with thresholds + severities.
type OpticsLane struct {
	InterfaceName    string            `json:"interface_name"`
	ChannelIndex     uint16            `json:"channel_index"`
	Timestamp        string            `json:"timestamp"`
	InputPower       float64           `json:"input_power"`
	OutputPower      float64           `json:"output_power"`
	LaserBiasCurrent float64           `json:"laser_bias_current"`
	InputSeverity    string            `json:"input_severity"`
	OutputSeverity   string            `json:"output_severity"`
	BiasSeverity     string            `json:"bias_severity"`
	OverallSeverity  string            `json:"overall_severity"`
	Thresholds       *OpticsThresholds `json:"thresholds,omitempty"`
}

// OpticsThresholds carries the warning and critical bounds for a lane.
// Pointers are used so JSON omits missing values rather than reporting 0.
type OpticsThresholds struct {
	InputWarningLower  *float64 `json:"input_warning_lower,omitempty"`
	InputWarningUpper  *float64 `json:"input_warning_upper,omitempty"`
	InputCriticalLower *float64 `json:"input_critical_lower,omitempty"`
	InputCriticalUpper *float64 `json:"input_critical_upper,omitempty"`

	OutputWarningLower  *float64 `json:"output_warning_lower,omitempty"`
	OutputWarningUpper  *float64 `json:"output_warning_upper,omitempty"`
	OutputCriticalLower *float64 `json:"output_critical_lower,omitempty"`
	OutputCriticalUpper *float64 `json:"output_critical_upper,omitempty"`

	BiasWarningLower  *float64 `json:"bias_warning_lower,omitempty"`
	BiasWarningUpper  *float64 `json:"bias_warning_upper,omitempty"`
	BiasCriticalLower *float64 `json:"bias_critical_lower,omitempty"`
	BiasCriticalUpper *float64 `json:"bias_critical_upper,omitempty"`
}

// OpticsSummary holds lane counts by overall severity.
type OpticsSummary struct {
	OK       int `json:"ok"`
	Warning  int `json:"warning"`
	Critical int `json:"critical"`
	Unknown  int `json:"unknown"`
	Total    int `json:"total"`
}

// GetDeviceOptics returns per-lane transceiver state joined to thresholds,
// with computed severity per metric. Reads from the telemetry_<env> database
// populated by the admin setup-remote-tables command.
func (a *API) GetDeviceOptics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}

	conn := a.envDB(ctx)
	tdb := TelemetryDatabaseForEnv(EnvFromContext(ctx))

	// Resolve device code (also confirms the device exists in the current env).
	var deviceCode string
	deviceQ := `SELECT code FROM dz_devices_current WHERE pk = ?`
	if err := conn.QueryRow(ctx, deviceQ, pk).Scan(&deviceCode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "device not found", http.StatusNotFound)
			return
		}
		logError("device optics: device lookup failed", "error", err, "pk", pk)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	query := fmt.Sprintf(`
		WITH thresholds AS (
			SELECT
				device_pubkey,
				interface_name,
				minIf(input_power_lower,        severity = 'CRITICAL') AS in_crit_lo,
				maxIf(input_power_upper,        severity = 'CRITICAL') AS in_crit_hi,
				minIf(input_power_lower,        severity = 'WARNING')  AS in_warn_lo,
				maxIf(input_power_upper,        severity = 'WARNING')  AS in_warn_hi,
				minIf(output_power_lower,       severity = 'CRITICAL') AS out_crit_lo,
				maxIf(output_power_upper,       severity = 'CRITICAL') AS out_crit_hi,
				minIf(output_power_lower,       severity = 'WARNING')  AS out_warn_lo,
				maxIf(output_power_upper,       severity = 'WARNING')  AS out_warn_hi,
				minIf(laser_bias_current_lower, severity = 'CRITICAL') AS bias_crit_lo,
				maxIf(laser_bias_current_upper, severity = 'CRITICAL') AS bias_crit_hi,
				minIf(laser_bias_current_lower, severity = 'WARNING')  AS bias_warn_lo,
				maxIf(laser_bias_current_upper, severity = 'WARNING')  AS bias_warn_hi,
				countIf(severity = 'CRITICAL') AS n_crit,
				countIf(severity = 'WARNING')  AS n_warn
			FROM %[1]s.transceiver_thresholds_latest
			WHERE device_pubkey = ?
			GROUP BY device_pubkey, interface_name
		)
		SELECT
			s.interface_name,
			s.channel_index,
			s.timestamp,
			s.input_power,
			s.output_power,
			s.laser_bias_current,
			t.in_crit_lo,  t.in_crit_hi,  t.in_warn_lo,  t.in_warn_hi,
			t.out_crit_lo, t.out_crit_hi, t.out_warn_lo, t.out_warn_hi,
			t.bias_crit_lo, t.bias_crit_hi, t.bias_warn_lo, t.bias_warn_hi,
			t.n_crit, t.n_warn
		FROM %[1]s.transceiver_state_latest s
		LEFT JOIN thresholds t
			ON s.device_pubkey = t.device_pubkey AND s.interface_name = t.interface_name
		WHERE s.device_pubkey = ?
		ORDER BY s.interface_name, s.channel_index
	`, "`"+tdb+"`")

	start := time.Now()
	rows, err := conn.Query(ctx, query, pk, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("device_optics", duration, err)
	if err != nil {
		logError("device optics query failed", "error", err, "pk", pk)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var (
		lanes   = []OpticsLane{}
		summary OpticsSummary
	)
	for rows.Next() {
		var (
			ifname                                         string
			ch                                             uint16
			ts                                             time.Time
			inP, outP, bias                                float64
			inCritLo, inCritHi, inWarnLo, inWarnHi         float64
			outCritLo, outCritHi, outWarnLo, outWarnHi     float64
			biasCritLo, biasCritHi, biasWarnLo, biasWarnHi float64
			nCrit, nWarn                                   uint64
		)
		if err := rows.Scan(
			&ifname, &ch, &ts, &inP, &outP, &bias,
			&inCritLo, &inCritHi, &inWarnLo, &inWarnHi,
			&outCritLo, &outCritHi, &outWarnLo, &outWarnHi,
			&biasCritLo, &biasCritHi, &biasWarnLo, &biasWarnHi,
			&nCrit, &nWarn,
		); err != nil {
			logError("device optics row scan failed", "error", err, "pk", pk)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}

		hasCrit := nCrit > 0
		hasWarn := nWarn > 0
		lane := OpticsLane{
			InterfaceName:    ifname,
			ChannelIndex:     ch,
			Timestamp:        ts.UTC().Format(time.RFC3339),
			InputPower:       inP,
			OutputPower:      outP,
			LaserBiasCurrent: bias,
			InputSeverity:    classifyOpticsValue(inP, inCritLo, inCritHi, hasCrit, inWarnLo, inWarnHi, hasWarn),
			OutputSeverity:   classifyOpticsValue(outP, outCritLo, outCritHi, hasCrit, outWarnLo, outWarnHi, hasWarn),
			BiasSeverity:     classifyOpticsValue(bias, biasCritLo, biasCritHi, hasCrit, biasWarnLo, biasWarnHi, hasWarn),
		}
		lane.OverallSeverity = worstSeverity(lane.InputSeverity, lane.OutputSeverity, lane.BiasSeverity)

		if hasCrit || hasWarn {
			lane.Thresholds = &OpticsThresholds{
				InputCriticalLower:  optFloat(inCritLo, hasCrit),
				InputCriticalUpper:  optFloat(inCritHi, hasCrit),
				InputWarningLower:   optFloat(inWarnLo, hasWarn),
				InputWarningUpper:   optFloat(inWarnHi, hasWarn),
				OutputCriticalLower: optFloat(outCritLo, hasCrit),
				OutputCriticalUpper: optFloat(outCritHi, hasCrit),
				OutputWarningLower:  optFloat(outWarnLo, hasWarn),
				OutputWarningUpper:  optFloat(outWarnHi, hasWarn),
				BiasCriticalLower:   optFloat(biasCritLo, hasCrit),
				BiasCriticalUpper:   optFloat(biasCritHi, hasCrit),
				BiasWarningLower:    optFloat(biasWarnLo, hasWarn),
				BiasWarningUpper:    optFloat(biasWarnHi, hasWarn),
			}
		}

		switch lane.OverallSeverity {
		case OpticsSeverityCritical:
			summary.Critical++
		case OpticsSeverityWarning:
			summary.Warning++
		case OpticsSeverityOK:
			summary.OK++
		default:
			summary.Unknown++
		}
		summary.Total++
		lanes = append(lanes, lane)
	}
	if err := rows.Err(); err != nil {
		logError("device optics rows iteration failed", "error", err, "pk", pk)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	resp := DeviceOpticsResponse{
		DevicePK:   pk,
		DeviceCode: deviceCode,
		Lanes:      lanes,
		Summary:    summary,
		FetchedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("device optics: failed to encode response", "error", err)
	}
}

// classifyOpticsValue returns the severity for a measurement against critical
// and warning thresholds. Missing thresholds (sentinel 0/0 from the source)
// produce "unknown" so the UI can omit a verdict rather than show false OK.
func classifyOpticsValue(v, critLo, critHi float64, hasCrit bool, warnLo, warnHi float64, hasWarn bool) string {
	critSet := hasCrit && !(critLo == 0 && critHi == 0)
	warnSet := hasWarn && !(warnLo == 0 && warnHi == 0)
	if !critSet && !warnSet {
		return OpticsSeverityUnknown
	}
	if critSet && (v < critLo || v > critHi) {
		return OpticsSeverityCritical
	}
	if warnSet && (v < warnLo || v > warnHi) {
		return OpticsSeverityWarning
	}
	return OpticsSeverityOK
}

func worstSeverity(svs ...string) string {
	rank := func(s string) int {
		switch s {
		case OpticsSeverityCritical:
			return 3
		case OpticsSeverityWarning:
			return 2
		case OpticsSeverityOK:
			return 1
		default:
			return 0
		}
	}
	best := OpticsSeverityUnknown
	bestR := 0
	for _, s := range svs {
		if r := rank(s); r > bestR {
			best = s
			bestR = r
		}
	}
	return best
}

func optFloat(v float64, present bool) *float64 {
	if !present {
		return nil
	}
	return &v
}

// DeviceOpticsHistoryResponse is the response for
// GET /api/devices/{pk}/optics/history.
type DeviceOpticsHistoryResponse struct {
	DevicePK      string                     `json:"device_pk"`
	InterfaceName string                     `json:"interface_name"`
	ChannelIndex  *uint16                    `json:"channel_index,omitempty"`
	BucketSeconds int                        `json:"bucket_seconds"`
	From          string                     `json:"from"`
	To            string                     `json:"to"`
	Buckets       []DeviceOpticsHistoryPoint `json:"buckets"`
}

// DeviceOpticsHistoryPoint is one bucketed sample. Avg/min/max are returned
// so the UI can render a band as well as a center line.
type DeviceOpticsHistoryPoint struct {
	TS                  string  `json:"ts"`
	ChannelIndex        uint16  `json:"channel_index"`
	AvgInputPower       float64 `json:"avg_input_power"`
	MinInputPower       float64 `json:"min_input_power"`
	MaxInputPower       float64 `json:"max_input_power"`
	AvgOutputPower      float64 `json:"avg_output_power"`
	MinOutputPower      float64 `json:"min_output_power"`
	MaxOutputPower      float64 `json:"max_output_power"`
	AvgLaserBiasCurrent float64 `json:"avg_laser_bias_current"`
}

// GetDeviceOpticsHistory returns bucketed transceiver_state samples for a
// specific (device, interface[, channel]).
func (a *API) GetDeviceOpticsHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	ifname := q.Get("interface")
	if ifname == "" {
		http.Error(w, "interface query parameter is required", http.StatusBadRequest)
		return
	}

	// Optional channel filter.
	var channelPtr *uint16
	if chStr := q.Get("channel"); chStr != "" {
		v, err := strconv.ParseUint(chStr, 10, 16)
		if err != nil {
			http.Error(w, "invalid channel", http.StatusBadRequest)
			return
		}
		ch := uint16(v)
		channelPtr = &ch
	}

	// Time range / custom window, matching the device-metrics endpoint shape.
	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	startTimeStr := q.Get("start_time")
	endTimeStr := q.Get("end_time")
	bucketStr := q.Get("bucket")

	var params bucketParams
	if startTimeStr != "" && endTimeStr != "" {
		startUnix, err1 := strconv.ParseInt(startTimeStr, 10, 64)
		endUnix, err2 := strconv.ParseInt(endTimeStr, 10, 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "invalid start_time or end_time", http.StatusBadRequest)
			return
		}
		startT := time.Unix(startUnix, 0).UTC()
		endT := time.Unix(endUnix, 0).UTC()
		params = parseBucketParamsCustom(startT, endT, 24)
	} else {
		now := time.Now().UTC()
		duration := presetToDuration(timeRange)
		startT := now.Add(-duration)
		params = parseBucketParamsCustom(startT, now, 24)
		params.TimeRange = timeRange
	}

	if bucketStr != "" && bucketStr != "auto" {
		interval, ok := parseBucketString(bucketStr)
		if !ok {
			http.Error(w, "invalid bucket value", http.StatusBadRequest)
			return
		}
		secs := intervalToSeconds(interval)
		params.BucketSeconds = secs
		params.BucketMinutes = secs / 60
		params.BucketInterval = interval
	}

	bucketSec := params.BucketSeconds
	if bucketSec <= 0 {
		bucketSec = 300
	}

	conn := a.envDB(ctx)
	tdb := TelemetryDatabaseForEnv(EnvFromContext(ctx))

	channelClause := ""
	args := []any{*params.StartTime, *params.EndTime, pk, ifname}
	if channelPtr != nil {
		channelClause = " AND channel_index = ?"
		args = append(args, *channelPtr)
	}

	query := fmt.Sprintf(`
		SELECT
			toDateTime(toStartOfInterval(timestamp, INTERVAL %[2]d SECOND)) AS bucket_ts,
			channel_index,
			avg(input_power)        AS avg_in,
			min(input_power)        AS min_in,
			max(input_power)        AS max_in,
			avg(output_power)       AS avg_out,
			min(output_power)       AS min_out,
			max(output_power)       AS max_out,
			avg(laser_bias_current) AS avg_bias
		FROM %[1]s.transceiver_state
		WHERE timestamp >= ?
		  AND timestamp < ?
		  AND device_pubkey = ?
		  AND interface_name = ?%[3]s
		GROUP BY bucket_ts, channel_index
		ORDER BY bucket_ts, channel_index
	`, "`"+tdb+"`", bucketSec, channelClause)

	start := time.Now()
	rows, err := conn.Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("device_optics_history", duration, err)
	if err != nil {
		logError("device optics history query failed", "error", err, "pk", pk, "interface", ifname)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	buckets := []DeviceOpticsHistoryPoint{}
	for rows.Next() {
		var (
			ts     time.Time
			ch     uint16
			avgIn  float64
			minIn  float64
			maxIn  float64
			avgOut float64
			minOut float64
			maxOut float64
			avgBs  float64
		)
		if err := rows.Scan(&ts, &ch, &avgIn, &minIn, &maxIn, &avgOut, &minOut, &maxOut, &avgBs); err != nil {
			logError("device optics history row scan failed", "error", err, "pk", pk)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		buckets = append(buckets, DeviceOpticsHistoryPoint{
			TS:                  ts.UTC().Format(time.RFC3339),
			ChannelIndex:        ch,
			AvgInputPower:       avgIn,
			MinInputPower:       minIn,
			MaxInputPower:       maxIn,
			AvgOutputPower:      avgOut,
			MinOutputPower:      minOut,
			MaxOutputPower:      maxOut,
			AvgLaserBiasCurrent: avgBs,
		})
	}
	if err := rows.Err(); err != nil {
		logError("device optics history rows iteration failed", "error", err, "pk", pk)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	resp := DeviceOpticsHistoryResponse{
		DevicePK:      pk,
		InterfaceName: ifname,
		ChannelIndex:  channelPtr,
		BucketSeconds: bucketSec,
		From:          params.StartTime.UTC().Format(time.RFC3339),
		To:            params.EndTime.UTC().Format(time.RFC3339),
		Buckets:       buckets,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("device optics history: failed to encode response", "error", err)
	}
}
