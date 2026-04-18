package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type GeolocExplorerResponse struct {
	Offsets []GeolocExplorerOffset `json:"offsets"`
}

type GeolocExplorerOffset struct {
	SenderPubkey     string   `json:"sender_pubkey"`
	ProbeCode        string   `json:"probe_code"`
	Lat              float64  `json:"lat"`
	Lng              float64  `json:"lng"`
	RttNs            uint64   `json:"rtt_ns"`
	MeasuredRttNs    uint64   `json:"measured_rtt_ns"`
	TargetIP         string   `json:"target_ip"`
	NumReferences    uint8    `json:"num_references"`
	RefMeasuredRttNs []uint64 `json:"ref_measured_rtt_ns"`
	RefRttNs         []uint64 `json:"ref_rtt_ns"`
}

func (a *API) GetGeolocExplorer(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	hours := 1
	if hoursStr := r.URL.Query().Get("hours"); hoursStr != "" {
		if v, err := strconv.Atoi(hoursStr); err == nil {
			hours = v
		}
	}
	if hours < 1 {
		hours = 1
	}
	if hours > 24 {
		hours = 24
	}

	envDB := string(EnvFromContext(r.Context()))
	lakeDB := a.DatabaseForEnvFromContext(r.Context())

	query := fmt.Sprintf(`
		SELECT
			lo.sender_pubkey,
			coalesce(gp.code, '') AS probe_code,
			lo.lat,
			lo.lng,
			lo.rtt_ns,
			lo.measured_rtt_ns,
			lo.target_ip,
			lo.num_references,
			lo.ref_measured_rtt_ns,
			lo.ref_rtt_ns
		FROM `+"`%s`"+`.location_offsets lo
		LEFT JOIN `+"`%s`"+`.geoloc_probes_current gp ON lo.sender_pubkey = gp.pk
		WHERE lo.received_at >= now() - INTERVAL %d HOUR
		ORDER BY lo.received_at DESC
		LIMIT 10000
	`, envDB, lakeDB, hours)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("geoloc explorer query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var offsets []GeolocExplorerOffset
	for rows.Next() {
		var o GeolocExplorerOffset
		if err := rows.Scan(
			&o.SenderPubkey,
			&o.ProbeCode,
			&o.Lat,
			&o.Lng,
			&o.RttNs,
			&o.MeasuredRttNs,
			&o.TargetIP,
			&o.NumReferences,
			&o.RefMeasuredRttNs,
			&o.RefRttNs,
		); err != nil {
			logError("geoloc explorer scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		offsets = append(offsets, o)
	}

	if err := rows.Err(); err != nil {
		logError("geoloc explorer rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if offsets == nil {
		offsets = []GeolocExplorerOffset{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(GeolocExplorerResponse{Offsets: offsets}); err != nil {
		logError("failed to encode response", "error", err)
	}
}
