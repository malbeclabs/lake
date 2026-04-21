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

	// Query location_offsets from the env-named database. This table is a
	// remoteSecure() proxy, so the entire query is pushed to ClickHouse Cloud.
	// We must NOT cross-database JOIN here because the local DB name ("default")
	// doesn't exist on Cloud.
	offsetQuery := fmt.Sprintf(`
		SELECT
			sender_pubkey, lat, lng, rtt_ns, measured_rtt_ns, target_ip,
			num_references, ref_measured_rtt_ns, ref_rtt_ns
		FROM `+"`%s`"+`.location_offsets
		WHERE received_at >= now() - INTERVAL %d HOUR
		ORDER BY received_at DESC
		LIMIT 10000
	`, envDB, hours)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, offsetQuery)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("geoloc explorer query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var offsets []GeolocExplorerOffset
	pubkeys := make(map[string]struct{})
	for rows.Next() {
		var o GeolocExplorerOffset
		if err := rows.Scan(
			&o.SenderPubkey,
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
		pubkeys[o.SenderPubkey] = struct{}{}
		offsets = append(offsets, o)
	}

	if err := rows.Err(); err != nil {
		logError("geoloc explorer rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Enrich with probe codes from the lake database via a separate query.
	// geoloc_probes_current lives in the lake DB (a.DB), not behind remoteSecure().
	if len(pubkeys) > 0 {
		pks := make([]string, 0, len(pubkeys))
		for pk := range pubkeys {
			pks = append(pks, pk)
		}
		probeRows, err := a.DB.Query(ctx, "SELECT pk, code FROM geoloc_probes_current WHERE pk IN (?)", pks)
		if err != nil {
			logError("geoloc explorer probe query error", "error", err)
			// Non-fatal: return offsets without probe codes
		} else {
			defer probeRows.Close()
			codeMap := make(map[string]string)
			for probeRows.Next() {
				var pk, code string
				if err := probeRows.Scan(&pk, &code); err == nil {
					codeMap[pk] = code
				}
			}
			for i := range offsets {
				if code, ok := codeMap[offsets[i].SenderPubkey]; ok {
					offsets[i].ProbeCode = code
				}
			}
		}
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
