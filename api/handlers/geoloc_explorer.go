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
	Devices []GeolocExplorerDevice `json:"devices"`
	Targets []GeolocExplorerTarget `json:"targets"`
}

type GeolocExplorerDevice struct {
	SenderPubkey        string  `json:"sender_pubkey"`
	ProbeCode           string  `json:"probe_code"`
	Lat                 float64 `json:"lat"`
	Lng                 float64 `json:"lng"`
	MinRefMeasuredRttNs uint64  `json:"min_ref_measured_rtt_ns"`
}

type GeolocExplorerTarget struct {
	SenderPubkey     string  `json:"sender_pubkey"`
	TargetIP         string  `json:"target_ip"`
	Lat              float64 `json:"lat"`
	Lng              float64 `json:"lng"`
	MinMeasuredRttNs uint64  `json:"min_measured_rtt_ns"`
}

func (a *API) GetGeolocExplorer(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	hours := 24
	if hoursStr := r.URL.Query().Get("hours"); hoursStr != "" {
		if v, err := strconv.Atoi(hoursStr); err == nil {
			hours = v
		}
	}
	if hours < 1 {
		hours = 1
	}
	if hours > 168 {
		hours = 168
	}

	envDB := string(EnvFromContext(r.Context()))

	// Query 1: Devices — one row per sender_pubkey with min geoprobe RTT.
	// ClickHouse arrays are 1-indexed, so ref_measured_rtt_ns[1] is the first element.
	deviceQuery := fmt.Sprintf(`
		SELECT
			sender_pubkey,
			any(lat) AS lat,
			any(lng) AS lng,
			min(ref_measured_rtt_ns[1]) AS min_ref_measured_rtt_ns
		FROM `+"`%s`"+`.location_offsets
		WHERE received_at >= now() - INTERVAL %d HOUR
		  AND length(ref_measured_rtt_ns) > 0
		GROUP BY sender_pubkey
	`, envDB, hours)

	start := time.Now()
	deviceRows, err := a.envDB(ctx).Query(ctx, deviceQuery)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		logError("geoloc explorer device query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer deviceRows.Close()

	var devices []GeolocExplorerDevice
	pubkeys := make(map[string]struct{})
	for deviceRows.Next() {
		var d GeolocExplorerDevice
		if err := deviceRows.Scan(&d.SenderPubkey, &d.Lat, &d.Lng, &d.MinRefMeasuredRttNs); err != nil {
			logError("geoloc explorer device scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		pubkeys[d.SenderPubkey] = struct{}{}
		devices = append(devices, d)
	}
	if err := deviceRows.Err(); err != nil {
		logError("geoloc explorer device rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Query 2: Targets — one row per (sender_pubkey, target_ip) with min measured RTT.
	targetQuery := fmt.Sprintf(`
		SELECT
			sender_pubkey,
			target_ip,
			any(lat) AS lat,
			any(lng) AS lng,
			min(measured_rtt_ns) AS min_measured_rtt_ns
		FROM `+"`%s`"+`.location_offsets
		WHERE received_at >= now() - INTERVAL %d HOUR
		GROUP BY sender_pubkey, target_ip
	`, envDB, hours)

	start = time.Now()
	targetRows, err := a.envDB(ctx).Query(ctx, targetQuery)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		logError("geoloc explorer target query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer targetRows.Close()

	var targets []GeolocExplorerTarget
	for targetRows.Next() {
		var t GeolocExplorerTarget
		if err := targetRows.Scan(&t.SenderPubkey, &t.TargetIP, &t.Lat, &t.Lng, &t.MinMeasuredRttNs); err != nil {
			logError("geoloc explorer target scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		targets = append(targets, t)
	}
	if err := targetRows.Err(); err != nil {
		logError("geoloc explorer target rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Enrich devices with probe codes from the lake database via a separate query.
	// geoloc_probes_current lives in the lake DB (a.DB), not behind remoteSecure().
	if len(pubkeys) > 0 {
		pks := make([]string, 0, len(pubkeys))
		for pk := range pubkeys {
			pks = append(pks, pk)
		}
		probeRows, err := a.DB.Query(ctx, "SELECT pk, code FROM geoloc_probes_current WHERE pk IN (?)", pks)
		if err != nil {
			logError("geoloc explorer probe query error", "error", err)
			// Non-fatal: return devices without probe codes
		} else {
			defer probeRows.Close()
			codeMap := make(map[string]string)
			for probeRows.Next() {
				var pk, code string
				if err := probeRows.Scan(&pk, &code); err == nil {
					codeMap[pk] = code
				}
			}
			for i := range devices {
				if code, ok := codeMap[devices[i].SenderPubkey]; ok {
					devices[i].ProbeCode = code
				}
			}
		}
	}

	if devices == nil {
		devices = []GeolocExplorerDevice{}
	}
	if targets == nil {
		targets = []GeolocExplorerTarget{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(GeolocExplorerResponse{Devices: devices, Targets: targets}); err != nil {
		logError("failed to encode response", "error", err)
	}
}
