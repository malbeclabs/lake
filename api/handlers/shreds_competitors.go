package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

const ShredsCompetitorsDefaultDays = 30
const shredsCompetitorsMaxDays = 366

// ShredsCompetitorDay is one UTC day of DZ's shred race against the commercial
// shred feeds, at the leader-slot grain.
type ShredsCompetitorDay struct {
	Day string `json:"day"`

	WinTypicalPct float64 `json:"win_typical_pct"`
	LeadTypicalMs float64 `json:"lead_typical_ms"`

	LeaderSlots uint64 `json:"leader_slots"`
}

// GetShredsCompetitors returns the daily series behind the "Win Rate vs
// Competitors" panel.
func (a *API) GetShredsCompetitors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	days := ShredsCompetitorsDefaultDays
	if v := r.URL.Query().Get("days"); v != "" {
		var parsed int
		if _, err := fmt.Sscanf(v, "%d", &parsed); err == nil && parsed > 0 {
			days = min(parsed, shredsCompetitorsMaxDays)
		}
	}

	query := `
		SELECT
			toString(bucket_date),
			win_typical_p50,
			lead_typical_ms,
			leader_slots
		FROM shred_competitor_rollup_1d FINAL
		WHERE bucket_date >= subtractDays(today(), ?)
		  AND bucket_date < today()
		ORDER BY bucket_date
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, days)
	metrics.RecordClickHouseQuery("shreds_competitors", time.Since(start), err)

	if err != nil {
		if isMissingTable(err) {
			logWarn("shreds competitor rollup table not available", "error", err)
			writeJSON(w, []ShredsCompetitorDay{})
			return
		}
		logError("shreds competitors query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	series := []ShredsCompetitorDay{}
	for rows.Next() {
		var d ShredsCompetitorDay
		var winFraction float64
		if err := rows.Scan(&d.Day, &winFraction, &d.LeadTypicalMs, &d.LeaderSlots); err != nil {
			logError("shreds competitors row scan", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		d.WinTypicalPct = winFraction * 100
		series = append(series, d)
	}
	if err := rows.Err(); err != nil {
		logError("shreds competitors rows", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, series)
}
