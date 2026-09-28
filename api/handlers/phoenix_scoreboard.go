package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

const phoenixScoreboardWindow = 24 * time.Hour
const phoenixScoreboardRefresh = time.Hour

type PhoenixScoreboardStat struct {
	WinPct float64 `json:"win_pct"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
}

type PhoenixScoreboardBucket struct {
	Start     time.Time `json:"start"`
	Races     uint64    `json:"races"`
	DZWins    uint64    `json:"dz_wins"`
	VenueWins uint64    `json:"venue_wins"`
	PhoenixScoreboardStat
}

type PhoenixScoreboardResponse struct {
	WindowStart   time.Time                 `json:"window_start"`
	WindowEnd     time.Time                 `json:"window_end"`
	Races         uint64                    `json:"races"`
	DZWins        uint64                    `json:"dz_wins"`
	VenueWins     uint64                    `json:"venue_wins"`
	All           PhoenixScoreboardStat     `json:"all"`
	Buckets       []PhoenixScoreboardBucket `json:"buckets"`
	SiteLabel     string                    `json:"site_label"`
	AsOf          *time.Time                `json:"as_of,omitempty"`
	NextRefreshAt *time.Time                `json:"next_refresh_at,omitempty"`
}

func newPhoenixScoreboardResponse() *PhoenixScoreboardResponse {
	return &PhoenixScoreboardResponse{Buckets: []PhoenixScoreboardBucket{}, SiteLabel: "Columbus"}
}

func winPct(wins, races uint64) float64 {
	if races == 0 {
		return 0
	}
	return 100 * float64(wins) / float64(races)
}

// FetchPhoenixScoreboardData reads the board from the rollup table.
func (a *API) FetchPhoenixScoreboardData(ctx context.Context) (*PhoenixScoreboardResponse, error) {
	resp := newPhoenixScoreboardResponse()

	start := time.Now()
	var stored uint64
	var newest, lastWrite time.Time
	err := a.DB.QueryRow(ctx,
		`SELECT count(), max(bucket_ts), max(ingested_at) FROM phoenix_race_rollup_15m`,
	).Scan(&stored, &newest, &lastWrite)
	metrics.RecordClickHouseQuery("phoenix_scoreboard_newest", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	if stored == 0 {
		return resp, nil
	}
	asOf := lastWrite.UTC()
	next := asOf.Truncate(phoenixScoreboardRefresh).Add(phoenixScoreboardRefresh)
	resp.AsOf, resp.NextRefreshAt = &asOf, &next

	windowEnd := newest.UTC().Add(15 * time.Minute)
	resp.WindowStart = windowEnd.Add(-phoenixScoreboardWindow)
	resp.WindowEnd = windowEnd

	start = time.Now()
	rows, err := a.DB.Query(ctx, `
		SELECT
			bucket_ts,
			sum(paired),
			sum(dz_wins),
			sum(venue_wins),
			arrayMap(x -> ifNotFinite(toFloat64(x), 0),
				quantilesTDigestMerge(0.5, 0.95, 0.99)(signed_lead_ms_state))
		FROM phoenix_race_rollup_15m FINAL
		WHERE bucket_ts >= toDateTime(?, 'UTC')
		  AND bucket_ts < toDateTime(?, 'UTC')
		  AND paired > 0
		GROUP BY bucket_ts WITH TOTALS
		ORDER BY bucket_ts`,
		resp.WindowStart.Unix(), resp.WindowEnd.Unix())
	metrics.RecordClickHouseQuery("phoenix_scoreboard_window", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b PhoenixScoreboardBucket
		var q []float64
		if err := rows.Scan(&b.Start, &b.Races, &b.DZWins, &b.VenueWins, &q); err != nil {
			return nil, err
		}
		b.Start = b.Start.UTC()
		b.WinPct = winPct(b.DZWins, b.Races)
		b.P50Ms, b.P95Ms, b.P99Ms = q[0], q[1], q[2]
		resp.Buckets = append(resp.Buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(resp.Buckets) == 0 {
		return resp, nil
	}
	var ignored time.Time
	var q []float64
	if err := rows.Totals(&ignored, &resp.Races, &resp.DZWins, &resp.VenueWins, &q); err != nil {
		return nil, err
	}
	resp.All = PhoenixScoreboardStat{WinPct: winPct(resp.DZWins, resp.Races), P50Ms: q[0], P95Ms: q[1], P99Ms: q[2]}
	return resp, nil
}

func (a *API) GetPhoenixScoreboard(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	data, err := a.FetchPhoenixScoreboardData(ctx)
	if err != nil {
		if isMissingTable(err) {
			logWarn("phoenix race rollup table not available", "error", err)
			writeJSON(w, newPhoenixScoreboardResponse())
			return
		}
		logError("phoenix scoreboard query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, data)
}
