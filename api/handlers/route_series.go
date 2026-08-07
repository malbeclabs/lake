package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// maxRouteSeriesPairs bounds the sparkline query. The customer-facing page shows
// a handful of routes; this keeps the uncached query cheap.
const maxRouteSeriesPairs = 10

// RouteSeriesPoint is one hourly bucket of the DZ-vs-internet comparison.
type RouteSeriesPoint struct {
	TS         time.Time `json:"ts"`
	DZMs       float64   `json:"dzMs"`
	InternetMs float64   `json:"internetMs"`
}

// RouteSeries is the 7-day history for one metro pair.
type RouteSeries struct {
	FromMetroCode string             `json:"fromMetroCode"`
	ToMetroCode   string             `json:"toMetroCode"`
	Points        []RouteSeriesPoint `json:"points"`
}

// RouteSeriesResponse is the response for the route series endpoint.
type RouteSeriesResponse struct {
	Series []RouteSeries `json:"series"`
	Error  string        `json:"error,omitempty"`
}

// parseRoutePairs turns "tyo-lon,fra-lon" into normalized metro-code pairs,
// ordered lexicographically to match the least/greatest convention used by the
// latency tables.
func parseRoutePairs(raw string) ([][2]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("pairs is required")
	}
	parts := strings.Split(raw, ",")
	if len(parts) > maxRouteSeriesPairs {
		return nil, fmt.Errorf("at most %d pairs allowed, got %d", maxRouteSeriesPairs, len(parts))
	}
	out := make([][2]string, 0, len(parts))
	for _, p := range parts {
		ab := strings.Split(strings.ToLower(strings.TrimSpace(p)), "-")
		if len(ab) != 2 || ab[0] == "" || ab[1] == "" {
			return nil, fmt.Errorf("malformed pair %q, want <metro>-<metro>", p)
		}
		if ab[0] == ab[1] {
			return nil, fmt.Errorf("pair %q has identical metros", p)
		}
		if ab[0] > ab[1] {
			ab[0], ab[1] = ab[1], ab[0]
		}
		out = append(out, [2]string{ab[0], ab[1]})
	}
	return out, nil
}

// sumHopsAtBucket returns the summed RTT across every hop in nodes, and ok=false
// when any hop is missing from links — callers must report zero rather than a
// short sum, which would understate the route.
func sumHopsAtBucket(nodes []string, links map[string]float64) (float64, bool) {
	if len(nodes) < 2 {
		return 0, false
	}
	var sum float64
	for h := 1; h < len(nodes); h++ {
		v, ok := links[nodes[h-1]+":"+nodes[h]]
		if !ok {
			return 0, false
		}
		sum += v
	}
	return sum, true
}

// GetRouteSeries returns 7 days of hourly DZ-vs-internet latency for the
// requested metro pairs. Not page-cached: it serves a small, user-chosen set.
func (a *API) GetRouteSeries(w http.ResponseWriter, r *http.Request) {
	pairs, err := parseRoutePairs(r.URL.Query().Get("pairs"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, RouteSeriesResponse{Series: []RouteSeries{}, Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	resp, err := a.FetchRouteSeries(ctx, pairs)
	if err != nil {
		logError("route series error", "error", err)
		writeJSON(w, RouteSeriesResponse{Series: []RouteSeries{}, Error: err.Error()})
		return
	}
	writeJSON(w, resp)
}

// FetchRouteSeries builds the hourly series for each requested pair. The DZ side
// sums per-hop rollup RTT per bucket along the path Dijkstra selected; the
// internet side comes straight from fact_dz_internet_metro_latency.
func (a *API) FetchRouteSeries(ctx context.Context, pairs [][2]string) (*RouteSeriesResponse, error) {
	g, err := a.loadTopologyGraph(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("loading topology graph: %w", err)
	}

	// Map each requested pair to the device sequence of its best path.
	pathByPair := make(map[string][]string)
	for _, mp := range computeMetroPairPaths(g) {
		a1, b1 := mp.FromMetroCode, mp.ToMetroCode
		if a1 > b1 {
			a1, b1 = b1, a1
		}
		pathByPair[a1+"-"+b1] = mp.Path.Nodes
	}

	// Hourly measured RTT per link.
	linkQuery := `
		SELECT
			toStartOfHour(r.bucket_ts, 'UTC') AS ts,
			l.side_a_pk,
			l.side_z_pk,
			sum(r.a_avg_rtt_us * r.a_samples + r.z_avg_rtt_us * r.z_samples) / greatest(sum(r.a_samples + r.z_samples), 1) / 1000.0 AS rtt_ms
		FROM dz_links_current l
		JOIN link_rollup_5m r FINAL ON l.pk = r.link_pk
		WHERE r.bucket_ts >= now() - INTERVAL 7 DAY
		  AND l.side_a_pk != '' AND l.side_z_pk != ''
		GROUP BY ts, l.side_a_pk, l.side_z_pk
	`
	linkRows, err := a.envDB(ctx).Query(ctx, linkQuery)
	if err != nil {
		return nil, fmt.Errorf("route series link query: %w", err)
	}
	defer linkRows.Close()

	// hourly[ts]["devA:devB"] = rtt
	hourly := make(map[time.Time]map[string]float64)
	for linkRows.Next() {
		var ts time.Time
		var sideA, sideZ string
		var rtt float64
		if err := linkRows.Scan(&ts, &sideA, &sideZ, &rtt); err != nil {
			return nil, fmt.Errorf("route series link scan: %w", err)
		}
		ts = ts.UTC()
		if hourly[ts] == nil {
			hourly[ts] = make(map[string]float64)
		}
		hourly[ts][sideA+":"+sideZ] = rtt
		hourly[ts][sideZ+":"+sideA] = rtt
	}
	if err := linkRows.Err(); err != nil {
		return nil, fmt.Errorf("route series link rows: %w", err)
	}

	// Hourly internet RTT per metro pair.
	inetQuery := `
		SELECT
			toStartOfHour(f.event_ts, 'UTC') AS ts,
			least(ma.code, mz.code) AS m1,
			greatest(ma.code, mz.code) AS m2,
			avg(f.rtt_us) / 1000.0 AS rtt_ms
		FROM fact_dz_internet_metro_latency f
		JOIN dz_metros_current ma ON f.origin_metro_pk = ma.pk
		JOIN dz_metros_current mz ON f.target_metro_pk = mz.pk
		WHERE f.event_ts >= now() - INTERVAL 7 DAY
		  AND ma.code != mz.code
		GROUP BY ts, m1, m2
	`
	inetRows, err := a.envDB(ctx).Query(ctx, inetQuery)
	if err != nil {
		return nil, fmt.Errorf("route series internet query: %w", err)
	}
	defer inetRows.Close()

	// inet["m1-m2"][ts] = rtt
	inet := make(map[string]map[time.Time]float64)
	for inetRows.Next() {
		var ts time.Time
		var m1, m2 string
		var rtt float64
		if err := inetRows.Scan(&ts, &m1, &m2, &rtt); err != nil {
			return nil, fmt.Errorf("route series internet scan: %w", err)
		}
		key := m1 + "-" + m2
		if inet[key] == nil {
			inet[key] = make(map[time.Time]float64)
		}
		inet[key][ts.UTC()] = rtt
	}
	if err := inetRows.Err(); err != nil {
		return nil, fmt.Errorf("route series internet rows: %w", err)
	}

	// Emit one point per hour across the window, oldest first.
	end := time.Now().UTC().Truncate(time.Hour)
	resp := &RouteSeriesResponse{Series: make([]RouteSeries, 0, len(pairs))}
	for _, pair := range pairs {
		key := pair[0] + "-" + pair[1]
		nodes := pathByPair[key]
		series := RouteSeries{
			FromMetroCode: pair[0],
			ToMetroCode:   pair[1],
			Points:        make([]RouteSeriesPoint, 0, 168),
		}
		for i := 167; i >= 0; i-- {
			ts := end.Add(-time.Duration(i) * time.Hour)
			pt := RouteSeriesPoint{TS: ts}
			if links := hourly[ts]; links != nil {
				// A bucket missing any hop is left at zero rather than reported
				// short — the frontend renders gaps as breaks in the line.
				if sum, ok := sumHopsAtBucket(nodes, links); ok {
					pt.DZMs = sum
				}
			}
			if m := inet[key]; m != nil {
				pt.InternetMs = m[ts]
			}
			series.Points = append(series.Points, pt)
		}
		resp.Series = append(resp.Series, series)
	}
	return resp, nil
}
