package handlers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// Flex-algo divergence: what the unicast topology costs against algo 0.
//
// Unicast forwarding follows a flex-algo whose link set is a subset of algo 0:
// a link joins it by carrying a topology tag and stays in it while it is not
// drained. Multicast follows algo 0, so it uses every activated link. Any link
// outside the unicast set is therefore a link multicast can use and unicast
// cannot, and the metro pairs whose best path crosses one of those links get a
// different latency depending on the traffic type.
//
// The link list is the cause and the pair list is the cost. Both are reported,
// because a newly-turned-up link that nobody tagged looks like nothing at all
// until you see the pairs it silently moved.

// ExcludedLink is an activated link that carries multicast but not unicast.
type ExcludedLink struct {
	Code        string  `json:"code"`
	FromMetro   string  `json:"fromMetro"`
	ToMetro     string  `json:"toMetro"`
	RttMs       float64 `json:"rttMs"`
	Drained     bool    `json:"drained"`
	EverTagged  bool    `json:"everTagged"`
	ExcludedAt  string  `json:"excludedAt"`
	ExcludedFor string  `json:"excludedFor"`
}

// AlgoDivergencePair is a metro pair whose unicast path is longer than its
// multicast path. Both figures are contracted (the IS-IS metric), so the
// difference is the routing decision alone and carries no measurement noise.
type AlgoDivergencePair struct {
	FromMetro     string   `json:"fromMetro"`
	ToMetro       string   `json:"toMetro"`
	MulticastMs   float64  `json:"multicastMs"`
	UnicastMs     float64  `json:"unicastMs"`
	DeltaMs       float64  `json:"deltaMs"`
	DeltaPct      float64  `json:"deltaPct"`
	MulticastPath []string `json:"multicastPath"`
	UnicastPath   []string `json:"unicastPath"`
	// UnicastReachable is false when the unicast topology has no path at all.
	// The other unicast fields are absent in that case, not zero.
	UnicastReachable bool `json:"unicastReachable"`
}

// AlgoDivergenceResponse reports every link outside the unicast topology and
// every metro pair those links move.
type AlgoDivergenceResponse struct {
	ExcludedLinks []ExcludedLink       `json:"excludedLinks"`
	Pairs         []AlgoDivergencePair `json:"pairs"`
	Summary       struct {
		ActivatedLinks   int     `json:"activatedLinks"`
		ExcludedLinks    int     `json:"excludedLinks"`
		MulticastPairs   int     `json:"multicastPairs"`
		DivergingPairs   int     `json:"divergingPairs"`
		UnreachablePairs int     `json:"unreachablePairs"`
		MaxDeltaMs       float64 `json:"maxDeltaMs"`
	} `json:"summary"`
}

// GetAlgoDivergence serves the unicast/multicast divergence report.
func (a *API) GetAlgoDivergence(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), "algo_divergence"); err == nil {
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
			return
		}
	}

	// Two full all-pairs path computations, so it gets a longer budget than
	// the single-graph endpoints.
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	resp, err := a.FetchAlgoDivergenceData(ctx)
	if err != nil {
		logError("algo divergence error", "error", err)
		http.Error(w, "failed to compute flex-algo divergence", http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

// FetchAlgoDivergenceData computes the report and publishes its headline
// figures as gauges. Used by both the handler and the page cache; the cache
// refresh is what makes an untagged link visible without anyone opening the
// page.
func (a *API) FetchAlgoDivergenceData(ctx context.Context) (*AlgoDivergenceResponse, error) {
	multicast, err := a.loadTopologyGraph(ctx, "multicast")
	if err != nil {
		return nil, fmt.Errorf("loading multicast graph: %w", err)
	}
	unicast, err := a.loadTopologyGraph(ctx, "unicast")
	if err != nil {
		return nil, fmt.Errorf("loading unicast graph: %w", err)
	}

	pairs, multicastPairs := divergingPairs(multicast, unicast)

	resp := &AlgoDivergenceResponse{
		ExcludedLinks: []ExcludedLink{},
		Pairs:         pairs,
	}
	resp.Summary.MulticastPairs = multicastPairs
	resp.Summary.DivergingPairs = len(pairs)
	for _, p := range pairs {
		if !p.UnicastReachable {
			resp.Summary.UnreachablePairs++
		}
		if p.DeltaMs > resp.Summary.MaxDeltaMs {
			resp.Summary.MaxDeltaMs = p.DeltaMs
		}
	}

	links, activated, err := a.excludedLinks(ctx)
	if err != nil {
		return nil, err
	}
	resp.ExcludedLinks = links
	resp.Summary.ActivatedLinks = activated
	resp.Summary.ExcludedLinks = len(links)

	metrics.FlexAlgoExcludedLinks.Set(float64(resp.Summary.ExcludedLinks))
	metrics.FlexAlgoDivergingPairs.Set(float64(resp.Summary.DivergingPairs))
	metrics.FlexAlgoUnreachablePairs.Set(float64(resp.Summary.UnreachablePairs))
	metrics.FlexAlgoMaxDeltaMs.Set(resp.Summary.MaxDeltaMs)

	return resp, nil
}

// divergingPairs compares the best path between every metro pair on the two
// link sets, and returns the pairs that differ plus the number of pairs
// multicast can reach at all.
func divergingPairs(multicast, unicast *kspGraph) ([]AlgoDivergencePair, int) {
	unicastPaths := make(map[string]metroPairPath)
	for _, mp := range computeMetroPairPaths(unicast) {
		unicastPaths[metroPairKey(mp.FromMetroCode, mp.ToMetroCode)] = mp
	}

	multicastPaths := computeMetroPairPaths(multicast)
	out := []AlgoDivergencePair{}

	for _, mc := range multicastPaths {
		pair := AlgoDivergencePair{
			FromMetro:     mc.FromMetroCode,
			ToMetro:       mc.ToMetroCode,
			MulticastMs:   metricToMs(mc.Path.TotalMetric),
			MulticastPath: pathMetroCodes(mc.Path.Nodes, multicast),
		}

		uc, ok := unicastPaths[metroPairKey(mc.FromMetroCode, mc.ToMetroCode)]
		if !ok {
			// Reachable by multicast, not by unicast. The worst shape this
			// report can find, so no delta test may filter it out.
			out = append(out, pair)
			continue
		}

		// The unicast link set is a subset of the multicast one, so its best
		// path can only be longer or identical. Equal means no divergence.
		if uc.Path.TotalMetric <= mc.Path.TotalMetric {
			continue
		}

		pair.UnicastReachable = true
		pair.UnicastMs = metricToMs(uc.Path.TotalMetric)
		pair.UnicastPath = pathMetroCodes(uc.Path.Nodes, unicast)
		pair.DeltaMs = round2(pair.UnicastMs - pair.MulticastMs)
		if pair.MulticastMs > 0 {
			pair.DeltaPct = round2(pair.DeltaMs / pair.MulticastMs * 100)
		}
		out = append(out, pair)
	}

	// Unreachable pairs first, then by how much latency the split costs.
	sort.SliceStable(out, func(i, j int) bool {
		x, y := out[i], out[j]
		if x.UnicastReachable != y.UnicastReachable {
			return !x.UnicastReachable
		}
		return x.DeltaMs > y.DeltaMs
	})

	return out, len(multicastPaths)
}

// excludedLinks returns the activated links the unicast topology leaves out,
// with the moment each one left it, plus the activated link total for context.
//
// excluded_since is the first snapshot of the link's current unbroken excluded
// run: for a link that was tagged and then untagged it is the untagging, and
// for one that was never tagged it falls out as the first snapshot of all,
// because last_included_ts is the zero time and every snapshot follows it.
func (a *API) excludedLinks(ctx context.Context) ([]ExcludedLink, int, error) {
	const query = `
		WITH included AS (
			SELECT
				entity_id,
				maxIf(snapshot_ts, link_topologies != '[]' AND link_topologies != '' AND unicast_drained = 0) AS last_included_ts
			FROM dim_dz_links_history
			WHERE is_deleted = 0
			GROUP BY entity_id
		),
		excluded_since AS (
			SELECT
				h.entity_id AS entity_id,
				min(h.snapshot_ts) AS since_ts,
				any(i.last_included_ts) AS last_included_ts
			FROM dim_dz_links_history h
			INNER JOIN included i ON i.entity_id = h.entity_id
			WHERE h.is_deleted = 0
			  AND (h.link_topologies = '[]' OR h.link_topologies = '' OR h.unicast_drained = 1)
			  AND h.snapshot_ts > i.last_included_ts
			GROUP BY h.entity_id
		)
		SELECT
			l.code AS code,
			COALESCE(ma.code, '') AS from_metro,
			COALESCE(mz.code, '') AS to_metro,
			l.committed_rtt_ns / 1000000.0 AS rtt_ms,
			l.unicast_drained AS drained,
			COALESCE(toUnixTimestamp(e.last_included_ts), 0) AS last_included_unix,
			COALESCE(toUnixTimestamp(e.since_ts), 0) AS since_unix
		FROM dz_links_current l
		JOIN dz_devices_current da ON da.pk = l.side_a_pk
		JOIN dz_devices_current dz ON dz.pk = l.side_z_pk
		LEFT JOIN dz_metros_current ma ON ma.pk = da.metro_pk
		LEFT JOIN dz_metros_current mz ON mz.pk = dz.metro_pk
		LEFT JOIN excluded_since e ON e.entity_id = l.entity_id
		WHERE l.status = 'activated'
		  AND (l.link_topologies = '[]' OR l.link_topologies = '' OR l.unicast_drained = 1)
		ORDER BY rtt_ms DESC
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("querying excluded links: %w", err)
	}
	defer rows.Close()

	out := []ExcludedLink{}
	now := time.Now().UTC()
	for rows.Next() {
		var (
			l           ExcludedLink
			drained     uint8
			lastInc     int64
			sinceUnix   int64
			fromM, toM  string
			codeScanned string
		)
		if err := rows.Scan(&codeScanned, &fromM, &toM, &l.RttMs, &drained, &lastInc, &sinceUnix); err != nil {
			return nil, 0, fmt.Errorf("scanning excluded link: %w", err)
		}
		l.Code = codeScanned
		l.FromMetro = fromM
		l.ToMetro = toM
		l.RttMs = round2(l.RttMs)
		l.Drained = drained == 1
		l.EverTagged = lastInc > 0
		if sinceUnix > 0 {
			since := time.Unix(sinceUnix, 0).UTC()
			l.ExcludedAt = since.Format(time.RFC3339)
			l.ExcludedFor = humanDuration(now.Sub(since))
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("reading excluded links: %w", err)
	}

	var activated uint64
	row := a.envDB(ctx).QueryRow(ctx, `SELECT count() FROM dz_links_current WHERE status = 'activated'`)
	if err := row.Scan(&activated); err != nil {
		return nil, 0, fmt.Errorf("counting activated links: %w", err)
	}

	return out, int(activated), nil
}

// metroPairKey orders the two codes so a pair has one key whichever way round
// the two graphs happened to produce it.
func metroPairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// metricToMs converts an IS-IS metric (microseconds) to milliseconds.
func metricToMs(metric uint32) float64 {
	return round2(float64(metric) / 1000.0)
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

// humanDuration renders an age the way an operator reads it: whole days once
// there is a day, whole hours below that.
func humanDuration(d time.Duration) string {
	if d < time.Hour {
		return "under an hour"
	}
	if d < 48*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}
