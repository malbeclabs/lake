package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// The feed-race recorder's own race: the venue's upstream against the multicast the publishers
// put on it, per site that records both. The rest of this page races DoubleZero against a
// competing feed; this races the venue against its own republication, which that comparison
// cannot express.
//
// **Fifteen minutes, not the window the page selects.** kalshi_book_race is a plain view over
// kalshi_book_top_occurrence, so it re-derives the pairing from kalshi_edge_book_top and
// kalshi_venue_book_top on every call, where the legacy summary is a table fed by a
// materialised view. Measured against mainnet: ~40s for fifteen minutes with quantileTDigest,
// and an hour does not return inside a 60s deadline. Serving the page's windows needs a
// materialised summary for this race, which is a cluster write with its own budget.
//
// Each side is matched on its own prefix — `venue-` and `edge-`, the recorder's inventory
// names — and never as the complement of the other. A row matching neither counts as neither
// and stays in Pairs, so a convention that moves reads as pairs going missing rather than as
// the wire silently winning.

const kalshiRecorderRaceCacheKey = "kalshi_recorder_race:v1"

// kalshiRecorderRaceWindowMinutes is what the view can be aggregated over inside a refresher
// cycle. See the file comment: this is a measured ceiling, not a preference.
const kalshiRecorderRaceWindowMinutes = 15

// KalshiRecorderRaceSite is one recording site's race over the window.
type KalshiRecorderRaceSite struct {
	Site string `json:"site"`

	// Pairs is book states both sides saw. Zero means no race at this site, however much
	// either side recorded on its own.
	Pairs   uint64 `json:"pairs"`
	Symbols uint64 `json:"symbols"`

	// VenueWins and WireWins are each matched on their own prefix, so they sum to Pairs less
	// any observation matching neither. That shortfall is the signal that the naming
	// convention moved; it is not an error and nothing rounds it away.
	VenueWins uint64 `json:"venue_wins"`
	WireWins  uint64 `json:"wire_wins"`

	// The lead in each direction, over the races that direction won. Reported separately
	// rather than signed, because they are two different distributions: at a site where the
	// margin is smaller than the jitter both sides win regularly and a single signed median
	// would average them into a number neither side ever saw.
	VenueP50Ms float64 `json:"venue_p50_ms"`
	VenueP95Ms float64 `json:"venue_p95_ms"`
	WireP50Ms  float64 `json:"wire_p50_ms"`
	WireP95Ms  float64 `json:"wire_p95_ms"`
}

// KalshiRecorderRace is the cached payload the scoreboard carries.
type KalshiRecorderRace struct {
	GeneratedAt   time.Time                `json:"generated_at"`
	WindowMinutes int                      `json:"window_minutes"`
	Sites         []KalshiRecorderRaceSite `json:"sites"`
}

// FetchKalshiRecorderRace aggregates the race view for the window.
func (a *API) FetchKalshiRecorderRace(ctx context.Context) (*KalshiRecorderRace, error) {
	out := &KalshiRecorderRace{
		GeneratedAt:   time.Now().UTC(),
		WindowMinutes: kalshiRecorderRaceWindowMinutes,
		Sites:         []KalshiRecorderRaceSite{},
	}

	exists, err := a.kalshiTableExists(ctx, "kalshi_book_race")
	if err != nil {
		return nil, err
	}
	if !exists {
		return out, nil
	}

	// **Both filters, and neither is optional.** `observations = 2` is what separates a race
	// from a lone sighting: the view emits a group per edge observation whether or not a venue
	// row ever matched it. `occurrence = 1` keeps out repeats of a book state, whose Nth
	// occurrences on the two sides are not the same moment. Measured on this data, dropping
	// either takes the venue's win share from 99% to 69% and the p95 lead from milliseconds
	// into seconds.
	q := fmt.Sprintf(`
		SELECT
			site,
			count() AS pairs,
			uniqExact(symbol) AS symbols,
			countIf(first_observation LIKE 'venue%%') AS venue_wins,
			countIf(first_observation LIKE 'edge-%%') AS wire_wins,
			-- TDigest and not Exact: the exact quantiles do not return inside the deadline on
			-- this view, and the market-by-price coverage query already made the same trade.
			ifNotFinite(toFloat64(quantileTDigestIf(0.50)(lead_ms, first_observation LIKE 'venue%%')), 0) AS venue_p50,
			ifNotFinite(toFloat64(quantileTDigestIf(0.95)(lead_ms, first_observation LIKE 'venue%%')), 0) AS venue_p95,
			ifNotFinite(toFloat64(quantileTDigestIf(0.50)(lead_ms, first_observation LIKE 'edge-%%')), 0) AS wire_p50,
			ifNotFinite(toFloat64(quantileTDigestIf(0.95)(lead_ms, first_observation LIKE 'edge-%%')), 0) AS wire_p95
		FROM %[1]s.kalshi_book_race
		WHERE observations = 2
			AND occurrence = 1
			AND bucket >= now() - toIntervalMinute(%[2]d)
		GROUP BY site
		ORDER BY site
		SETTINGS max_execution_time = 150, timeout_before_checking_execution_speed = 0`,
		"`"+a.FeedsDB+"`", kalshiRecorderRaceWindowMinutes)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("kalshi_recorder_race", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s KalshiRecorderRaceSite
		if err := rows.Scan(&s.Site, &s.Pairs, &s.Symbols, &s.VenueWins, &s.WireWins,
			&s.VenueP50Ms, &s.VenueP95Ms, &s.WireP50Ms, &s.WireP95Ms); err != nil {
			return nil, err
		}
		out.Sites = append(out.Sites, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
