package rollup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	lakelogger "github.com/malbeclabs/lake/utils/pkg/logger"
)

// The Phoenix race: DoubleZero's top-of-book feed against Phoenix's own, recorded side by side
// on one host per site.
const (
	phoenixFeed      = "tob_edge_phoenix"
	phoenixMaxLeadMs = 5000
)

type phoenixSite struct {
	Code          string
	DZRecorder    string
	VenueRecorder string
}

var phoenixSites = []phoenixSite{
	{Code: "cmh", DZRecorder: "cmh/aws-cmh-mn-recorder1", VenueRecorder: "venue-aws-cmh-mn-recorder1"},
	{Code: "fra", DZRecorder: "fra/aws-fra-mn-recorder1", VenueRecorder: "venue-aws-fra-mn-recorder1"},
	{Code: "tyo", DZRecorder: "tyo/aws-tyo-mn-recorder1", VenueRecorder: "venue-aws-tyo-mn-recorder1"},
}

const phoenixMaxSites = 3

const (
	phoenixBucket   = 15 * time.Minute
	phoenixSettle   = 20 * time.Minute
	phoenixLookback = 24 * time.Hour
	phoenixHeal     = 3 * time.Hour
	phoenixCadence  = time.Hour

	phoenixRetryAfter = 10 * time.Minute
	phoenixErrorAfter = 30 * time.Minute
)

const phoenixScanTimeout = 60 * time.Second
const phoenixScanMaxExecutionSeconds = int(phoenixScanTimeout / time.Second)
const phoenixHeartbeatTimeout = 2 * time.Minute
const phoenixRollupActivityTimeout = phoenixMaxSites*phoenixScanTimeout + time.Minute

// PhoenixRaceInput asks for one hourly pass of the Phoenix race rollup.
type PhoenixRaceInput struct {
	Now time.Time
}

type phoenixFailures struct {
	mu    sync.Mutex
	sites map[string]phoenixFailure
	esc   *lakelogger.Escalator
	clock time.Time
}

const phoenixDueCheck = "due_check"

type phoenixFailure struct {
	at  time.Time
	err error
}

func (f *phoenixFailures) waiting(key string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	last, ok := f.sites[key]
	return ok && last.err != nil && now.Before(last.at.Add(phoenixRetryAfter))
}

func (f *phoenixFailures) observe(log lakelogger.Logger, key string, now time.Time, err error, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sites == nil {
		f.sites = map[string]phoenixFailure{}
	}
	f.sites[key] = phoenixFailure{at: now, err: err}
	if f.esc == nil {
		n := int(phoenixErrorAfter/phoenixRetryAfter) + 1
		f.esc = &lakelogger.Escalator{
			ErrorAfter:          n,
			TransientErrorAfter: n,
			ErrorAfterDuration:  phoenixErrorAfter,
			Now:                 func() time.Time { return f.clock },
		}
	}
	f.clock = now
	f.esc.Observe(log, "phoenix_race_rollup:"+key, "phoenix race rollup failed", err, args...)
}

func (a *Activities) phoenixSiteList() []phoenixSite {
	if a.phoenixSites != nil {
		return a.phoenixSites
	}
	return phoenixSites
}

// phoenixRaceWindow returns the windows a pass at now recomputes, as [start, end) aligned to
// phoenixBucket.
func phoenixRaceWindow(now, newest time.Time) (start, end time.Time, ok bool) {
	end = now.UTC().Add(-phoenixSettle).Truncate(phoenixBucket)
	start = end.Add(-phoenixLookback)
	if !newest.IsZero() {
		if heal := newest.UTC().Truncate(phoenixBucket).Add(-phoenixHeal); heal.After(start) {
			start = heal
		}
	}
	return start, end, start.Before(end)
}

// RollupPhoenixRace writes, for each site, the Phoenix race windows that closed since that site's
// last pass. Each site runs at most once per clock hour, and a failed site waits
// phoenixRetryAfter without holding the others back.
func (a *Activities) RollupPhoenixRace(ctx context.Context, input PhoenixRaceInput) (int, error) {
	if a.RecorderDatabase == "" {
		return 0, nil
	}
	sites := a.phoenixSiteList()

	if a.phoenixFailures.waiting(phoenixDueCheck, input.Now) {
		return 0, nil
	}
	stored, err := a.phoenixRaceStored(ctx)
	a.phoenixFailures.observe(a.Log, phoenixDueCheck, input.Now, err, "step", phoenixDueCheck)
	if err != nil {
		return 0, nil
	}

	hour := input.Now.UTC().Truncate(phoenixCadence)
	var written int
	for _, site := range sites {
		if a.phoenixFailures.waiting(site.Code, input.Now) {
			continue
		}
		last, ok := stored[site.Code]
		if ok && !last.lastWrite.Before(hour) {
			continue
		}
		start, end, ok := phoenixRaceWindow(input.Now, last.newest)
		if !ok {
			continue
		}

		var n int
		err := a.IngestionLog.Wrap(ctx, "rollup", "RollupPhoenixRace", a.Network, func() (ingestionlog.RefreshResult, error) {
			var result ingestionlog.RefreshResult
			var err error
			n, err = a.writePhoenixRaceWindows(ctx, site, start, end, input.Now)
			result.RowsAffected = int64(n)
			return result, err
		})
		a.phoenixFailures.observe(a.Log, site.Code, input.Now, err, "site", site.Code,
			"window", fmt.Sprintf("[%s, %s)", start.Format(time.RFC3339), end.Format(time.RFC3339)))
		if err == nil {
			written += n
		}
	}
	return written, nil
}

type phoenixStored struct {
	lastWrite, newest time.Time
}

// phoenixRaceStored returns, per site, the newest write and the start of the newest stored
// window. A site with nothing stored is absent.
func (a *Activities) phoenixRaceStored(ctx context.Context) (map[string]phoenixStored, error) {
	out := map[string]phoenixStored{}
	rows, err := a.ClickHouse.Query(ctx,
		`SELECT site, max(ingested_at), max(bucket_ts) FROM phoenix_race_rollup_15m GROUP BY site`)
	if err != nil {
		if isUnknownTable(err) {
			a.Log.Info("phoenix race rollup: table not present yet, treating as due")
			return out, nil
		}
		return nil, fmt.Errorf("phoenix race rollup due check: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var site string
		var s phoenixStored
		if err := rows.Scan(&site, &s.lastWrite, &s.newest); err != nil {
			return nil, fmt.Errorf("phoenix race rollup due check: %w", err)
		}
		out[site] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("phoenix race rollup due check: %w", err)
	}
	return out, nil
}

// writePhoenixRaceWindows recomputes every window in [start, end) for one site in one
// INSERT ... SELECT, so no race row crosses into the indexer.
func (a *Activities) writePhoenixRaceWindows(ctx context.Context, site phoenixSite, start, end, ingestedAt time.Time) (int, error) {
	n := int(end.Sub(start) / phoenixBucket)

	query := fmt.Sprintf(`
		INSERT INTO phoenix_race_rollup_15m (
			bucket_ts, site, ingested_at, paired, dz_wins, venue_wins,
			signed_lead_p50_ms, signed_lead_p95_ms, signed_lead_p99_ms, signed_lead_ms_state
		)
		WITH
			buckets AS (
				SELECT toDateTime(?, 'UTC') + toIntervalSecond(number * %[3]d) AS bucket_ts
				FROM numbers(?)
			),
			races AS (
				SELECT
					toDateTime(toStartOfInterval(first_recv_ts, INTERVAL 15 MINUTE), 'UTC') AS bucket_ts,
					count() AS paired,
					countIf(first_observation = ? AND lead_ms > 0) AS dz_wins,
					countIf(first_observation = ? AND lead_ms > 0) AS venue_wins,
					quantilesTDigestState(0.5, 0.95, 0.99)(if(first_observation = ?, 1, -1) * lead_ms) AS state
				FROM (
					SELECT
						argMin(observation, first_seen_ts) AS first_observation,
						min(first_seen_ts) AS first_recv_ts,
						(toUnixTimestamp64Nano(max(first_seen_ts)) - toUnixTimestamp64Nano(min(first_seen_ts))) / 1e6 AS lead_ms
					FROM (
						SELECT
							observation,
							symbol_key,
							book_key,
							min(recv_ts) AS first_seen_ts,
							argMin(price_exp, recv_ts) AS first_price_exp,
							argMin(qty_exp, recv_ts) AS first_qty_exp
						FROM (
							SELECT observation, upper(trimBoth(symbol)) AS symbol_key, book_key, recv_ts, price_exp, qty_exp
							FROM %[1]s
							WHERE feed = ? AND observation = ? AND recv_ts < fromUnixTimestamp64Milli(?, 'UTC')
							UNION ALL
							SELECT observation, upper(trimBoth(symbol)) AS symbol_key, book_key, recv_ts, price_exp, qty_exp
							FROM %[2]s
							WHERE feed = ? AND observation = ? AND from_anchor = 0 AND book_key != 0
							  AND recv_ts < fromUnixTimestamp64Milli(?, 'UTC')
						)
						GROUP BY observation, symbol_key, book_key
						HAVING first_seen_ts >= toDateTime(?, 'UTC')
					)
					GROUP BY symbol_key, book_key
					HAVING count() = 2
					   AND uniqExact(first_price_exp) = 1
					   AND uniqExact(first_qty_exp) = 1
					   AND lead_ms <= ?
				)
				WHERE first_recv_ts >= toDateTime(?, 'UTC')
				  AND first_recv_ts <  toDateTime(?, 'UTC')
				GROUP BY bucket_ts
			)
		SELECT
			b.bucket_ts,
			?,
			toDateTime64(?, 3, 'UTC'),
			r.paired,
			r.dz_wins,
			r.venue_wins,
			ifNotFinite(finalizeAggregation(r.state)[1], 0),
			ifNotFinite(finalizeAggregation(r.state)[2], 0),
			ifNotFinite(finalizeAggregation(r.state)[3], 0),
			r.state
		FROM buckets AS b
		LEFT JOIN races AS r ON r.bucket_ts = b.bucket_ts
		SETTINGS join_use_nulls = 0`,
		tableRef(a.RecorderDatabase, "venue_book_top"), tableRef(a.RecorderDatabase, "book_top"),
		int(phoenixBucket.Seconds()))

	queryCtx, cancel := context.WithTimeout(ctx, phoenixScanTimeout)
	defer cancel()
	queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": phoenixScanMaxExecutionSeconds,
	}))

	cutoff := end.UnixMilli() + phoenixMaxLeadMs
	began := time.Now()
	err := heartbeatDuring(ctx, "computing phoenix race rollup", func() error {
		return a.ClickHouse.Exec(queryCtx, query,
			start.Unix(), n,
			site.DZRecorder, site.VenueRecorder, site.DZRecorder,
			phoenixFeed, site.VenueRecorder, cutoff,
			phoenixFeed, site.DZRecorder, cutoff,
			start.Unix(),
			phoenixMaxLeadMs,
			start.Unix(), end.Unix(),
			site.Code, ingestedAt.UTC().Format("2006-01-02 15:04:05.000"),
		)
	})
	if err != nil {
		return 0, fmt.Errorf("phoenix race rollup %s [%s, %s): %w",
			site.Code, start.Format(time.RFC3339), end.Format(time.RFC3339), err)
	}

	a.Log.Info("wrote phoenix race rollup",
		"site", site.Code,
		"windows", n,
		"window", fmt.Sprintf("[%s, %s)", start.Format(time.RFC3339), end.Format(time.RFC3339)),
		"duration", time.Since(began))
	return n, nil
}
