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
// on one Columbus host.
const (
	phoenixFeed          = "tob_edge_phoenix"
	phoenixDZRecorder    = "cmh/aws-cmh-mn-recorder1"
	phoenixVenueRecorder = "venue-aws-cmh-mn-recorder1"
	phoenixMaxLeadMs     = 5000
)

const (
	phoenixBucket   = 15 * time.Minute
	phoenixSettle   = 20 * time.Minute
	phoenixLookback = 24 * time.Hour
	phoenixHeal     = 3 * time.Hour
	phoenixCadence  = time.Hour

	phoenixRetryAfter = 10 * time.Minute
	phoenixErrorAfter = 30 * time.Minute
)

const phoenixScanTimeout = 90 * time.Second
const phoenixScanMaxExecutionSeconds = int(phoenixScanTimeout / time.Second)
const phoenixHeartbeatTimeout = 2 * time.Minute
const phoenixRollupActivityTimeout = phoenixScanTimeout + time.Minute

// PhoenixRaceInput asks for one hourly pass of the Phoenix race rollup.
type PhoenixRaceInput struct {
	Now time.Time
}

type phoenixFailure struct {
	mu  sync.Mutex
	at  time.Time
	err error
	esc *lakelogger.Escalator
}

func (f *phoenixFailure) waiting(now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err != nil && now.Before(f.at.Add(phoenixRetryAfter))
}

func (f *phoenixFailure) observe(log lakelogger.Logger, now time.Time, err error, args ...any) {
	f.mu.Lock()
	f.at, f.err = now, err
	if f.esc == nil {
		f.esc = &lakelogger.Escalator{ErrorAfterDuration: phoenixErrorAfter}
	}
	esc := f.esc
	f.mu.Unlock()
	esc.Observe(log, "phoenix_race_rollup", "phoenix race rollup failed", err, args...)
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

// RollupPhoenixRace writes the Phoenix race windows that closed since the last pass. It runs at
// most once per clock hour.
func (a *Activities) RollupPhoenixRace(ctx context.Context, input PhoenixRaceInput) (int, error) {
	if a.RecorderDatabase == "" {
		return 0, nil
	}
	if a.phoenixFailure.waiting(input.Now) {
		return 0, nil
	}

	due, newest, err := a.phoenixRaceDue(ctx, input.Now)
	if err != nil {
		a.phoenixFailure.observe(a.Log, input.Now, err)
		return 0, nil
	}
	if !due {
		return 0, nil
	}

	start, end, ok := phoenixRaceWindow(input.Now, newest)
	if !ok {
		return 0, nil
	}

	var written int
	err = a.IngestionLog.Wrap(ctx, "rollup", "RollupPhoenixRace", a.Network, func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		n, err := a.writePhoenixRaceWindows(ctx, a.RecorderDatabase, start, end, input.Now)
		if err != nil {
			return result, err
		}
		written = n
		result.RowsAffected = int64(n)
		return result, nil
	})
	a.phoenixFailure.observe(a.Log, input.Now, err,
		"window", fmt.Sprintf("[%s, %s)", start.Format(time.RFC3339), end.Format(time.RFC3339)))
	if err != nil {
		return 0, nil
	}
	return written, nil
}

// phoenixRaceDue reports whether this clock hour has not been written yet, and the start of the
// newest stored window.
func (a *Activities) phoenixRaceDue(ctx context.Context, now time.Time) (bool, time.Time, error) {
	var lastWrite, newest time.Time
	var rows uint64
	err := a.ClickHouse.QueryRow(ctx,
		`SELECT count(), max(ingested_at), max(bucket_ts) FROM phoenix_race_rollup_15m`,
	).Scan(&rows, &lastWrite, &newest)
	if err != nil {
		if isUnknownTable(err) {
			a.Log.Info("phoenix race rollup: table not present yet, treating as due")
			return true, time.Time{}, nil
		}
		return false, time.Time{}, fmt.Errorf("phoenix race rollup due check: %w", err)
	}
	if rows == 0 {
		return true, time.Time{}, nil
	}
	return lastWrite.Before(now.UTC().Truncate(phoenixCadence)), newest, nil
}

// writePhoenixRaceWindows recomputes every window in [start, end) in one INSERT ... SELECT, so
// no race row crosses into the indexer.
func (a *Activities) writePhoenixRaceWindows(ctx context.Context, recorderDB string, start, end, ingestedAt time.Time) (int, error) {
	n := int(end.Sub(start) / phoenixBucket)
	src := tableRef(recorderDB, "feed_race")

	query := fmt.Sprintf(`
		INSERT INTO phoenix_race_rollup_15m (
			bucket_ts, ingested_at, paired, dz_wins, venue_wins,
			signed_lead_p50_ms, signed_lead_p95_ms, signed_lead_p99_ms, signed_lead_ms_state
		)
		WITH
			buckets AS (
				SELECT toDateTime(?, 'UTC') + toIntervalSecond(number * %[2]d) AS bucket_ts
				FROM numbers(?)
			),
			races AS (
				SELECT
					toDateTime(toStartOfInterval(first_recv_ts, INTERVAL 15 MINUTE), 'UTC') AS bucket_ts,
					count() AS paired,
					countIf(first_observation = ? AND lead_ms > 0) AS dz_wins,
					countIf(first_observation = ? AND lead_ms > 0) AS venue_wins,
					quantilesTDigestState(0.5, 0.95, 0.99)(
						assumeNotNull(if(first_observation = ?, 1, -1) * toFloat64(lead_ms))
					) AS state
				FROM %[1]s
				WHERE feed = ?
				  AND hasAll(observed_by, [?, ?])
				  AND occurrence = 1
				  AND observations = 2
				  AND exponents_agree = 1
				  AND lead_ms <= ?
				  AND first_recv_ts >= toDateTime(?, 'UTC')
				  AND first_recv_ts <  toDateTime(?, 'UTC')
				GROUP BY bucket_ts
			)
		SELECT
			b.bucket_ts,
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
		src, int(phoenixBucket.Seconds()))

	queryCtx, cancel := context.WithTimeout(ctx, phoenixScanTimeout)
	defer cancel()
	queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": phoenixScanMaxExecutionSeconds,
	}))

	began := time.Now()
	err := heartbeatDuring(ctx, "computing phoenix race rollup", func() error {
		return a.ClickHouse.Exec(queryCtx, query,
			start.Unix(), n,
			phoenixDZRecorder, phoenixVenueRecorder, phoenixDZRecorder,
			phoenixFeed, phoenixDZRecorder, phoenixVenueRecorder, phoenixMaxLeadMs, start.Unix(), end.Unix(),
			ingestedAt.UTC().Format("2006-01-02 15:04:05.000"),
		)
	})
	if err != nil {
		return 0, fmt.Errorf("phoenix race rollup [%s, %s): %w",
			start.Format(time.RFC3339), end.Format(time.RFC3339), err)
	}

	a.Log.Info("wrote phoenix race rollup",
		"windows", n,
		"window", fmt.Sprintf("[%s, %s)", start.Format(time.RFC3339), end.Format(time.RFC3339)),
		"duration", time.Since(began))
	return n, nil
}
