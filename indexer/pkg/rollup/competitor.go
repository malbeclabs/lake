package rollup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
)

// The per-day budget is far above the measured cost — a day reduces in well under
// a second through the remote proxy — but it is what two other limits are derived
// from, and both matter more than the scan does. competitorHeartbeatTimeout must
// exceed it, or Temporal kills a slow day mid-scan and the day never lands. And a
// whole pass must finish well inside rollupWindow, because the pass runs inline in
// the live rollup loop: while it holds the loop, link and device-interface buckets
// are not being processed, and a stall longer than the window loses them for good.
const competitorDayTimeout = 60 * time.Second
const competitorDayMaxExecutionSeconds = 60
const competitorHeartbeatInterval = 20 * time.Second
const competitorHeartbeatTimeout = 2 * time.Minute
const competitorRollupActivityTimeout = competitorRollupHealDays*competitorDayTimeout + time.Minute

// ComputeCompetitorDay reduces one closed UTC day of
// competitors_pairwise_feed_race into a single CompetitorDay.
func (a *Activities) ComputeCompetitorDay(ctx context.Context, input CompetitorDayInput) (*CompetitorDay, error) {
	if input.CompetitorDatabase == "" {
		return nil, nil
	}

	safeHeartbeat(ctx, "computing competitor rollup")
	start := time.Now()

	day := input.Day.UTC().Truncate(24 * time.Hour)
	src := tableRef(input.CompetitorDatabase, "competitors_pairwise_feed_race")

	// The toFloat64 casts are load-bearing, not decoration. win_rate and
	// diff_ms_p50 are Float32 in the source, and both quantile functions return
	// their input type, so the result columns are Float32 — which clickhouse-go
	// refuses to scan into a float64 field ("converting Float32 to float64 is
	// unsupported"). Casting here keeps the contract at the query boundary, where
	// the rollup table and the API response are both Float64, rather than letting
	// the source's column width dictate three layers of Go types.
	query := fmt.Sprintf(`
		WITH leader_slot AS (
			SELECT
				slot,
				quantileExact(0.5)(win_rate)     AS win_typical,
				quantileExact(0.5)(-diff_ms_p50) AS lead_typical_ms
			FROM %s
			WHERE dz_feed = 'dz'
			  AND event_ts >= ?
			  AND event_ts <  ?
			GROUP BY slot
		)
		SELECT
			count()                                           AS leader_slots,
			toFloat64(quantileTDigest(0.50)(win_typical))     AS win_typical_p50,
			toFloat64(quantileTDigest(0.50)(lead_typical_ms)) AS lead_typical_ms
		FROM leader_slot
		SETTINGS final = 1
	`, src)

	queryCtx, cancel := context.WithTimeout(ctx, competitorDayTimeout)
	defer cancel()
	queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": competitorDayMaxExecutionSeconds,
	}))

	var d CompetitorDay
	err := heartbeatDuring(ctx, "computing competitor rollup", func() error {
		return a.ClickHouse.QueryRow(queryCtx, query, day, day.AddDate(0, 0, 1)).Scan(
			&d.LeaderSlots, &d.WinTypicalP50, &d.LeadTypicalMs,
		)
	})
	if err != nil {
		return nil, fmt.Errorf("competitor rollup query for %s: %w", day.Format(time.DateOnly), err)
	}

	d.BucketDate = day
	d.IngestedAt = time.Now().UTC()

	// A day with no leader slots is recorded, not skipped. The due gate keys on
	// max(bucket_date), so a legitimately empty newest day would otherwise hold the
	// gate open and re-run the whole seven-day pass every 30s indefinitely. The
	// handler filters these rows out so an empty day never renders as a real point,
	// and the heal window replaces the row if the source fills in later.
	if d.LeaderSlots == 0 {
		a.Log.Info("competitor rollup: no leader slots for day",
			"day", day.Format(time.DateOnly), "duration", time.Since(start))
		return &d, nil
	}

	a.Log.Info("computed competitor rollup",
		"day", day.Format(time.DateOnly),
		"leader_slots", d.LeaderSlots,
		"win_typical_p50", d.WinTypicalP50,
		"duration", time.Since(start))

	return &d, nil
}

// heartbeatDuring keeps the activity's heartbeat alive while fn runs. The scan is
// one blocking call, so the heartbeat recorded before it is the only sign of life
// Temporal gets; a day that runs longer than HeartbeatTimeout would be killed
// mid-query and never land. The interval is well under competitorHeartbeatTimeout.
func heartbeatDuring(ctx context.Context, detail string, fn func() error) error {
	done := make(chan struct{})
	defer close(done)

	go func() {
		t := time.NewTicker(competitorHeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				safeHeartbeat(ctx, detail)
			}
		}
	}()

	return fn()
}

// WriteCompetitorDay upserts one day of the competitor rollup.
func (a *Activities) WriteCompetitorDay(ctx context.Context, d *CompetitorDay) error {
	if d == nil {
		return nil
	}
	safeHeartbeat(ctx, "writing competitor rollup")

	batch, err := a.ClickHouse.PrepareBatch(ctx, `INSERT INTO shred_competitor_rollup_1d (
		bucket_date, ingested_at,
		leader_slots,
		win_typical_p50, lead_typical_ms
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	if err := batch.Append(
		d.BucketDate, d.IngestedAt,
		d.LeaderSlots,
		d.WinTypicalP50, d.LeadTypicalMs,
	); err != nil {
		return fmt.Errorf("append batch: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	a.Log.Info("wrote competitor rollup", "day", d.BucketDate.Format(time.DateOnly))
	return nil
}

// RollupCompetitors computes and writes the competitor rollup for the day that
// closed most recently, plus a short trailing window behind it.
func (a *Activities) RollupCompetitors(ctx context.Context, input CompetitorDayInput) (int, error) {
	if input.CompetitorDatabase == "" {
		input.CompetitorDatabase = a.CompetitorDatabase
	}
	if input.CompetitorDatabase == "" {
		a.Log.Info("competitor rollup: no source database configured, skipping")
		return 0, nil
	}

	due, newest, err := a.competitorRollupDue(ctx, input.Day)
	if err != nil {
		return 0, err
	}
	if !due {
		return 0, nil
	}

	var written int
	err = a.IngestionLog.Wrap(ctx, "rollup", "RollupCompetitors", a.Network, func() (ingestionlog.RefreshResult, error) {
		var result ingestionlog.RefreshResult
		a.Log.Info("competitor rollup due", "newest_stored_day", newest)

		for _, day := range competitorRollupDays(input.Day) {
			d, err := a.ComputeCompetitorDay(ctx, CompetitorDayInput{
				Day:                day,
				CompetitorDatabase: input.CompetitorDatabase,
			})
			if err != nil {
				return result, err
			}
			if d == nil {
				continue
			}
			if err := a.WriteCompetitorDay(ctx, d); err != nil {
				return result, err
			}
			written++
		}

		result.RowsAffected = int64(written)
		return result, nil
	})
	return written, err
}

// isUnknownTable reports a ClickHouse UNKNOWN_TABLE (60).
func isUnknownTable(err error) bool {
	var chErr *proto.Exception
	return errors.As(err, &chErr) && chErr.Code == 60
}

// competitorRollupDue reports whether a pass should run, and the newest day already
// stored.
func (a *Activities) competitorRollupDue(ctx context.Context, now time.Time) (bool, string, error) {
	days := competitorRollupDays(now)
	target := days[len(days)-1]

	var newest time.Time
	err := a.ClickHouse.QueryRow(ctx,
		`SELECT max(bucket_date) FROM shred_competitor_rollup_1d`).Scan(&newest)
	if err != nil {
		if isUnknownTable(err) {
			a.Log.Info("competitor rollup: table not present yet, treating as due")
			return true, "", nil
		}
		return false, "", fmt.Errorf("competitor rollup due check: %w", err)
	}
	return newest.Before(target), newest.Format(time.DateOnly), nil
}

const competitorRollupHealDays = 7

// competitorRollupDays returns the closed UTC days a run should compute, oldest
// first, for a run happening at now.
func competitorRollupDays(now time.Time) []time.Time {
	today := now.UTC().Truncate(24 * time.Hour)
	days := make([]time.Time, 0, competitorRollupHealDays)
	for i := competitorRollupHealDays; i >= 1; i-- {
		days = append(days, today.AddDate(0, 0, -i))
	}
	return days
}
