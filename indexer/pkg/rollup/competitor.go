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

const competitorDayTimeout = 4 * time.Minute
const competitorDayMaxExecutionSeconds = 240
const competitorRollupActivityTimeout = competitorRollupHealDays*competitorDayTimeout + 2*time.Minute

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
			count()                                AS leader_slots,
			quantileTDigest(0.50)(win_typical)     AS win_typical_p50,
			quantileTDigest(0.50)(lead_typical_ms) AS lead_typical_ms
		FROM leader_slot
		SETTINGS final = 1
	`, src)

	queryCtx, cancel := context.WithTimeout(ctx, competitorDayTimeout)
	defer cancel()
	queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": competitorDayMaxExecutionSeconds,
	}))

	var d CompetitorDay
	err := a.ClickHouse.QueryRow(queryCtx, query, day, day.AddDate(0, 0, 1)).Scan(
		&d.LeaderSlots, &d.WinTypicalP50, &d.LeadTypicalMs,
	)
	if err != nil {
		return nil, fmt.Errorf("competitor rollup query for %s: %w", day.Format(time.DateOnly), err)
	}

	if d.LeaderSlots == 0 {
		a.Log.Info("competitor rollup: no leader slots for day",
			"day", day.Format(time.DateOnly), "duration", time.Since(start))
		return nil, nil
	}

	d.BucketDate = day
	d.IngestedAt = time.Now().UTC()

	a.Log.Info("computed competitor rollup",
		"day", day.Format(time.DateOnly),
		"leader_slots", d.LeaderSlots,
		"win_typical_p50", d.WinTypicalP50,
		"duration", time.Since(start))

	return &d, nil
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
