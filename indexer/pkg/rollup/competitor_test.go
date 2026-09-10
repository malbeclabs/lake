package rollup

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The panel's whole contract is that a point describes a closed UTC day, so a run
// must never produce today. A partial day written under today's bucket_date would
// render as a real point and then silently change as the day filled.
func TestCompetitorRollupDays_NeverIncludesToday(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		now  time.Time
	}{
		{"midday", time.Date(2026, 9, 3, 17, 48, 0, 0, time.UTC)},
		{"one second past midnight", time.Date(2026, 9, 3, 0, 0, 1, 0, time.UTC)},
		{"one second before midnight", time.Date(2026, 9, 3, 23, 59, 59, 0, time.UTC)},
		// A non-UTC clock must not shift the bucket: 2026-09-03T20:00-07:00 is
		// already 2026-09-04 in UTC, so "yesterday" is the 3rd, not the 2nd.
		{"west of Greenwich", time.Date(2026, 9, 3, 20, 0, 0, 0, time.FixedZone("PDT", -7*3600))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			today := tc.now.UTC().Truncate(24 * time.Hour)
			days := competitorRollupDays(tc.now)

			if len(days) != competitorRollupHealDays {
				t.Fatalf("got %d days, want %d", len(days), competitorRollupHealDays)
			}
			for _, d := range days {
				if !d.Before(today) {
					t.Errorf("day %s is not before today %s",
						d.Format(time.DateOnly), today.Format(time.DateOnly))
				}
				if !d.Equal(d.Truncate(24 * time.Hour)) {
					t.Errorf("day %s is not midnight-aligned", d)
				}
			}
			// Oldest first, and the newest is yesterday.
			for i := 1; i < len(days); i++ {
				if !days[i-1].Before(days[i]) {
					t.Errorf("days not ascending: %s then %s", days[i-1], days[i])
				}
			}
			if want := today.AddDate(0, 0, -1); !days[len(days)-1].Equal(want) {
				t.Errorf("newest day = %s, want yesterday %s",
					days[len(days)-1].Format(time.DateOnly), want.Format(time.DateOnly))
			}
		})
	}
}

// The heal window is what makes a late source row correct itself rather than
// leaving a permanently wrong point, so it has to cover more than just yesterday.
func TestCompetitorRollupDays_CoversAHealWindow(t *testing.T) {
	t.Parallel()

	if competitorRollupHealDays < 2 {
		t.Fatalf("competitorRollupHealDays = %d; a window of one heals nothing",
			competitorRollupHealDays)
	}

	now := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	days := competitorRollupDays(now)
	oldest := days[0]
	if want := now.Truncate(24*time.Hour).AddDate(0, 0, -competitorRollupHealDays); !oldest.Equal(want) {
		t.Errorf("oldest day = %s, want %s", oldest.Format(time.DateOnly), want.Format(time.DateOnly))
	}
}

// A privilege error on the rollup table the indexer writes itself is a
// misconfiguration to surface, not a dependency race to wait out — which is the
// deliberate difference from the API's isMissingTable, and the reason this helper
// exists separately.
func TestIsUnknownTable_DistinguishesPrivilegeErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unknown table", &proto.Exception{Code: 60}, true},
		{"wrapped unknown table", fmt.Errorf("due check: %w", &proto.Exception{Code: 60}), true},
		{"not enough privileges", &proto.Exception{Code: 497}, false},
		{"other clickhouse error", &proto.Exception{Code: 241}, false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isUnknownTable(tc.err); got != tc.want {
				t.Errorf("isUnknownTable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// One pass must fit inside the Temporal budget the workflow gives it, or a day
// that scans slowly is killed mid-pass every time and the rollup never advances.
func TestCompetitorRollupActivityTimeout_CoversAWholePass(t *testing.T) {
	t.Parallel()

	scans := competitorRollupHealDays * competitorDayTimeout
	if competitorRollupActivityTimeout <= scans {
		t.Errorf("activity timeout %s does not cover %d scans of %s (%s)",
			competitorRollupActivityTimeout, competitorRollupHealDays, competitorDayTimeout, scans)
	}

	// The server-side limit has to match the context deadline, or one of the two
	// silently governs and the other is decoration.
	if want := int(competitorDayTimeout.Seconds()); competitorDayMaxExecutionSeconds != want {
		t.Errorf("competitorDayMaxExecutionSeconds = %d, want %d to match competitorDayTimeout",
			competitorDayMaxExecutionSeconds, want)
	}
}

// The two budgets either side of a day's scan have to bracket it: the heartbeat
// above, or Temporal kills a slow day mid-query; the live loop's window below, or
// a slow pass stalls the loop it runs inline in for longer than link and
// device-interface buckets survive.
func TestCompetitorTimeouts_BracketTheDayScan(t *testing.T) {
	t.Parallel()

	if competitorHeartbeatTimeout <= competitorDayTimeout {
		t.Errorf("heartbeat timeout %s does not exceed the per-day budget %s: a day that "+
			"uses its budget is killed as a heartbeat timeout",
			competitorHeartbeatTimeout, competitorDayTimeout)
	}
	if competitorHeartbeatInterval >= competitorHeartbeatTimeout {
		t.Errorf("heartbeat interval %s does not fit inside the timeout %s",
			competitorHeartbeatInterval, competitorHeartbeatTimeout)
	}
	if competitorRollupActivityTimeout >= rollupWindow {
		t.Errorf("a whole pass may take %s, which is not inside rollupWindow %s: the pass "+
			"runs inline in the live loop and would lose buckets while it holds it",
			competitorRollupActivityTimeout, rollupWindow)
	}
}

// setupCompetitorSource creates a source table shaped like the real
// dzf_data.competitors_pairwise_feed_race and returns a connection plus the
// database holding it. Two properties of the real schema are reproduced on
// purpose, because a convenient stand-in hides the faults that matter: the metric
// columns are Float32, which is what the scan trips over, and the engine is
// ReplacingMergeTree, without which FINAL is a no-op and unverified.
func setupCompetitorSource(t *testing.T) (clickhouse.Conn, string) {
	t.Helper()
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn := openRawConn(t, sharedDB, info.Database)
	require.NoError(t, conn.Exec(t.Context(), `
		CREATE TABLE competitors_pairwise_feed_race (
			event_ts    DateTime64(3),
			slot        UInt64,
			dz_feed     String,
			competitor  String,
			win_rate    Float32,
			diff_ms_p50 Float32,
			ingested_at DateTime64(3)
		) ENGINE = ReplacingMergeTree(ingested_at)
		ORDER BY (event_ts, slot, dz_feed, competitor)`))
	return conn, info.Database
}

// The bug this pins shipped: quantileExact and quantileTDigest both return their
// input type, so over Float32 source columns the result columns are Float32, and
// clickhouse-go refuses to scan those into the float64 fields of CompetitorDay.
// Every day errored, the pass never got past its first day, and the rollup table
// stayed empty with nothing above INFO in the log.
func TestComputeCompetitorDay_ScansAFloat32Source(t *testing.T) {
	t.Parallel()
	conn, db := setupCompetitorSource(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	day := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

	require.NoError(t, conn.Exec(t.Context(), `
		INSERT INTO competitors_pairwise_feed_race
			(event_ts, slot, dz_feed, competitor, win_rate, diff_ms_p50, ingested_at) VALUES
			-- slot 100: three competitors, median win 0.6, median lead 0.3
			('2026-09-02 01:00:00', 100, 'dz', 'a', 0.4, -0.2, '2026-09-02 01:00:01'),
			('2026-09-02 01:00:00', 100, 'dz', 'b', 0.6, -0.3, '2026-09-02 01:00:01'),
			('2026-09-02 01:00:00', 100, 'dz', 'c', 0.8, -0.4, '2026-09-02 01:00:01'),
			-- slot 200: same shape, so the outer median is unambiguous
			('2026-09-02 02:00:00', 200, 'dz', 'a', 0.4, -0.2, '2026-09-02 02:00:01'),
			('2026-09-02 02:00:00', 200, 'dz', 'b', 0.6, -0.3, '2026-09-02 02:00:01'),
			('2026-09-02 02:00:00', 200, 'dz', 'c', 0.8, -0.4, '2026-09-02 02:00:01'),
			-- excluded: not DZ's leader slot
			('2026-09-02 03:00:00', 300, 'other', 'a', 0.9, -9.0, '2026-09-02 03:00:01'),
			-- excluded: the next day, which the window must not reach
			('2026-09-03 01:00:00', 400, 'dz', 'a', 0.1, -0.1, '2026-09-03 01:00:01')`))

	d, err := a.ComputeCompetitorDay(t.Context(), CompetitorDayInput{Day: day, CompetitorDatabase: db})
	require.NoError(t, err)
	require.NotNil(t, d)

	assert.EqualValues(t, 2, d.LeaderSlots, "only DZ leader slots inside the day")
	assert.InDelta(t, 0.6, d.WinTypicalP50, 1e-6)
	assert.InDelta(t, 0.3, d.LeadTypicalMs, 1e-6, "lead is the negated diff")
	assert.Equal(t, day, d.BucketDate)
}

// A day the source has nothing for still has to produce a row. The due gate keys
// on max(bucket_date), so returning nil here leaves the newest day permanently
// unwritten and the gate permanently open — seven remote scans every 30s forever.
func TestComputeCompetitorDay_RecordsADayWithNoRows(t *testing.T) {
	t.Parallel()
	conn, db := setupCompetitorSource(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	day := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

	d, err := a.ComputeCompetitorDay(t.Context(), CompetitorDayInput{Day: day, CompetitorDatabase: db})
	require.NoError(t, err)
	require.NotNil(t, d, "an empty day must still be recorded, or the due gate never closes")
	assert.EqualValues(t, 0, d.LeaderSlots)
	assert.Equal(t, day, d.BucketDate)
}

// The other half of the same fault, one level up: with the newest day written as
// an empty row, the gate must close.
func TestCompetitorRollupDue_ClosesWhenTheNewestDayIsEmpty(t *testing.T) {
	t.Parallel()
	conn, _ := setupCompetitorSource(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	due, _, err := a.competitorRollupDue(t.Context(), now)
	require.NoError(t, err)
	require.True(t, due, "nothing stored yet")

	days := competitorRollupDays(now)
	for i, day := range days {
		d := &CompetitorDay{BucketDate: day, IngestedAt: time.Now().UTC()}
		if i < len(days)-1 {
			d.LeaderSlots, d.WinTypicalP50, d.LeadTypicalMs = 1000, 0.65, 0.2
		}
		require.NoError(t, a.WriteCompetitorDay(t.Context(), d))
	}

	due, newest, err := a.competitorRollupDue(t.Context(), now)
	require.NoError(t, err)
	assert.False(t, due, "newest stored day is %s, the target day", newest)
}

// The source is a ReplacingMergeTree fed by a 5-minute append, so a re-observed
// (slot, competitor) pair exists twice until a merge collapses it. Both copies
// landing in the quantile is the fault SETTINGS final = 1 prevents, and until this
// test the suite could not have caught its removal: the fixture was a plain
// MergeTree, where FINAL is a no-op.
func TestComputeCompetitorDay_CollapsesReObservations(t *testing.T) {
	t.Parallel()
	conn, db := setupCompetitorSource(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	day := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

	// The two copies have to land in different parts, and merges have to stay off.
	// A ReplacingMergeTree collapses duplicates within a single inserted block, so
	// writing both copies in one statement dedups them at insert time and the test
	// passes whether or not the query asks for FINAL — which is exactly how the
	// first version of this test silently had no teeth.
	require.NoError(t, conn.Exec(t.Context(), `SYSTEM STOP MERGES competitors_pairwise_feed_race`))

	// Competitor c is observed at 0.9 and corrected to 0.8. Deduped the slot's
	// values are {0.4, 0.6, 0.8} and the median is 0.6; with the stale copy still
	// present they are {0.4, 0.6, 0.8, 0.9} and it is not.
	require.NoError(t, conn.Exec(t.Context(), `
		INSERT INTO competitors_pairwise_feed_race
			(event_ts, slot, dz_feed, competitor, win_rate, diff_ms_p50, ingested_at) VALUES
			('2026-09-02 01:00:00', 100, 'dz', 'a', 0.4, -0.3, '2026-09-02 01:00:01'),
			('2026-09-02 01:00:00', 100, 'dz', 'b', 0.6, -0.3, '2026-09-02 01:00:01'),
			('2026-09-02 01:00:00', 100, 'dz', 'c', 0.9, -0.3, '2026-09-02 01:00:01')`))
	require.NoError(t, conn.Exec(t.Context(), `
		INSERT INTO competitors_pairwise_feed_race
			(event_ts, slot, dz_feed, competitor, win_rate, diff_ms_p50, ingested_at) VALUES
			('2026-09-02 01:00:00', 100, 'dz', 'c', 0.8, -0.3, '2026-09-02 01:00:09')`))

	d, err := a.ComputeCompetitorDay(t.Context(), CompetitorDayInput{Day: day, CompetitorDatabase: db})
	require.NoError(t, err)
	require.NotNil(t, d)

	assert.EqualValues(t, 1, d.LeaderSlots)
	assert.InDelta(t, 0.6, d.WinTypicalP50, 1e-6,
		"the stale copy of competitor c must not reach the quantile")
}
