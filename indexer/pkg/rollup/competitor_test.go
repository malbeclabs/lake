package rollup

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
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
