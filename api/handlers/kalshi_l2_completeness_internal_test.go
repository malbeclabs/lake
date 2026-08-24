package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// genAt is a fixed scan time, so the window cutoff the merge derives is deterministic.
var genAt = time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC)

func dayRow(day string, gapped uint64) KalshiL2Day {
	return KalshiL2Day{Day: day, Lanes: 1, Instruments: 10, GappedInstruments: gapped}
}

func payload(days ...KalshiL2Day) *KalshiL2CompletenessResponse {
	return &KalshiL2CompletenessResponse{
		GeneratedAt: genAt,
		DayCount:    kalshiL2CompletenessDays,
		Days:        days,
	}
}

func dayKeys(resp *KalshiL2CompletenessResponse) []string {
	out := make([]string, 0, len(resp.Days))
	for _, d := range resp.Days {
		out = append(out, d.Day)
	}
	return out
}

// The whole point of the incremental refresh: a three-day scan plus the previous payload has to
// produce the same seven days a full scan would, newest first.
func TestMergeKalshiL2Days_PartialScanKeepsTheWindow(t *testing.T) {
	fresh := payload(dayRow("2026-08-24", 0), dayRow("2026-08-23", 0), dayRow("2026-08-22", 0))
	cached := payload(
		dayRow("2026-08-23", 5), dayRow("2026-08-22", 5), dayRow("2026-08-21", 5),
		dayRow("2026-08-20", 5), dayRow("2026-08-19", 5), dayRow("2026-08-18", 5),
	)

	got := mergeKalshiL2Days(fresh, cached)

	assert.Equal(t, []string{
		"2026-08-24", "2026-08-23", "2026-08-22", "2026-08-21",
		"2026-08-20", "2026-08-19", "2026-08-18",
	}, dayKeys(got))
	assert.Equal(t, kalshiL2CompletenessDays, got.DayCount)
	assert.Equal(t, genAt, got.GeneratedAt)
}

// A rescanned day wins. Yesterday's partition can still gain rows after midnight, so the point
// of re-reading it is that the fresh answer replaces the cached one rather than being dropped
// as a duplicate.
func TestMergeKalshiL2Days_FreshDayReplacesCached(t *testing.T) {
	fresh := payload(dayRow("2026-08-23", 0))
	cached := payload(dayRow("2026-08-23", 9), dayRow("2026-08-22", 9))

	got := mergeKalshiL2Days(fresh, cached)

	require.Len(t, got.Days, 2)
	assert.Equal(t, "2026-08-23", got.Days[0].Day)
	assert.EqualValues(t, 0, got.Days[0].GappedInstruments, "the rescan must win")
	assert.EqualValues(t, 9, got.Days[1].GappedInstruments, "the older day carries forward")
}

// A day that has fallen out of the window is a day the capture has already deleted. Carrying it
// forward would leave the page calling a day replayable whose rows no longer exist, and a
// trim-by-count would keep it here because the payload holds fewer than a full window.
func TestMergeKalshiL2Days_DropsDaysBelowTheWindow(t *testing.T) {
	fresh := payload(dayRow("2026-08-24", 0))
	cached := payload(dayRow("2026-08-17", 0), dayRow("2026-08-10", 0))

	got := mergeKalshiL2Days(fresh, cached)

	assert.Equal(t, []string{"2026-08-24"}, dayKeys(got),
		"2026-08-18 is the oldest day in the window at this scan time")
}

// The cutoff applies to the scan's own rows too, not only to what it carries forward.
func TestMergeKalshiL2Days_CutoffAppliesToFreshRows(t *testing.T) {
	fresh := payload(dayRow("2026-08-24", 0), dayRow("2026-08-17", 0))
	cached := payload()

	got := mergeKalshiL2Days(fresh, cached)

	assert.Equal(t, []string{"2026-08-24"}, dayKeys(got))
}

// Never more days than the view reports, whatever the two sides hold.
func TestMergeKalshiL2Days_TrimsToTheWindow(t *testing.T) {
	fresh := payload(dayRow("2026-08-24", 0))
	cached := payload(
		dayRow("2026-08-23", 0), dayRow("2026-08-22", 0), dayRow("2026-08-21", 0),
		dayRow("2026-08-20", 0), dayRow("2026-08-19", 0), dayRow("2026-08-18", 0),
		dayRow("2026-08-18", 0),
	)

	got := mergeKalshiL2Days(fresh, cached)

	assert.Len(t, got.Days, kalshiL2CompletenessDays)
}
