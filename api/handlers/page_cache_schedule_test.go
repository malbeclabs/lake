package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextDailyMarkUTC(t *testing.T) {
	day := func(h, m int) time.Time { return time.Date(2026, 9, 24, h, m, 0, 0, time.UTC) }
	for _, tc := range []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{"just after midnight is still today's refresh", day(0, 30), day(9, 0)},
		{"a payload computed at the refresh hour is replaced tomorrow", day(9, 0), day(9, 0).AddDate(0, 0, 1)},
		{"a payload computed after the refresh hour is replaced tomorrow", day(9, 7), day(9, 0).AddDate(0, 0, 1)},
		{"a non-UTC clock is read in UTC", day(12, 0).In(time.FixedZone("UTC-10", -10*3600)), day(9, 0).AddDate(0, 0, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := NextDailyMarkUTC(tc.now, 9*time.Hour)
			require.True(t, got.Equal(tc.want), "got %s, want %s", got, tc.want)
		})
	}
}
