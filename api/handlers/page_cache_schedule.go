package handlers

import "time"

// NextDailyMarkUTC is the first occurrence of offset-past-00:00 UTC strictly after now. The
// page-cache worker schedules daily entries from it, and payloads report their next refresh
// from it, so the two share one boundary.
//
// Built from the calendar date rather than by truncating, so it stays the same wall-clock hour
// across a DST change in whatever zone the pod happens to think it is in.
func NextDailyMarkUTC(now time.Time, offset time.Duration) time.Time {
	u := now.UTC()
	mark := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).Add(offset)
	if !mark.After(u) {
		mark = mark.AddDate(0, 0, 1)
	}
	return mark
}
