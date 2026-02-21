package handlers

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPacketLossSeverity(t *testing.T) {
	tests := []struct {
		name     string
		peak     float64
		expected string
	}{
		{"zero loss", 0.0, "degraded"},
		{"below threshold", 9.99, "degraded"},
		{"at threshold", 10.0, "outage"},
		{"above threshold", 50.0, "outage"},
		{"100 percent", 100.0, "outage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := packetLossSeverity(tt.peak)
			if got != tt.expected {
				t.Errorf("packetLossSeverity(%v) = %q, want %q", tt.peak, got, tt.expected)
			}
		})
	}
}

func TestParseThreshold(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"1", 1.0},
		{"10", 10.0},
		{"", 10.0},
		{"5", 10.0},
		{"abc", 10.0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseThreshold(tt.input)
			if got != tt.expected {
				t.Errorf("parseThreshold(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseTimeRange(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"3h", 3 * time.Hour},
		{"6h", 6 * time.Hour},
		{"12h", 12 * time.Hour},
		{"24h", 24 * time.Hour},
		{"3d", 3 * 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"", 24 * time.Hour},
		{"unknown", 24 * time.Hour},
		{"1h", 24 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseTimeRange(tt.input)
			if got != tt.expected {
				t.Errorf("parseTimeRange(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseOutageFilters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []OutageFilter
	}{
		{"empty string", "", nil},
		{"single filter", "metro:SAO", []OutageFilter{{Type: "metro", Value: "SAO"}}},
		{"multiple filters", "metro:SAO,link:WAN-LAX-01", []OutageFilter{
			{Type: "metro", Value: "SAO"},
			{Type: "link", Value: "WAN-LAX-01"},
		}},
		{"with spaces", " metro:SAO , link:WAN-LAX-01 ", []OutageFilter{
			{Type: "metro", Value: "SAO"},
			{Type: "link", Value: "WAN-LAX-01"},
		}},
		{"malformed no colon", "badfilter", nil},
		{"mixed valid and malformed", "metro:SAO,bad,link:LAX", []OutageFilter{
			{Type: "metro", Value: "SAO"},
			{Type: "link", Value: "LAX"},
		}},
		{"trailing comma", "metro:SAO,", []OutageFilter{
			{Type: "metro", Value: "SAO"},
		}},
		{"value with colon", "link:WAN:LAX-01", []OutageFilter{
			{Type: "link", Value: "WAN:LAX-01"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseOutageFilters(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIsDefaultOutagesRequest(t *testing.T) {
	makeReq := func(params map[string]string) *http.Request {
		u := &url.URL{Path: "/api/outages"}
		q := u.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
		return &http.Request{URL: u}
	}

	tests := []struct {
		name     string
		params   map[string]string
		expected bool
	}{
		{"no params", nil, true},
		{"explicit defaults", map[string]string{"range": "24h", "threshold": "10", "type": "all"}, true},
		{"non-default range", map[string]string{"range": "3h"}, false},
		{"non-default threshold", map[string]string{"threshold": "1"}, false},
		{"non-default type", map[string]string{"type": "status"}, false},
		{"with filter", map[string]string{"filter": "metro:SAO"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDefaultOutagesRequest(makeReq(tt.params))
			if got != tt.expected {
				t.Errorf("isDefaultOutagesRequest() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestCoalescePacketLossOutages(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	strPtr := func(s string) *string { return &s }
	floatPtr := func(f float64) *float64 { return &f }
	int64Ptr := func(i int64) *int64 { return &i }

	makeOutage := func(link string, start, end time.Duration, ongoing bool, peak float64) LinkOutage {
		o := LinkOutage{
			LinkCode:    link,
			OutageType:  "packet_loss",
			StartedAt:   ts(start),
			IsOngoing:   ongoing,
			PeakLossPct: floatPtr(peak),
		}
		if !ongoing {
			o.EndedAt = strPtr(ts(end))
			dur := int64(end.Seconds() - start.Seconds())
			o.DurationSeconds = int64Ptr(dur)
		}
		return o
	}

	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, coalescePacketLossOutages(nil))
	})

	t.Run("single outage passthrough", func(t *testing.T) {
		in := []LinkOutage{makeOutage("LINK-1", 0, 10*time.Minute, false, 15.0)}
		got := coalescePacketLossOutages(in)
		assert.Len(t, got, 1)
	})

	t.Run("two within 15min gap are merged", func(t *testing.T) {
		in := []LinkOutage{
			makeOutage("LINK-1", 0, 10*time.Minute, false, 15.0),
			makeOutage("LINK-1", 20*time.Minute, 30*time.Minute, false, 25.0),
		}
		got := coalescePacketLossOutages(in)
		assert.Len(t, got, 1)
		assert.Equal(t, ts(0), got[0].StartedAt)
		assert.Equal(t, ts(30*time.Minute), *got[0].EndedAt)
		assert.Equal(t, 25.0, *got[0].PeakLossPct)
	})

	t.Run("two beyond 15min gap stay separate", func(t *testing.T) {
		in := []LinkOutage{
			makeOutage("LINK-1", 0, 10*time.Minute, false, 15.0),
			makeOutage("LINK-1", 30*time.Minute, 40*time.Minute, false, 20.0),
		}
		got := coalescePacketLossOutages(in)
		assert.Len(t, got, 2)
	})

	t.Run("ongoing absorbs later", func(t *testing.T) {
		in := []LinkOutage{
			makeOutage("LINK-1", 0, 0, true, 15.0),
			makeOutage("LINK-1", 10*time.Minute, 20*time.Minute, false, 30.0),
		}
		got := coalescePacketLossOutages(in)
		assert.Len(t, got, 1)
		assert.True(t, got[0].IsOngoing)
		assert.Equal(t, 30.0, *got[0].PeakLossPct)
	})

	t.Run("different links not merged", func(t *testing.T) {
		in := []LinkOutage{
			makeOutage("LINK-1", 0, 10*time.Minute, false, 15.0),
			makeOutage("LINK-2", 5*time.Minute, 15*time.Minute, false, 20.0),
		}
		got := coalescePacketLossOutages(in)
		assert.Len(t, got, 2)
	})

	t.Run("merge with ongoing tail becomes ongoing", func(t *testing.T) {
		in := []LinkOutage{
			makeOutage("LINK-1", 0, 10*time.Minute, false, 15.0),
			makeOutage("LINK-1", 20*time.Minute, 0, true, 25.0),
		}
		got := coalescePacketLossOutages(in)
		assert.Len(t, got, 1)
		assert.True(t, got[0].IsOngoing)
		assert.Nil(t, got[0].EndedAt)
		assert.Nil(t, got[0].DurationSeconds)
	})
}

func TestPairPacketLossOutagesCompleted(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	bucket := func(offset time.Duration) time.Time { return base.Add(offset) }

	meta := map[string]linkMetadata{
		"pk-1": {LinkPK: "pk-1", LinkCode: "LINK-1", LinkType: "WAN", SideAMetro: "LAX", SideZMetro: "SAO"},
	}

	t.Run("single bucket spike discarded", func(t *testing.T) {
		// Only one bucket above threshold — should be discarded (consecutive requirement)
		buckets := []lossBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), LossPct: 0.0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), LossPct: 50.0},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), LossPct: 0.0},
		}
		got := pairPacketLossOutagesCompleted(buckets, meta, 10.0, nil)
		assert.Empty(t, got)
	})

	t.Run("multi bucket outage recorded", func(t *testing.T) {
		buckets := []lossBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), LossPct: 0.0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), LossPct: 15.0},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), LossPct: 25.0},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), LossPct: 0.0},
		}
		got := pairPacketLossOutagesCompleted(buckets, meta, 10.0, nil)
		assert.Len(t, got, 1)
		assert.Equal(t, "packet_loss", got[0].OutageType)
		assert.Equal(t, "LINK-1", got[0].LinkCode)
		assert.Equal(t, 25.0, *got[0].PeakLossPct)
		assert.False(t, got[0].IsOngoing)
		assert.Equal(t, "outage", got[0].Severity)
	})

	t.Run("severity degraded below 10 percent peak", func(t *testing.T) {
		buckets := []lossBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), LossPct: 0.0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), LossPct: 5.0},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), LossPct: 8.0},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), LossPct: 0.0},
		}
		got := pairPacketLossOutagesCompleted(buckets, meta, 1.0, nil)
		assert.Len(t, got, 1)
		assert.Equal(t, "degraded", got[0].Severity)
	})

	t.Run("excluded links skipped", func(t *testing.T) {
		buckets := []lossBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), LossPct: 0.0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), LossPct: 50.0},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), LossPct: 50.0},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), LossPct: 0.0},
		}
		excluded := map[string]bool{"LINK-1": true}
		got := pairPacketLossOutagesCompleted(buckets, meta, 10.0, excluded)
		assert.Empty(t, got)
	})

	t.Run("no metadata skipped", func(t *testing.T) {
		buckets := []lossBucket{
			{LinkPK: "pk-unknown", Bucket: bucket(0), LossPct: 50.0},
			{LinkPK: "pk-unknown", Bucket: bucket(5 * time.Minute), LossPct: 50.0},
		}
		got := pairPacketLossOutagesCompleted(buckets, meta, 10.0, nil)
		assert.Empty(t, got)
	})

	t.Run("outage at end of window with 2+ buckets recorded", func(t *testing.T) {
		buckets := []lossBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), LossPct: 0.0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), LossPct: 20.0},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), LossPct: 30.0},
		}
		got := pairPacketLossOutagesCompleted(buckets, meta, 10.0, nil)
		assert.Len(t, got, 1)
		assert.Equal(t, 30.0, *got[0].PeakLossPct)
	})
}

func TestPairStatusOutagesCompleted(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	makeSC := func(linkPK, linkCode, prev, next string, offset time.Duration) statusChange {
		return statusChange{
			LinkPK:         linkPK,
			LinkCode:       linkCode,
			PreviousStatus: prev,
			NewStatus:      next,
			ChangedTS:      base.Add(offset),
			SideAMetro:     "LAX",
			SideZMetro:     "SAO",
			LinkType:       "WAN",
		}
	}

	t.Run("drain then activate pair", func(t *testing.T) {
		changes := []statusChange{
			makeSC("pk-1", "LINK-1", "activated", "soft-drained", 0),
			makeSC("pk-1", "LINK-1", "soft-drained", "activated", 30*time.Minute),
		}
		got := pairStatusOutagesCompleted(changes, nil)
		assert.Len(t, got, 1)
		assert.Equal(t, "status", got[0].OutageType)
		assert.False(t, got[0].IsOngoing)
		assert.Equal(t, "activated", *got[0].PreviousStatus)
		assert.Equal(t, "soft-drained", *got[0].NewStatus)
		assert.NotNil(t, got[0].DurationSeconds)
		assert.Equal(t, int64(1800), *got[0].DurationSeconds)
	})

	t.Run("excluded links skipped", func(t *testing.T) {
		changes := []statusChange{
			makeSC("pk-1", "LINK-1", "activated", "soft-drained", 0),
			makeSC("pk-1", "LINK-1", "soft-drained", "activated", 30*time.Minute),
		}
		excluded := map[string]bool{"LINK-1": true}
		got := pairStatusOutagesCompleted(changes, excluded)
		assert.Empty(t, got)
	})

	t.Run("status change within outage soft to hard", func(t *testing.T) {
		changes := []statusChange{
			makeSC("pk-1", "LINK-1", "activated", "soft-drained", 0),
			makeSC("pk-1", "LINK-1", "soft-drained", "hard-drained", 10*time.Minute),
			makeSC("pk-1", "LINK-1", "hard-drained", "activated", 30*time.Minute),
		}
		got := pairStatusOutagesCompleted(changes, nil)
		assert.Len(t, got, 1)
		assert.Equal(t, "hard-drained", *got[0].NewStatus)
		assert.Equal(t, int64(1800), *got[0].DurationSeconds)
	})

	t.Run("empty changes", func(t *testing.T) {
		got := pairStatusOutagesCompleted(nil, nil)
		assert.Empty(t, got)
	})

	t.Run("activate without prior drain ignored", func(t *testing.T) {
		changes := []statusChange{
			makeSC("pk-1", "LINK-1", "soft-drained", "activated", 0),
		}
		got := pairStatusOutagesCompleted(changes, nil)
		assert.Empty(t, got)
	})
}

func TestGapOverlapsDrainedPeriod(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	timePtr := func(d time.Duration) *time.Time {
		t := base.Add(d)
		return &t
	}

	t.Run("no overlap", func(t *testing.T) {
		periods := []drainedPeriod{
			{Start: base, End: timePtr(10 * time.Minute)},
		}
		// Gap is entirely after the drained period
		got := gapOverlapsDrainedPeriod(base.Add(20*time.Minute), base.Add(30*time.Minute), periods)
		assert.False(t, got)
	})

	t.Run("full overlap", func(t *testing.T) {
		periods := []drainedPeriod{
			{Start: base, End: timePtr(30 * time.Minute)},
		}
		// Gap is entirely within the drained period
		got := gapOverlapsDrainedPeriod(base.Add(5*time.Minute), base.Add(10*time.Minute), periods)
		assert.True(t, got)
	})

	t.Run("partial overlap", func(t *testing.T) {
		periods := []drainedPeriod{
			{Start: base, End: timePtr(10 * time.Minute)},
		}
		// Gap starts before period ends
		got := gapOverlapsDrainedPeriod(base.Add(5*time.Minute), base.Add(20*time.Minute), periods)
		assert.True(t, got)
	})

	t.Run("ongoing drain period overlaps", func(t *testing.T) {
		periods := []drainedPeriod{
			{Start: base, End: nil}, // ongoing
		}
		got := gapOverlapsDrainedPeriod(base.Add(5*time.Minute), base.Add(10*time.Minute), periods)
		assert.True(t, got)
	})

	t.Run("gap before period", func(t *testing.T) {
		periods := []drainedPeriod{
			{Start: base.Add(30 * time.Minute), End: timePtr(40 * time.Minute)},
		}
		got := gapOverlapsDrainedPeriod(base, base.Add(10*time.Minute), periods)
		assert.False(t, got)
	})

	t.Run("empty periods", func(t *testing.T) {
		got := gapOverlapsDrainedPeriod(base, base.Add(10*time.Minute), nil)
		assert.False(t, got)
	})
}
