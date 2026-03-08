package handlers

import (
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDefaultIncidentsRequest(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"empty params", "/api/incidents/links", true},
		{"all defaults explicit", "/api/incidents/links?range=24h&threshold=10&type=all&errors_threshold=10&discards_threshold=10&carrier_threshold=1&min_duration=30&coalesce_gap=720", true},
		{"custom range", "/api/incidents/links?range=6h", false},
		{"custom threshold", "/api/incidents/links?threshold=20", false},
		{"custom type", "/api/incidents/links?type=errors", false},
		{"with filter", "/api/incidents/links?filter=metro:NYC", false},
		{"custom errors_threshold", "/api/incidents/links?errors_threshold=50", false},
		{"custom min_duration", "/api/incidents/links?min_duration=60", false},
		{"custom coalesce_gap", "/api/incidents/links?coalesce_gap=300", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", tt.url, nil)
			assert.Equal(t, tt.expected, isDefaultIncidentsRequest(req))
		})
	}
}

func TestMinBuckets(t *testing.T) {
	tests := []struct {
		duration time.Duration
		expected int
	}{
		{0, 1},
		{3 * time.Minute, 1},
		{5 * time.Minute, 1},
		{10 * time.Minute, 2},
		{30 * time.Minute, 6},
		{60 * time.Minute, 12},
	}
	for _, tt := range tests {
		t.Run(tt.duration.String(), func(t *testing.T) {
			dp := incidentDetectionParams{MinDuration: tt.duration}
			assert.Equal(t, tt.expected, dp.minBuckets())
		})
	}
}

func TestParseIntParam(t *testing.T) {
	tests := []struct {
		value      string
		defaultVal int64
		expected   int64
	}{
		{"", 10, 10},
		{"abc", 10, 10},
		{"5", 10, 5},
		{"0", 10, 0},
		{"-1", 10, -1},
		{"100", 30, 100},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q_default_%d", tt.value, tt.defaultVal), func(t *testing.T) {
			assert.Equal(t, tt.expected, parseIntParam(tt.value, tt.defaultVal))
		})
	}
}

func TestIncidentSeverity(t *testing.T) {
	tests := []struct {
		name      string
		incType   string
		peakLoss  float64
		peakCount int64
		expected  string
	}{
		{"packet_loss below 10%", "packet_loss", 5.0, 0, "degraded"},
		{"packet_loss at 10%", "packet_loss", 10.0, 0, "incident"},
		{"packet_loss above 10%", "packet_loss", 50.0, 0, "incident"},
		{"carrier always incident", "carrier", 0, 5, "incident"},
		{"errors degraded", "errors", 0, 100, "degraded"},
		{"discards degraded", "discards", 0, 50, "degraded"},
		{"no_data degraded", "no_data", 0, 0, "degraded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, incidentSeverity(tt.incType, tt.peakLoss, tt.peakCount))
		})
	}
}

func TestCoalesceIncidents(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	strPtr := func(s string) *string { return &s }
	int64Ptr := func(i int64) *int64 { return &i }

	makeIncident := func(link string, incType string, start, end time.Duration, ongoing bool, peakCount int64) LinkIncident {
		inc := LinkIncident{
			LinkPK:       link,
			LinkCode:     link,
			IncidentType: incType,
			StartedAt:    ts(start),
			IsOngoing:    ongoing,
		}
		if peakCount > 0 {
			inc.PeakCount = int64Ptr(peakCount)
		}
		if !ongoing {
			endStr := ts(end)
			inc.EndedAt = &endStr
			dur := int64(end.Seconds() - start.Seconds())
			inc.DurationSeconds = int64Ptr(dur)
		}
		return inc
	}

	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, coalesceIncidents(nil, 30*time.Minute))
	})

	t.Run("single incident passthrough", func(t *testing.T) {
		in := []LinkIncident{makeIncident("LINK-1", "errors", 0, 10*time.Minute, false, 50)}
		got := coalesceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 1)
	})

	t.Run("two within gap are merged", func(t *testing.T) {
		in := []LinkIncident{
			makeIncident("LINK-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("LINK-1", "errors", 25*time.Minute, 35*time.Minute, false, 100),
		}
		got := coalesceIncidents(in, 30*time.Minute)
		require.Len(t, got, 1)
		assert.Equal(t, ts(0), got[0].StartedAt)
		assert.Equal(t, strPtr(ts(35*time.Minute)), got[0].EndedAt)
		assert.Equal(t, int64(100), *got[0].PeakCount)
	})

	t.Run("two beyond gap stay separate", func(t *testing.T) {
		in := []LinkIncident{
			makeIncident("LINK-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("LINK-1", "errors", 45*time.Minute, 55*time.Minute, false, 100),
		}
		got := coalesceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 2)
	})

	t.Run("different types not merged", func(t *testing.T) {
		in := []LinkIncident{
			makeIncident("LINK-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("LINK-1", "discards", 5*time.Minute, 15*time.Minute, false, 100),
		}
		got := coalesceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 2)
	})

	t.Run("different links not merged", func(t *testing.T) {
		in := []LinkIncident{
			makeIncident("LINK-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("LINK-2", "errors", 5*time.Minute, 15*time.Minute, false, 100),
		}
		got := coalesceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 2)
	})

	t.Run("ongoing absorbs nearby completed", func(t *testing.T) {
		in := []LinkIncident{
			makeIncident("LINK-1", "errors", 0, 0, true, 50),
			makeIncident("LINK-1", "errors", 10*time.Minute, 20*time.Minute, false, 100),
		}
		// Sort by start time (desc) as the real code would
		sort.Slice(in, func(i, j int) bool { return in[i].StartedAt < in[j].StartedAt })
		got := coalesceIncidents(in, 30*time.Minute)
		require.Len(t, got, 1)
		assert.True(t, got[0].IsOngoing)
		assert.Equal(t, int64(100), *got[0].PeakCount)
	})

	t.Run("zero gap means no coalescing", func(t *testing.T) {
		in := []LinkIncident{
			makeIncident("LINK-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("LINK-1", "errors", 15*time.Minute, 25*time.Minute, false, 100),
		}
		got := coalesceIncidents(in, 0)
		assert.Len(t, got, 2)
	})
}

func TestPairCounterIncidentsCompleted(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	bucket := func(offset time.Duration) time.Time { return base.Add(offset) }

	meta := map[string]linkMetadataWithStatus{
		"pk-1": {
			linkMetadata: linkMetadata{LinkPK: "pk-1", LinkCode: "LINK-1", LinkType: "WAN", SideAMetro: "LAX", SideZMetro: "SAO"},
			Status:       "activated",
		},
	}

	dp := incidentDetectionParams{
		MinDuration: 10 * time.Minute,
		CoalesceGap: 30 * time.Minute,
	}

	t.Run("single bucket spike discarded", func(t *testing.T) {
		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 50},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 0},
		}
		got := pairCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp)
		assert.Empty(t, got)
	})

	t.Run("two consecutive buckets above threshold recorded", func(t *testing.T) {
		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		got := pairCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.Equal(t, "errors", got[0].IncidentType)
		assert.Equal(t, "LINK-1", got[0].LinkCode)
		assert.Equal(t, int64(25), *got[0].PeakCount)
		assert.False(t, got[0].IsOngoing)
	})

	t.Run("min duration 30m requires 6 buckets", func(t *testing.T) {
		dp30 := incidentDetectionParams{MinDuration: 30 * time.Minute, CoalesceGap: 30 * time.Minute}

		// Only 3 consecutive buckets above threshold — should be discarded with 30m min
		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 20},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 25},
			{LinkPK: "pk-1", Bucket: bucket(20 * time.Minute), Value: 0},
		}
		got := pairCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp30)
		assert.Empty(t, got, "3 consecutive buckets should not meet 30m min duration")
	})

	t.Run("six consecutive buckets meets 30m min", func(t *testing.T) {
		dp30 := incidentDetectionParams{MinDuration: 30 * time.Minute, CoalesceGap: 30 * time.Minute}

		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
		}
		for i := 1; i <= 6; i++ {
			buckets = append(buckets, counterBucket{
				LinkPK: "pk-1", Bucket: bucket(time.Duration(i*5) * time.Minute), Value: int64(10 + i),
			})
		}
		buckets = append(buckets, counterBucket{
			LinkPK: "pk-1", Bucket: bucket(35 * time.Minute), Value: 0,
		})

		got := pairCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp30)
		require.Len(t, got, 1)
		assert.Equal(t, int64(16), *got[0].PeakCount)
	})

	t.Run("excluded links are skipped", func(t *testing.T) {
		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		excluded := map[string]bool{"LINK-1": true}
		got := pairCounterIncidentsCompleted(buckets, meta, 10, "errors", excluded, dp)
		assert.Empty(t, got)
	})

	t.Run("incident at end of window", func(t *testing.T) {
		// Incident still above threshold at the last bucket
		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
		}
		got := pairCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.Equal(t, int64(25), *got[0].PeakCount)
	})

	t.Run("drained link gets is_drained flag", func(t *testing.T) {
		drainedMeta := map[string]linkMetadataWithStatus{
			"pk-1": {
				linkMetadata: linkMetadata{LinkPK: "pk-1", LinkCode: "LINK-1", LinkType: "WAN", SideAMetro: "LAX", SideZMetro: "SAO"},
				Status:       "soft-drained",
			},
		}
		buckets := []counterBucket{
			{LinkPK: "pk-1", Bucket: bucket(0), Value: 0},
			{LinkPK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{LinkPK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{LinkPK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		got := pairCounterIncidentsCompleted(buckets, drainedMeta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.True(t, got[0].IsDrained)
	})
}

func TestBuildDrainedLinksInfo(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	int64Ptr := func(i int64) *int64 { return &i }

	meta := map[string]linkMetadataWithStatus{
		"pk-1": {
			linkMetadata: linkMetadata{LinkPK: "pk-1", LinkCode: "LINK-1", LinkType: "WAN", SideAMetro: "LAX", SideZMetro: "SAO"},
			Status:       "soft-drained",
		},
		"pk-2": {
			linkMetadata: linkMetadata{LinkPK: "pk-2", LinkCode: "LINK-2", LinkType: "WAN", SideAMetro: "FRA", SideZMetro: "SIN"},
			Status:       "hard-drained",
		},
		"pk-3": {
			linkMetadata: linkMetadata{LinkPK: "pk-3", LinkCode: "LINK-3", LinkType: "WAN", SideAMetro: "DUB", SideZMetro: "MUC"},
			Status:       "activated",
		},
	}

	drainedSince := map[string]time.Time{
		"pk-1": base.Add(-24 * time.Hour),
		"pk-2": base.Add(-48 * time.Hour),
	}

	t.Run("activated links excluded", func(t *testing.T) {
		got := buildDrainedLinksInfo(meta, nil, drainedSince)
		for _, dl := range got {
			assert.NotEqual(t, "LINK-3", dl.LinkCode, "activated links should not appear")
		}
	})

	t.Run("drained link with no incidents has gray readiness", func(t *testing.T) {
		got := buildDrainedLinksInfo(meta, nil, drainedSince)
		var found bool
		for _, dl := range got {
			if dl.LinkCode == "LINK-1" {
				found = true
				assert.Equal(t, "gray", dl.Readiness)
				assert.Empty(t, dl.ActiveIncidents)
				assert.Empty(t, dl.RecentIncidents)
			}
		}
		assert.True(t, found, "LINK-1 should be in drained list")
	})

	t.Run("drained link with ongoing incident has red readiness", func(t *testing.T) {
		incidents := map[string][]LinkIncident{
			"pk-1": {
				{
					LinkPK:       "pk-1",
					LinkCode:     "LINK-1",
					IncidentType: "errors",
					StartedAt:    ts(-10 * time.Minute),
					IsOngoing:    true,
				},
			},
		}
		got := buildDrainedLinksInfo(meta, incidents, drainedSince)
		for _, dl := range got {
			if dl.LinkCode == "LINK-1" {
				assert.Equal(t, "red", dl.Readiness)
				assert.Len(t, dl.ActiveIncidents, 1)
			}
		}
	})

	t.Run("drained link with recent completed incident has yellow or green readiness", func(t *testing.T) {
		recentEnd := ts(-5 * time.Minute)
		incidents := map[string][]LinkIncident{
			"pk-1": {
				{
					LinkPK:          "pk-1",
					LinkCode:        "LINK-1",
					IncidentType:    "errors",
					StartedAt:       ts(-20 * time.Minute),
					EndedAt:         &recentEnd,
					DurationSeconds: int64Ptr(900),
					IsOngoing:       false,
				},
			},
		}
		got := buildDrainedLinksInfo(meta, incidents, drainedSince)
		for _, dl := range got {
			if dl.LinkCode == "LINK-1" {
				assert.Contains(t, []string{"yellow", "green"}, dl.Readiness)
				assert.Empty(t, dl.ActiveIncidents)
				assert.Len(t, dl.RecentIncidents, 1)
				assert.NotNil(t, dl.ClearForSeconds)
			}
		}
	})

	t.Run("drained_since is populated", func(t *testing.T) {
		got := buildDrainedLinksInfo(meta, nil, drainedSince)
		for _, dl := range got {
			if dl.LinkCode == "LINK-1" {
				assert.Equal(t, base.Add(-24*time.Hour).UTC().Format(time.RFC3339), dl.DrainedSince)
			}
			if dl.LinkCode == "LINK-2" {
				assert.Equal(t, base.Add(-48*time.Hour).UTC().Format(time.RFC3339), dl.DrainedSince)
			}
		}
	})
}
