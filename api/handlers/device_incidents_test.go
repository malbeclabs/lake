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

func TestIsDeviceDrained(t *testing.T) {
	tests := []struct {
		status   string
		expected bool
	}{
		{"activated", false},
		{"soft-drained", true},
		{"hard-drained", true},
		{"suspended", true},
		{"decommissioned", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.expected, isDeviceDrained(tt.status))
		})
	}
}

func TestIsDefaultDeviceIncidentsRequest(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"empty params", "/api/incidents/devices", true},
		{"all defaults explicit", "/api/incidents/devices?range=24h&type=all&errors_threshold=10&discards_threshold=10&carrier_threshold=1&min_duration=30&coalesce_gap=720", true},
		{"custom range", "/api/incidents/devices?range=6h", false},
		{"custom type", "/api/incidents/devices?type=errors", false},
		{"with filter", "/api/incidents/devices?filter=metro:NYC", false},
		{"custom errors_threshold", "/api/incidents/devices?errors_threshold=50", false},
		{"custom min_duration", "/api/incidents/devices?min_duration=60", false},
		{"custom coalesce_gap", "/api/incidents/devices?coalesce_gap=300", false},
		{"link_interfaces true", "/api/incidents/devices?link_interfaces=true", false},
		{"link_interfaces false (default)", "/api/incidents/devices?link_interfaces=false", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", tt.url, nil)
			assert.Equal(t, tt.expected, isDefaultDeviceIncidentsRequest(req))
		})
	}
}

func TestCoalesceDeviceIncidents(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	int64Ptr := func(i int64) *int64 { return &i }

	makeIncident := func(device string, incType string, start, end time.Duration, ongoing bool, peakCount int64) DeviceIncident {
		inc := DeviceIncident{
			DevicePK:     device,
			DeviceCode:   device,
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
		assert.Nil(t, coalesceDeviceIncidents(nil, 30*time.Minute))
	})

	t.Run("single incident passthrough", func(t *testing.T) {
		in := []DeviceIncident{makeIncident("DEV-1", "errors", 0, 10*time.Minute, false, 50)}
		got := coalesceDeviceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 1)
	})

	t.Run("two within gap are merged", func(t *testing.T) {
		in := []DeviceIncident{
			makeIncident("DEV-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("DEV-1", "errors", 25*time.Minute, 35*time.Minute, false, 100),
		}
		got := coalesceDeviceIncidents(in, 30*time.Minute)
		require.Len(t, got, 1)
		assert.Equal(t, ts(0), got[0].StartedAt)
		assert.Equal(t, ts(35*time.Minute), *got[0].EndedAt)
		assert.Equal(t, int64(100), *got[0].PeakCount)
	})

	t.Run("two beyond gap stay separate", func(t *testing.T) {
		in := []DeviceIncident{
			makeIncident("DEV-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("DEV-1", "errors", 45*time.Minute, 55*time.Minute, false, 100),
		}
		got := coalesceDeviceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 2)
	})

	t.Run("different types not merged", func(t *testing.T) {
		in := []DeviceIncident{
			makeIncident("DEV-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("DEV-1", "discards", 5*time.Minute, 15*time.Minute, false, 100),
		}
		got := coalesceDeviceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 2)
	})

	t.Run("different devices not merged", func(t *testing.T) {
		in := []DeviceIncident{
			makeIncident("DEV-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("DEV-2", "errors", 5*time.Minute, 15*time.Minute, false, 100),
		}
		got := coalesceDeviceIncidents(in, 30*time.Minute)
		assert.Len(t, got, 2)
	})

	t.Run("ongoing absorbs nearby completed", func(t *testing.T) {
		in := []DeviceIncident{
			makeIncident("DEV-1", "errors", 0, 0, true, 50),
			makeIncident("DEV-1", "errors", 10*time.Minute, 20*time.Minute, false, 100),
		}
		sort.Slice(in, func(i, j int) bool { return in[i].StartedAt < in[j].StartedAt })
		got := coalesceDeviceIncidents(in, 30*time.Minute)
		require.Len(t, got, 1)
		assert.True(t, got[0].IsOngoing)
		assert.Equal(t, int64(100), *got[0].PeakCount)
	})

	t.Run("zero gap means no coalescing", func(t *testing.T) {
		in := []DeviceIncident{
			makeIncident("DEV-1", "errors", 0, 10*time.Minute, false, 50),
			makeIncident("DEV-1", "errors", 15*time.Minute, 25*time.Minute, false, 100),
		}
		got := coalesceDeviceIncidents(in, 0)
		assert.Len(t, got, 2)
	})
}

func TestPairDeviceCounterIncidentsCompleted(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	bucket := func(offset time.Duration) time.Time { return base.Add(offset) }

	meta := map[string]deviceMetadata{
		"pk-1": {DevicePK: "pk-1", DeviceCode: "DEV-1", DeviceType: "router", Metro: "LAX", ContributorCode: "CONTRIB-1", Status: "activated"},
	}

	dp := incidentDetectionParams{
		MinDuration: 10 * time.Minute,
		CoalesceGap: 30 * time.Minute,
	}

	t.Run("single bucket spike discarded", func(t *testing.T) {
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 50},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 0},
		}
		got := pairDeviceCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp)
		assert.Empty(t, got)
	})

	t.Run("two consecutive buckets above threshold recorded", func(t *testing.T) {
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{DevicePK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		got := pairDeviceCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.Equal(t, "errors", got[0].IncidentType)
		assert.Equal(t, "DEV-1", got[0].DeviceCode)
		assert.Equal(t, int64(25), *got[0].PeakCount)
		assert.False(t, got[0].IsOngoing)
	})

	t.Run("min duration 30m requires 6 buckets", func(t *testing.T) {
		dp30 := incidentDetectionParams{MinDuration: 30 * time.Minute, CoalesceGap: 30 * time.Minute}
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 20},
			{DevicePK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 25},
			{DevicePK: "pk-1", Bucket: bucket(20 * time.Minute), Value: 0},
		}
		got := pairDeviceCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp30)
		assert.Empty(t, got, "3 consecutive buckets should not meet 30m min duration")
	})

	t.Run("six consecutive buckets meets 30m min", func(t *testing.T) {
		dp30 := incidentDetectionParams{MinDuration: 30 * time.Minute, CoalesceGap: 30 * time.Minute}
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
		}
		for i := 1; i <= 6; i++ {
			buckets = append(buckets, deviceCounterBucket{
				DevicePK: "pk-1", Bucket: bucket(time.Duration(i*5) * time.Minute), Value: int64(10 + i),
			})
		}
		buckets = append(buckets, deviceCounterBucket{
			DevicePK: "pk-1", Bucket: bucket(35 * time.Minute), Value: 0,
		})

		got := pairDeviceCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp30)
		require.Len(t, got, 1)
		assert.Equal(t, int64(16), *got[0].PeakCount)
	})

	t.Run("excluded devices are skipped", func(t *testing.T) {
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{DevicePK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		excluded := map[string]bool{"DEV-1": true}
		got := pairDeviceCounterIncidentsCompleted(buckets, meta, 10, "errors", excluded, dp)
		assert.Empty(t, got)
	})

	t.Run("incident at end of window", func(t *testing.T) {
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
		}
		got := pairDeviceCounterIncidentsCompleted(buckets, meta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.Equal(t, int64(25), *got[0].PeakCount)
	})

	t.Run("drained device gets is_drained flag", func(t *testing.T) {
		drainedMeta := map[string]deviceMetadata{
			"pk-1": {DevicePK: "pk-1", DeviceCode: "DEV-1", DeviceType: "router", Metro: "LAX", Status: "soft-drained"},
		}
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{DevicePK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		got := pairDeviceCounterIncidentsCompleted(buckets, drainedMeta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.True(t, got[0].IsDrained)
	})

	t.Run("suspended device gets is_drained flag", func(t *testing.T) {
		suspendedMeta := map[string]deviceMetadata{
			"pk-1": {DevicePK: "pk-1", DeviceCode: "DEV-1", DeviceType: "router", Metro: "LAX", Status: "suspended"},
		}
		buckets := []deviceCounterBucket{
			{DevicePK: "pk-1", Bucket: bucket(0), Value: 0},
			{DevicePK: "pk-1", Bucket: bucket(5 * time.Minute), Value: 15},
			{DevicePK: "pk-1", Bucket: bucket(10 * time.Minute), Value: 25},
			{DevicePK: "pk-1", Bucket: bucket(15 * time.Minute), Value: 0},
		}
		got := pairDeviceCounterIncidentsCompleted(buckets, suspendedMeta, 10, "errors", nil, dp)
		require.Len(t, got, 1)
		assert.True(t, got[0].IsDrained)
	})
}

func TestBuildDrainedDevicesInfo(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return base.Add(d).Format(time.RFC3339) }
	int64Ptr := func(i int64) *int64 { return &i }

	meta := map[string]deviceMetadata{
		"pk-1": {DevicePK: "pk-1", DeviceCode: "DEV-1", DeviceType: "router", Metro: "LAX", Status: "soft-drained"},
		"pk-2": {DevicePK: "pk-2", DeviceCode: "DEV-2", DeviceType: "switch", Metro: "FRA", Status: "suspended"},
		"pk-3": {DevicePK: "pk-3", DeviceCode: "DEV-3", DeviceType: "router", Metro: "DUB", Status: "activated"},
	}

	drainedSince := map[string]time.Time{
		"pk-1": base.Add(-24 * time.Hour),
		"pk-2": base.Add(-48 * time.Hour),
	}

	t.Run("activated devices excluded", func(t *testing.T) {
		got := buildDrainedDevicesInfo(meta, nil, drainedSince)
		for _, dd := range got {
			assert.NotEqual(t, "DEV-3", dd.DeviceCode, "activated devices should not appear")
		}
	})

	t.Run("drained device with no incidents has gray readiness", func(t *testing.T) {
		got := buildDrainedDevicesInfo(meta, nil, drainedSince)
		var found bool
		for _, dd := range got {
			if dd.DeviceCode == "DEV-1" {
				found = true
				assert.Equal(t, "gray", dd.Readiness)
				assert.Empty(t, dd.ActiveIncidents)
				assert.Empty(t, dd.RecentIncidents)
			}
		}
		assert.True(t, found, "DEV-1 should be in drained list")
	})

	t.Run("suspended device included in drained list", func(t *testing.T) {
		got := buildDrainedDevicesInfo(meta, nil, drainedSince)
		var found bool
		for _, dd := range got {
			if dd.DeviceCode == "DEV-2" {
				found = true
				assert.Equal(t, "suspended", dd.DrainStatus)
			}
		}
		assert.True(t, found, "DEV-2 (suspended) should be in drained list")
	})

	t.Run("drained device with ongoing incident has red readiness", func(t *testing.T) {
		incidents := map[string][]DeviceIncident{
			"pk-1": {
				{
					DevicePK:     "pk-1",
					DeviceCode:   "DEV-1",
					IncidentType: "errors",
					StartedAt:    ts(-10 * time.Minute),
					IsOngoing:    true,
				},
			},
		}
		got := buildDrainedDevicesInfo(meta, incidents, drainedSince)
		for _, dd := range got {
			if dd.DeviceCode == "DEV-1" {
				assert.Equal(t, "red", dd.Readiness)
				assert.Len(t, dd.ActiveIncidents, 1)
			}
		}
	})

	t.Run("drained device with recent completed incident has yellow or green readiness", func(t *testing.T) {
		recentEnd := ts(-5 * time.Minute)
		incidents := map[string][]DeviceIncident{
			"pk-1": {
				{
					DevicePK:        "pk-1",
					DeviceCode:      "DEV-1",
					IncidentType:    "errors",
					StartedAt:       ts(-20 * time.Minute),
					EndedAt:         &recentEnd,
					DurationSeconds: int64Ptr(900),
					IsOngoing:       false,
				},
			},
		}
		got := buildDrainedDevicesInfo(meta, incidents, drainedSince)
		for _, dd := range got {
			if dd.DeviceCode == "DEV-1" {
				assert.Contains(t, []string{"yellow", "green"}, dd.Readiness)
				assert.Empty(t, dd.ActiveIncidents)
				assert.Len(t, dd.RecentIncidents, 1)
				assert.NotNil(t, dd.ClearForSeconds)
			}
		}
	})

	t.Run("drained_since is populated", func(t *testing.T) {
		got := buildDrainedDevicesInfo(meta, nil, drainedSince)
		for _, dd := range got {
			if dd.DeviceCode == "DEV-1" {
				assert.Equal(t, base.Add(-24*time.Hour).UTC().Format(time.RFC3339), dd.DrainedSince)
			}
			if dd.DeviceCode == "DEV-2" {
				assert.Equal(t, base.Add(-48*time.Hour).UTC().Format(time.RFC3339), dd.DrainedSince)
			}
		}
	})

	t.Run("sorted by readiness then code", func(t *testing.T) {
		incidents := map[string][]DeviceIncident{
			"pk-1": {
				{DevicePK: "pk-1", DeviceCode: "DEV-1", IncidentType: "errors", StartedAt: ts(-10 * time.Minute), IsOngoing: true},
			},
		}
		got := buildDrainedDevicesInfo(meta, incidents, drainedSince)
		require.Len(t, got, 2)
		// DEV-1 has red readiness (ongoing), DEV-2 has gray (no incidents)
		assert.Equal(t, "red", got[0].Readiness)
		assert.Equal(t, "gray", got[1].Readiness)
	})
}

func TestCounterTypeFilter(t *testing.T) {
	tests := []struct {
		incidentType string
		expectMatch  bool
	}{
		{"errors", true},
		{"discards", true},
		{"carrier", true},
		{"fcs", true},
		{"no_data", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.incidentType, func(t *testing.T) {
			f := counterTypeFilter(tt.incidentType)
			if tt.expectMatch {
				assert.NotEqual(t, "1=0", f)
			} else {
				assert.Equal(t, "1=0", f)
			}
		})
	}
}

func TestIncidentSeverityDevice(t *testing.T) {
	tests := []struct {
		name      string
		incType   string
		peakCount int64
		expected  string
	}{
		{"carrier always incident", "carrier", 5, "incident"},
		{"errors degraded", "errors", 100, "degraded"},
		{"discards degraded", "discards", 50, "degraded"},
		{"no_data degraded", "no_data", 0, "degraded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := incidentSeverity(tt.incType, 0, tt.peakCount)
			assert.Equal(t, tt.expected, fmt.Sprintf("%s", got))
		})
	}
}
