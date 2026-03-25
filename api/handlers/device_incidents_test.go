package handlers

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDeviceDrained(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"empty params", "/api/incidents/devices", true},
		{"all defaults explicit", "/api/incidents/devices?range=24h&type=all&errors_threshold=1&discards_threshold=1&carrier_threshold=1&min_duration=30&coalesce_gap=180", true},
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

func TestBuildDrainedDevicesInfo(t *testing.T) {
	t.Parallel()
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

func TestIncidentSeverityDevice(t *testing.T) {
	t.Parallel()
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
