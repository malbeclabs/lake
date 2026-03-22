package handlers

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsDefaultIncidentsRequest(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"empty params", "/api/incidents/links", true},
		{"all defaults explicit", "/api/incidents/links?range=24h&threshold=10&type=all&errors_threshold=1&discards_threshold=1&carrier_threshold=1&min_duration=30&coalesce_gap=180", true},
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
