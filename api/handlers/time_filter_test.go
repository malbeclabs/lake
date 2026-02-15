package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseBucket(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"10s", "10 SECOND"},
		{"30s", "30 SECOND"},
		{"1m", "1 MINUTE"},
		{"5m", "5 MINUTE"},
		{"10m", "10 MINUTE"},
		{"15m", "15 MINUTE"},
		{"30m", "30 MINUTE"},
		{"1h", "1 HOUR"},
		{"", ""},
		{"invalid", ""},
		{"auto", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseBucket(tt.input)
			if got != tt.expected {
				t.Errorf("parseBucket(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestEffectiveBucket(t *testing.T) {
	// When bucket is provided, it should be returned as-is
	got := effectiveBucket("1h", "5 MINUTE")
	if got != "5 MINUTE" {
		t.Errorf("effectiveBucket with explicit bucket = %q, want %q", got, "5 MINUTE")
	}

	// When bucket is empty, should auto-resolve based on time range
	tests := []struct {
		timeRange string
		expected  string
	}{
		{"1h", "10 SECOND"},
		{"3h", "30 SECOND"},
		{"6h", "1 MINUTE"},
		{"12h", "1 MINUTE"},
		{"24h", "5 MINUTE"},
		{"3d", "10 MINUTE"},
		{"7d", "30 MINUTE"},
		{"14d", "1 HOUR"},
		{"30d", "1 HOUR"},
		{"unknown", "1 MINUTE"},
	}
	for _, tt := range tests {
		t.Run(tt.timeRange, func(t *testing.T) {
			got := effectiveBucket(tt.timeRange, "")
			if got != tt.expected {
				t.Errorf("effectiveBucket(%q, \"\") = %q, want %q", tt.timeRange, got, tt.expected)
			}
		})
	}
}

func TestBucketForDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{"30min", 30 * time.Minute, "10 SECOND"},
		{"2h", 2 * time.Hour, "10 SECOND"},
		{"5h", 5 * time.Hour, "1 MINUTE"},
		{"1d", 24 * time.Hour, "5 MINUTE"},
		{"5d", 5 * 24 * time.Hour, "30 MINUTE"},
		{"14d", 14 * 24 * time.Hour, "1 HOUR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bucketForDuration(tt.duration)
			if got != tt.expected {
				t.Errorf("bucketForDuration(%v) = %q, want %q", tt.duration, got, tt.expected)
			}
		})
	}
}

func TestDashboardTimeFilter(t *testing.T) {
	t.Run("preset time range", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/test?time_range=6h", nil)
		timeFilter, bucketInterval := dashboardTimeFilter(req)
		if timeFilter != "event_ts >= now() - INTERVAL 6 HOUR" {
			t.Errorf("timeFilter = %q, want preset interval", timeFilter)
		}
		if bucketInterval != "1 MINUTE" {
			t.Errorf("bucketInterval = %q, want %q", bucketInterval, "1 MINUTE")
		}
	})

	t.Run("default time range", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
		timeFilter, bucketInterval := dashboardTimeFilter(req)
		if timeFilter != "event_ts >= now() - INTERVAL 12 HOUR" {
			t.Errorf("timeFilter = %q, want 12h default", timeFilter)
		}
		if bucketInterval != "1 MINUTE" {
			t.Errorf("bucketInterval = %q, want %q", bucketInterval, "1 MINUTE")
		}
	})

	t.Run("custom time range", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/test?start_time=1700000000&end_time=1700003600", nil)
		timeFilter, bucketInterval := dashboardTimeFilter(req)
		expected := "event_ts BETWEEN toDateTime(1700000000) AND toDateTime(1700003600)"
		if timeFilter != expected {
			t.Errorf("timeFilter = %q, want %q", timeFilter, expected)
		}
		// 1h duration -> 10 SECOND bucket
		if bucketInterval != "10 SECOND" {
			t.Errorf("bucketInterval = %q, want %q", bucketInterval, "10 SECOND")
		}
	})

	t.Run("custom time range with explicit bucket", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/test?start_time=1700000000&end_time=1700003600&bucket=5m", nil)
		timeFilter, bucketInterval := dashboardTimeFilter(req)
		expected := "event_ts BETWEEN toDateTime(1700000000) AND toDateTime(1700003600)"
		if timeFilter != expected {
			t.Errorf("timeFilter = %q, want %q", timeFilter, expected)
		}
		if bucketInterval != "5 MINUTE" {
			t.Errorf("bucketInterval = %q, want %q", bucketInterval, "5 MINUTE")
		}
	})

	t.Run("preset with explicit bucket", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/test?time_range=1h&bucket=30s", nil)
		timeFilter, bucketInterval := dashboardTimeFilter(req)
		if timeFilter != "event_ts >= now() - INTERVAL 1 HOUR" {
			t.Errorf("timeFilter = %q, want 1h interval", timeFilter)
		}
		if bucketInterval != "30 SECOND" {
			t.Errorf("bucketInterval = %q, want %q", bucketInterval, "30 SECOND")
		}
	})

	t.Run("invalid custom range falls back to preset", func(t *testing.T) {
		// end before start
		req := httptest.NewRequest(http.MethodGet, "/api/test?start_time=1700003600&end_time=1700000000", nil)
		timeFilter, _ := dashboardTimeFilter(req)
		if timeFilter != "event_ts >= now() - INTERVAL 12 HOUR" {
			t.Errorf("timeFilter = %q, want fallback to default", timeFilter)
		}
	})

	t.Run("non-numeric custom range falls back to preset", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/test?start_time=abc&end_time=def", nil)
		timeFilter, _ := dashboardTimeFilter(req)
		if timeFilter != "event_ts >= now() - INTERVAL 12 HOUR" {
			t.Errorf("timeFilter = %q, want fallback to default", timeFilter)
		}
	})

	t.Run("long custom range gets large bucket", func(t *testing.T) {
		// 30 day range
		req := httptest.NewRequest(http.MethodGet, "/api/test?start_time=1700000000&end_time=1702592000", nil)
		_, bucketInterval := dashboardTimeFilter(req)
		if bucketInterval != "1 HOUR" {
			t.Errorf("bucketInterval = %q, want %q for 30d range", bucketInterval, "1 HOUR")
		}
	})
}
