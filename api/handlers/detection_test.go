package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseThreshold(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestParseIncidentFilters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected []IncidentFilter
	}{
		{"empty string", "", nil},
		{"single filter", "metro:SAO", []IncidentFilter{{Type: "metro", Value: "SAO"}}},
		{"multiple filters", "metro:SAO,link:WAN-LAX-01", []IncidentFilter{
			{Type: "metro", Value: "SAO"},
			{Type: "link", Value: "WAN-LAX-01"},
		}},
		{"with spaces", " metro:SAO , link:WAN-LAX-01 ", []IncidentFilter{
			{Type: "metro", Value: "SAO"},
			{Type: "link", Value: "WAN-LAX-01"},
		}},
		{"malformed no colon", "badfilter", nil},
		{"mixed valid and malformed", "metro:SAO,bad,link:LAX", []IncidentFilter{
			{Type: "metro", Value: "SAO"},
			{Type: "link", Value: "LAX"},
		}},
		{"trailing comma", "metro:SAO,", []IncidentFilter{
			{Type: "metro", Value: "SAO"},
		}},
		{"value with colon", "link:WAN:LAX-01", []IncidentFilter{
			{Type: "link", Value: "WAN:LAX-01"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseIncidentFilters(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
