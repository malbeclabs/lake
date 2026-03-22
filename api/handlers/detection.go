package handlers

import (
	"strings"
	"time"
)

// parseTimeRange converts a time range string to a duration
func parseTimeRange(rangeStr string) time.Duration {
	switch rangeStr {
	case "3h":
		return 3 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "24h":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "7d":
		return 7 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// parseThreshold returns the packet loss threshold percentage
func parseThreshold(thresholdStr string) float64 {
	switch thresholdStr {
	case "1":
		return 1.0
	case "10":
		return 10.0
	default:
		return 10.0
	}
}

// IncidentFilter represents a filter for incidents (e.g., metro:SAO, link:WAN-LAX-01)
type IncidentFilter struct {
	Type  string // device, link, metro, contributor
	Value string
}

// parseIncidentFilters parses a comma-separated filter string into IncidentFilter structs
func parseIncidentFilters(filterStr string) []IncidentFilter {
	if filterStr == "" {
		return nil
	}
	var filters []IncidentFilter
	for _, f := range strings.Split(filterStr, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		parts := strings.SplitN(f, ":", 2)
		if len(parts) == 2 {
			filters = append(filters, IncidentFilter{Type: parts[0], Value: parts[1]})
		}
	}
	return filters
}
