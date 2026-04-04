package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"golang.org/x/sync/errgroup"
)

// TimelineEvent represents a single event in the timeline
type TimelineEvent struct {
	ID          string `json:"id"`
	EventType   string `json:"event_type"`
	Timestamp   string `json:"timestamp"`
	Category    string `json:"category"`
	Severity    string `json:"severity"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	EntityType  string `json:"entity_type"`
	EntityPK    string `json:"entity_pk"`
	EntityCode  string `json:"entity_code"`
	Details     any    `json:"details,omitempty"`
}

// EntityChangeDetails contains details for entity change events
type EntityChangeDetails struct {
	ChangeType string        `json:"change_type"` // "created", "updated", "deleted"
	Changes    []FieldChange `json:"changes,omitempty"`
	Entity     any           `json:"entity,omitempty"` // Full entity data
}

// FieldChange represents a single field that changed
type FieldChange struct {
	Field    string `json:"field"`
	OldValue any    `json:"old_value,omitempty"`
	NewValue any    `json:"new_value,omitempty"`
}

// DeviceEntity represents a device's current state
type DeviceEntity struct {
	PK            string `json:"pk"`
	Code          string `json:"code"`
	Status        string `json:"status"`
	DeviceType    string `json:"device_type"`
	PublicIP      string `json:"public_ip"`
	ContributorPK string `json:"contributor_pk"`
	MetroPK       string `json:"metro_pk"`
	MaxUsers      int32  `json:"max_users"`
	// Joined fields
	ContributorCode string `json:"contributor_code,omitempty"`
	MetroCode       string `json:"metro_code,omitempty"`
}

// LinkEntity represents a link's current state
type LinkEntity struct {
	PK                string `json:"pk"`
	Code              string `json:"code"`
	Status            string `json:"status"`
	LinkType          string `json:"link_type"`
	TunnelNet         string `json:"tunnel_net"`
	ContributorPK     string `json:"contributor_pk"`
	SideAPK           string `json:"side_a_pk"`
	SideZPK           string `json:"side_z_pk"`
	SideAIfaceName    string `json:"side_a_iface_name"`
	SideZIfaceName    string `json:"side_z_iface_name"`
	CommittedRttNs    int64  `json:"committed_rtt_ns"`
	CommittedJitterNs int64  `json:"committed_jitter_ns"`
	BandwidthBps      int64  `json:"bandwidth_bps"`
	ISISDelayOverride int64  `json:"isis_delay_override_ns"`
	// Joined fields
	ContributorCode string `json:"contributor_code,omitempty"`
	SideACode       string `json:"side_a_code,omitempty"`
	SideZCode       string `json:"side_z_code,omitempty"`
	SideAMetroCode  string `json:"side_a_metro_code,omitempty"`
	SideZMetroCode  string `json:"side_z_metro_code,omitempty"`
	SideAMetroPK    string `json:"side_a_metro_pk,omitempty"`
	SideZMetroPK    string `json:"side_z_metro_pk,omitempty"`
}

// MetroEntity represents a metro's current state
type MetroEntity struct {
	PK        string  `json:"pk"`
	Code      string  `json:"code"`
	Name      string  `json:"name"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
}

// ContributorEntity represents a contributor's current state
type ContributorEntity struct {
	PK   string `json:"pk"`
	Code string `json:"code"`
	Name string `json:"name"`
}

// UserEntity represents a user's current state
type UserEntity struct {
	PK          string `json:"pk"`
	OwnerPubkey string `json:"owner_pubkey"`
	Status      string `json:"status"`
	Kind        string `json:"kind"`
	ClientIP    string `json:"client_ip"`
	DZIP        string `json:"dz_ip"`
	DevicePK    string `json:"device_pk"`
	TunnelID    int32  `json:"tunnel_id"`
	// Joined fields
	DeviceCode string `json:"device_code,omitempty"`
	MetroCode  string `json:"metro_code,omitempty"`
}

// IncidentEventDetails contains details for incident-based timeline events.
type IncidentEventDetails struct {
	EntityPK        string  `json:"entity_pk"`
	EntityCode      string  `json:"entity_code"`
	EntityType      string  `json:"entity_type"`   // "link" or "device"
	IncidentType    string  `json:"incident_type"` // "packet_loss", "errors", etc.
	PeakValue       float64 `json:"peak_value"`
	DurationSeconds int64   `json:"duration_seconds"`
	IsOngoing       bool    `json:"is_ongoing"`
	LinkType        string  `json:"link_type,omitempty"`
	SideAMetro      string  `json:"side_a_metro,omitempty"`
	SideZMetro      string  `json:"side_z_metro,omitempty"`
	Metro           string  `json:"metro,omitempty"`
	ContributorCode string  `json:"contributor_code,omitempty"`
	Status          string  `json:"status,omitempty"`
}

// ValidatorEventDetails contains details for validator join/leave events
type ValidatorEventDetails struct {
	OwnerPubkey                string  `json:"owner_pubkey"`
	DZIP                       string  `json:"dz_ip,omitempty"`
	VotePubkey                 string  `json:"vote_pubkey,omitempty"`
	NodePubkey                 string  `json:"node_pubkey,omitempty"`
	StakeLamports              int64   `json:"stake_lamports,omitempty"`
	StakeSol                   float64 `json:"stake_sol,omitempty"`
	StakeSharePct              float64 `json:"stake_share_pct,omitempty"`
	StakeShareChangePct        float64 `json:"stake_share_change_pct,omitempty"`
	DZTotalStakeSharePct       float64 `json:"dz_total_stake_share_pct,omitempty"`
	UserPK                     string  `json:"user_pk,omitempty"`
	DevicePK                   string  `json:"device_pk,omitempty"`
	DeviceCode                 string  `json:"device_code,omitempty"`
	MetroCode                  string  `json:"metro_code,omitempty"`
	ContributorCode            string  `json:"contributor_code,omitempty"`
	Kind                       string  `json:"kind"`   // "validator" or "gossip_only"
	Action                     string  `json:"action"` // "joined" or "left"
	ContributionChangeLamports int64   `json:"contribution_change_lamports,omitempty"`
	PrevGossipIP               string  `json:"prev_gossip_ip,omitempty"`
}

// HistogramBucket represents a time bucket with event counts
type HistogramBucket struct {
	Timestamp string `json:"timestamp"`
	Count     int    `json:"count"`
}

// TimelineResponse is the API response for the timeline endpoint
type TimelineResponse struct {
	Events    []TimelineEvent   `json:"events"`
	Total     int               `json:"total"`
	Limit     int               `json:"limit"`
	Offset    int               `json:"offset"`
	TimeRange TimeRange         `json:"time_range"`
	Histogram []HistogramBucket `json:"histogram,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// TimeRange represents the time range for the query
type TimeRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// TimelineParams holds parsed query parameters
type TimelineParams struct {
	StartTime       time.Time
	EndTime         time.Time
	Categories      []string // "state_change" or "telemetry"
	EntityTypes     []string // "device", "link", "metro", "contributor", "validator", "gossip_node"
	Severities      []string
	Actions         []string // "added", "removed", "changed", "alerting", "resolved"
	DZFilter        string   // "on_dz", "off_dz", or "" for all
	MinStakePct     float64  // Minimum stake_share_pct to include (0 = no filter)
	Search          []string // Search terms to filter by (entity codes, device codes, etc.)
	Limit           int
	Offset          int
	IncludeInternal bool // Whether to include internal users (default: false)
}

// Internal user pubkeys to exclude by default
var internalUserPubkeys = []string{
	"DZfHfcCXTLwgZeCRKQ1FL1UuwAwFAZM93g86NMYpfYan",
}

// TimelineBoundsResponse contains the available date range for timeline data
type TimelineBoundsResponse struct {
	EarliestData string `json:"earliest_data"` // ISO 8601 timestamp
	LatestData   string `json:"latest_data"`   // ISO 8601 timestamp
}

// GetTimelineBounds returns the available date range for timeline data
func (a *API) GetTimelineBounds(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Query the earliest snapshot_ts across all history tables
	query := `
		SELECT
			min(earliest) as earliest,
			max(latest) as latest
		FROM (
			SELECT min(snapshot_ts) as earliest, max(snapshot_ts) as latest FROM dim_dz_devices_history
			UNION ALL
			SELECT min(snapshot_ts), max(snapshot_ts) FROM dim_dz_links_history
			UNION ALL
			SELECT min(snapshot_ts), max(snapshot_ts) FROM dim_dz_users_history
			UNION ALL
			SELECT min(event_ts), max(event_ts) FROM fact_dz_device_link_latency
		)
	`

	var earliest, latest time.Time
	err := a.envDB(ctx).QueryRow(ctx, query).Scan(&earliest, &latest)
	if err != nil {
		http.Error(w, "Failed to get timeline bounds", http.StatusInternalServerError)
		return
	}

	resp := TimelineBoundsResponse{
		EarliestData: earliest.Format(time.RFC3339),
		LatestData:   latest.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func parseTimelineParams(r *http.Request) TimelineParams {
	now := time.Now().UTC()
	endTime := now
	startTime := now.Add(-24 * time.Hour) // Default 24h

	// Check for custom start/end dates first (takes precedence over range)
	// Support both RFC3339 and datetime-local format (YYYY-MM-DDTHH:MM)
	parseTime := func(s string) (time.Time, bool) {
		if parsed, err := time.Parse(time.RFC3339, s); err == nil {
			return parsed, true
		}
		if parsed, err := time.Parse("2006-01-02T15:04", s); err == nil {
			return parsed.UTC(), true
		}
		return time.Time{}, false
	}
	if startStr := r.URL.Query().Get("start"); startStr != "" {
		if parsed, ok := parseTime(startStr); ok {
			startTime = parsed
		}
	}
	if endStr := r.URL.Query().Get("end"); endStr != "" {
		if parsed, ok := parseTime(endStr); ok {
			endTime = parsed
		}
	}

	// Parse range parameter (only used if custom dates not provided)
	if r.URL.Query().Get("start") == "" && r.URL.Query().Get("end") == "" {
		if rangeStr := r.URL.Query().Get("range"); rangeStr != "" {
			switch rangeStr {
			case "1h":
				startTime = now.Add(-1 * time.Hour)
			case "6h":
				startTime = now.Add(-6 * time.Hour)
			case "12h":
				startTime = now.Add(-12 * time.Hour)
			case "24h":
				startTime = now.Add(-24 * time.Hour)
			case "3d":
				startTime = now.Add(-72 * time.Hour)
			case "7d":
				startTime = now.Add(-168 * time.Hour)
			}
		}
	}

	// Parse category filter (state_change or telemetry)
	var categories []string
	if catStr := r.URL.Query().Get("category"); catStr != "" {
		categories = strings.Split(catStr, ",")
	}

	// Parse entity type filter
	var entityTypes []string
	if etStr := r.URL.Query().Get("entity_type"); etStr != "" {
		entityTypes = strings.Split(etStr, ",")
	}

	// Parse severity filter
	var severities []string
	if sevStr := r.URL.Query().Get("severity"); sevStr != "" {
		severities = strings.Split(sevStr, ",")
	}

	// Parse pagination
	pagination := ParsePagination(r, 50)
	if pagination.Limit > 500 {
		pagination.Limit = 500 // Lower max for timeline to avoid huge result sets
	}

	// Parse include_internal filter (default: false - exclude internal users)
	includeInternal := r.URL.Query().Get("include_internal") == "true"

	// Parse action filter (added, removed, changed, alerting, resolved)
	var actions []string
	if actStr := r.URL.Query().Get("action"); actStr != "" {
		actions = strings.Split(actStr, ",")
	}

	// Parse DZ filter for Solana events (on_dz, off_dz, or empty for all)
	dzFilter := r.URL.Query().Get("dz_filter")

	// Parse minimum stake percentage filter
	var minStakePct float64
	if minStakeStr := r.URL.Query().Get("min_stake_pct"); minStakeStr != "" {
		if v, err := strconv.ParseFloat(minStakeStr, 64); err == nil && v > 0 {
			minStakePct = v
		}
	}

	// Parse search filter (comma-separated search terms)
	var search []string
	if searchStr := r.URL.Query().Get("search"); searchStr != "" {
		for _, s := range strings.Split(searchStr, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				search = append(search, strings.ToLower(s))
			}
		}
	}

	return TimelineParams{
		StartTime:       startTime,
		EndTime:         endTime,
		Categories:      categories,
		EntityTypes:     entityTypes,
		Severities:      severities,
		Actions:         actions,
		DZFilter:        dzFilter,
		MinStakePct:     minStakePct,
		Search:          search,
		Limit:           pagination.Limit,
		Offset:          pagination.Offset,
		IncludeInternal: includeInternal,
	}
}

func generateEventID(entityID string, timestamp time.Time, eventType string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%s:%s", entityID, timestamp.Format(time.RFC3339Nano), eventType)))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// eventMatchesFieldSearch checks if an event matches a single field:value filter.
// The value is already lowercased.
func eventMatchesFieldSearch(event TimelineEvent, field, value string) bool {
	contains := func(s string) bool {
		return s != "" && strings.Contains(strings.ToLower(s), value)
	}

	switch field {
	case "device":
		if contains(event.EntityCode) && event.EntityType == "device" {
			return true
		}
		switch details := event.Details.(type) {
		case EntityChangeDetails:
			if entity, ok := details.Entity.(DeviceEntity); ok {
				return contains(entity.Code)
			}
		case IncidentEventDetails:
			return contains(details.EntityCode)
		case ValidatorEventDetails:
			return contains(details.DeviceCode)
		}
	case "link":
		if contains(event.EntityCode) && event.EntityType == "link" {
			return true
		}
		switch details := event.Details.(type) {
		case EntityChangeDetails:
			if entity, ok := details.Entity.(LinkEntity); ok {
				return contains(entity.Code) || contains(entity.SideACode) || contains(entity.SideZCode)
			}
		case IncidentEventDetails:
			return contains(details.EntityCode)
		}
	case "metro":
		if contains(event.EntityCode) && event.EntityType == "metro" {
			return true
		}
		switch details := event.Details.(type) {
		case EntityChangeDetails:
			if entity, ok := details.Entity.(DeviceEntity); ok {
				return contains(entity.MetroCode)
			}
			if entity, ok := details.Entity.(LinkEntity); ok {
				return contains(entity.SideAMetroCode) || contains(entity.SideZMetroCode)
			}
			if entity, ok := details.Entity.(MetroEntity); ok {
				return contains(entity.Code)
			}
			if entity, ok := details.Entity.(UserEntity); ok {
				return contains(entity.MetroCode)
			}
		case IncidentEventDetails:
			return contains(details.SideAMetro) || contains(details.SideZMetro) || contains(details.Metro)
		case ValidatorEventDetails:
			return contains(details.MetroCode)
		}
	case "contributor":
		if contains(event.EntityCode) && event.EntityType == "contributor" {
			return true
		}
		switch details := event.Details.(type) {
		case EntityChangeDetails:
			if entity, ok := details.Entity.(DeviceEntity); ok {
				return contains(entity.ContributorCode)
			}
			if entity, ok := details.Entity.(LinkEntity); ok {
				return contains(entity.ContributorCode)
			}
			if entity, ok := details.Entity.(ContributorEntity); ok {
				return contains(entity.Code)
			}
		case IncidentEventDetails:
			return contains(details.ContributorCode)
		case ValidatorEventDetails:
			return contains(details.ContributorCode)
		}
	case "validator":
		// Match by owner_pubkey, vote_pubkey, or node_pubkey
		if (contains(event.EntityCode) || contains(event.EntityPK)) && (event.EntityType == "validator" || event.EntityType == "gossip_node") {
			return true
		}
		switch details := event.Details.(type) {
		case ValidatorEventDetails:
			return contains(details.OwnerPubkey) || contains(details.VotePubkey) || contains(details.NodePubkey)
		}
	case "user":
		// Match by owner_pubkey or user PK
		if contains(event.EntityCode) && event.EntityType == "user" {
			return true
		}
		switch details := event.Details.(type) {
		case EntityChangeDetails:
			if entity, ok := details.Entity.(UserEntity); ok {
				return contains(entity.OwnerPubkey) || contains(entity.PK)
			}
		case ValidatorEventDetails:
			return contains(details.OwnerPubkey) || contains(details.UserPK)
		}
	}
	return false
}

// eventMatchesSearch checks if an event matches the search filters.
// Supports field-prefixed terms (e.g. "contributor:cherry,metro:ams").
// Uses AND across different fields, OR within the same field.
// Search terms are already lowercased.
func eventMatchesSearch(event TimelineEvent, searchTerms []string) bool {
	// Group terms by field prefix
	grouped := make(map[string][]string) // field -> values
	for _, term := range searchTerms {
		if idx := strings.Index(term, ":"); idx > 0 {
			field := term[:idx]
			value := term[idx+1:]
			if value != "" {
				grouped[field] = append(grouped[field], value)
			}
		}
	}

	// AND across fields, OR within same field
	for field, values := range grouped {
		matched := false
		for _, value := range values {
			if eventMatchesFieldSearch(event, field, value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return len(grouped) > 0
}

// GetTimeline returns timeline events across the network
func (a *API) GetTimeline(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	start := time.Now()
	params := parseTimelineParams(r)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(10)
	var (
		entityChangeEvents []TimelineEvent
		incidentEvents     []TimelineEvent
		validatorEvents    []TimelineEvent
		mu                 sync.Mutex
	)

	// Check if category is requested (empty means all)
	shouldIncludeCategory := func(category string) bool {
		if len(params.Categories) == 0 {
			return true
		}
		for _, c := range params.Categories {
			if c == category {
				return true
			}
		}
		return false
	}

	// Query consolidated entity changes (devices, links, metros, contributors, users)
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			events, err := a.queryEntityChangeEvents(ctx, params.StartTime, params.EndTime, params.IncludeInternal)
			if err != nil {
				logError("error querying entity changes", "error", err)
				return nil // Don't fail the whole request
			}
			mu.Lock()
			entityChangeEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query incident events (packet loss, errors, discards, carrier, no_data, isis_down, etc.)
	hasIncidentCategory := false
	for _, ic := range incidentCategories {
		if shouldIncludeCategory(ic) {
			hasIncidentCategory = true
			break
		}
	}
	if hasIncidentCategory {
		g.Go(func() error {
			events, err := a.queryIncidentEvents(ctx, params.StartTime, params.EndTime)
			if err != nil {
				logError("error querying incident events", "error", err)
				return nil
			}
			// Filter by requested categories
			if len(params.Categories) > 0 {
				catSet := make(map[string]bool, len(params.Categories))
				for _, c := range params.Categories {
					catSet[c] = true
				}
				filtered := events[:0]
				for _, e := range events {
					if catSet[e.Category] {
						filtered = append(filtered, e)
					}
				}
				events = filtered
			}
			mu.Lock()
			incidentEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query validator/gossip node events
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			events, err := a.queryValidatorEvents(ctx, params.StartTime, params.EndTime, params.IncludeInternal)
			if err != nil {
				logError("error querying validator events", "error", err)
				return nil
			}
			mu.Lock()
			validatorEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query gossip node network changes (nodes going online/offline on Solana)
	var gossipNetworkEvents []TimelineEvent
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			events, err := a.queryGossipNetworkChanges(ctx, params.StartTime, params.EndTime)
			if err != nil {
				logError("error querying gossip network changes", "error", err)
				return nil
			}
			mu.Lock()
			gossipNetworkEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query vote account changes (validators joining/leaving the network)
	var voteAccountEvents []TimelineEvent
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			events, err := a.queryVoteAccountChanges(ctx, params.StartTime, params.EndTime)
			if err != nil {
				logError("error querying vote account changes", "error", err)
				return nil
			}
			mu.Lock()
			voteAccountEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query stake changes (significant stake increases/decreases)
	var stakeChangeEvents []TimelineEvent
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			events, err := a.queryStakeChanges(ctx, params.StartTime, params.EndTime)
			if err != nil {
				logError("error querying stake changes", "error", err)
				return nil
			}
			mu.Lock()
			stakeChangeEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query DZ stake attribution events (why DZ total changed)
	var dzStakeAttrEvents []TimelineEvent
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			events, err := a.queryDZStakeAttribution(ctx, params.StartTime, params.EndTime)
			if err != nil {
				logError("error querying DZ stake attribution", "error", err)
				return nil
			}
			mu.Lock()
			dzStakeAttrEvents = events
			mu.Unlock()
			return nil
		})
	}

	// Query current DZ total stake share
	var dzTotalInfo dzTotalStakeInfo
	if shouldIncludeCategory("state_change") {
		g.Go(func() error {
			info, err := a.queryCurrentDZTotalStakeShare(ctx)
			if err != nil {
				logError("error querying DZ total stake share", "error", err)
				return nil
			}
			mu.Lock()
			dzTotalInfo = info
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		logError("error in timeline queries", "error", err)
	}

	// Merge all events
	allEvents := make([]TimelineEvent, 0)
	allEvents = append(allEvents, entityChangeEvents...)
	allEvents = append(allEvents, incidentEvents...)
	allEvents = append(allEvents, validatorEvents...)
	allEvents = append(allEvents, gossipNetworkEvents...)
	allEvents = append(allEvents, voteAccountEvents...)
	allEvents = append(allEvents, stakeChangeEvents...)
	allEvents = append(allEvents, dzStakeAttrEvents...)

	// Populate DZ total stake share for validator events by walking backwards
	// from the current DZ total. Only attribution events (with ContributionChangeLamports)
	// adjust the running total. This must run BEFORE filters so all attribution
	// events are visible for the walk. Events are sorted descending (newest first).
	if dzTotalInfo.DZTotalPct > 0 && dzTotalInfo.TotalStakeLamports > 0 {
		sort.Slice(allEvents, func(i, j int) bool {
			if allEvents[i].Timestamp != allEvents[j].Timestamp {
				return allEvents[i].Timestamp > allEvents[j].Timestamp
			}
			return allEvents[i].ID > allEvents[j].ID
		})

		runningDZTotalPct := dzTotalInfo.DZTotalPct
		totalStake := float64(dzTotalInfo.TotalStakeLamports)
		for i := range allEvents {
			if allEvents[i].EntityType != "validator" && allEvents[i].EntityType != "gossip_node" {
				continue
			}
			details, ok := allEvents[i].Details.(ValidatorEventDetails)
			if !ok {
				continue
			}

			// Override the DZ total from the walk (attribution query's own
			// DZTotalStakeSharePct is unreliable for older snapshots due to
			// ASOF JOIN gaps in gossip history).
			details.DZTotalStakeSharePct = math.Round(runningDZTotalPct*100) / 100

			// Only attribution events carry the authoritative DZ contribution change.
			if details.ContributionChangeLamports != 0 {
				changePct := float64(details.ContributionChangeLamports) * 100.0 / totalStake
				if details.StakeShareChangePct == 0 {
					details.StakeShareChangePct = math.Round(changePct*1000) / 1000
				}
				runningDZTotalPct -= changePct
			}

			allEvents[i].Details = details
		}
	}

	// Deduplicate validator events: both queryValidatorEvents and queryDZStakeAttribution
	// can produce validator_joined_dz/validator_left_dz for the same validator at nearly
	// the same time. Dedup by (vote_pubkey, event_type, timestamp) keeping the event with
	// ContributionChangeLamports (from attribution) over the one without.
	{
		type dedupKey struct {
			votePubkey string
			eventType  string
			timestamp  string
		}
		indices := make(map[dedupKey]int) // index of first seen event
		remove := make(map[int]bool)
		for i, e := range allEvents {
			if !strings.HasPrefix(e.EventType, "validator_") {
				continue
			}
			if !strings.Contains(e.EventType, "_joined_") && !strings.Contains(e.EventType, "_left_") && !strings.Contains(e.EventType, "_stake_changed") {
				continue
			}
			details, ok := e.Details.(ValidatorEventDetails)
			if !ok || details.VotePubkey == "" {
				continue
			}
			key := dedupKey{details.VotePubkey, e.EventType, e.Timestamp}
			if prevIdx, exists := indices[key]; exists {
				// Keep the one with ContributionChangeLamports, remove the other
				prevDetails := allEvents[prevIdx].Details.(ValidatorEventDetails)
				if prevDetails.ContributionChangeLamports != 0 {
					remove[i] = true
				} else {
					remove[prevIdx] = true
					indices[key] = i
				}
			} else {
				indices[key] = i
			}
		}
		if len(remove) > 0 {
			filtered := make([]TimelineEvent, 0, len(allEvents)-len(remove))
			for i, e := range allEvents {
				if !remove[i] {
					filtered = append(filtered, e)
				}
			}
			allEvents = filtered
		}
	}

	// Filter by entity type if specified
	if len(params.EntityTypes) > 0 {
		filtered := make([]TimelineEvent, 0)
		for _, e := range allEvents {
			for _, et := range params.EntityTypes {
				if e.EntityType == et {
					filtered = append(filtered, e)
					break
				}
			}
		}
		allEvents = filtered
	}

	// Filter by severity if specified
	if len(params.Severities) > 0 {
		filtered := make([]TimelineEvent, 0)
		for _, e := range allEvents {
			for _, s := range params.Severities {
				if e.Severity == s {
					filtered = append(filtered, e)
					break
				}
			}
		}
		allEvents = filtered
	}

	// Filter by action if specified
	// Maps action categories to event type patterns
	if len(params.Actions) > 0 {
		filtered := make([]TimelineEvent, 0)
		for _, e := range allEvents {
			for _, action := range params.Actions {
				matched := false
				switch action {
				case "added":
					matched = strings.Contains(e.EventType, "_created") || strings.Contains(e.EventType, "_joined")
				case "removed":
					matched = strings.Contains(e.EventType, "_deleted") || strings.Contains(e.EventType, "_left")
				case "changed":
					matched = strings.Contains(e.EventType, "_updated") || strings.Contains(e.EventType, "_stake_changed")
				case "alerting":
					matched = strings.Contains(e.EventType, "_started") || strings.Contains(e.EventType, "_stake_increased")
				case "resolved":
					matched = strings.Contains(e.EventType, "_stopped") || strings.Contains(e.EventType, "_recovered") || strings.Contains(e.EventType, "_ended") || strings.Contains(e.EventType, "_stake_decreased")
				}
				if matched {
					filtered = append(filtered, e)
					break
				}
			}
		}
		allEvents = filtered
	}

	// Filter Solana events by DZ connection status
	if params.DZFilter == "on_dz" || params.DZFilter == "off_dz" {
		filtered := make([]TimelineEvent, 0)
		for _, e := range allEvents {
			// Only filter validator and gossip_node events
			if e.EntityType == "validator" || e.EntityType == "gossip_node" {
				// Check if event has ValidatorEventDetails
				if details, ok := e.Details.(ValidatorEventDetails); ok {
					isOnDZ := details.OwnerPubkey != "" || details.DevicePK != ""
					// Disconnect/left events represent validators that *were* on DZ, so include
					// them in the "on_dz" filter even though current lookup shows them off DZ
					isDZRelated := details.Action == "validator_joined_dz" || details.Action == "validator_left_dz" || details.Action == "validator_stake_changed" || details.Action == "left_solana"
					if params.DZFilter == "on_dz" && (isOnDZ || isDZRelated) {
						filtered = append(filtered, e)
					} else if params.DZFilter == "off_dz" && !isOnDZ && !isDZRelated {
						filtered = append(filtered, e)
					}
				} else {
					// If we can't determine, include based on title
					isOnDZ := strings.HasPrefix(e.Title, "DZ ")
					if params.DZFilter == "on_dz" && isOnDZ {
						filtered = append(filtered, e)
					} else if params.DZFilter == "off_dz" && !isOnDZ {
						filtered = append(filtered, e)
					}
				}
			} else {
				// Non-Solana events pass through
				filtered = append(filtered, e)
			}
		}
		allEvents = filtered
	}

	// Filter by search terms if specified
	if len(params.Search) > 0 {
		filtered := make([]TimelineEvent, 0)
		for _, e := range allEvents {
			if eventMatchesSearch(e, params.Search) {
				filtered = append(filtered, e)
			}
		}
		allEvents = filtered
	}

	// Sort by timestamp descending, then by ID for consistent ordering
	sort.Slice(allEvents, func(i, j int) bool {
		if allEvents[i].Timestamp != allEvents[j].Timestamp {
			return allEvents[i].Timestamp > allEvents[j].Timestamp
		}
		return allEvents[i].ID > allEvents[j].ID
	})

	// Filter validator/gossip_node events by minimum stake share percentage
	if params.MinStakePct > 0 {
		filtered := make([]TimelineEvent, 0)
		for _, e := range allEvents {
			if e.EntityType == "validator" || e.EntityType == "gossip_node" {
				if details, ok := e.Details.(ValidatorEventDetails); ok {
					if e.EventType == "validator_stake_increased" || e.EventType == "validator_stake_decreased" {
						if math.Abs(details.StakeShareChangePct) >= params.MinStakePct {
							filtered = append(filtered, e)
						}
					} else if details.StakeSharePct >= params.MinStakePct {
						filtered = append(filtered, e)
					}
				} else {
					filtered = append(filtered, e)
				}
			} else {
				filtered = append(filtered, e)
			}
		}
		allEvents = filtered
	}

	total := len(allEvents)

	// Apply pagination
	startIdx := params.Offset
	endIdx := params.Offset + params.Limit
	if startIdx > len(allEvents) {
		startIdx = len(allEvents)
	}
	if endIdx > len(allEvents) {
		endIdx = len(allEvents)
	}
	paginatedEvents := allEvents[startIdx:endIdx]

	// Compute histogram from all events (before pagination)
	histogram := computeHistogram(allEvents, params.StartTime, params.EndTime)

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	resp := TimelineResponse{
		Events: paginatedEvents,
		Total:  total,
		Limit:  params.Limit,
		Offset: params.Offset,
		TimeRange: TimeRange{
			Start: params.StartTime.Format(time.RFC3339),
			End:   params.EndTime.Format(time.RFC3339),
		},
		Histogram: histogram,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("error encoding timeline response", "error", err)
	}
}

// computeHistogram creates time buckets from events for visualization
func computeHistogram(events []TimelineEvent, startTime, endTime time.Time) []HistogramBucket {
	if len(events) == 0 {
		return nil
	}

	// Calculate bucket size based on time range
	// Aim for ~24-48 buckets depending on range
	duration := endTime.Sub(startTime)
	var bucketDuration time.Duration
	switch {
	case duration <= 2*time.Hour:
		bucketDuration = 5 * time.Minute
	case duration <= 12*time.Hour:
		bucketDuration = 15 * time.Minute
	case duration <= 24*time.Hour:
		bucketDuration = 30 * time.Minute
	case duration <= 3*24*time.Hour:
		bucketDuration = 2 * time.Hour
	default:
		bucketDuration = 6 * time.Hour
	}

	// Create bucket map
	bucketCounts := make(map[time.Time]int)

	// Count events per bucket
	for _, event := range events {
		ts, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			continue
		}
		// Round down to bucket start
		bucketStart := ts.Truncate(bucketDuration)
		bucketCounts[bucketStart]++
	}

	// Generate all buckets in range (including empty ones)
	var buckets []HistogramBucket
	for t := startTime.Truncate(bucketDuration); !t.After(endTime); t = t.Add(bucketDuration) {
		buckets = append(buckets, HistogramBucket{
			Timestamp: t.Format(time.RFC3339),
			Count:     bucketCounts[t],
		})
	}

	return buckets
}

// entityChangeRow represents a normalized row from the entity_changes_v view.
type entityChangeRow struct {
	EntityType      string
	EntityID        string
	EntityPK        string
	EntityCode      string
	SnapshotTS      time.Time
	ChangeType      string
	ChangedFields   []string
	NewStatus       string
	ContributorCode string
	MetroCode       string
}

// queryEntityChanges queries the entity_changes_v view for all entity state changes.
func (a *API) queryEntityChanges(ctx context.Context, startTime, endTime time.Time, includeInternal bool) ([]entityChangeRow, error) {
	internalFilter := ""
	if !includeInternal && len(internalUserPubkeys) > 0 {
		internalFilter = fmt.Sprintf(" AND NOT (entity_type = 'user' AND entity_code IN ('%s'))", strings.Join(internalUserPubkeys, "','"))
	}

	query := fmt.Sprintf(`
		SELECT entity_type, entity_id, entity_pk, entity_code, snapshot_ts,
			   change_type, changed_fields, new_status, contributor_code, metro_code
		FROM entity_changes_v
		WHERE snapshot_ts >= ? AND snapshot_ts <= ?%s
		ORDER BY snapshot_ts DESC
		LIMIT 500
	`, internalFilter)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	var result []entityChangeRow
	for rows.Next() {
		var r entityChangeRow
		if err := rows.Scan(
			&r.EntityType, &r.EntityID, &r.EntityPK, &r.EntityCode, &r.SnapshotTS,
			&r.ChangeType, &r.ChangedFields, &r.NewStatus, &r.ContributorCode, &r.MetroCode,
		); err != nil {
			return nil, fmt.Errorf("entity change scan error: %w", err)
		}
		result = append(result, r)
	}
	return result, nil
}

// fetchDeviceChangeDetails batch-fetches device entity details for the given (entity_pk, snapshot_ts) pairs.
func (a *API) fetchDeviceChangeDetails(ctx context.Context, rows []entityChangeRow) (map[string]EntityChangeDetails, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Build pk list and ts range
	pks := make([]string, len(rows))
	for i, r := range rows {
		pks[i] = r.EntityPK
	}

	query := `
		WITH target AS (
			SELECT entity_id, snapshot_ts, pk, code, status, device_type, public_ip,
				   contributor_pk, metro_pk, max_users,
				   lag(status) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_status,
				   lag(device_type) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_device_type,
				   lag(public_ip) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_public_ip,
				   lag(contributor_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_contributor_pk,
				   lag(metro_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_metro_pk,
				   lag(max_users) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_max_users
			FROM dim_dz_devices_history
			WHERE pk IN (?)
		)
		SELECT t.entity_id, t.snapshot_ts, t.pk, t.code, t.status, t.device_type, t.public_ip,
			   t.contributor_pk, t.metro_pk, t.max_users,
			   t.prev_status, t.prev_device_type, t.prev_public_ip,
			   t.prev_contributor_pk, t.prev_metro_pk, t.prev_max_users,
			   COALESCE(c.code, '') AS contributor_code,
			   COALESCE(m.code, '') AS metro_code
		FROM target t
		LEFT JOIN dz_contributors_current c ON t.contributor_pk = c.pk
		LEFT JOIN dz_metros_current m ON t.metro_pk = m.pk
		WHERE (t.entity_id, t.snapshot_ts) IN (
			SELECT entity_id, snapshot_ts FROM dim_dz_devices_history
			WHERE pk IN (?) AND snapshot_ts IN (?)
		)
	`

	// Build snapshot_ts list
	tsList := make([]time.Time, len(rows))
	for i, r := range rows {
		tsList[i] = r.SnapshotTS
	}

	start := time.Now()
	dbRows, err := a.envDB(ctx).Query(ctx, query, pks, pks, tsList)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	// Build a set of requested (entityID, snapshotTS) for fast lookup
	type rowKey struct {
		entityID   string
		snapshotTS time.Time
	}
	requested := make(map[rowKey]entityChangeRow, len(rows))
	for _, r := range rows {
		requested[rowKey{r.EntityID, r.SnapshotTS}] = r
	}

	result := make(map[string]EntityChangeDetails)
	for dbRows.Next() {
		var (
			entityID        string
			snapshotTS      time.Time
			pk, code        string
			status          string
			deviceType      string
			publicIP        string
			contributorPK   string
			metroPK         string
			maxUsers        int32
			prevStatus      *string
			prevDeviceType  *string
			prevPublicIP    *string
			prevContribPK   *string
			prevMetroPK     *string
			prevMaxUsers    *int32
			contributorCode string
			metroCode       string
		)
		if err := dbRows.Scan(
			&entityID, &snapshotTS, &pk, &code, &status, &deviceType, &publicIP,
			&contributorPK, &metroPK, &maxUsers,
			&prevStatus, &prevDeviceType, &prevPublicIP, &prevContribPK, &prevMetroPK, &prevMaxUsers,
			&contributorCode, &metroCode,
		); err != nil {
			return nil, fmt.Errorf("device detail scan error: %w", err)
		}

		key := rowKey{entityID, snapshotTS}
		feedRow, ok := requested[key]
		if !ok {
			continue
		}

		var changes []FieldChange
		if feedRow.ChangeType == "updated" {
			if prevStatus != nil && *prevStatus != status {
				changes = append(changes, FieldChange{Field: "status", OldValue: *prevStatus, NewValue: status})
			}
			if prevDeviceType != nil && *prevDeviceType != deviceType {
				changes = append(changes, FieldChange{Field: "device_type", OldValue: *prevDeviceType, NewValue: deviceType})
			}
			if prevPublicIP != nil && *prevPublicIP != publicIP {
				changes = append(changes, FieldChange{Field: "public_ip", OldValue: *prevPublicIP, NewValue: publicIP})
			}
			if prevContribPK != nil && *prevContribPK != contributorPK {
				changes = append(changes, FieldChange{Field: "contributor", OldValue: *prevContribPK, NewValue: contributorPK})
			}
			if prevMetroPK != nil && *prevMetroPK != metroPK {
				changes = append(changes, FieldChange{Field: "metro", OldValue: *prevMetroPK, NewValue: metroPK})
			}
			if prevMaxUsers != nil && *prevMaxUsers != maxUsers {
				changes = append(changes, FieldChange{Field: "max_users", OldValue: *prevMaxUsers, NewValue: maxUsers})
			}
		}

		entity := DeviceEntity{
			PK:              pk,
			Code:            code,
			Status:          status,
			DeviceType:      deviceType,
			PublicIP:        publicIP,
			ContributorPK:   contributorPK,
			MetroPK:         metroPK,
			MaxUsers:        maxUsers,
			ContributorCode: contributorCode,
			MetroCode:       metroCode,
		}

		mapKey := entityID + snapshotTS.Format(time.RFC3339Nano)
		result[mapKey] = EntityChangeDetails{
			ChangeType: feedRow.ChangeType,
			Changes:    changes,
			Entity:     entity,
		}
	}
	return result, nil
}

// fetchLinkChangeDetails batch-fetches link entity details for the given feed rows.
func (a *API) fetchLinkChangeDetails(ctx context.Context, rows []entityChangeRow) (map[string]EntityChangeDetails, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	pks := make([]string, len(rows))
	tsList := make([]time.Time, len(rows))
	for i, r := range rows {
		pks[i] = r.EntityPK
		tsList[i] = r.SnapshotTS
	}

	query := `
		WITH target AS (
			SELECT entity_id, snapshot_ts, pk, code, status, link_type, tunnel_net,
				   contributor_pk, side_a_pk, side_z_pk, side_a_iface_name, side_z_iface_name,
				   committed_rtt_ns, committed_jitter_ns, bandwidth_bps, isis_delay_override_ns,
				   lag(status) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_status,
				   lag(link_type) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_link_type,
				   lag(tunnel_net) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_tunnel_net,
				   lag(contributor_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_contributor_pk,
				   lag(side_a_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_side_a_pk,
				   lag(side_z_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_side_z_pk,
				   lag(committed_rtt_ns) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_committed_rtt_ns,
				   lag(committed_jitter_ns) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_committed_jitter_ns,
				   lag(bandwidth_bps) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_bandwidth_bps,
				   lag(isis_delay_override_ns) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_isis_delay_override_ns
			FROM dim_dz_links_history
			WHERE pk IN (?)
		)
		SELECT t.entity_id, t.snapshot_ts, t.pk, t.code, t.status, t.link_type, t.tunnel_net,
			   t.contributor_pk, t.side_a_pk, t.side_z_pk, t.side_a_iface_name, t.side_z_iface_name,
			   t.committed_rtt_ns, t.committed_jitter_ns, t.bandwidth_bps, t.isis_delay_override_ns,
			   t.prev_status, t.prev_link_type, t.prev_tunnel_net, t.prev_contributor_pk,
			   t.prev_side_a_pk, t.prev_side_z_pk, t.prev_committed_rtt_ns, t.prev_committed_jitter_ns,
			   t.prev_bandwidth_bps, t.prev_isis_delay_override_ns,
			   COALESCE(c.code, '') AS contributor_code,
			   COALESCE(da.code, '') AS side_a_code,
			   COALESCE(dz.code, '') AS side_z_code,
			   COALESCE(ma.code, '') AS side_a_metro_code,
			   COALESCE(mz.code, '') AS side_z_metro_code,
			   COALESCE(ma.pk, '') AS side_a_metro_pk,
			   COALESCE(mz.pk, '') AS side_z_metro_pk
		FROM target t
		LEFT JOIN dz_contributors_current c ON t.contributor_pk = c.pk
		LEFT JOIN dz_devices_current da ON t.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON t.side_z_pk = dz.pk
		LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		WHERE (t.entity_id, t.snapshot_ts) IN (
			SELECT entity_id, snapshot_ts FROM dim_dz_links_history
			WHERE pk IN (?) AND snapshot_ts IN (?)
		)
	`

	start := time.Now()
	dbRows, err := a.envDB(ctx).Query(ctx, query, pks, pks, tsList)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	type rowKey struct {
		entityID   string
		snapshotTS time.Time
	}
	requested := make(map[rowKey]entityChangeRow, len(rows))
	for _, r := range rows {
		requested[rowKey{r.EntityID, r.SnapshotTS}] = r
	}

	result := make(map[string]EntityChangeDetails)
	for dbRows.Next() {
		var (
			entityID              string
			snapshotTS            time.Time
			pk, code              string
			status                string
			linkType              string
			tunnelNet             string
			contributorPK         string
			sideAPK               string
			sideZPK               string
			sideAIfaceName        string
			sideZIfaceName        string
			committedRttNs        int64
			committedJitterNs     int64
			bandwidthBps          int64
			isisDelayOverride     int64
			prevStatus            *string
			prevLinkType          *string
			prevTunnelNet         *string
			prevContributorPK     *string
			prevSideAPK           *string
			prevSideZPK           *string
			prevCommittedRttNs    *int64
			prevCommittedJitter   *int64
			prevBandwidthBps      *int64
			prevISISDelayOverride *int64
			contributorCode       string
			sideACode             string
			sideZCode             string
			sideAMetroCode        string
			sideZMetroCode        string
			sideAMetroPK          string
			sideZMetroPK          string
		)
		if err := dbRows.Scan(
			&entityID, &snapshotTS, &pk, &code, &status, &linkType, &tunnelNet,
			&contributorPK, &sideAPK, &sideZPK, &sideAIfaceName, &sideZIfaceName,
			&committedRttNs, &committedJitterNs, &bandwidthBps, &isisDelayOverride,
			&prevStatus, &prevLinkType, &prevTunnelNet, &prevContributorPK,
			&prevSideAPK, &prevSideZPK, &prevCommittedRttNs, &prevCommittedJitter,
			&prevBandwidthBps, &prevISISDelayOverride,
			&contributorCode, &sideACode, &sideZCode, &sideAMetroCode, &sideZMetroCode,
			&sideAMetroPK, &sideZMetroPK,
		); err != nil {
			return nil, fmt.Errorf("link detail scan error: %w", err)
		}

		key := rowKey{entityID, snapshotTS}
		feedRow, ok := requested[key]
		if !ok {
			continue
		}

		var changes []FieldChange
		if feedRow.ChangeType == "updated" {
			if prevStatus != nil && *prevStatus != status {
				changes = append(changes, FieldChange{Field: "status", OldValue: *prevStatus, NewValue: status})
			}
			if prevLinkType != nil && *prevLinkType != linkType {
				changes = append(changes, FieldChange{Field: "link_type", OldValue: *prevLinkType, NewValue: linkType})
			}
			if prevTunnelNet != nil && *prevTunnelNet != tunnelNet {
				changes = append(changes, FieldChange{Field: "tunnel_net", OldValue: *prevTunnelNet, NewValue: tunnelNet})
			}
			if prevContributorPK != nil && *prevContributorPK != contributorPK {
				changes = append(changes, FieldChange{Field: "contributor", OldValue: *prevContributorPK, NewValue: contributorPK})
			}
			if prevSideAPK != nil && *prevSideAPK != sideAPK {
				changes = append(changes, FieldChange{Field: "side_a", OldValue: *prevSideAPK, NewValue: sideAPK})
			}
			if prevSideZPK != nil && *prevSideZPK != sideZPK {
				changes = append(changes, FieldChange{Field: "side_z", OldValue: *prevSideZPK, NewValue: sideZPK})
			}
			if prevCommittedRttNs != nil && *prevCommittedRttNs != committedRttNs {
				changes = append(changes, FieldChange{Field: "committed_rtt", OldValue: *prevCommittedRttNs, NewValue: committedRttNs})
			}
			if prevCommittedJitter != nil && *prevCommittedJitter != committedJitterNs {
				changes = append(changes, FieldChange{Field: "committed_jitter", OldValue: *prevCommittedJitter, NewValue: committedJitterNs})
			}
			if prevBandwidthBps != nil && *prevBandwidthBps != bandwidthBps {
				changes = append(changes, FieldChange{Field: "bandwidth", OldValue: *prevBandwidthBps, NewValue: bandwidthBps})
			}
			if prevISISDelayOverride != nil && *prevISISDelayOverride != isisDelayOverride {
				changes = append(changes, FieldChange{Field: "isis_delay_override", OldValue: *prevISISDelayOverride, NewValue: isisDelayOverride})
			}
		}

		entity := LinkEntity{
			PK:                pk,
			Code:              code,
			Status:            status,
			LinkType:          linkType,
			TunnelNet:         tunnelNet,
			ContributorPK:     contributorPK,
			SideAPK:           sideAPK,
			SideZPK:           sideZPK,
			SideAIfaceName:    sideAIfaceName,
			SideZIfaceName:    sideZIfaceName,
			CommittedRttNs:    committedRttNs,
			CommittedJitterNs: committedJitterNs,
			BandwidthBps:      bandwidthBps,
			ISISDelayOverride: isisDelayOverride,
			ContributorCode:   contributorCode,
			SideACode:         sideACode,
			SideZCode:         sideZCode,
			SideAMetroCode:    sideAMetroCode,
			SideZMetroCode:    sideZMetroCode,
			SideAMetroPK:      sideAMetroPK,
			SideZMetroPK:      sideZMetroPK,
		}

		mapKey := entityID + snapshotTS.Format(time.RFC3339Nano)
		result[mapKey] = EntityChangeDetails{
			ChangeType: feedRow.ChangeType,
			Changes:    changes,
			Entity:     entity,
		}
	}
	return result, nil
}

// fetchMetroChangeDetails batch-fetches metro entity details for the given feed rows.
func (a *API) fetchMetroChangeDetails(ctx context.Context, rows []entityChangeRow) (map[string]EntityChangeDetails, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	pks := make([]string, len(rows))
	tsList := make([]time.Time, len(rows))
	for i, r := range rows {
		pks[i] = r.EntityPK
		tsList[i] = r.SnapshotTS
	}

	query := `
		WITH target AS (
			SELECT entity_id, snapshot_ts, pk, code, name, longitude, latitude,
				   lag(name) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_name,
				   lag(longitude) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_longitude,
				   lag(latitude) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_latitude
			FROM dim_dz_metros_history
			WHERE pk IN (?)
		)
		SELECT t.entity_id, t.snapshot_ts, t.pk, t.code, t.name, t.longitude, t.latitude,
			   t.prev_name, t.prev_longitude, t.prev_latitude
		FROM target t
		WHERE (t.entity_id, t.snapshot_ts) IN (
			SELECT entity_id, snapshot_ts FROM dim_dz_metros_history
			WHERE pk IN (?) AND snapshot_ts IN (?)
		)
	`

	start := time.Now()
	dbRows, err := a.envDB(ctx).Query(ctx, query, pks, pks, tsList)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	type rowKey struct {
		entityID   string
		snapshotTS time.Time
	}
	requested := make(map[rowKey]entityChangeRow, len(rows))
	for _, r := range rows {
		requested[rowKey{r.EntityID, r.SnapshotTS}] = r
	}

	result := make(map[string]EntityChangeDetails)
	for dbRows.Next() {
		var (
			entityID      string
			snapshotTS    time.Time
			pk, code      string
			name          string
			longitude     float64
			latitude      float64
			prevName      *string
			prevLongitude *float64
			prevLatitude  *float64
		)
		if err := dbRows.Scan(
			&entityID, &snapshotTS, &pk, &code, &name, &longitude, &latitude,
			&prevName, &prevLongitude, &prevLatitude,
		); err != nil {
			return nil, fmt.Errorf("metro detail scan error: %w", err)
		}

		key := rowKey{entityID, snapshotTS}
		feedRow, ok := requested[key]
		if !ok {
			continue
		}

		var changes []FieldChange
		if feedRow.ChangeType == "updated" {
			if prevName != nil && *prevName != name {
				changes = append(changes, FieldChange{Field: "name", OldValue: *prevName, NewValue: name})
			}
			if prevLongitude != nil && *prevLongitude != longitude {
				changes = append(changes, FieldChange{Field: "longitude", OldValue: *prevLongitude, NewValue: longitude})
			}
			if prevLatitude != nil && *prevLatitude != latitude {
				changes = append(changes, FieldChange{Field: "latitude", OldValue: *prevLatitude, NewValue: latitude})
			}
		}

		entity := MetroEntity{
			PK:        pk,
			Code:      code,
			Name:      name,
			Longitude: longitude,
			Latitude:  latitude,
		}

		mapKey := entityID + snapshotTS.Format(time.RFC3339Nano)
		result[mapKey] = EntityChangeDetails{
			ChangeType: feedRow.ChangeType,
			Changes:    changes,
			Entity:     entity,
		}
	}
	return result, nil
}

// fetchContributorChangeDetails batch-fetches contributor entity details for the given feed rows.
func (a *API) fetchContributorChangeDetails(ctx context.Context, rows []entityChangeRow) (map[string]EntityChangeDetails, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	pks := make([]string, len(rows))
	tsList := make([]time.Time, len(rows))
	for i, r := range rows {
		pks[i] = r.EntityPK
		tsList[i] = r.SnapshotTS
	}

	query := `
		WITH target AS (
			SELECT entity_id, snapshot_ts, pk, code, name,
				   lag(code) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_code,
				   lag(name) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_name
			FROM dim_dz_contributors_history
			WHERE pk IN (?)
		)
		SELECT t.entity_id, t.snapshot_ts, t.pk, t.code, t.name,
			   t.prev_code, t.prev_name
		FROM target t
		WHERE (t.entity_id, t.snapshot_ts) IN (
			SELECT entity_id, snapshot_ts FROM dim_dz_contributors_history
			WHERE pk IN (?) AND snapshot_ts IN (?)
		)
	`

	start := time.Now()
	dbRows, err := a.envDB(ctx).Query(ctx, query, pks, pks, tsList)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	type rowKey struct {
		entityID   string
		snapshotTS time.Time
	}
	requested := make(map[rowKey]entityChangeRow, len(rows))
	for _, r := range rows {
		requested[rowKey{r.EntityID, r.SnapshotTS}] = r
	}

	result := make(map[string]EntityChangeDetails)
	for dbRows.Next() {
		var (
			entityID   string
			snapshotTS time.Time
			pk, code   string
			name       string
			prevCode   *string
			prevName   *string
		)
		if err := dbRows.Scan(
			&entityID, &snapshotTS, &pk, &code, &name, &prevCode, &prevName,
		); err != nil {
			return nil, fmt.Errorf("contributor detail scan error: %w", err)
		}

		key := rowKey{entityID, snapshotTS}
		feedRow, ok := requested[key]
		if !ok {
			continue
		}

		var changes []FieldChange
		if feedRow.ChangeType == "updated" {
			if prevCode != nil && *prevCode != code {
				changes = append(changes, FieldChange{Field: "code", OldValue: *prevCode, NewValue: code})
			}
			if prevName != nil && *prevName != name {
				changes = append(changes, FieldChange{Field: "name", OldValue: *prevName, NewValue: name})
			}
		}

		entity := ContributorEntity{
			PK:   pk,
			Code: code,
			Name: name,
		}

		mapKey := entityID + snapshotTS.Format(time.RFC3339Nano)
		result[mapKey] = EntityChangeDetails{
			ChangeType: feedRow.ChangeType,
			Changes:    changes,
			Entity:     entity,
		}
	}
	return result, nil
}

// fetchUserChangeDetails batch-fetches user entity details for the given feed rows.
func (a *API) fetchUserChangeDetails(ctx context.Context, rows []entityChangeRow) (map[string]EntityChangeDetails, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	pks := make([]string, len(rows))
	tsList := make([]time.Time, len(rows))
	for i, r := range rows {
		pks[i] = r.EntityPK
		tsList[i] = r.SnapshotTS
	}

	query := `
		WITH target AS (
			SELECT entity_id, snapshot_ts, pk, owner_pubkey, kind, status, client_ip, dz_ip,
				   device_pk, tunnel_id,
				   lag(status) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_status,
				   lag(kind) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_kind,
				   lag(client_ip) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_client_ip,
				   lag(dz_ip) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_dz_ip,
				   lag(device_pk) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_device_pk,
				   lag(tunnel_id) OVER (PARTITION BY entity_id ORDER BY snapshot_ts, ingested_at, op_id) AS prev_tunnel_id
			FROM dim_dz_users_history
			WHERE pk IN (?) AND kind NOT IN ('validator', 'gossip_only')
		)
		SELECT t.entity_id, t.snapshot_ts, t.pk, t.owner_pubkey, t.kind, t.status,
			   t.client_ip, t.dz_ip, t.device_pk, t.tunnel_id,
			   t.prev_status, t.prev_kind, t.prev_client_ip, t.prev_dz_ip,
			   t.prev_device_pk, t.prev_tunnel_id,
			   COALESCE(d.code, '') AS device_code,
			   COALESCE(m.code, '') AS metro_code
		FROM target t
		LEFT JOIN dz_devices_current d ON t.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE (t.entity_id, t.snapshot_ts) IN (
			SELECT entity_id, snapshot_ts FROM dim_dz_users_history
			WHERE pk IN (?) AND snapshot_ts IN (?)
		)
	`

	start := time.Now()
	dbRows, err := a.envDB(ctx).Query(ctx, query, pks, pks, tsList)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	type rowKey struct {
		entityID   string
		snapshotTS time.Time
	}
	requested := make(map[rowKey]entityChangeRow, len(rows))
	for _, r := range rows {
		requested[rowKey{r.EntityID, r.SnapshotTS}] = r
	}

	result := make(map[string]EntityChangeDetails)
	for dbRows.Next() {
		var (
			entityID     string
			snapshotTS   time.Time
			pk           string
			ownerPubkey  string
			kind         string
			status       string
			clientIP     string
			dzIP         string
			devicePK     string
			tunnelID     int32
			prevStatus   *string
			prevKind     *string
			prevClientIP *string
			prevDZIP     *string
			prevDevicePK *string
			prevTunnelID *int32
			deviceCode   string
			metroCode    string
		)
		if err := dbRows.Scan(
			&entityID, &snapshotTS, &pk, &ownerPubkey, &kind, &status,
			&clientIP, &dzIP, &devicePK, &tunnelID,
			&prevStatus, &prevKind, &prevClientIP, &prevDZIP, &prevDevicePK, &prevTunnelID,
			&deviceCode, &metroCode,
		); err != nil {
			return nil, fmt.Errorf("user detail scan error: %w", err)
		}

		key := rowKey{entityID, snapshotTS}
		feedRow, ok := requested[key]
		if !ok {
			continue
		}

		var changes []FieldChange
		if feedRow.ChangeType == "updated" {
			if prevStatus != nil && *prevStatus != status {
				changes = append(changes, FieldChange{Field: "status", OldValue: *prevStatus, NewValue: status})
			}
			if prevKind != nil && *prevKind != kind {
				changes = append(changes, FieldChange{Field: "kind", OldValue: *prevKind, NewValue: kind})
			}
			if prevClientIP != nil && *prevClientIP != clientIP {
				changes = append(changes, FieldChange{Field: "client_ip", OldValue: *prevClientIP, NewValue: clientIP})
			}
			if prevDZIP != nil && *prevDZIP != dzIP {
				changes = append(changes, FieldChange{Field: "dz_ip", OldValue: *prevDZIP, NewValue: dzIP})
			}
			if prevDevicePK != nil && *prevDevicePK != devicePK {
				changes = append(changes, FieldChange{Field: "device", OldValue: *prevDevicePK, NewValue: devicePK})
			}
			if prevTunnelID != nil && *prevTunnelID != tunnelID {
				changes = append(changes, FieldChange{Field: "tunnel_id", OldValue: *prevTunnelID, NewValue: tunnelID})
			}
		}

		entity := UserEntity{
			PK:          pk,
			OwnerPubkey: ownerPubkey,
			Status:      status,
			Kind:        kind,
			ClientIP:    clientIP,
			DZIP:        dzIP,
			DevicePK:    devicePK,
			TunnelID:    tunnelID,
			DeviceCode:  deviceCode,
			MetroCode:   metroCode,
		}

		mapKey := entityID + snapshotTS.Format(time.RFC3339Nano)
		result[mapKey] = EntityChangeDetails{
			ChangeType: feedRow.ChangeType,
			Changes:    changes,
			Entity:     entity,
		}
	}
	return result, nil
}

// batchFetchEntityDetails fetches full entity details for all feed rows, grouped by entity type.
func (a *API) batchFetchEntityDetails(ctx context.Context, rows []entityChangeRow) (map[string]EntityChangeDetails, error) {
	// Group rows by entity type
	grouped := make(map[string][]entityChangeRow)
	for _, r := range rows {
		grouped[r.EntityType] = append(grouped[r.EntityType], r)
	}

	g, ctx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	result := make(map[string]EntityChangeDetails)

	fetchAndMerge := func(fetcher func(context.Context, []entityChangeRow) (map[string]EntityChangeDetails, error), typeRows []entityChangeRow) {
		g.Go(func() error {
			details, err := fetcher(ctx, typeRows)
			if err != nil {
				return err
			}
			mu.Lock()
			for k, v := range details {
				result[k] = v
			}
			mu.Unlock()
			return nil
		})
	}

	if typeRows, ok := grouped["device"]; ok {
		fetchAndMerge(a.fetchDeviceChangeDetails, typeRows)
	}
	if typeRows, ok := grouped["link"]; ok {
		fetchAndMerge(a.fetchLinkChangeDetails, typeRows)
	}
	if typeRows, ok := grouped["metro"]; ok {
		fetchAndMerge(a.fetchMetroChangeDetails, typeRows)
	}
	if typeRows, ok := grouped["contributor"]; ok {
		fetchAndMerge(a.fetchContributorChangeDetails, typeRows)
	}
	if typeRows, ok := grouped["user"]; ok {
		fetchAndMerge(a.fetchUserChangeDetails, typeRows)
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

// buildEntityChangeTitle builds the title for an entity change event.
func buildEntityChangeTitle(row entityChangeRow, details *EntityChangeDetails) string {
	entityLabel := strings.Title(row.EntityType) //nolint:staticcheck
	code := row.EntityCode

	// User codes are pubkeys - truncate for display
	if row.EntityType == "user" && len(code) > 8 {
		code = code[:8] + "..."
	}

	switch row.ChangeType {
	case "created":
		switch row.EntityType {
		case "metro":
			return fmt.Sprintf("Metro %s added", code)
		case "contributor":
			return fmt.Sprintf("Contributor %s added", code)
		case "user":
			if details != nil {
				if entity, ok := details.Entity.(UserEntity); ok && entity.DeviceCode != "" {
					return fmt.Sprintf("User %s connected to %s", code, entity.DeviceCode)
				}
			}
			return fmt.Sprintf("User %s created", code)
		default:
			return fmt.Sprintf("%s %s created", entityLabel, code)
		}
	case "deleted":
		switch row.EntityType {
		case "metro":
			return fmt.Sprintf("Metro %s removed", code)
		case "contributor":
			return fmt.Sprintf("Contributor %s removed", code)
		case "user":
			if details != nil {
				if entity, ok := details.Entity.(UserEntity); ok && entity.DeviceCode != "" {
					return fmt.Sprintf("User %s disconnected from %s", code, entity.DeviceCode)
				}
			}
			return fmt.Sprintf("User %s deleted", code)
		default:
			return fmt.Sprintf("%s %s deleted", entityLabel, code)
		}
	default:
		// Updated
		changes := row.ChangedFields
		if details != nil && len(details.Changes) > 0 {
			// Use detail changes for more accurate count
			changes = make([]string, len(details.Changes))
			for i, c := range details.Changes {
				changes[i] = c.Field
			}
		}

		if len(changes) == 1 {
			field := changes[0]
			switch field {
			case "status":
				switch row.EntityType {
				case "device":
					if row.NewStatus == "activated" {
						return fmt.Sprintf("Device %s activated", code)
					} else if row.NewStatus == "disabled" {
						return fmt.Sprintf("Device %s disabled", code)
					}
				case "link":
					if row.NewStatus == "activated" {
						return fmt.Sprintf("Link %s activated", code)
					} else if row.NewStatus == "disabled" {
						return fmt.Sprintf("Link %s disabled", code)
					}
				case "user":
					if row.NewStatus == "activated" {
						return fmt.Sprintf("User %s activated", code)
					} else if row.NewStatus == "disabled" {
						return fmt.Sprintf("User %s disabled", code)
					}
				}
				if details != nil && len(details.Changes) == 1 {
					c := details.Changes[0]
					return fmt.Sprintf("%s %s status: %s → %s", entityLabel, code, c.OldValue, c.NewValue)
				}
				return fmt.Sprintf("%s %s status changed", entityLabel, code)
			case "name":
				if details != nil && len(details.Changes) == 1 {
					c := details.Changes[0]
					return fmt.Sprintf("%s %s renamed: %s → %s", entityLabel, code, c.OldValue, c.NewValue)
				}
				return fmt.Sprintf("%s %s renamed", entityLabel, code)
			case "code":
				if row.EntityType == "contributor" && details != nil && len(details.Changes) == 1 {
					c := details.Changes[0]
					return fmt.Sprintf("Contributor code changed: %s → %s", c.OldValue, c.NewValue)
				}
			case "bandwidth":
				return fmt.Sprintf("Link %s bandwidth changed", code)
			case "committed_rtt":
				return fmt.Sprintf("Link %s committed RTT changed", code)
			case "isis_delay_override":
				return fmt.Sprintf("Link %s ISIS delay override changed", code)
			case "device":
				if row.EntityType == "user" && details != nil {
					if entity, ok := details.Entity.(UserEntity); ok && entity.DeviceCode != "" {
						return fmt.Sprintf("User %s moved to %s", code, entity.DeviceCode)
					}
				}
			}
			return fmt.Sprintf("%s %s %s changed", entityLabel, code, field)
		}
		return fmt.Sprintf("%s %s updated (%d fields)", entityLabel, code, len(changes))
	}
}

// buildEntityChangeSeverity determines the severity for an entity change event.
func buildEntityChangeSeverity(row entityChangeRow) string {
	switch row.ChangeType {
	case "deleted":
		return "warning"
	case "updated":
		if row.NewStatus == "disabled" {
			return "warning"
		}
	}
	return "info"
}

// queryEntityChangeEvents queries the entity_changes_v view and batch-fetches details,
// returning fully assembled TimelineEvent slice.
func (a *API) queryEntityChangeEvents(ctx context.Context, startTime, endTime time.Time, includeInternal bool) ([]TimelineEvent, error) {
	feedRows, err := a.queryEntityChanges(ctx, startTime, endTime, includeInternal)
	if err != nil {
		return nil, fmt.Errorf("entity changes feed: %w", err)
	}
	if len(feedRows) == 0 {
		return nil, nil
	}

	detailMap, err := a.batchFetchEntityDetails(ctx, feedRows)
	if err != nil {
		return nil, fmt.Errorf("entity changes details: %w", err)
	}

	events := make([]TimelineEvent, 0, len(feedRows))
	for _, row := range feedRows {
		var eventType string
		switch row.ChangeType {
		case "created":
			eventType = "entity_created"
		case "deleted":
			eventType = "entity_deleted"
		default:
			eventType = "entity_updated"
		}

		mapKey := row.EntityID + row.SnapshotTS.Format(time.RFC3339Nano)
		details, hasDetails := detailMap[mapKey]

		title := buildEntityChangeTitle(row, func() *EntityChangeDetails {
			if hasDetails {
				return &details
			}
			return nil
		}())

		severity := buildEntityChangeSeverity(row)

		event := TimelineEvent{
			ID:         generateEventID(row.EntityID, row.SnapshotTS, eventType),
			EventType:  eventType,
			Timestamp:  row.SnapshotTS.Format(time.RFC3339),
			Category:   "state_change",
			Severity:   severity,
			Title:      title,
			EntityType: row.EntityType,
			EntityPK:   row.EntityPK,
			EntityCode: row.EntityCode,
		}

		if hasDetails {
			event.Details = details
		} else {
			// Fallback: construct minimal details from feed row
			event.Details = EntityChangeDetails{
				ChangeType: row.ChangeType,
			}
		}

		events = append(events, event)
	}

	return events, nil
}

// incidentCategories lists all incident-related timeline categories.
var incidentCategories = []string{
	"packet_loss", "errors", "fcs", "discards", "carrier",
	"no_data", "isis_down", "isis_overload", "isis_unreachable",
}

func (a *API) queryIncidentEvents(ctx context.Context, startTime, endTime time.Time) ([]TimelineEvent, error) {
	query := `
		SELECT 'link' AS entity_type, entity_pk, incident_type, started_at, ended_at,
			is_ongoing, peak_value, duration_seconds,
			link_code AS entity_code, contributor_code,
			link_type, side_a_metro, side_z_metro, status,
			'' AS metro
		FROM link_incidents_v
		WHERE started_at <= ? AND (ended_at >= ? OR is_ongoing)
		UNION ALL
		SELECT 'device', entity_pk, incident_type, started_at, ended_at,
			is_ongoing, peak_value, duration_seconds,
			device_code, contributor_code,
			'' AS link_type, '' AS side_a_metro, '' AS side_z_metro, status,
			metro
		FROM device_incidents_v
		WHERE started_at <= ? AND (ended_at >= ? OR is_ongoing)
		ORDER BY started_at DESC
		LIMIT 400
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, endTime, startTime, endTime, startTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	var events []TimelineEvent
	for rows.Next() {
		var (
			entityType      string
			entityPK        string
			incidentType    string
			startedAt       time.Time
			endedAt         time.Time
			isOngoing       bool
			peakValue       float64
			durationSeconds int64
			entityCode      string
			contributorCode string
			linkType        string
			sideAMetro      string
			sideZMetro      string
			status          string
			metro           string
		)

		if err := rows.Scan(
			&entityType, &entityPK, &incidentType, &startedAt, &endedAt,
			&isOngoing, &peakValue, &durationSeconds,
			&entityCode, &contributorCode,
			&linkType, &sideAMetro, &sideZMetro, &status,
			&metro,
		); err != nil {
			return nil, fmt.Errorf("incident event scan error: %w", err)
		}

		details := IncidentEventDetails{
			EntityPK:        entityPK,
			EntityCode:      entityCode,
			EntityType:      entityType,
			IncidentType:    incidentType,
			PeakValue:       peakValue,
			DurationSeconds: durationSeconds,
			IsOngoing:       isOngoing,
			LinkType:        linkType,
			SideAMetro:      sideAMetro,
			SideZMetro:      sideZMetro,
			Metro:           metro,
			ContributorCode: contributorCode,
			Status:          status,
		}

		severity := timelineIncidentSeverity(incidentType, peakValue)

		// Emit incident_started event
		startTitle := fmt.Sprintf("%s started on %s", incidentTypeLabel(incidentType), entityCode)
		events = append(events, TimelineEvent{
			ID:         generateEventID(entityPK, startedAt, "incident_started_"+incidentType),
			EventType:  "incident_started",
			Timestamp:  startedAt.Format(time.RFC3339),
			Category:   incidentType,
			Severity:   severity,
			Title:      startTitle,
			EntityType: entityType,
			EntityPK:   entityPK,
			EntityCode: entityCode,
			Details:    details,
		})

		// Emit incident_ended event (only if not ongoing)
		if !isOngoing {
			endTitle := fmt.Sprintf("%s ended on %s", incidentTypeLabel(incidentType), entityCode)
			events = append(events, TimelineEvent{
				ID:         generateEventID(entityPK, endedAt, "incident_ended_"+incidentType),
				EventType:  "incident_ended",
				Timestamp:  endedAt.Format(time.RFC3339),
				Category:   incidentType,
				Severity:   "success",
				Title:      endTitle,
				EntityType: entityType,
				EntityPK:   entityPK,
				EntityCode: entityCode,
				Details:    details,
			})
		}
	}

	return events, nil
}

// timelineIncidentSeverity maps incident type and peak value to timeline severity
// (critical/warning/info — not the incidents page "incident"/"degraded" scale).
func timelineIncidentSeverity(incidentType string, peakValue float64) string {
	switch incidentType {
	case "packet_loss":
		if peakValue >= 50 {
			return "critical"
		}
		return "warning"
	case "carrier", "isis_down", "isis_overload", "isis_unreachable":
		return "critical"
	case "errors", "fcs":
		if peakValue >= 100 {
			return "critical"
		}
		return "warning"
	case "no_data":
		return "warning"
	case "discards":
		return "info"
	default:
		return "warning"
	}
}

// incidentTypeLabel returns a human-readable label for an incident type.
func incidentTypeLabel(incidentType string) string {
	switch incidentType {
	case "packet_loss":
		return "Packet loss"
	case "errors":
		return "Errors"
	case "fcs":
		return "FCS errors"
	case "discards":
		return "Discards"
	case "carrier":
		return "Carrier transitions"
	case "no_data":
		return "No data"
	case "isis_down":
		return "ISIS down"
	case "isis_overload":
		return "ISIS overload"
	case "isis_unreachable":
		return "ISIS unreachable"
	default:
		return incidentType
	}
}

func (a *API) queryValidatorEvents(ctx context.Context, startTime, endTime time.Time, includeInternal bool) ([]TimelineEvent, error) {
	// Build internal user filter
	internalFilter := ""
	if !includeInternal && len(internalUserPubkeys) > 0 {
		internalFilter = fmt.Sprintf(" AND u.owner_pubkey NOT IN ('%s')", strings.Join(internalUserPubkeys, "','"))
	}

	// Validators/gossip nodes are identified by joining users with gossip_nodes via client_ip = gossip_ip
	// A user is a "validator" if their gossip node has a vote account, otherwise "gossip_node"
	// We use history tables to detect both join AND leave events (current tables miss users who left)
	query := fmt.Sprintf(`
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		-- Get gossip IPs that were ever active during the time range (using history table)
		gossip_ips AS (
			SELECT DISTINCT gossip_ip
			FROM dim_solana_gossip_nodes_history
			WHERE snapshot_ts >= ? AND snapshot_ts <= ?
		),
		all_history AS (
			SELECT
				u.entity_id,
				u.snapshot_ts,
				u.pk,
				u.owner_pubkey,
				u.kind,
				u.status,
				u.is_deleted,
				u.dz_ip,
				u.client_ip,
				u.device_pk,
				u.attrs_hash,
				row_number() OVER (PARTITION BY u.entity_id ORDER BY u.snapshot_ts, u.ingested_at, u.op_id) as row_num,
				lag(u.status) OVER (PARTITION BY u.entity_id ORDER BY u.snapshot_ts, u.ingested_at, u.op_id) as prev_status,
				lag(u.is_deleted) OVER (PARTITION BY u.entity_id ORDER BY u.snapshot_ts, u.ingested_at, u.op_id) as prev_is_deleted,
				lag(u.attrs_hash) OVER (PARTITION BY u.entity_id ORDER BY u.snapshot_ts, u.ingested_at, u.op_id) as prev_attrs_hash
			FROM dim_dz_users_history u
			-- Include users whose client_ip was in the gossip nodes during the time range
			WHERE u.client_ip IN (SELECT gossip_ip FROM gossip_ips)%s
		),
		-- Get latest gossip node info for each IP from history (for validators who may have left)
		latest_gossip AS (
			SELECT gossip_ip, argMax(pubkey, snapshot_ts) as pubkey
			FROM dim_solana_gossip_nodes_history
			WHERE snapshot_ts >= ? AND snapshot_ts <= ?
			GROUP BY gossip_ip
		),
		-- Get latest vote account info for each node from history
		latest_vote AS (
			SELECT node_pubkey, vote_pubkey, argMax(activated_stake_lamports, snapshot_ts) as stake_lamports
			FROM dim_solana_vote_accounts_history
			WHERE snapshot_ts >= ? AND snapshot_ts <= ?
			GROUP BY node_pubkey, vote_pubkey
		)
		SELECT
			uc.entity_id,
			uc.snapshot_ts,
			uc.pk,
			uc.owner_pubkey,
			uc.kind,
			uc.status,
			uc.is_deleted,
			uc.prev_status,
			COALESCE(uc.dz_ip, '') as dz_ip,
			COALESCE(uc.device_pk, '') as device_pk,
			COALESCE(d.code, '') as device_code,
			COALESCE(m.code, '') as metro_code,
			COALESCE(cont.code, '') as contributor_code,
			-- Use current gossip info if available, fall back to historical
			COALESCE(gn_curr.pubkey, gn_hist.pubkey, '') as node_pubkey,
			COALESCE(va_curr.vote_pubkey, va_hist.vote_pubkey, '') as vote_pubkey,
			COALESCE(va_curr.activated_stake_lamports, va_hist.stake_lamports, 0) as stake_lamports,
			COALESCE(va_curr.activated_stake_lamports, va_hist.stake_lamports, 0) * 100.0 / NULLIF(ts.total, 0) as stake_share_pct,
			CASE WHEN COALESCE(va_curr.vote_pubkey, va_hist.vote_pubkey, '') != '' THEN 'validator' ELSE 'gossip_only' END as validator_kind
		FROM all_history uc
		CROSS JOIN total_stake ts
		LEFT JOIN dz_devices_current d ON uc.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current cont ON d.contributor_pk = cont.pk
		-- Current gossip/vote info (for active validators)
		LEFT JOIN solana_gossip_nodes_current gn_curr ON uc.client_ip = gn_curr.gossip_ip
		LEFT JOIN solana_vote_accounts_current va_curr ON gn_curr.pubkey = va_curr.node_pubkey
		-- Historical gossip/vote info (for validators who have left)
		LEFT JOIN latest_gossip gn_hist ON uc.client_ip = gn_hist.gossip_ip
		LEFT JOIN latest_vote va_hist ON gn_hist.pubkey = va_hist.node_pubkey
		WHERE (uc.attrs_hash != uc.prev_attrs_hash OR uc.prev_attrs_hash = 0)
		  AND ((uc.status = 'activated' AND uc.prev_status != 'activated')
		       OR (uc.status != 'activated' AND uc.prev_status = 'activated')
		       OR (uc.is_deleted = 1 AND (uc.prev_is_deleted = 0 OR uc.prev_is_deleted IS NULL) AND uc.prev_status = 'activated'))
		  AND uc.snapshot_ts >= ? AND uc.snapshot_ts <= ?
		ORDER BY uc.snapshot_ts DESC, uc.entity_id
		LIMIT 200
	`, internalFilter)

	start := time.Now()
	// Query has 4 pairs of time parameters: gossip_ips, latest_gossip, latest_vote, and final WHERE
	rows, err := a.envDB(ctx).Query(ctx, query,
		startTime, endTime, // gossip_ips CTE
		startTime, endTime, // latest_gossip CTE
		startTime, endTime, // latest_vote CTE
		startTime, endTime, // final WHERE clause
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	var events []TimelineEvent
	for rows.Next() {
		var (
			entityID        string
			snapshotTS      time.Time
			pk              string
			ownerPubkey     string
			kind            string
			status          string
			isDeleted       uint8
			prevStatus      *string
			dzIP            string
			devicePK        string
			deviceCode      string
			metroCode       string
			contributorCode string
			nodePubkey      string
			votePubkey      string
			stakeLamports   int64
			stakeSharePct   float64
			validatorKind   string // "validator" or "gossip_only" based on vote account presence
		)

		if err := rows.Scan(&entityID, &snapshotTS, &pk, &ownerPubkey, &kind, &status, &isDeleted, &prevStatus, &dzIP, &devicePK, &deviceCode, &metroCode, &contributorCode, &nodePubkey, &votePubkey, &stakeLamports, &stakeSharePct, &validatorKind); err != nil {
			return nil, fmt.Errorf("validator event scan error: %w", err)
		}
		var title string
		var action string
		var eventType string
		var severity string

		stakeSol := float64(stakeLamports) / 1_000_000_000
		entityTypeStr := "validator"
		if validatorKind == "gossip_only" {
			entityTypeStr = "gossip_node"
		}

		isJoining := isDeleted == 0 && status == "activated" && (prevStatus == nil || *prevStatus != "activated")

		if isJoining {
			if validatorKind == "validator" {
				eventType = "validator_joined_dz"
				action = "validator_joined_dz"
				title = "Validator joined DZ"
			} else {
				title = "Gossip node joined DZ"
				eventType = "gossip_node_joined_dz"
				action = "joined"
			}
			severity = "info"
		} else {
			if validatorKind == "validator" {
				eventType = "validator_left_dz"
				action = "validator_left_dz"
				title = "Validator left DZ"
			} else {
				title = "Gossip node left DZ"
				eventType = "gossip_node_left_dz"
				action = "left"
			}
			severity = "warning"
		}

		events = append(events, TimelineEvent{
			ID:          generateEventID(entityID, snapshotTS, eventType),
			EventType:   eventType,
			Timestamp:   snapshotTS.Format(time.RFC3339),
			Category:    "state_change",
			Severity:    severity,
			Title:       title,
			Description: "",
			EntityType:  entityTypeStr,
			EntityPK:    pk,
			EntityCode:  ownerPubkey, // Full pubkey - frontend handles truncation
			Details: ValidatorEventDetails{
				OwnerPubkey:     ownerPubkey,
				DZIP:            dzIP,
				VotePubkey:      votePubkey,
				NodePubkey:      nodePubkey,
				StakeLamports:   stakeLamports,
				StakeSol:        stakeSol,
				StakeSharePct:   stakeSharePct,
				UserPK:          pk,
				DevicePK:        devicePK,
				DeviceCode:      deviceCode,
				MetroCode:       metroCode,
				ContributorCode: contributorCode,
				Kind:            validatorKind,
				Action:          action,
			},
		})
	}

	return events, nil
}

// queryGossipNetworkChanges detects when gossip nodes appear or disappear from the Solana network
// This is separate from DZ user status - it tracks the Solana gossip network itself
func (a *API) queryGossipNetworkChanges(ctx context.Context, startTime, endTime time.Time) ([]TimelineEvent, error) {
	// Find gossip nodes that disappeared from the network
	// by tracking node PUBKEYS (not IPs) that are no longer in the current gossip table
	// This correctly handles validators that change IP addresses
	query := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		-- Current node pubkeys (nodes that are still online)
		current_pubkeys AS (
			SELECT DISTINCT pubkey FROM solana_gossip_nodes_current
		),
		-- Find nodes (by pubkey) that were seen in history but are no longer in current
		-- Their "offline" time is their last seen timestamp
		disappeared AS (
			SELECT
				gn.pubkey,
				argMax(gn.gossip_ip, gn.snapshot_ts) as last_gossip_ip,
				max(gn.snapshot_ts) as last_seen_ts
			FROM dim_solana_gossip_nodes_history gn
			WHERE gn.pubkey NOT IN (SELECT pubkey FROM current_pubkeys)
			GROUP BY gn.pubkey
			HAVING max(gn.snapshot_ts) >= ? AND max(gn.snapshot_ts) <= ?
		)
		SELECT
			d.last_gossip_ip as gossip_ip,
			d.pubkey as node_pubkey,
			d.last_seen_ts as event_ts,
			'offline' as change_type,
			COALESCE(va_hist.vote_pubkey, '') as vote_pubkey,
			COALESCE(va_hist.stake_lamports, 0) as stake_lamports,
			COALESCE(va_hist.stake_lamports * 100.0 / NULLIF(ts.total, 0), 0) as stake_share_pct,
			COALESCE(u.owner_pubkey, '') as dz_owner_pubkey,
			COALESCE(u.pk, '') as user_pk,
			COALESCE(dev.code, '') as device_code,
			COALESCE(dev.pk, '') as device_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(cont.code, '') as contributor_code
		FROM disappeared d
		CROSS JOIN total_stake ts
		-- Get historical vote account info (since node is offline, won't be in current)
		LEFT JOIN (
			SELECT node_pubkey, argMax(vote_pubkey, snapshot_ts) as vote_pubkey,
			       argMax(activated_stake_lamports, snapshot_ts) as stake_lamports
			FROM dim_solana_vote_accounts_history
			GROUP BY node_pubkey
		) va_hist ON d.pubkey = va_hist.node_pubkey
		-- Check if this node was connected to DZ (using last known IP)
		LEFT JOIN dz_users_current u ON d.last_gossip_ip = u.client_ip
		LEFT JOIN dz_devices_current dev ON u.device_pk = dev.pk
		LEFT JOIN dz_metros_current m ON dev.metro_pk = m.pk
		LEFT JOIN dz_contributors_current cont ON dev.contributor_pk = cont.pk
		ORDER BY d.last_seen_ts DESC, d.pubkey
		LIMIT 100
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	var events []TimelineEvent
	for rows.Next() {
		var (
			gossipIP        string
			nodePubkey      string
			eventTS         time.Time
			changeType      string
			votePubkey      string
			stakeLamports   int64
			stakeSharePct   float64
			dzOwnerPubkey   string
			userPK          string
			deviceCode      string
			devicePK        string
			metroCode       string
			contributorCode string
		)

		if err := rows.Scan(&gossipIP, &nodePubkey, &eventTS, &changeType, &votePubkey, &stakeLamports, &stakeSharePct, &dzOwnerPubkey, &userPK, &deviceCode, &devicePK, &metroCode, &contributorCode); err != nil {
			return nil, fmt.Errorf("gossip network change scan error: %w", err)
		}
		stakeSol := float64(stakeLamports) / 1_000_000_000
		isValidator := votePubkey != ""

		var title string
		var eventType string
		var entityType string
		var severity string

		if changeType == "offline" {
			if isValidator {
				title = "Validator left Solana network"
				eventType = "validator_left_solana"
				entityType = "validator"
			} else {
				title = "Gossip node left Solana network"
				eventType = "gossip_node_left_solana"
				entityType = "gossip_node"
			}
			severity = "warning"
		}

		// Use node pubkey as entity code since that's the Solana identity
		entityCode := nodePubkey
		entityPK := nodePubkey

		events = append(events, TimelineEvent{
			ID:          generateEventID(nodePubkey, eventTS, eventType),
			EventType:   eventType,
			Timestamp:   eventTS.Format(time.RFC3339),
			Category:    "state_change",
			Severity:    severity,
			Title:       title,
			Description: "",
			EntityType:  entityType,
			EntityPK:    entityPK,
			EntityCode:  entityCode,
			Details: ValidatorEventDetails{
				OwnerPubkey:     dzOwnerPubkey,
				DZIP:            gossipIP,
				VotePubkey:      votePubkey,
				NodePubkey:      nodePubkey,
				StakeLamports:   stakeLamports,
				StakeSol:        stakeSol,
				StakeSharePct:   stakeSharePct,
				UserPK:          userPK,
				DevicePK:        devicePK,
				DeviceCode:      deviceCode,
				MetroCode:       metroCode,
				ContributorCode: contributorCode,
				Kind:            map[bool]string{true: "validator", false: "gossip_only"}[isValidator],
				Action:          "left_solana",
			},
		})
	}

	return events, nil
}

func (a *API) queryVoteAccountChanges(ctx context.Context, startTime, endTime time.Time) ([]TimelineEvent, error) {
	// Track validators (vote accounts) joining or leaving the network
	// A validator "joins" when their vote_pubkey first appears in the vote accounts table
	// A validator "leaves" when their vote_pubkey is no longer in the current vote accounts table
	query := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		-- IPs that are connected to DZ (current state)
		dz_ips AS (
			SELECT DISTINCT client_ip FROM dz_users_current WHERE client_ip != ''
		),
		-- Current vote pubkeys (validators that are currently active)
		current_vote_pubkeys AS (
			SELECT DISTINCT vote_pubkey FROM solana_vote_accounts_current
		),
		-- Find validators that left: were in history but not in current
		-- Their "left" time is their last seen timestamp
		left_validators AS (
			SELECT
				va.vote_pubkey,
				argMax(va.node_pubkey, va.snapshot_ts) as node_pubkey,
				argMax(va.activated_stake_lamports, va.snapshot_ts) as last_stake,
				max(va.snapshot_ts) as last_seen_ts
			FROM dim_solana_vote_accounts_history va
			WHERE va.vote_pubkey NOT IN (SELECT vote_pubkey FROM current_vote_pubkeys)
			  AND va.activated_stake_lamports > 0
			GROUP BY va.vote_pubkey
			HAVING max(va.snapshot_ts) >= ? AND max(va.snapshot_ts) <= ?
		),
		-- Find validators that joined: first appeared within the time range
		joined_validators AS (
			SELECT
				va.vote_pubkey,
				argMin(va.node_pubkey, va.snapshot_ts) as node_pubkey,
				argMin(va.activated_stake_lamports, va.snapshot_ts) as first_stake,
				min(va.snapshot_ts) as first_seen_ts
			FROM dim_solana_vote_accounts_history va
			WHERE va.activated_stake_lamports > 0
			GROUP BY va.vote_pubkey
			HAVING min(va.snapshot_ts) >= ? AND min(va.snapshot_ts) <= ?
		)
		SELECT
			lv.vote_pubkey,
			lv.node_pubkey,
			lv.last_seen_ts as event_ts,
			'left' as change_type,
			lv.last_stake as stake_lamports,
			lv.last_stake * 100.0 / NULLIF(ts.total, 0) as stake_share_pct,
			COALESCE(gn.gossip_ip, '') as gossip_ip,
			COALESCE(u.owner_pubkey, '') as dz_owner_pubkey,
			COALESCE(u.pk, '') as user_pk,
			COALESCE(dev.code, '') as device_code,
			COALESCE(dev.pk, '') as device_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(cont.code, '') as contributor_code
		FROM left_validators lv
		CROSS JOIN total_stake ts
		LEFT JOIN solana_gossip_nodes_current gn ON lv.node_pubkey = gn.pubkey
		LEFT JOIN dz_users_current u ON gn.gossip_ip = u.client_ip
		LEFT JOIN dz_devices_current dev ON u.device_pk = dev.pk
		LEFT JOIN dz_metros_current m ON dev.metro_pk = m.pk
		LEFT JOIN dz_contributors_current cont ON dev.contributor_pk = cont.pk

		UNION ALL

		SELECT
			jv.vote_pubkey,
			jv.node_pubkey,
			jv.first_seen_ts as event_ts,
			'joined' as change_type,
			jv.first_stake as stake_lamports,
			jv.first_stake * 100.0 / NULLIF(ts.total, 0) as stake_share_pct,
			COALESCE(gn.gossip_ip, '') as gossip_ip,
			COALESCE(u.owner_pubkey, '') as dz_owner_pubkey,
			COALESCE(u.pk, '') as user_pk,
			COALESCE(dev.code, '') as device_code,
			COALESCE(dev.pk, '') as device_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(cont.code, '') as contributor_code
		FROM joined_validators jv
		CROSS JOIN total_stake ts
		LEFT JOIN solana_gossip_nodes_current gn ON jv.node_pubkey = gn.pubkey
		LEFT JOIN dz_users_current u ON gn.gossip_ip = u.client_ip
		LEFT JOIN dz_devices_current dev ON u.device_pk = dev.pk
		LEFT JOIN dz_metros_current m ON dev.metro_pk = m.pk
		LEFT JOIN dz_contributors_current cont ON dev.contributor_pk = cont.pk

		ORDER BY event_ts DESC, vote_pubkey
		LIMIT 100
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, startTime, endTime, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	var events []TimelineEvent
	for rows.Next() {
		var (
			votePubkey      string
			nodePubkey      string
			eventTS         time.Time
			changeType      string
			stakeLamports   int64
			stakeSharePct   float64
			gossipIP        string
			dzOwnerPubkey   string
			userPK          string
			deviceCode      string
			devicePK        string
			metroCode       string
			contributorCode string
		)

		if err := rows.Scan(&votePubkey, &nodePubkey, &eventTS, &changeType, &stakeLamports, &stakeSharePct, &gossipIP, &dzOwnerPubkey, &userPK, &deviceCode, &devicePK, &metroCode, &contributorCode); err != nil {
			return nil, fmt.Errorf("vote account change scan error: %w", err)
		}
		stakeSol := float64(stakeLamports) / 1_000_000_000

		var title string
		var eventType string
		var severity string

		if changeType == "left" {
			title = "Validator left Solana network"
			eventType = "validator_left_solana"
			severity = "warning"
		} else {
			title = "Validator joined Solana network"
			eventType = "validator_joined_solana"
			severity = "info"
		}

		events = append(events, TimelineEvent{
			ID:          generateEventID(votePubkey, eventTS, eventType),
			EventType:   eventType,
			Timestamp:   eventTS.Format(time.RFC3339),
			Category:    "state_change",
			Severity:    severity,
			Title:       title,
			Description: "",
			EntityType:  "validator",
			EntityPK:    votePubkey,
			EntityCode:  votePubkey,
			Details: ValidatorEventDetails{
				OwnerPubkey:     dzOwnerPubkey,
				DZIP:            gossipIP,
				VotePubkey:      votePubkey,
				NodePubkey:      nodePubkey,
				StakeLamports:   stakeLamports,
				StakeSol:        stakeSol,
				StakeSharePct:   stakeSharePct,
				UserPK:          userPK,
				DevicePK:        devicePK,
				DeviceCode:      deviceCode,
				MetroCode:       metroCode,
				ContributorCode: contributorCode,
				Kind:            "validator",
				Action:          eventType,
			},
		})
	}

	return events, nil
}

func (a *API) queryStakeChanges(ctx context.Context, startTime, endTime time.Time) ([]TimelineEvent, error) {
	// Track significant stake changes for validators
	// A significant change is >10k SOL or >5% change
	query := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		dz_ips AS (
			SELECT DISTINCT client_ip FROM dz_users_current WHERE client_ip != ''
		),
		-- Get stake snapshots within the time range
		stake_snapshots AS (
			SELECT
				va.vote_pubkey,
				va.node_pubkey,
				va.snapshot_ts,
				va.activated_stake_lamports as stake,
				lagInFrame(va.activated_stake_lamports) OVER (PARTITION BY va.vote_pubkey ORDER BY va.snapshot_ts) as prev_stake
			FROM dim_solana_vote_accounts_history va
			WHERE va.snapshot_ts >= ? AND va.snapshot_ts <= ?
			  AND va.activated_stake_lamports > 0
		),
		-- Find significant changes
		significant_changes AS (
			SELECT
				vote_pubkey,
				node_pubkey,
				snapshot_ts,
				stake,
				prev_stake,
				toInt64(stake) - toInt64(prev_stake) as change,
				CASE WHEN prev_stake > 0 THEN (toInt64(stake) - toInt64(prev_stake)) * 100.0 / prev_stake ELSE 0 END as change_pct
			FROM stake_snapshots
			WHERE prev_stake IS NOT NULL
			  AND prev_stake > 0
			  AND (
			  	abs(toInt64(stake) - toInt64(prev_stake)) >= 10000000000000  -- >10k SOL in lamports
			  	OR abs((toInt64(stake) - toInt64(prev_stake)) * 100.0 / prev_stake) >= 5  -- >5% change
			  )
		)
		SELECT
			sc.vote_pubkey,
			sc.node_pubkey,
			sc.snapshot_ts as event_ts,
			toInt64(sc.stake) as current_stake,
			toInt64(sc.prev_stake) as prev_stake,
			sc.change,
			sc.change_pct,
			sc.stake * 100.0 / NULLIF(ts.total, 0) as stake_share_pct,
			sc.change * 100.0 / NULLIF(ts.total, 0) as stake_share_change_pct,
			COALESCE(gn.gossip_ip, '') as gossip_ip,
			gn.gossip_ip IN (SELECT client_ip FROM dz_ips) as is_on_dz,
			COALESCE(u.owner_pubkey, '') as dz_owner_pubkey,
			COALESCE(u.pk, '') as user_pk,
			COALESCE(dev.code, '') as device_code,
			COALESCE(dev.pk, '') as device_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(cont.code, '') as contributor_code
		FROM significant_changes sc
		CROSS JOIN total_stake ts
		LEFT JOIN solana_gossip_nodes_current gn ON sc.node_pubkey = gn.pubkey
		LEFT JOIN dz_users_current u ON gn.gossip_ip = u.client_ip
		LEFT JOIN dz_devices_current dev ON u.device_pk = dev.pk
		LEFT JOIN dz_metros_current m ON dev.metro_pk = m.pk
		LEFT JOIN dz_contributors_current cont ON dev.contributor_pk = cont.pk
		ORDER BY sc.snapshot_ts DESC, sc.vote_pubkey
		LIMIT 200
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	var events []TimelineEvent
	for rows.Next() {
		var (
			votePubkey          string
			nodePubkey          string
			eventTS             time.Time
			currentStake        int64
			prevStake           int64
			change              int64
			changePct           float64
			stakeSharePct       float64
			stakeShareChangePct float64
			gossipIP            string
			isOnDZ              bool
			dzOwnerPubkey       string
			userPK              string
			deviceCode          string
			devicePK            string
			metroCode           string
			contributorCode     string
		)

		if err := rows.Scan(&votePubkey, &nodePubkey, &eventTS, &currentStake, &prevStake, &change, &changePct, &stakeSharePct, &stakeShareChangePct, &gossipIP, &isOnDZ, &dzOwnerPubkey, &userPK, &deviceCode, &devicePK, &metroCode, &contributorCode); err != nil {
			return nil, fmt.Errorf("stake change scan error: %w", err)
		}

		currentSol := float64(currentStake) / 1_000_000_000

		var title string
		var eventType string
		var severity string
		var action string

		// Prefix with DZ status
		dzPrefix := ""
		if isOnDZ {
			dzPrefix = "DZ "
		}

		if change > 0 {
			title = fmt.Sprintf("%sValidator stake increased", dzPrefix)
			eventType = "validator_stake_increased"
			severity = "info"
			action = "increased"
		} else {
			title = fmt.Sprintf("%sValidator stake decreased", dzPrefix)
			eventType = "validator_stake_decreased"
			severity = "warning"
			action = "decreased"
		}

		events = append(events, TimelineEvent{
			ID:          generateEventID(votePubkey, eventTS, eventType),
			EventType:   eventType,
			Timestamp:   eventTS.Format(time.RFC3339),
			Category:    "state_change",
			Severity:    severity,
			Title:       title,
			Description: "",
			EntityType:  "validator",
			EntityPK:    votePubkey,
			EntityCode:  votePubkey,
			Details: ValidatorEventDetails{
				OwnerPubkey:         dzOwnerPubkey,
				DZIP:                gossipIP,
				VotePubkey:          votePubkey,
				NodePubkey:          nodePubkey,
				StakeLamports:       currentStake,
				StakeSol:            currentSol,
				StakeSharePct:       stakeSharePct,
				StakeShareChangePct: stakeShareChangePct,
				UserPK:              userPK,
				DevicePK:            devicePK,
				DeviceCode:          deviceCode,
				MetroCode:           metroCode,
				ContributorCode:     contributorCode,
				Kind:                "validator",
				Action:              action,
			},
		})
	}

	return events, nil
}

// queryDZStakeAttribution finds snapshots where the DZ total stake changed significantly
// and attributes the change to specific validators (connected, disconnected, stake changed, or left).
func (a *API) queryDZStakeAttribution(ctx context.Context, startTime, endTime time.Time) ([]TimelineEvent, error) {
	// Phase 1: Find interesting snapshot pairs where DZ total changed significantly
	querySnapshots := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		dz_ips AS (
			SELECT DISTINCT client_ip FROM dz_users_current WHERE client_ip != ''
		),
		per_validator AS (
			SELECT
				va.snapshot_ts,
				toInt64(va.activated_stake_lamports) * toInt64(COALESCE(gn.gossip_ip, '') IN (SELECT client_ip FROM dz_ips)) as dz_contribution
			FROM dim_solana_vote_accounts_history va
			ASOF LEFT JOIN dim_solana_gossip_nodes_history gn
				ON va.node_pubkey = gn.pubkey AND va.snapshot_ts >= gn.snapshot_ts
			WHERE va.activated_stake_lamports > 0
				AND va.snapshot_ts >= ? AND va.snapshot_ts <= ?
		),
		dz_totals AS (
			SELECT
				snapshot_ts,
				sum(dz_contribution) as dz_stake,
				lagInFrame(sum(dz_contribution)) OVER (ORDER BY snapshot_ts) as prev_dz_stake,
				lagInFrame(snapshot_ts) OVER (ORDER BY snapshot_ts) as prev_snapshot_ts,
				row_number() OVER (ORDER BY snapshot_ts) as rn
			FROM per_validator
			GROUP BY snapshot_ts
		)
		SELECT
			snapshot_ts,
			prev_snapshot_ts,
			dz_stake * 100.0 / NULLIF(ts.total, 0) as dz_total_pct,
			prev_dz_stake * 100.0 / NULLIF(ts.total, 0) as prev_dz_total_pct
		FROM dz_totals
		CROSS JOIN total_stake ts
		WHERE rn > 1
			AND toInt64(dz_stake) != toInt64(prev_dz_stake)
		ORDER BY abs(toInt64(dz_stake) - toInt64(prev_dz_stake)) DESC
	`

	start := time.Now()
	snapRows, err := a.envDB(ctx).Query(ctx, querySnapshots, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer snapRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start), err)

	type snapshotPair struct {
		currTS         time.Time
		prevTS         time.Time
		dzTotalPct     float64
		prevDZTotalPct float64
	}
	var pairs []snapshotPair
	tsSet := make(map[time.Time]bool)
	for snapRows.Next() {
		var curr, prev time.Time
		var dzTotalPct, prevDZTotalPct float64
		if err := snapRows.Scan(&curr, &prev, &dzTotalPct, &prevDZTotalPct); err != nil {
			return nil, fmt.Errorf("snapshot pair scan error: %w", err)
		}
		pairs = append(pairs, snapshotPair{curr, prev, dzTotalPct, prevDZTotalPct})
		tsSet[curr] = true
		tsSet[prev] = true
	}

	if len(pairs) == 0 {
		return nil, nil
	}

	// Build IN clause for all interesting timestamps
	tsList := make([]string, 0, len(tsSet))
	for ts := range tsSet {
		tsList = append(tsList, fmt.Sprintf("'%s'", ts.Format("2006-01-02 15:04:05")))
	}
	tsInClause := strings.Join(tsList, ",")

	// Phase 2: Get per-validator data at those timestamps
	queryValidators := fmt.Sprintf(`
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		dz_ips AS (
			SELECT DISTINCT client_ip FROM dz_users_current WHERE client_ip != ''
		)
		SELECT
			va.vote_pubkey,
			va.node_pubkey,
			va.snapshot_ts,
			va.activated_stake_lamports as stake,
			COALESCE(gn.gossip_ip, '') as gossip_ip,
			COALESCE(gn.gossip_ip, '') IN (SELECT client_ip FROM dz_ips) as is_on_dz,
			toInt64(va.activated_stake_lamports) * toInt64(COALESCE(gn.gossip_ip, '') IN (SELECT client_ip FROM dz_ips)) as dz_contribution,
			va.activated_stake_lamports * 100.0 / NULLIF(ts.total, 0) as stake_share_pct
		FROM dim_solana_vote_accounts_history va
		CROSS JOIN total_stake ts
		ASOF LEFT JOIN dim_solana_gossip_nodes_history gn
			ON va.node_pubkey = gn.pubkey AND va.snapshot_ts >= gn.snapshot_ts
		WHERE va.activated_stake_lamports > 0
			AND va.snapshot_ts IN (%s)
		ORDER BY va.snapshot_ts, va.vote_pubkey
	`, tsInClause)

	start2 := time.Now()
	valRows, err := a.envDB(ctx).Query(ctx, queryValidators)
	if err != nil {
		return nil, err
	}
	defer valRows.Close()
	metrics.RecordClickHouseQuery(time.Since(start2), err)

	// Index validators by (snapshot_ts, vote_pubkey)
	type valData struct {
		votePubkey     string
		nodePubkey     string
		stake          int64
		gossipIP       string
		isOnDZ         bool
		dzContribution int64
		stakeSharePct  float64
	}
	type tsKey struct {
		ts         time.Time
		votePubkey string
	}
	valMap := make(map[tsKey]*valData)
	valsByTS := make(map[time.Time][]string) // vote_pubkeys at each timestamp

	for valRows.Next() {
		var v valData
		var snapshotTS time.Time
		if err := valRows.Scan(&v.votePubkey, &v.nodePubkey, &snapshotTS, &v.stake, &v.gossipIP, &v.isOnDZ, &v.dzContribution, &v.stakeSharePct); err != nil {
			return nil, fmt.Errorf("validator data scan error: %w", err)
		}
		key := tsKey{snapshotTS, v.votePubkey}
		valMap[key] = &v
		valsByTS[snapshotTS] = append(valsByTS[snapshotTS], v.votePubkey)
	}

	// Compare each snapshot pair
	var events []TimelineEvent
	seen := make(map[string]bool) // dedup by vote_pubkey+timestamp

	for _, pair := range pairs {
		// Collect all vote_pubkeys from both timestamps
		allPubkeys := make(map[string]bool)
		for _, vp := range valsByTS[pair.currTS] {
			allPubkeys[vp] = true
		}
		for _, vp := range valsByTS[pair.prevTS] {
			allPubkeys[vp] = true
		}

		for vp := range allPubkeys {
			curr := valMap[tsKey{pair.currTS, vp}]
			prev := valMap[tsKey{pair.prevTS, vp}]

			var currContrib, prevContrib int64
			if curr != nil {
				currContrib = curr.dzContribution
			}
			if prev != nil {
				prevContrib = prev.dzContribution
			}

			if currContrib == prevContrib {
				continue
			}

			dedupKey := fmt.Sprintf("%s:%s", vp, pair.currTS.Format(time.RFC3339))
			if seen[dedupKey] {
				continue
			}
			seen[dedupKey] = true

			contributionChange := currContrib - prevContrib
			var eventType, title, action, severity string
			var stake int64
			var stakeSharePct float64
			var gossipIP, prevGossipIP, nodePubkey string

			if curr != nil {
				stake = curr.stake
				stakeSharePct = curr.stakeSharePct
				gossipIP = curr.gossipIP
				nodePubkey = curr.nodePubkey
			}
			if prev != nil {
				prevGossipIP = prev.gossipIP
				if nodePubkey == "" {
					nodePubkey = prev.nodePubkey
				}
			}

			stakeSol := float64(stake) / 1_000_000_000
			prevOnDZ := prev != nil && prev.isOnDZ
			currOnDZ := curr != nil && curr.isOnDZ

			var stakeShareChangePct float64
			switch {
			case prev != nil && curr == nil && prevOnDZ:
				// Validator left Solana, was on DZ
				eventType = "validator_left_dz"
				action = "validator_left_dz"
				stake = prev.stake
				stakeSol = float64(prev.stake) / 1_000_000_000
				stakeSharePct = prev.stakeSharePct
				stakeShareChangePct = -prev.stakeSharePct
				title = "Validator left Solana, was on DZ"
				severity = "warning"
			case prevOnDZ && !currOnDZ:
				eventType = "validator_left_dz"
				action = "validator_left_dz"
				stakeShareChangePct = -stakeSharePct
				title = "Validator left DZ"
				severity = "warning"
			case !prevOnDZ && currOnDZ:
				eventType = "validator_joined_dz"
				action = "validator_joined_dz"
				stakeShareChangePct = stakeSharePct
				title = "Validator joined DZ"
				severity = "info"
			case currOnDZ && prevOnDZ:
				eventType = "validator_stake_changed"
				action = "validator_stake_changed"
				if prev != nil {
					stakeShareChangePct = stakeSharePct - prev.stakeSharePct
				}
				if contributionChange > 0 {
					title = "DZ validator stake increased"
				} else {
					title = "DZ validator stake decreased"
				}
				severity = "info"
			default:
				continue
			}

			events = append(events, TimelineEvent{
				ID:         generateEventID(vp, pair.currTS, eventType),
				EventType:  eventType,
				Timestamp:  pair.currTS.Format(time.RFC3339),
				Category:   "state_change",
				Severity:   severity,
				Title:      title,
				EntityType: "validator",
				EntityPK:   vp,
				EntityCode: vp,
				Details: ValidatorEventDetails{
					VotePubkey:                 vp,
					NodePubkey:                 nodePubkey,
					StakeLamports:              stake,
					StakeSol:                   stakeSol,
					StakeSharePct:              stakeSharePct,
					StakeShareChangePct:        stakeShareChangePct,
					DZTotalStakeSharePct:       pair.dzTotalPct,
					Kind:                       "validator",
					Action:                     action,
					DZIP:                       gossipIP,
					PrevGossipIP:               prevGossipIP,
					ContributionChangeLamports: contributionChange,
				},
			})
		}
	}

	return events, nil
}

// dzTotalStakeInfo holds the current DZ total stake share and total network stake.
type dzTotalStakeInfo struct {
	DZTotalPct         float64
	TotalStakeLamports int64
}

// queryCurrentDZTotalStakeShare returns the current DZ-connected total stake
// share as a percentage of total network stake, plus the total network stake.
func (a *API) queryCurrentDZTotalStakeShare(ctx context.Context) (dzTotalStakeInfo, error) {
	query := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
		),
		dz_ips AS (
			SELECT DISTINCT client_ip FROM dz_users_current WHERE client_ip != ''
		)
		SELECT
			sum(va.activated_stake_lamports * toUInt64(COALESCE(gn.gossip_ip, '') IN (SELECT client_ip FROM dz_ips))) * 100.0
				/ NULLIF(any(ts.total), 0) as dz_total_pct,
			any(ts.total) as total_stake
		FROM solana_vote_accounts_current va
		CROSS JOIN total_stake ts
		LEFT JOIN solana_gossip_nodes_current gn ON va.node_pubkey = gn.pubkey
	`

	var info dzTotalStakeInfo
	err := a.envDB(ctx).QueryRow(ctx, query).Scan(&info.DZTotalPct, &info.TotalStakeLamports)
	if err != nil {
		return dzTotalStakeInfo{}, err
	}
	return info, nil
}
