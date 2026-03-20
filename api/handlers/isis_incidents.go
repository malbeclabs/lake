package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fetchISISDownIncidents detects periods where a link is missing its ISIS adjacency
// by walking the SCD2 adjacency history.
func fetchISISDownIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, linkMeta map[string]linkMetadataWithStatus, dp incidentDetectionParams) ([]LinkIncident, error) {
	totalHours := int(duration.Hours())
	if totalHours < 1 {
		totalHours = 1
	}
	now := time.Now().UTC()
	windowStart := now.Add(-duration)

	// Build set of link PKs that are activated and should have adjacencies
	// (same criteria as status page: activated, has tunnel_net, not provisioning)
	linkTunnelNets := make(map[string]string) // link_pk -> tunnel_net
	tunnelNetQuery := `
		SELECT pk, tunnel_net
		FROM dz_links_current
		WHERE status = 'activated'
		  AND tunnel_net != ''
		  AND committed_rtt_ns != ?
	`
	tnRows, err := conn.Query(ctx, tunnelNetQuery, committedRttProvisioningNs)
	if err != nil {
		return nil, fmt.Errorf("tunnel_net query: %w", err)
	}
	defer tnRows.Close()
	for tnRows.Next() {
		var pk, tn string
		if err := tnRows.Scan(&pk, &tn); err == nil {
			linkTunnelNets[pk] = tn
		}
	}

	// Build sibling tunnel_net set: tunnel_nets that have at least one link with a current adjacency
	coveredTunnelNets := make(map[string]bool)
	siblingQuery := `
		SELECT DISTINCT l.tunnel_net
		FROM dz_links_current l
		JOIN isis_adjacencies_current a ON a.link_pk = l.pk
		WHERE l.tunnel_net != '' AND a.link_pk != ''
	`
	sibRows, err := conn.Query(ctx, siblingQuery)
	if err != nil {
		slog.Warn("isis incidents: failed to query sibling tunnel_nets", "error", err)
	} else {
		defer sibRows.Close()
		for sibRows.Next() {
			var tn string
			if sibRows.Scan(&tn) == nil {
				coveredTunnelNets[tn] = true
			}
		}
	}

	// Query SCD2 history for adjacency state changes
	type histEntry struct {
		snapshotTS time.Time
		isDeleted  bool
	}
	adjHistory := make(map[string][]histEntry) // link_pk -> sorted entries

	histQuery := `
		SELECT link_pk, snapshot_ts, is_deleted
		FROM dim_isis_adjacencies_history
		WHERE snapshot_ts > now() - INTERVAL ? HOUR
		  AND link_pk != ''
		ORDER BY link_pk, snapshot_ts
	`
	histRows, err := conn.Query(ctx, histQuery, totalHours)
	if err != nil {
		return nil, fmt.Errorf("isis adjacency history query: %w", err)
	}
	defer histRows.Close()
	for histRows.Next() {
		var linkPK string
		var snapshotTS time.Time
		var isDeleted uint8
		if err := histRows.Scan(&linkPK, &snapshotTS, &isDeleted); err != nil {
			slog.Error("isis adjacency history scan error", "error", err)
			continue
		}
		adjHistory[linkPK] = append(adjHistory[linkPK], histEntry{
			snapshotTS: snapshotTS,
			isDeleted:  isDeleted == 1,
		})
	}

	// Query baseline state before the time range
	adjBaseline := make(map[string]bool) // link_pk -> is_deleted
	baselineQuery := `
		SELECT link_pk, argMax(is_deleted, snapshot_ts) as is_deleted, count() as cnt
		FROM dim_isis_adjacencies_history
		WHERE snapshot_ts <= now() - INTERVAL ? HOUR
		  AND link_pk != ''
		GROUP BY link_pk
	`
	baselineRows, err := conn.Query(ctx, baselineQuery, totalHours)
	if err != nil {
		slog.Warn("isis adjacency baseline query error", "error", err)
	} else {
		defer baselineRows.Close()
		for baselineRows.Next() {
			var linkPK string
			var isDeleted uint8
			var cnt uint64
			if baselineRows.Scan(&linkPK, &isDeleted, &cnt) == nil && cnt > 0 {
				adjBaseline[linkPK] = isDeleted == 1
			}
		}
	}

	// Track which link_pks have any ISIS history at all
	hasHistory := make(map[string]bool)
	for pk := range adjHistory {
		hasHistory[pk] = true
	}
	for pk := range adjBaseline {
		hasHistory[pk] = true
	}

	// Build set of currently missing links (no history, no current adjacency, not sibling-covered)
	currentlyMissing := make(map[string]bool)
	missingQuery := `
		SELECT l.pk
		FROM dz_links_current l
		WHERE l.status = 'activated'
		  AND l.tunnel_net != ''
		  AND l.committed_rtt_ns != ?
		  AND l.pk NOT IN (
		    SELECT DISTINCT link_pk FROM isis_adjacencies_current WHERE link_pk != ''
		  )
		  AND l.tunnel_net NOT IN (
		    SELECT DISTINCT l2.tunnel_net
		    FROM dz_links_current l2
		    JOIN isis_adjacencies_current a ON a.link_pk = l2.pk
		    WHERE l2.tunnel_net != '' AND a.link_pk != ''
		  )
	`
	missingRows, err := conn.Query(ctx, missingQuery, committedRttProvisioningNs)
	if err != nil {
		slog.Warn("isis missing adjacency query error", "error", err)
	} else {
		defer missingRows.Close()
		for missingRows.Next() {
			var pk string
			if missingRows.Scan(&pk) == nil {
				currentlyMissing[pk] = true
			}
		}
	}

	var incidents []LinkIncident
	idCounter := 0

	// Process links with SCD2 history — walk state transitions to find incident periods
	for linkPK, entries := range adjHistory {
		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}

		// Determine initial state from baseline
		isDown := false
		if baseline, ok := adjBaseline[linkPK]; ok {
			isDown = baseline
		}

		var incidentStart *time.Time
		if isDown {
			// Was already down before the window — incident started at window start
			t := windowStart
			incidentStart = &t
		}

		for _, entry := range entries {
			if entry.isDeleted && incidentStart == nil {
				// Adjacency removed — incident starts
				t := entry.snapshotTS
				incidentStart = &t
			} else if !entry.isDeleted && incidentStart != nil {
				// Adjacency restored — incident ends
				inc := buildISISLinkIncident(idCounter, linkPK, meta, *incidentStart, &entry.snapshotTS, false)
				incidents = append(incidents, inc)
				idCounter++
				incidentStart = nil
			}
		}

		// If still down at end of window — ongoing incident
		if incidentStart != nil {
			inc := buildISISLinkIncident(idCounter, linkPK, meta, *incidentStart, nil, true)
			incidents = append(incidents, inc)
			idCounter++
		}
	}

	// Process links with baseline only (no in-range history) — check if still down
	for linkPK, isDeleted := range adjBaseline {
		if _, hasEntries := adjHistory[linkPK]; hasEntries {
			continue // Already processed above
		}
		if !isDeleted {
			continue // Was up before the window and no changes — still up
		}

		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}

		// Check if sibling-covered
		if tn := linkTunnelNets[linkPK]; tn != "" && coveredTunnelNets[tn] {
			continue
		}

		inc := buildISISLinkIncident(idCounter, linkPK, meta, windowStart, nil, true)
		incidents = append(incidents, inc)
		idCounter++
	}

	// Process links with no history at all but currently missing
	for linkPK := range currentlyMissing {
		if hasHistory[linkPK] {
			continue // Already handled
		}

		meta, ok := linkMeta[linkPK]
		if !ok {
			continue
		}

		inc := buildISISLinkIncident(idCounter, linkPK, meta, windowStart, nil, true)
		incidents = append(incidents, inc)
		idCounter++
	}

	return incidents, nil
}

func buildISISLinkIncident(id int, linkPK string, meta linkMetadataWithStatus, start time.Time, end *time.Time, ongoing bool) LinkIncident {
	inc := LinkIncident{
		ID:              fmt.Sprintf("isis-down-%d", id),
		LinkPK:          linkPK,
		LinkCode:        meta.LinkCode,
		LinkType:        meta.LinkType,
		SideAMetro:      meta.SideAMetro,
		SideZMetro:      meta.SideZMetro,
		ContributorCode: meta.ContributorCode,
		IncidentType:    "isis_down",
		StartedAt:       start.Format(time.RFC3339),
		IsOngoing:       ongoing,
		IsDrained:       meta.Status == "soft-drained" || meta.Status == "hard-drained",
		Severity:        "incident",
	}
	if end != nil {
		endStr := end.Format(time.RFC3339)
		inc.EndedAt = &endStr
		dur := int64(end.Sub(start).Seconds())
		inc.DurationSeconds = &dur
	}
	return inc
}

// fetchISISDeviceIncidents detects periods where a device is in ISIS overload or unreachable state.
func fetchISISDeviceIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, deviceMeta map[string]deviceMetadata, incidentType string, flagColumn string) ([]DeviceIncident, error) {
	totalHours := int(duration.Hours())
	if totalHours < 1 {
		totalHours = 1
	}
	now := time.Now().UTC()
	windowStart := now.Add(-duration)

	// Query SCD2 history for state changes
	type histEntry struct {
		snapshotTS time.Time
		flagActive bool
	}
	deviceHistory := make(map[string][]histEntry) // device_pk -> sorted entries

	histQuery := fmt.Sprintf(`
		SELECT device_pk, snapshot_ts, %s as flag_value, is_deleted
		FROM dim_isis_devices_history
		WHERE snapshot_ts > now() - INTERVAL ? HOUR
		  AND device_pk != ''
		ORDER BY device_pk, snapshot_ts
	`, flagColumn)
	histRows, err := conn.Query(ctx, histQuery, totalHours)
	if err != nil {
		return nil, fmt.Errorf("isis device history query: %w", err)
	}
	defer histRows.Close()
	for histRows.Next() {
		var devicePK string
		var snapshotTS time.Time
		var flagValue, isDeleted uint8
		if err := histRows.Scan(&devicePK, &snapshotTS, &flagValue, &isDeleted); err != nil {
			slog.Error("isis device history scan error", "error", err)
			continue
		}
		// Flag is active only if device is not deleted AND flag is set
		deviceHistory[devicePK] = append(deviceHistory[devicePK], histEntry{
			snapshotTS: snapshotTS,
			flagActive: isDeleted == 0 && flagValue == 1,
		})
	}

	// Query baseline state
	deviceBaseline := make(map[string]bool) // device_pk -> flag active
	baselineQuery := fmt.Sprintf(`
		SELECT device_pk,
			argMax(%s, snapshot_ts) as flag_value,
			argMax(is_deleted, snapshot_ts) as is_deleted,
			count() as cnt
		FROM dim_isis_devices_history
		WHERE snapshot_ts <= now() - INTERVAL ? HOUR
		  AND device_pk != ''
		GROUP BY device_pk
	`, flagColumn)
	baselineRows, err := conn.Query(ctx, baselineQuery, totalHours)
	if err != nil {
		slog.Warn("isis device baseline query error", "error", err)
	} else {
		defer baselineRows.Close()
		for baselineRows.Next() {
			var devicePK string
			var flagValue, isDeleted uint8
			var cnt uint64
			if baselineRows.Scan(&devicePK, &flagValue, &isDeleted, &cnt) == nil && cnt > 0 {
				deviceBaseline[devicePK] = isDeleted == 0 && flagValue == 1
			}
		}
	}

	var incidents []DeviceIncident
	idCounter := 0

	// Walk state transitions for devices with history
	for devicePK, entries := range deviceHistory {
		meta, ok := deviceMeta[devicePK]
		if !ok {
			continue
		}

		isActive := false
		if baseline, ok := deviceBaseline[devicePK]; ok {
			isActive = baseline
		}

		var incidentStart *time.Time
		if isActive {
			t := windowStart
			incidentStart = &t
		}

		for _, entry := range entries {
			if entry.flagActive && incidentStart == nil {
				t := entry.snapshotTS
				incidentStart = &t
			} else if !entry.flagActive && incidentStart != nil {
				inc := buildISISDeviceIncident(idCounter, incidentType, devicePK, meta, *incidentStart, &entry.snapshotTS, false)
				incidents = append(incidents, inc)
				idCounter++
				incidentStart = nil
			}
		}

		if incidentStart != nil {
			inc := buildISISDeviceIncident(idCounter, incidentType, devicePK, meta, *incidentStart, nil, true)
			incidents = append(incidents, inc)
			idCounter++
		}
	}

	// Devices with baseline only (no in-range history)
	for devicePK, isActive := range deviceBaseline {
		if _, hasEntries := deviceHistory[devicePK]; hasEntries {
			continue
		}
		if !isActive {
			continue
		}

		meta, ok := deviceMeta[devicePK]
		if !ok {
			continue
		}

		inc := buildISISDeviceIncident(idCounter, incidentType, devicePK, meta, windowStart, nil, true)
		incidents = append(incidents, inc)
		idCounter++
	}

	// Check current state for devices with no history
	if incidentType == "isis_overload" || incidentType == "isis_unreachable" {
		currentQuery := fmt.Sprintf(`
			SELECT device_pk
			FROM isis_devices_current
			WHERE %s = 1 AND device_pk != ''
		`, flagColumn)
		currentRows, err := conn.Query(ctx, currentQuery)
		if err == nil {
			defer currentRows.Close()
			for currentRows.Next() {
				var devicePK string
				if currentRows.Scan(&devicePK) != nil {
					continue
				}
				if _, hasHist := deviceHistory[devicePK]; hasHist {
					continue
				}
				if _, hasBaseline := deviceBaseline[devicePK]; hasBaseline {
					continue
				}
				meta, ok := deviceMeta[devicePK]
				if !ok {
					continue
				}
				inc := buildISISDeviceIncident(idCounter, incidentType, devicePK, meta, windowStart, nil, true)
				incidents = append(incidents, inc)
				idCounter++
			}
		}
	}

	return incidents, nil
}

func buildISISDeviceIncident(id int, incidentType string, devicePK string, meta deviceMetadata, start time.Time, end *time.Time, ongoing bool) DeviceIncident {
	inc := DeviceIncident{
		ID:              fmt.Sprintf("%s-%d", incidentType, id),
		DevicePK:        devicePK,
		DeviceCode:      meta.DeviceCode,
		DeviceType:      meta.DeviceType,
		Metro:           meta.Metro,
		ContributorCode: meta.ContributorCode,
		IncidentType:    incidentType,
		StartedAt:       start.Format(time.RFC3339),
		IsOngoing:       ongoing,
		IsDrained:       meta.Status == "soft-drained" || meta.Status == "hard-drained",
		Severity:        "incident",
	}
	if end != nil {
		endStr := end.Format(time.RFC3339)
		inc.EndedAt = &endStr
		dur := int64(end.Sub(start).Seconds())
		inc.DurationSeconds = &dur
	}
	return inc
}
