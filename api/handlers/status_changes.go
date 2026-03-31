package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// EntityStatusChange represents a status transition for a link or device.
type EntityStatusChange struct {
	PreviousStatus string `json:"previous_status"`
	NewStatus      string `json:"new_status"`
	ChangedTS      string `json:"changed_ts"`
}

// fetchLinkStatusChanges queries dz_link_status_changes for status transitions
// during the given time range.
func fetchLinkStatusChanges(ctx context.Context, conn driver.Conn, linkPK, startedAt string, endedAt *string) []EntityStatusChange {
	startTS, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return nil
	}
	endTS := time.Now().UTC()
	if endedAt != nil {
		if t, err := time.Parse(time.RFC3339, *endedAt); err == nil {
			endTS = t
		}
	}

	query := `
		SELECT previous_status, new_status, changed_ts
		FROM dz_link_status_changes
		WHERE link_pk = $1
		  AND changed_ts >= $2
		  AND changed_ts <= $3
		ORDER BY changed_ts ASC
	`

	rows, err := conn.Query(ctx, query, linkPK, startTS, endTS)
	if err != nil {
		slog.Warn("failed to fetch link status changes", "error", err)
		return nil
	}
	defer rows.Close()

	var changes []EntityStatusChange
	for rows.Next() {
		var sc EntityStatusChange
		var changedTS time.Time
		if err := rows.Scan(&sc.PreviousStatus, &sc.NewStatus, &changedTS); err != nil {
			continue
		}
		sc.ChangedTS = changedTS.UTC().Format(time.RFC3339)
		changes = append(changes, sc)
	}
	return changes
}

// fetchDeviceStatusChanges queries device status transitions from the dimension
// history table during the given time range.
func fetchDeviceStatusChanges(ctx context.Context, conn driver.Conn, devicePK, startedAt string, endedAt *string) []EntityStatusChange {
	startTS, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return nil
	}
	endTS := time.Now().UTC()
	if endedAt != nil {
		if t, err := time.Parse(time.RFC3339, *endedAt); err == nil {
			endTS = t
		}
	}

	query := `
		WITH transitions AS (
			SELECT
				status AS new_status,
				snapshot_ts AS changed_ts,
				lag(status) OVER (PARTITION BY pk ORDER BY snapshot_ts) AS previous_status
			FROM dim_dz_devices_history
			WHERE pk = $1 AND is_deleted = 0
		)
		SELECT previous_status, new_status, changed_ts
		FROM transitions
		WHERE previous_status IS NOT NULL
		  AND previous_status != new_status
		  AND changed_ts >= $2
		  AND changed_ts <= $3
		ORDER BY changed_ts ASC
	`

	rows, err := conn.Query(ctx, query, devicePK, startTS, endTS)
	if err != nil {
		slog.Warn("failed to fetch device status changes", "error", err)
		return nil
	}
	defer rows.Close()

	var changes []EntityStatusChange
	for rows.Next() {
		var sc EntityStatusChange
		var changedTS time.Time
		if err := rows.Scan(&sc.PreviousStatus, &sc.NewStatus, &changedTS); err != nil {
			continue
		}
		sc.ChangedTS = changedTS.UTC().Format(time.RFC3339)
		changes = append(changes, sc)
	}
	return changes
}
