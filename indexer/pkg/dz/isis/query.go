package isis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
)

// QueryCurrentAdjacencies returns all current (non-deleted) ISIS adjacencies from ClickHouse.
func QueryCurrentAdjacencies(ctx context.Context, log *slog.Logger, db clickhouse.Client) ([]Adjacency, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, `
		SELECT system_id, neighbor_system_id, neighbor_addr,
		       device_pk, link_pk, hostname, router_id, local_addr, metric, adj_sids
		FROM isis_adjacencies_current
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query adjacencies: %w", err)
	}
	defer rows.Close()

	var adjacencies []Adjacency
	for rows.Next() {
		var a Adjacency
		if err := rows.Scan(
			&a.SystemID, &a.NeighborSystemID, &a.NeighborAddr,
			&a.DevicePK, &a.LinkPK, &a.Hostname, &a.RouterID, &a.LocalAddr, &a.Metric, &a.AdjSIDs,
		); err != nil {
			return nil, fmt.Errorf("failed to scan adjacency: %w", err)
		}
		adjacencies = append(adjacencies, a)
	}
	return adjacencies, nil
}

// QueryCurrentDevices returns all current (non-deleted) ISIS devices from ClickHouse.
func QueryCurrentDevices(ctx context.Context, log *slog.Logger, db clickhouse.Client) ([]Device, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, `
		SELECT system_id, device_pk, hostname, router_id, overload, node_unreachable, sequence
		FROM isis_devices_current
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query devices: %w", err)
	}
	defer rows.Close()

	var devices []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(
			&d.SystemID, &d.DevicePK, &d.Hostname, &d.RouterID, &d.Overload, &d.NodeUnreachable, &d.Sequence,
		); err != nil {
			return nil, fmt.Errorf("failed to scan device: %w", err)
		}
		devices = append(devices, d)
	}
	return devices, nil
}
