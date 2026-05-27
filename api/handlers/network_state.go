package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// NetworkStateResponse is the response for GET /api/dz/network-state.
type NetworkStateResponse struct {
	Env        string                  `json:"env"`
	FetchedAt  string                  `json:"fetched_at"`
	Freshness  []TelemetryFreshness    `json:"freshness"`
	Interfaces NetworkInterfaceSummary `json:"interfaces"`
	BGP        NetworkBGPSummary       `json:"bgp"`
	ISIS       NetworkISISSummary      `json:"isis"`
	Optics     NetworkOpticsSummary    `json:"optics"`
	KnownGaps  []string                `json:"known_gaps"`
}

// TelemetryFreshness summarizes row volume and latest sample time for one
// telemetry table.
type TelemetryFreshness struct {
	Table        string `json:"table"`
	Rows         uint64 `json:"rows"`
	Devices      uint64 `json:"devices"`
	LastSeen     string `json:"last_seen,omitempty"`
	SecondsStale *int64 `json:"seconds_stale,omitempty"`
}

// NetworkInterfaceSummary groups the latest interface snapshot by interface
// family. Families are derived from interface name prefixes and intentionally
// separate Switch* fabric interfaces from front-panel Ethernet ports.
type NetworkInterfaceSummary struct {
	Families []InterfaceFamilySummary `json:"families"`
}

// InterfaceFamilySummary is an aggregate over interface_state_latest.
type InterfaceFamilySummary struct {
	Family      string `json:"family"`
	Interfaces  uint64 `json:"interfaces"`
	Devices     uint64 `json:"devices"`
	AdminUp     uint64 `json:"admin_up"`
	AdminDown   uint64 `json:"admin_down"`
	OperUp      uint64 `json:"oper_up"`
	OperDown    uint64 `json:"oper_down"`
	OperUnknown uint64 `json:"oper_unknown"`
}

// NetworkBGPSummary groups latest BGP neighbors by raw OpenConfig state.
type NetworkBGPSummary struct {
	States []BGPStateSummary `json:"states"`
}

// BGPStateSummary is an aggregate over bgp_neighbors_latest.
type BGPStateSummary struct {
	State             string `json:"state"`
	Neighbors         uint64 `json:"neighbors"`
	Devices           uint64 `json:"devices"`
	InternalNeighbors uint64 `json:"internal_neighbors"`
	ExternalNeighbors uint64 `json:"external_neighbors"`
}

// NetworkISISSummary groups latest ISIS adjacencies by raw OpenConfig state.
type NetworkISISSummary struct {
	States []ISISStateSummary `json:"states"`
}

// ISISStateSummary is an aggregate over isis_adjacencies_latest.
type ISISStateSummary struct {
	State       string `json:"state"`
	Adjacencies uint64 `json:"adjacencies"`
	Devices     uint64 `json:"devices"`
	Systems     uint64 `json:"systems"`
}

// NetworkOpticsSummary summarizes the latest transceiver telemetry volume.
type NetworkOpticsSummary struct {
	Lanes                    uint64 `json:"lanes"`
	Devices                  uint64 `json:"devices"`
	Interfaces               uint64 `json:"interfaces"`
	ThresholdRows            uint64 `json:"threshold_rows"`
	DevicesWithThresholds    uint64 `json:"devices_with_thresholds"`
	InterfacesWithThresholds uint64 `json:"interfaces_with_thresholds"`
}

var networkStateFreshnessTables = []string{
	"interface_state",
	"bgp_neighbors",
	"isis_adjacencies",
	"isis_global_state",
	"isis_overload_bit",
	"transceiver_state",
	"transceiver_thresholds",
}

// GetNetworkState returns a read-only overview of the existing gNMI telemetry
// for the selected DoubleZero environment. It reports raw observed telemetry
// state only; callers should not treat these aggregates as incident status.
func (a *API) GetNetworkState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	env := EnvFromContext(ctx)
	conn := a.envDB(ctx)
	tdb := quoteClickHouseIdent(TelemetryDatabaseForEnv(env))

	freshness, err := queryNetworkFreshness(ctx, conn, tdb)
	if err != nil {
		logError("network state freshness query failed", "error", err, "env", env)
		http.Error(w, networkStateUserMessage(err), http.StatusInternalServerError)
		return
	}

	families, err := queryNetworkInterfaceFamilies(ctx, conn, tdb)
	if err != nil {
		logError("network state interface summary query failed", "error", err, "env", env)
		http.Error(w, networkStateUserMessage(err), http.StatusInternalServerError)
		return
	}

	bgpStates, err := queryNetworkBGPStates(ctx, conn, tdb)
	if err != nil {
		logError("network state bgp summary query failed", "error", err, "env", env)
		http.Error(w, networkStateUserMessage(err), http.StatusInternalServerError)
		return
	}

	isisStates, err := queryNetworkISISStates(ctx, conn, tdb)
	if err != nil {
		logError("network state isis summary query failed", "error", err, "env", env)
		http.Error(w, networkStateUserMessage(err), http.StatusInternalServerError)
		return
	}

	optics, err := queryNetworkOptics(ctx, conn, tdb)
	if err != nil {
		logError("network state optics summary query failed", "error", err, "env", env)
		http.Error(w, networkStateUserMessage(err), http.StatusInternalServerError)
		return
	}

	resp := NetworkStateResponse{
		Env:        string(env),
		FetchedAt:  time.Now().UTC().Format(time.RFC3339),
		Freshness:  freshness,
		Interfaces: NetworkInterfaceSummary{Families: families},
		BGP:        NetworkBGPSummary{States: bgpStates},
		ISIS:       NetworkISISSummary{States: isisStates},
		Optics:     optics,
		KnownGaps:  networkStateKnownGaps(env, freshness),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("network state: failed to encode response", "error", err)
	}
}

func queryNetworkFreshness(ctx context.Context, conn driver.Conn, tdb string) ([]TelemetryFreshness, error) {
	freshness := make([]TelemetryFreshness, 0, len(networkStateFreshnessTables))
	for _, table := range networkStateFreshnessTables {
		query := fmt.Sprintf(`
			SELECT
				count() AS rows,
				uniqExact(device_pubkey) AS devices,
				if(count() = 0, toDateTime64(0, 9), max(timestamp)) AS last_seen,
				if(count() = 0, -1, dateDiff('second', max(timestamp), now())) AS seconds_stale
			FROM %[1]s.%[2]s
		`, tdb, quoteClickHouseIdent(table))

		var (
			rows         uint64
			devices      uint64
			lastSeen     time.Time
			secondsStale int64
		)
		start := time.Now()
		err := conn.QueryRow(ctx, query).Scan(&rows, &devices, &lastSeen, &secondsStale)
		metrics.RecordClickHouseQuery("network_state:freshness", time.Since(start), err)
		if err != nil {
			return nil, fmt.Errorf("freshness %s: %w", table, err)
		}

		item := TelemetryFreshness{Table: table, Rows: rows, Devices: devices}
		if rows > 0 {
			item.LastSeen = lastSeen.UTC().Format(time.RFC3339)
			item.SecondsStale = &secondsStale
		}
		freshness = append(freshness, item)
	}
	return freshness, nil
}

func queryNetworkInterfaceFamilies(ctx context.Context, conn driver.Conn, tdb string) ([]InterfaceFamilySummary, error) {
	query := fmt.Sprintf(`
		SELECT
			family,
			count() AS interfaces,
			uniqExact(device_pubkey) AS devices,
			countIf(upper(admin_status) = 'UP') AS admin_up,
			countIf(upper(admin_status) NOT IN ('', 'UP')) AS admin_down,
			countIf(upper(oper_status) = 'UP') AS oper_up,
			countIf(upper(oper_status) NOT IN ('', 'UP')) AS oper_down,
			countIf(oper_status = '') AS oper_unknown
		FROM (
			SELECT
				device_pubkey,
				admin_status,
				oper_status,
				multiIf(
					startsWith(lower(interface_name), 'switch'), 'switch',
					startsWith(lower(interface_name), 'ethernet'), 'ethernet',
					startsWith(lower(interface_name), 'port-channel') OR startsWith(lower(interface_name), 'portchannel'), 'port_channel',
					startsWith(lower(interface_name), 'loopback'), 'loopback',
					startsWith(lower(interface_name), 'vlan'), 'vlan',
					startsWith(lower(interface_name), 'tunnel'), 'tunnel',
					startsWith(lower(interface_name), 'management') OR startsWith(lower(interface_name), 'mgmt'), 'management',
					interface_name = '', 'unknown',
					'other'
				) AS family
			FROM %[1]s.interface_state_latest
		)
		GROUP BY family
		ORDER BY interfaces DESC, family ASC
	`, tdb)

	start := time.Now()
	rows, err := conn.Query(ctx, query)
	metrics.RecordClickHouseQuery("network_state:interfaces", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	families := []InterfaceFamilySummary{}
	for rows.Next() {
		var item InterfaceFamilySummary
		if err := rows.Scan(
			&item.Family,
			&item.Interfaces,
			&item.Devices,
			&item.AdminUp,
			&item.AdminDown,
			&item.OperUp,
			&item.OperDown,
			&item.OperUnknown,
		); err != nil {
			return nil, err
		}
		families = append(families, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return families, nil
}

func queryNetworkBGPStates(ctx context.Context, conn driver.Conn, tdb string) ([]BGPStateSummary, error) {
	query := fmt.Sprintf(`
		SELECT
			if(session_state = '', 'UNKNOWN', session_state) AS state,
			count() AS neighbors,
			uniqExact(device_pubkey) AS devices,
			countIf(upper(peer_type) = 'INTERNAL') AS internal_neighbors,
			countIf(upper(peer_type) = 'EXTERNAL') AS external_neighbors
		FROM %[1]s.bgp_neighbors_latest
		GROUP BY state
		ORDER BY neighbors DESC, state ASC
	`, tdb)

	start := time.Now()
	rows, err := conn.Query(ctx, query)
	metrics.RecordClickHouseQuery("network_state:bgp", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := []BGPStateSummary{}
	for rows.Next() {
		var item BGPStateSummary
		if err := rows.Scan(
			&item.State,
			&item.Neighbors,
			&item.Devices,
			&item.InternalNeighbors,
			&item.ExternalNeighbors,
		); err != nil {
			return nil, err
		}
		states = append(states, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return states, nil
}

func queryNetworkISISStates(ctx context.Context, conn driver.Conn, tdb string) ([]ISISStateSummary, error) {
	query := fmt.Sprintf(`
		SELECT
			if(adjacency_state = '', 'UNKNOWN', adjacency_state) AS state,
			count() AS adjacencies,
			uniqExact(device_pubkey) AS devices,
			uniqExact(system_id) AS systems
		FROM %[1]s.isis_adjacencies_latest
		GROUP BY state
		ORDER BY adjacencies DESC, state ASC
	`, tdb)

	start := time.Now()
	rows, err := conn.Query(ctx, query)
	metrics.RecordClickHouseQuery("network_state:isis", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := []ISISStateSummary{}
	for rows.Next() {
		var item ISISStateSummary
		if err := rows.Scan(&item.State, &item.Adjacencies, &item.Devices, &item.Systems); err != nil {
			return nil, err
		}
		states = append(states, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return states, nil
}

func queryNetworkOptics(ctx context.Context, conn driver.Conn, tdb string) (NetworkOpticsSummary, error) {
	query := fmt.Sprintf(`
		WITH
			state AS (
				SELECT
					count() AS lanes,
					uniqExact(device_pubkey) AS devices,
					uniqExact(device_pubkey, interface_name) AS interfaces
				FROM %[1]s.transceiver_state_latest
			),
			thresholds AS (
				SELECT
					count() AS threshold_rows,
					uniqExact(device_pubkey) AS devices_with_thresholds,
					uniqExact(device_pubkey, interface_name) AS interfaces_with_thresholds
				FROM %[1]s.transceiver_thresholds_latest
			)
		SELECT
			state.lanes,
			state.devices,
			state.interfaces,
			thresholds.threshold_rows,
			thresholds.devices_with_thresholds,
			thresholds.interfaces_with_thresholds
		FROM state CROSS JOIN thresholds
	`, tdb)

	var summary NetworkOpticsSummary
	start := time.Now()
	err := conn.QueryRow(ctx, query).Scan(
		&summary.Lanes,
		&summary.Devices,
		&summary.Interfaces,
		&summary.ThresholdRows,
		&summary.DevicesWithThresholds,
		&summary.InterfacesWithThresholds,
	)
	metrics.RecordClickHouseQuery("network_state:optics", time.Since(start), err)
	if err != nil {
		return NetworkOpticsSummary{}, err
	}
	return summary, nil
}

func networkStateKnownGaps(env DZEnv, freshness []TelemetryFreshness) []string {
	gaps := []string{
		"raw_telemetry_states_not_incidents",
		"system_state_stdout_only",
	}

	var totalRows uint64
	var interfaceRows uint64
	var overloadRows uint64
	for _, item := range freshness {
		totalRows += item.Rows
		switch item.Table {
		case "interface_state":
			interfaceRows = item.Rows
		case "isis_overload_bit":
			overloadRows = item.Rows
		}
	}

	if totalRows == 0 {
		gaps = append(gaps, "telemetry_empty")
	}
	if env == EnvMainnet && interfaceRows == 0 {
		gaps = append(gaps, "mainnet_beta_telemetry_pilot_not_flowing")
	}
	if overloadRows == 0 {
		gaps = append(gaps, "isis_overload_bit_empty")
	}
	return gaps
}

func networkStateUserMessage(err error) string {
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unknown table") ||
		strings.Contains(msg, "table not found") ||
		strings.Contains(msg, "no such table") ||
		strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "unknown column") ||
		strings.Contains(msg, "no such column") {
		return "Telemetry schema unavailable or outdated. Please verify telemetry remote tables are configured."
	}
	return dberror.UserMessage(err)
}

func quoteClickHouseIdent(name string) string {
	return "`" + name + "`"
}
