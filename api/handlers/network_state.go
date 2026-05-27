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

const (
	networkStateCacheTTL       = 60 * time.Second
	networkStateRequestTimeout = 20 * time.Second
)

type networkStateCacheEntry struct {
	Response NetworkStateResponse
	CachedAt time.Time
	Stale    bool
}

type networkStateCacheResult struct {
	Response    NetworkStateResponse
	CacheStatus string
}

// GetNetworkState returns a read-only overview of the existing gNMI telemetry
// for the selected DoubleZero environment. It reports raw observed telemetry
// state only; callers should not treat these aggregates as incident status.
func (a *API) GetNetworkState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), networkStateRequestTimeout)
	defer cancel()

	env := EnvFromContext(ctx)
	resp, cacheStatus, err := a.cachedNetworkState(ctx, env)
	if err != nil {
		logError("network state query failed", "error", err, "env", env)
		http.Error(w, networkStateUserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", cacheStatus)
	w.Header().Add("Vary", "X-DZ-Env")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("network state: failed to encode response", "error", err)
	}
}

func (a *API) cachedNetworkState(ctx context.Context, env DZEnv) (NetworkStateResponse, string, error) {
	if entry, ok := a.freshNetworkStateCacheEntry(env, time.Now()); ok {
		return entry.Response, entry.cacheStatus(), nil
	}

	value, err, _ := a.networkStateGroup.Do(string(env), func() (any, error) {
		if entry, ok := a.freshNetworkStateCacheEntry(env, time.Now()); ok {
			return networkStateCacheResult{Response: entry.Response, CacheStatus: entry.cacheStatus()}, nil
		}

		resp, err := a.FetchNetworkStateData(ctx, env)
		if err != nil {
			if entry, ok := a.networkStateCacheEntry(env); ok {
				logError("network state refresh failed, serving stale cache", "error", err, "env", env)
				a.storeNetworkStateCacheEntry(env, entry.Response, true)
				return networkStateCacheResult{Response: entry.Response, CacheStatus: "STALE"}, nil
			}
			return nil, err
		}

		a.storeNetworkStateCacheEntry(env, resp, false)
		return networkStateCacheResult{Response: resp, CacheStatus: "MISS"}, nil
	})
	if err != nil {
		return NetworkStateResponse{}, "", err
	}

	result, ok := value.(networkStateCacheResult)
	if !ok {
		return NetworkStateResponse{}, "", fmt.Errorf("network state cache returned unexpected type %T", value)
	}
	return result.Response, result.CacheStatus, nil
}

func (a *API) freshNetworkStateCacheEntry(env DZEnv, now time.Time) (networkStateCacheEntry, bool) {
	entry, ok := a.networkStateCacheEntry(env)
	if !ok || now.Sub(entry.CachedAt) >= networkStateCacheTTL {
		return networkStateCacheEntry{}, false
	}
	return entry, true
}

func (a *API) networkStateCacheEntry(env DZEnv) (networkStateCacheEntry, bool) {
	a.networkStateCacheMu.RLock()
	defer a.networkStateCacheMu.RUnlock()
	entry, ok := a.networkStateCache[env]
	return entry, ok
}

func (entry networkStateCacheEntry) cacheStatus() string {
	if entry.Stale {
		return "STALE"
	}
	return "HIT"
}

func (a *API) storeNetworkStateCacheEntry(env DZEnv, resp NetworkStateResponse, stale bool) {
	a.networkStateCacheMu.Lock()
	defer a.networkStateCacheMu.Unlock()
	if a.networkStateCache == nil {
		a.networkStateCache = make(map[DZEnv]networkStateCacheEntry)
	}
	a.networkStateCache[env] = networkStateCacheEntry{Response: resp, CachedAt: time.Now(), Stale: stale}
}

// FetchNetworkStateData queries ClickHouse for the current network-state
// overview without consulting the request cache.
func (a *API) FetchNetworkStateData(ctx context.Context, env DZEnv) (NetworkStateResponse, error) {
	ctx = ContextWithEnv(ctx, env)
	conn := a.envDB(ctx)
	tdb := quoteClickHouseIdent(TelemetryDatabaseForEnv(env))

	freshness, err := queryNetworkFreshness(ctx, conn, tdb)
	if err != nil {
		return NetworkStateResponse{}, fmt.Errorf("freshness query failed: %w", err)
	}

	families, err := queryNetworkInterfaceFamilies(ctx, conn, tdb)
	if err != nil {
		return NetworkStateResponse{}, fmt.Errorf("interface summary query failed: %w", err)
	}

	bgpStates, err := queryNetworkBGPStates(ctx, conn, tdb)
	if err != nil {
		return NetworkStateResponse{}, fmt.Errorf("bgp summary query failed: %w", err)
	}

	isisStates, err := queryNetworkISISStates(ctx, conn, tdb)
	if err != nil {
		return NetworkStateResponse{}, fmt.Errorf("isis summary query failed: %w", err)
	}

	optics, err := queryNetworkOptics(ctx, conn, tdb)
	if err != nil {
		return NetworkStateResponse{}, fmt.Errorf("optics summary query failed: %w", err)
	}

	return NetworkStateResponse{
		Env:        string(env),
		FetchedAt:  time.Now().UTC().Format(time.RFC3339),
		Freshness:  freshness,
		Interfaces: NetworkInterfaceSummary{Families: families},
		BGP:        NetworkBGPSummary{States: bgpStates},
		ISIS:       NetworkISISSummary{States: isisStates},
		Optics:     optics,
		KnownGaps:  networkStateKnownGaps(env, freshness),
	}, nil
}

func queryNetworkFreshness(ctx context.Context, conn driver.Conn, tdb string) ([]TelemetryFreshness, error) {
	parts := make([]string, 0, len(networkStateFreshnessTables))
	for i, table := range networkStateFreshnessTables {
		parts = append(parts, fmt.Sprintf(`
			SELECT
				%[1]d AS table_order,
				'%[2]s' AS table_name,
				count() AS rows,
				uniqExact(device_pubkey) AS devices,
				if(count() = 0, toDateTime64(0, 9), max(timestamp)) AS last_seen,
				if(count() = 0, -1, dateDiff('second', max(timestamp), now())) AS seconds_stale
			FROM %[3]s.%[4]s
		`, i, table, tdb, quoteClickHouseIdent(table)))
	}

	query := `
		SELECT table_name, rows, devices, last_seen, seconds_stale
		FROM (` + strings.Join(parts, ` UNION ALL `) + `)
		ORDER BY table_order
	`

	start := time.Now()
	resultRows, err := conn.Query(ctx, query)
	metrics.RecordClickHouseQuery("network_state:freshness", time.Since(start), err)
	if err != nil {
		return nil, fmt.Errorf("freshness: %w", err)
	}
	defer resultRows.Close()

	freshness := make([]TelemetryFreshness, 0, len(networkStateFreshnessTables))
	for resultRows.Next() {
		var (
			tableName    string
			rowCount     uint64
			devices      uint64
			lastSeen     time.Time
			secondsStale int64
		)
		if err := resultRows.Scan(&tableName, &rowCount, &devices, &lastSeen, &secondsStale); err != nil {
			return nil, fmt.Errorf("freshness scan: %w", err)
		}

		item := TelemetryFreshness{Table: tableName, Rows: rowCount, Devices: devices}
		if rowCount > 0 {
			item.LastSeen = lastSeen.UTC().Format(time.RFC3339)
			item.SecondsStale = &secondsStale
		}
		freshness = append(freshness, item)
	}
	if err := resultRows.Err(); err != nil {
		return nil, fmt.Errorf("freshness rows: %w", err)
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
