package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTelemetryNetworkStateTables(t *testing.T, api *handlers.API, env handlers.DZEnv) {
	t.Helper()
	ctx := t.Context()
	db := handlers.TelemetryDatabaseForEnv(env)
	qdb := "`" + db + "`"

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.interface_state (
			timestamp DateTime64(9),
			device_pubkey String,
			interface_name String,
			admin_status String,
			oper_status String
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.interface_state_latest AS %s.interface_state
		ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name)
	`, qdb, qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.bgp_neighbors (
			timestamp DateTime64(9),
			device_pubkey String,
			session_state String,
			peer_type String
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, session_state, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.bgp_neighbors_latest AS %s.bgp_neighbors
		ENGINE = MergeTree
		ORDER BY (device_pubkey, session_state)
	`, qdb, qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.isis_adjacencies (
			timestamp DateTime64(9),
			device_pubkey String,
			adjacency_state String,
			system_id String
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, adjacency_state, system_id, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.isis_adjacencies_latest AS %s.isis_adjacencies
		ENGINE = MergeTree
		ORDER BY (device_pubkey, adjacency_state, system_id)
	`, qdb, qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.isis_global_state (
			timestamp DateTime64(9),
			device_pubkey String,
			network_instance String,
			instance String,
			net String,
			level_capability String
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, network_instance, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.isis_global_state_latest AS %s.isis_global_state
		ENGINE = MergeTree
		ORDER BY (device_pubkey, network_instance)
	`, qdb, qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.isis_overload_bit (
			timestamp DateTime64(9),
			device_pubkey String,
			network_instance String,
			overload_bit Bool
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, network_instance, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.isis_overload_bit_latest AS %s.isis_overload_bit
		ENGINE = MergeTree
		ORDER BY (device_pubkey, network_instance)
	`, qdb, qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.transceiver_state (
			timestamp DateTime64(9),
			device_pubkey String,
			interface_name String,
			channel_index UInt16,
			input_power Float64,
			output_power Float64,
			laser_bias_current Float64
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, channel_index, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.transceiver_state_latest AS %s.transceiver_state
		ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, channel_index)
	`, qdb, qdb)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.transceiver_thresholds (
			timestamp DateTime64(9),
			device_pubkey String,
			interface_name String,
			severity String,
			input_power_lower Float64,
			input_power_upper Float64,
			output_power_lower Float64,
			output_power_upper Float64,
			laser_bias_current_lower Float64,
			laser_bias_current_upper Float64,
			module_temperature_lower Float64,
			module_temperature_upper Float64,
			supply_voltage_lower Float64,
			supply_voltage_upper Float64
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, severity, timestamp)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.transceiver_thresholds_latest AS %s.transceiver_thresholds
		ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, severity)
	`, qdb, qdb)))
}

func networkStateRequest(env handlers.DZEnv) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/dz/network-state", nil)
	return req.WithContext(handlers.ContextWithEnv(req.Context(), env))
}

func decodeNetworkStateResponse(t *testing.T, rr *httptest.ResponseRecorder) handlers.NetworkStateResponse {
	t.Helper()
	var resp handlers.NetworkStateResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	return resp
}

func TestGetNetworkState_EmptyTelemetryReturnsKnownGap(t *testing.T) {
	for _, env := range []handlers.DZEnv{handlers.EnvMainnet, handlers.EnvDevnet, handlers.EnvTestnet} {
		t.Run(string(env), func(t *testing.T) {
			api := apitesting.NewTestAPI(t, testChDB)
			api.EnvDatabases[string(env)] = api.Database
			api.EnvDBs[string(env)] = api.DB
			createTelemetryNetworkStateTables(t, api, env)

			rr := httptest.NewRecorder()
			api.GetNetworkState(rr, networkStateRequest(env))

			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			resp := decodeNetworkStateResponse(t, rr)

			assert.Equal(t, string(env), resp.Env)
			assert.Len(t, resp.Freshness, 7)
			for _, item := range resp.Freshness {
				assert.Zero(t, item.Rows)
				assert.Zero(t, item.Devices)
				assert.Empty(t, item.LastSeen)
				assert.Nil(t, item.SecondsStale)
			}
			assert.NotNil(t, resp.Interfaces.Families)
			assert.Empty(t, resp.Interfaces.Families)
			assert.NotNil(t, resp.BGP.States)
			assert.Empty(t, resp.BGP.States)
			assert.NotNil(t, resp.ISIS.States)
			assert.Empty(t, resp.ISIS.States)
			assert.Contains(t, resp.KnownGaps, "telemetry_empty")
			if env == handlers.EnvMainnet {
				assert.Contains(t, resp.KnownGaps, "mainnet_beta_telemetry_pilot_not_flowing")
			}
		})
	}
}

func TestGetNetworkState_MissingTelemetryTablesReturnsUsefulError(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB
	require.NoError(t, api.DB.Exec(t.Context(), "DROP DATABASE IF EXISTS `telemetry_mainnet_beta`"))

	rr := httptest.NewRecorder()
	api.GetNetworkState(rr, networkStateRequest(handlers.EnvMainnet))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "Telemetry schema unavailable or outdated")
}

func TestGetNetworkState_SummarizesTelemetry(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB
	createTelemetryNetworkStateTables(t, api, handlers.EnvMainnet)
	ctx := t.Context()
	qdb := "`telemetry_mainnet_beta`"

	interfaces := `
		(timestamp, device_pubkey, interface_name, admin_status, oper_status) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'Switch1/11/2', 'UP', 'UP'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'Switch1/11/4', 'UP', 'DOWN'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-b', 'Ethernet1', 'UP', 'UP')
	`
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.interface_state %s", qdb, interfaces)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.interface_state_latest %s", qdb, interfaces)))

	bgp := `
		(timestamp, device_pubkey, session_state, peer_type) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'ESTABLISHED', 'INTERNAL'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'ACTIVE', 'EXTERNAL'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-b', 'ESTABLISHED', 'EXTERNAL')
	`
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.bgp_neighbors %s", qdb, bgp)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.bgp_neighbors_latest %s", qdb, bgp)))

	isis := `
		(timestamp, device_pubkey, adjacency_state, system_id) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'UP', '0000.0000.0001'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'DOWN', '0000.0000.0002'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-b', 'UP', '0000.0000.0003')
	`
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.isis_adjacencies %s", qdb, isis)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.isis_adjacencies_latest %s", qdb, isis)))

	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.isis_global_state
		(timestamp, device_pubkey, network_instance, instance, net, level_capability) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'default', 'default', '49.0001', 'LEVEL_2')
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.isis_global_state_latest
		(timestamp, device_pubkey, network_instance, instance, net, level_capability) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'default', 'default', '49.0001', 'LEVEL_2')
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.isis_overload_bit
		(timestamp, device_pubkey, network_instance, overload_bit) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'default', false)
	`, qdb)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.isis_overload_bit_latest
		(timestamp, device_pubkey, network_instance, overload_bit) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'default', false)
	`, qdb)))

	opticsState := `
		(timestamp, device_pubkey, interface_name, channel_index) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'Ethernet1', 0),
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'Ethernet1', 1),
		(now64(9) - INTERVAL 2 SECOND, 'dev-b', 'Ethernet2', 0)
	`
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.transceiver_state %s", qdb, opticsState)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.transceiver_state_latest %s", qdb, opticsState)))

	thresholds := `
		(timestamp, device_pubkey, interface_name, severity) VALUES
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'Ethernet1', 'CRITICAL'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-a', 'Ethernet1', 'WARNING'),
		(now64(9) - INTERVAL 2 SECOND, 'dev-b', 'Ethernet2', 'CRITICAL')
	`
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.transceiver_thresholds %s", qdb, thresholds)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("INSERT INTO %s.transceiver_thresholds_latest %s", qdb, thresholds)))

	rr := httptest.NewRecorder()
	api.GetNetworkState(rr, networkStateRequest(handlers.EnvMainnet))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	resp := decodeNetworkStateResponse(t, rr)

	families := map[string]handlers.InterfaceFamilySummary{}
	for _, family := range resp.Interfaces.Families {
		families[family.Family] = family
	}
	require.Contains(t, families, "switch")
	assert.Equal(t, uint64(2), families["switch"].Interfaces)
	assert.Equal(t, uint64(1), families["switch"].Devices)
	assert.Equal(t, uint64(2), families["switch"].AdminUp)
	assert.Equal(t, uint64(1), families["switch"].OperUp)
	assert.Equal(t, uint64(1), families["switch"].OperDown)

	bgpStates := map[string]handlers.BGPStateSummary{}
	for _, state := range resp.BGP.States {
		bgpStates[state.State] = state
	}
	require.Contains(t, bgpStates, "ESTABLISHED")
	assert.Equal(t, uint64(2), bgpStates["ESTABLISHED"].Neighbors)
	assert.Equal(t, uint64(2), bgpStates["ESTABLISHED"].Devices)
	assert.Equal(t, uint64(1), bgpStates["ESTABLISHED"].InternalNeighbors)
	assert.Equal(t, uint64(1), bgpStates["ESTABLISHED"].ExternalNeighbors)
	require.Contains(t, bgpStates, "ACTIVE")
	assert.Equal(t, uint64(1), bgpStates["ACTIVE"].Neighbors)

	isisStates := map[string]handlers.ISISStateSummary{}
	for _, state := range resp.ISIS.States {
		isisStates[state.State] = state
	}
	require.Contains(t, isisStates, "UP")
	assert.Equal(t, uint64(2), isisStates["UP"].Adjacencies)
	assert.Equal(t, uint64(2), isisStates["UP"].Devices)
	assert.Equal(t, uint64(2), isisStates["UP"].Systems)
	require.Contains(t, isisStates, "DOWN")
	assert.Equal(t, uint64(1), isisStates["DOWN"].Adjacencies)

	assert.Equal(t, uint64(3), resp.Optics.Lanes)
	assert.Equal(t, uint64(2), resp.Optics.Devices)
	assert.Equal(t, uint64(2), resp.Optics.Interfaces)
	assert.Equal(t, uint64(3), resp.Optics.ThresholdRows)
	assert.Equal(t, uint64(2), resp.Optics.DevicesWithThresholds)
	assert.Equal(t, uint64(2), resp.Optics.InterfacesWithThresholds)

	freshness := map[string]handlers.TelemetryFreshness{}
	for _, item := range resp.Freshness {
		freshness[item.Table] = item
	}
	require.Contains(t, freshness, "interface_state")
	assert.Equal(t, uint64(3), freshness["interface_state"].Rows)
	assert.NotEmpty(t, freshness["interface_state"].LastSeen)
	assert.NotNil(t, freshness["interface_state"].SecondsStale)
	assert.NotContains(t, resp.KnownGaps, "telemetry_empty")
}
