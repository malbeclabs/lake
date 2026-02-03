package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMetroConnectivity_FiltersMetrosWithoutMaxUsers(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	// Insert devices into dim_dz_devices_history - NYC has max_users > 0, LAX does not
	ctx := t.Context()
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users
		) VALUES
		('dev-nyc-1', now(), now(), generateUUIDv4(), 1, 0, 'dev-nyc-1', 'active', 'router', 'NYC-CORE-01', '10.0.0.1', '', 'metro-nyc', 100),
		('dev-lax-1', now(), now(), generateUUIDv4(), 2, 0, 'dev-lax-1', 'active', 'router', 'LAX-CORE-01', '10.0.1.1', '', 'metro-lax', 0)
	`)
	require.NoError(t, err)

	// Seed Neo4j with metros and ISIS-enabled devices
	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (nyc:Metro {pk: 'metro-nyc', code: 'NYC', name: 'New York'})
			CREATE (lax:Metro {pk: 'metro-lax', code: 'LAX', name: 'Los Angeles'})
			CREATE (devNyc:Device {pk: 'dev-nyc-1', code: 'NYC-CORE-01', isis_system_id: '0000.0000.0001'})
			CREATE (devLax:Device {pk: 'dev-lax-1', code: 'LAX-CORE-01', isis_system_id: '0000.0000.0002'})
			CREATE (devNyc)-[:LOCATED_IN]->(nyc)
			CREATE (devLax)-[:LOCATED_IN]->(lax)
			CREATE (devNyc)-[:ISIS_ADJACENT {metric: 10}]->(devLax)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	req := httptest.NewRequest(http.MethodGet, "/api/topology/metro-connectivity", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetroConnectivity(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.MetroConnectivityResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Error)

	// Only NYC should be in the metros list (LAX has max_users = 0)
	assert.Len(t, response.Metros, 1)
	assert.Equal(t, "NYC", response.Metros[0].Code)

	// No connectivity since only one metro qualifies
	assert.Empty(t, response.Connectivity)
}

func TestGetMetroConnectivity_IncludesMetrosWithMaxUsers(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	// Insert devices - both metros have max_users > 0
	ctx := t.Context()
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users
		) VALUES
		('dev-nyc-1', now(), now(), generateUUIDv4(), 1, 0, 'dev-nyc-1', 'active', 'router', 'NYC-CORE-01', '10.0.0.1', '', 'metro-nyc', 100),
		('dev-lax-1', now(), now(), generateUUIDv4(), 2, 0, 'dev-lax-1', 'active', 'router', 'LAX-CORE-01', '10.0.1.1', '', 'metro-lax', 50)
	`)
	require.NoError(t, err)

	// Seed Neo4j with metros and ISIS-enabled devices
	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (nyc:Metro {pk: 'metro-nyc', code: 'NYC', name: 'New York'})
			CREATE (lax:Metro {pk: 'metro-lax', code: 'LAX', name: 'Los Angeles'})
			CREATE (devNyc:Device {pk: 'dev-nyc-1', code: 'NYC-CORE-01', isis_system_id: '0000.0000.0001'})
			CREATE (devLax:Device {pk: 'dev-lax-1', code: 'LAX-CORE-01', isis_system_id: '0000.0000.0002'})
			CREATE (devNyc)-[:LOCATED_IN]->(nyc)
			CREATE (devLax)-[:LOCATED_IN]->(lax)
			CREATE (devNyc)-[:ISIS_ADJACENT {metric: 10}]->(devLax)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	req := httptest.NewRequest(http.MethodGet, "/api/topology/metro-connectivity", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetroConnectivity(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.MetroConnectivityResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Error)

	// Both metros should be included
	assert.Len(t, response.Metros, 2)
	metroCodes := []string{response.Metros[0].Code, response.Metros[1].Code}
	assert.Contains(t, metroCodes, "NYC")
	assert.Contains(t, metroCodes, "LAX")

	// Should have connectivity between them (both directions)
	assert.Len(t, response.Connectivity, 2)
}

func TestGetMetroConnectivity_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	// No devices with max_users > 0
	ctx := t.Context()
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users
		) VALUES
		('dev-nyc-1', now(), now(), generateUUIDv4(), 1, 0, 'dev-nyc-1', 'active', 'router', 'NYC-CORE-01', '10.0.0.1', '', 'metro-nyc', 0)
	`)
	require.NoError(t, err)

	// Seed Neo4j with metros and ISIS-enabled devices
	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (nyc:Metro {pk: 'metro-nyc', code: 'NYC', name: 'New York'})
			CREATE (devNyc:Device {pk: 'dev-nyc-1', code: 'NYC-CORE-01', isis_system_id: '0000.0000.0001'})
			CREATE (devNyc)-[:LOCATED_IN]->(nyc)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	req := httptest.NewRequest(http.MethodGet, "/api/topology/metro-connectivity", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetroConnectivity(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.MetroConnectivityResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Error)

	// No metros should be returned
	assert.Empty(t, response.Metros)
	assert.Empty(t, response.Connectivity)
}
