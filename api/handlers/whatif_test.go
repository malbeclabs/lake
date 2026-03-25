package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postWhatIfRemoval(t *testing.T, req handlers.WhatIfRemovalRequest) handlers.WhatIfRemovalResponse {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)

	httpReq := httptest.NewRequest(http.MethodPost, "/api/topology/whatif-removal", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	handlers.PostWhatIfRemoval(rr, httpReq)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.WhatIfRemovalResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Error)
	return response
}

// TestPostWhatIfRemoval_DeviceRemoval_AffectedPath verifies that when a device
// is removed and the through-metric is cheaper than the alternate, the path is
// reported as affected.
//
// Topology:
//
//	A --10-- B --10-- C
//	|                 |
//	50               50
//	|                 |
//	D ------50------ E
//
// Remove B: neighbors A(10) and C(10), throughMetric=20.
// Alt path A-D-E-C: 50+50+50=150, so 20 <= 150 → affected, degraded.
func TestPostWhatIfRemoval_DeviceRemoval_AffectedPath(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (a:Device {pk: 'dev-a', code: 'DEV-A', isis_system_id: '0000.0000.0001'})
			CREATE (b:Device {pk: 'dev-b', code: 'DEV-B', isis_system_id: '0000.0000.0002'})
			CREATE (c:Device {pk: 'dev-c', code: 'DEV-C', isis_system_id: '0000.0000.0003'})
			CREATE (d:Device {pk: 'dev-d', code: 'DEV-D', isis_system_id: '0000.0000.0004'})
			CREATE (e:Device {pk: 'dev-e', code: 'DEV-E', isis_system_id: '0000.0000.0005'})
			CREATE (ma:Metro {pk: 'metro-a', code: 'AAA'})
			CREATE (mb:Metro {pk: 'metro-b', code: 'BBB'})
			CREATE (a)-[:LOCATED_IN]->(ma)
			CREATE (b)-[:LOCATED_IN]->(mb)
			CREATE (c)-[:LOCATED_IN]->(mb)
			CREATE (d)-[:LOCATED_IN]->(ma)
			CREATE (e)-[:LOCATED_IN]->(mb)
			CREATE (a)-[:ISIS_ADJACENT {metric: 10}]->(b)
			CREATE (b)-[:ISIS_ADJACENT {metric: 10}]->(c)
			CREATE (a)-[:ISIS_ADJACENT {metric: 50}]->(d)
			CREATE (d)-[:ISIS_ADJACENT {metric: 50}]->(e)
			CREATE (e)-[:ISIS_ADJACENT {metric: 50}]->(c)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	response := postWhatIfRemoval(t, handlers.WhatIfRemovalRequest{
		Devices: []string{"dev-b"},
	})

	require.Len(t, response.Items, 1)
	item := response.Items[0]
	assert.Equal(t, "device", item.Type)
	assert.Equal(t, "dev-b", item.PK)
	assert.Equal(t, "DEV-B", item.Code)

	// Pair (A, C) should be affected — throughMetric 20 <= altMetric 150
	require.NotEmpty(t, item.AffectedPaths)
	assert.Equal(t, 1, item.AffectedPathCount)

	path := item.AffectedPaths[0]
	codes := []string{path.Source, path.Target}
	sort.Strings(codes)
	assert.Equal(t, []string{"DEV-A", "DEV-C"}, codes)
	assert.Equal(t, 2, path.HopsBefore)
	assert.Equal(t, 20, path.MetricBefore)
	assert.Equal(t, 150, path.MetricAfter)
	assert.Equal(t, 3, path.HopsAfter) // A-D-E-C = 3 relationships
	assert.Equal(t, "degraded", path.Status)
}

// TestPostWhatIfRemoval_DeviceRemoval_NotAffected verifies that when the
// alternate path is cheaper than the through-metric, the pair is NOT reported.
//
// Topology:
//
//	A --10-- B --10-- C
//	|                 |
//	5                 5
//	|                 |
//	D -------5------- E
//
// Remove B: neighbors A(10) and C(10), throughMetric=20.
// Alt path A-D-E-C: 5+5+5=15, so 20 > 15 → NOT affected (ISIS routes via D-E-C).
func TestPostWhatIfRemoval_DeviceRemoval_NotAffected(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (a:Device {pk: 'dev-a', code: 'DEV-A', isis_system_id: '0000.0000.0001'})
			CREATE (b:Device {pk: 'dev-b', code: 'DEV-B', isis_system_id: '0000.0000.0002'})
			CREATE (c:Device {pk: 'dev-c', code: 'DEV-C', isis_system_id: '0000.0000.0003'})
			CREATE (d:Device {pk: 'dev-d', code: 'DEV-D', isis_system_id: '0000.0000.0004'})
			CREATE (e:Device {pk: 'dev-e', code: 'DEV-E', isis_system_id: '0000.0000.0005'})
			CREATE (a)-[:ISIS_ADJACENT {metric: 10}]->(b)
			CREATE (b)-[:ISIS_ADJACENT {metric: 10}]->(c)
			CREATE (a)-[:ISIS_ADJACENT {metric: 5}]->(d)
			CREATE (d)-[:ISIS_ADJACENT {metric: 5}]->(e)
			CREATE (e)-[:ISIS_ADJACENT {metric: 5}]->(c)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	response := postWhatIfRemoval(t, handlers.WhatIfRemovalRequest{
		Devices: []string{"dev-b"},
	})

	require.Len(t, response.Items, 1)
	item := response.Items[0]
	assert.Equal(t, "DEV-B", item.Code)

	// No affected paths — alt is cheaper than through
	assert.Empty(t, item.AffectedPaths)
	assert.Equal(t, 0, item.AffectedPathCount)
}

// TestPostWhatIfRemoval_DeviceRemoval_DisconnectedLeaf verifies that leaf
// neighbors (degree 1) are reported as disconnected when the device is removed.
//
// Topology:
//
//	A --10-- B --10-- C --10-- D
//
// Remove B: A is a leaf (only connected to B) → disconnected.
func TestPostWhatIfRemoval_DeviceRemoval_DisconnectedLeaf(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (a:Device {pk: 'dev-a', code: 'DEV-A', isis_system_id: '0000.0000.0001'})
			CREATE (b:Device {pk: 'dev-b', code: 'DEV-B', isis_system_id: '0000.0000.0002'})
			CREATE (c:Device {pk: 'dev-c', code: 'DEV-C', isis_system_id: '0000.0000.0003'})
			CREATE (d:Device {pk: 'dev-d', code: 'DEV-D', isis_system_id: '0000.0000.0004'})
			CREATE (a)-[:ISIS_ADJACENT {metric: 10}]->(b)
			CREATE (b)-[:ISIS_ADJACENT {metric: 10}]->(c)
			CREATE (c)-[:ISIS_ADJACENT {metric: 10}]->(d)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	response := postWhatIfRemoval(t, handlers.WhatIfRemovalRequest{
		Devices: []string{"dev-b"},
	})

	require.Len(t, response.Items, 1)
	item := response.Items[0]

	// A is a leaf — should be disconnected
	assert.True(t, item.CausesPartition)
	assert.Contains(t, item.DisconnectedDevices, "DEV-A")
	assert.Equal(t, 1, item.DisconnectedCount)
}

// TestPostWhatIfRemoval_LinkRemoval_AffectedPath verifies link removal with
// metric-based routing. When the through-link metric is cheaper than the alt,
// the path is reported as affected.
//
// Topology:
//
//	A --10-- B --10-- C
//	         |        |
//	        50       50
//	         |        |
//	         D --50-- E
//
// Link B-C (metric 10). Source=B neighbors (excl C): A(10), D(50).
// Target=C neighbors (excl B): E(50).
// Pair (A, E): throughMetric = 10 + 10 + 50 = 70.
// Alt path A-B-D-E: 10+50+50=110. So 70 <= 110 → affected.
func TestPostWhatIfRemoval_LinkRemoval_AffectedPath(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	ctx := t.Context()
	// Insert devices into ClickHouse so envDB can look them up
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users
		) VALUES
		('dev-a', now(), now(), generateUUIDv4(), 1, 0, 'dev-a', 'active', 'router', 'DEV-A', '10.0.0.1', '', '', 0),
		('dev-b', now(), now(), generateUUIDv4(), 2, 0, 'dev-b', 'active', 'router', 'DEV-B', '10.0.0.2', '', '', 0),
		('dev-c', now(), now(), generateUUIDv4(), 3, 0, 'dev-c', 'active', 'router', 'DEV-C', '10.0.0.3', '', '', 0),
		('dev-d', now(), now(), generateUUIDv4(), 4, 0, 'dev-d', 'active', 'router', 'DEV-D', '10.0.0.4', '', '', 0),
		('dev-e', now(), now(), generateUUIDv4(), 5, 0, 'dev-e', 'active', 'router', 'DEV-E', '10.0.0.5', '', '', 0)
	`)
	require.NoError(t, err)

	// Insert the link into ClickHouse
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_links_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, code, tunnel_net, contributor_pk,
			side_a_pk, side_z_pk, side_a_iface_name, side_z_iface_name,
			link_type, committed_rtt_ns, committed_jitter_ns, bandwidth_bps
		) VALUES
		('link-bc', now(), now(), generateUUIDv4(), 1, 0,
		 'link-bc', 'active', 'LINK-BC', '', '',
		 'dev-b', 'dev-c', 'eth0', 'eth1',
		 'fiber', 0, 0, 0)
	`)
	require.NoError(t, err)

	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (a:Device {pk: 'dev-a', code: 'DEV-A', isis_system_id: '0000.0000.0001'})
			CREATE (b:Device {pk: 'dev-b', code: 'DEV-B', isis_system_id: '0000.0000.0002'})
			CREATE (c:Device {pk: 'dev-c', code: 'DEV-C', isis_system_id: '0000.0000.0003'})
			CREATE (d:Device {pk: 'dev-d', code: 'DEV-D', isis_system_id: '0000.0000.0004'})
			CREATE (e:Device {pk: 'dev-e', code: 'DEV-E', isis_system_id: '0000.0000.0005'})
			CREATE (ma:Metro {pk: 'metro-a', code: 'AAA'})
			CREATE (mb:Metro {pk: 'metro-b', code: 'BBB'})
			CREATE (a)-[:LOCATED_IN]->(ma)
			CREATE (b)-[:LOCATED_IN]->(ma)
			CREATE (c)-[:LOCATED_IN]->(mb)
			CREATE (d)-[:LOCATED_IN]->(ma)
			CREATE (e)-[:LOCATED_IN]->(mb)
			CREATE (a)-[:ISIS_ADJACENT {metric: 10}]->(b)
			CREATE (b)-[:ISIS_ADJACENT {metric: 10}]->(c)
			CREATE (b)-[:ISIS_ADJACENT {metric: 50}]->(d)
			CREATE (d)-[:ISIS_ADJACENT {metric: 50}]->(e)
			CREATE (c)-[:ISIS_ADJACENT {metric: 50}]->(e)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	response := postWhatIfRemoval(t, handlers.WhatIfRemovalRequest{
		Links: []string{"link-bc"},
	})

	require.Len(t, response.Items, 1)
	item := response.Items[0]
	assert.Equal(t, "link", item.Type)
	assert.Equal(t, "link-bc", item.PK)
	assert.Equal(t, "DEV-B - DEV-C", item.Code)

	// Should have affected paths
	require.NotEmpty(t, item.AffectedPaths)
	assert.Greater(t, item.AffectedPathCount, 0)

	// Find the (A, E) pair
	var foundAE bool
	for _, p := range item.AffectedPaths {
		codes := []string{p.Source, p.Target}
		sort.Strings(codes)
		if codes[0] == "DEV-A" && codes[1] == "DEV-E" {
			foundAE = true
			assert.Equal(t, 3, p.HopsBefore)
			assert.Equal(t, 70, p.MetricBefore) // 10 + 10 + 50
			assert.Greater(t, p.HopsAfter, 0)
			assert.Greater(t, p.MetricAfter, 0)
		}
	}
	assert.True(t, foundAE, "expected affected path between DEV-A and DEV-E")
}

// TestPostWhatIfRemoval_LinkRemoval_NotAffected verifies that when an alternate
// path is cheaper than through the removed link, the pair is not reported.
//
// Topology:
//
//	A --10-- B --100-- C --10-- D
//	         |                  |
//	         5                  5
//	         |                  |
//	         E ------5--------- F
//
// Link B-C (metric 100). Source=B neighbors (excl C): A(10), E(5).
// Target=C neighbors (excl B): D(10).
// Pair (A, D): throughMetric = 10 + 100 + 10 = 120. Alt A-B-E-F-D-C path?
// Actually the alt for (A, D) avoiding B-C link: A-B-E-F-D = 10+5+5+5 = 25. 120 > 25 → NOT affected.
func TestPostWhatIfRemoval_LinkRemoval_NotAffected(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	ctx := t.Context()
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users
		) VALUES
		('dev-a', now(), now(), generateUUIDv4(), 1, 0, 'dev-a', 'active', 'router', 'DEV-A', '10.0.0.1', '', '', 0),
		('dev-b', now(), now(), generateUUIDv4(), 2, 0, 'dev-b', 'active', 'router', 'DEV-B', '10.0.0.2', '', '', 0),
		('dev-c', now(), now(), generateUUIDv4(), 3, 0, 'dev-c', 'active', 'router', 'DEV-C', '10.0.0.3', '', '', 0),
		('dev-d', now(), now(), generateUUIDv4(), 4, 0, 'dev-d', 'active', 'router', 'DEV-D', '10.0.0.4', '', '', 0),
		('dev-e', now(), now(), generateUUIDv4(), 5, 0, 'dev-e', 'active', 'router', 'DEV-E', '10.0.0.5', '', '', 0),
		('dev-f', now(), now(), generateUUIDv4(), 6, 0, 'dev-f', 'active', 'router', 'DEV-F', '10.0.0.6', '', '', 0)
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_links_history (
			entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, is_deleted,
			pk, status, code, tunnel_net, contributor_pk,
			side_a_pk, side_z_pk, side_a_iface_name, side_z_iface_name,
			link_type, committed_rtt_ns, committed_jitter_ns, bandwidth_bps
		) VALUES
		('link-bc', now(), now(), generateUUIDv4(), 1, 0,
		 'link-bc', 'active', 'LINK-BC', '', '',
		 'dev-b', 'dev-c', 'eth0', 'eth1',
		 'fiber', 0, 0, 0)
	`)
	require.NoError(t, err)

	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (a:Device {pk: 'dev-a', code: 'DEV-A', isis_system_id: '0000.0000.0001'})
			CREATE (b:Device {pk: 'dev-b', code: 'DEV-B', isis_system_id: '0000.0000.0002'})
			CREATE (c:Device {pk: 'dev-c', code: 'DEV-C', isis_system_id: '0000.0000.0003'})
			CREATE (d:Device {pk: 'dev-d', code: 'DEV-D', isis_system_id: '0000.0000.0004'})
			CREATE (e:Device {pk: 'dev-e', code: 'DEV-E', isis_system_id: '0000.0000.0005'})
			CREATE (f:Device {pk: 'dev-f', code: 'DEV-F', isis_system_id: '0000.0000.0006'})
			CREATE (a)-[:ISIS_ADJACENT {metric: 10}]->(b)
			CREATE (b)-[:ISIS_ADJACENT {metric: 100}]->(c)
			CREATE (c)-[:ISIS_ADJACENT {metric: 10}]->(d)
			CREATE (b)-[:ISIS_ADJACENT {metric: 5}]->(e)
			CREATE (e)-[:ISIS_ADJACENT {metric: 5}]->(f)
			CREATE (f)-[:ISIS_ADJACENT {metric: 5}]->(d)
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	response := postWhatIfRemoval(t, handlers.WhatIfRemovalRequest{
		Links: []string{"link-bc"},
	})

	require.Len(t, response.Items, 1)
	item := response.Items[0]
	assert.Equal(t, "DEV-B - DEV-C", item.Code)

	// All pairs should have cheaper alt paths — nothing affected
	assert.Empty(t, item.AffectedPaths)
	assert.Equal(t, 0, item.AffectedPathCount)
}

// TestPostWhatIfRemoval_DeviceNotFound verifies the handler returns gracefully
// when the device PK doesn't exist in Neo4j.
func TestPostWhatIfRemoval_DeviceNotFound(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		return nil // empty graph
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	response := postWhatIfRemoval(t, handlers.WhatIfRemovalRequest{
		Devices: []string{"nonexistent-device"},
	})

	require.Len(t, response.Items, 1)
	item := response.Items[0]
	assert.Equal(t, "device", item.Type)
	assert.Equal(t, "nonexistent-device", item.PK)
	assert.Empty(t, item.AffectedPaths)
	assert.Equal(t, 0, item.AffectedPathCount)
}

// TestPostWhatIfRemoval_EmptyRequest verifies the handler returns an error
// when no devices or links are specified.
func TestPostWhatIfRemoval_EmptyRequest(t *testing.T) {
	body, err := json.Marshal(handlers.WhatIfRemovalRequest{})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/topology/whatif-removal", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	handlers.PostWhatIfRemoval(rr, req)

	var response handlers.WhatIfRemovalResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Contains(t, response.Error, "No devices or links specified")
}
