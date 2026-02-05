//go:build evals

package evals_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
	serviceability "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	dztelemlatency "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/latency"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/stretchr/testify/require"
)

// TestLake_Agent_Evals_Anthropic_MultiHopLatency tests multi-hop latency queries.
// Uses multiple devices per metro to create competing paths, with no telemetry
// for the shortest path (agent must use committed RTT).
func TestLake_Agent_Evals_Anthropic_MultiHopLatency(t *testing.T) {
	t.Parallel()
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY not set, skipping eval test")
	}

	runTest_MultiHopLatency(t, newAnthropicLLMClient)
}

func runTest_MultiHopLatency(t *testing.T, llmFactory LLMClientFactory) {
	ctx := context.Background()
	debugLevel, debug := getDebugLevel()
	clientInfo := testClientInfo(t)

	conn, err := clientInfo.Client.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// Seed ClickHouse data (topology + distractor latency, but NO latency for answer path)
	seedMultiHopLatencyData(t, ctx, conn)
	validateMultiHopLatencyQuery(t, ctx, conn)

	// Get Neo4j client and seed graph data - REQUIRED for this test
	neo4jClient := testNeo4jClient(t)
	if neo4jClient == nil {
		t.Skip("Neo4j not available, skipping multi-hop latency test (requires graph database)")
	}
	seedMultiHopLatencyGraphData(t, ctx, neo4jClient)
	validateGraphData(t, ctx, neo4jClient, 10, 12) // 10 devices, 12 links (multi-device per metro)

	if testing.Short() {
		t.Log("Skipping workflow execution in short mode")
		return
	}

	p := setupWorkflowWithNeo4j(t, ctx, clientInfo, neo4jClient, llmFactory, debug, debugLevel)

	// Ask about latency between two metros that are NOT directly connected
	question := "what's the latency between Tokyo and Amsterdam?"
	result, err := p.Run(ctx, question)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Answer)

	response := result.Answer
	t.Logf("Workflow response:\n%s", response)

	// Check that Cypher was used to find the path (required since there's no direct TYO-AMS link)
	require.NotEmpty(t, result.ExecutedQueries, "Should have executed at least one query")

	foundCypher := false
	queryCount := 0
	for _, eq := range result.ExecutedQueries {
		query := eq.Result.QueryText()
		queryUpper := strings.ToUpper(query)
		queryCount++

		if debug {
			if debugLevel == 1 {
				t.Logf("Executed query: %s", truncate(query, 200))
			} else {
				t.Logf("Executed query: %s", query)
			}
		}

		if strings.Contains(queryUpper, "MATCH") {
			foundCypher = true
		}
	}

	// The agent must use Cypher to find the path (since there's no direct TYO-AMS link)
	require.True(t, foundCypher, "Should have used Cypher to find the path between non-directly-connected metros")

	// Agent shouldn't need excessive queries - if it's fumbling, it will do many
	// A good agent: 1-2 Cypher queries to find path, maybe 1 SQL to check for telemetry
	t.Logf("Total queries executed: %d", queryCount)
	if queryCount > 5 {
		t.Logf("WARNING: Agent executed %d queries - may be fumbling", queryCount)
	}

	// Verify the response contains path and latency information
	// Since there's NO measured latency for the TYO-FRA-AMS path, agent must use committed RTT:
	// - Committed RTT: 130ms TYO-FRA + 10ms FRA-AMS = ~140ms total
	expectations := []Expectation{
		{
			Description:   "Response identifies Frankfurt as intermediate hop",
			ExpectedValue: "Mentions Frankfurt (FRA) as the intermediate metro between Tokyo and Amsterdam",
			Rationale:     "There is no direct TYO-AMS link; shortest path goes through FRA",
		},
		{
			Description:   "Response includes a clear total latency estimate",
			ExpectedValue: "States a total end-to-end latency around 130-150ms (sum of committed RTT values)",
			Rationale:     "No measured telemetry exists for these links; agent must use committed RTT (~130ms + ~10ms = ~140ms)",
		},
		{
			Description:   "Response does NOT report latency from unrelated links",
			ExpectedValue: "Does NOT mention NYC-LON, SIN-HKG, or other distractor link latencies as the answer",
			Rationale:     "Distractor links have telemetry but are not on the TYO-AMS path",
		},
	}
	isCorrect, err := evaluateResponse(t, ctx, question, response, expectations...)
	require.NoError(t, err)
	require.True(t, isCorrect, "Evaluation failed for multi-hop latency")
}

// seedMultiHopLatencyData creates topology with multiple devices per metro.
// Paths TYO->AMS: via FRA (140ms, shortest), via LON (160ms), via SIN-FRA (220ms).
func seedMultiHopLatencyData(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	log := testLogger(t)
	now := testTime()

	metros := []serviceability.Metro{
		{PK: "metro1", Code: "tyo", Name: "Tokyo"},
		{PK: "metro2", Code: "fra", Name: "Frankfurt"},
		{PK: "metro3", Code: "ams", Name: "Amsterdam"},
		{PK: "metro4", Code: "sin", Name: "Singapore"},
		{PK: "metro5", Code: "lon", Name: "London"},
	}
	seedMetros(t, ctx, conn, metros, now, now)

	devices := []serviceability.Device{
		// Tokyo - 3 devices
		{PK: "device1", Code: "tyo-dzd1", Status: "activated", MetroPK: "metro1", DeviceType: "DZD"},
		{PK: "device2", Code: "tyo-dzd2", Status: "activated", MetroPK: "metro1", DeviceType: "DZD"},
		{PK: "device3", Code: "tyo-dzd3", Status: "activated", MetroPK: "metro1", DeviceType: "DZD"},
		// Frankfurt - 2 devices
		{PK: "device4", Code: "fra-dzd1", Status: "activated", MetroPK: "metro2", DeviceType: "DZD"},
		{PK: "device5", Code: "fra-dzd2", Status: "activated", MetroPK: "metro2", DeviceType: "DZD"},
		// Amsterdam - 2 devices
		{PK: "device6", Code: "ams-dzd1", Status: "activated", MetroPK: "metro3", DeviceType: "DZD"},
		{PK: "device7", Code: "ams-dzd2", Status: "activated", MetroPK: "metro3", DeviceType: "DZD"},
		// Singapore - 2 devices
		{PK: "device8", Code: "sin-dzd1", Status: "activated", MetroPK: "metro4", DeviceType: "DZD"},
		{PK: "device9", Code: "sin-dzd2", Status: "activated", MetroPK: "metro4", DeviceType: "DZD"},
		// London - 1 device
		{PK: "device10", Code: "lon-dzd1", Status: "activated", MetroPK: "metro5", DeviceType: "DZD"},
	}
	seedDevices(t, ctx, conn, devices, now, now)

	linkDS, err := serviceability.NewLinkDataset(log)
	require.NoError(t, err)

	links := []serviceability.Link{
		// TYO-FRA-AMS path (140ms total)
		{PK: "link1", Code: "tyo-fra-direct", Status: "activated", LinkType: "WAN", SideAPK: "device1", SideZPK: "device4", SideAIfaceName: "Ethernet1", SideZIfaceName: "Ethernet1", Bandwidth: 10000000000, CommittedRTTNs: 130000000, CommittedJitterNs: 5000000},
		{PK: "link2", Code: "fra-ams-direct", Status: "activated", LinkType: "WAN", SideAPK: "device4", SideZPK: "device6", SideAIfaceName: "Ethernet2", SideZIfaceName: "Ethernet1", Bandwidth: 10000000000, CommittedRTTNs: 10000000, CommittedJitterNs: 1000000},

		// TYO-SIN-FRA-AMS path (220ms)
		{PK: "link3", Code: "tyo-sin-1", Status: "activated", LinkType: "WAN", SideAPK: "device2", SideZPK: "device8", SideAIfaceName: "Ethernet1", SideZIfaceName: "Ethernet1", Bandwidth: 10000000000, CommittedRTTNs: 70000000, CommittedJitterNs: 3000000},
		{PK: "link4", Code: "sin-fra-1", Status: "activated", LinkType: "WAN", SideAPK: "device8", SideZPK: "device5", SideAIfaceName: "Ethernet2", SideZIfaceName: "Ethernet1", Bandwidth: 10000000000, CommittedRTTNs: 140000000, CommittedJitterNs: 6000000},
		{PK: "link5", Code: "fra-ams-via-sin", Status: "activated", LinkType: "WAN", SideAPK: "device5", SideZPK: "device7", SideAIfaceName: "Ethernet2", SideZIfaceName: "Ethernet1", Bandwidth: 10000000000, CommittedRTTNs: 10000000, CommittedJitterNs: 1000000},

		// TYO-LON-AMS path (160ms)
		{PK: "link6", Code: "tyo-lon-1", Status: "activated", LinkType: "WAN", SideAPK: "device3", SideZPK: "device10", SideAIfaceName: "Ethernet1", SideZIfaceName: "Ethernet1", Bandwidth: 10000000000, CommittedRTTNs: 150000000, CommittedJitterNs: 7000000},
		{PK: "link7", Code: "lon-ams-1", Status: "activated", LinkType: "WAN", SideAPK: "device10", SideZPK: "device6", SideAIfaceName: "Ethernet2", SideZIfaceName: "Ethernet2", Bandwidth: 10000000000, CommittedRTTNs: 10000000, CommittedJitterNs: 1000000},

		// Intra-metro links
		{PK: "link8", Code: "tyo-internal-1", Status: "activated", LinkType: "LAN", SideAPK: "device1", SideZPK: "device2", SideAIfaceName: "Ethernet3", SideZIfaceName: "Ethernet3", Bandwidth: 100000000000, CommittedRTTNs: 500000, CommittedJitterNs: 100000},
		{PK: "link9", Code: "tyo-internal-2", Status: "activated", LinkType: "LAN", SideAPK: "device2", SideZPK: "device3", SideAIfaceName: "Ethernet4", SideZIfaceName: "Ethernet3", Bandwidth: 100000000000, CommittedRTTNs: 500000, CommittedJitterNs: 100000},
		{PK: "link10", Code: "fra-internal-1", Status: "activated", LinkType: "LAN", SideAPK: "device4", SideZPK: "device5", SideAIfaceName: "Ethernet3", SideZIfaceName: "Ethernet3", Bandwidth: 100000000000, CommittedRTTNs: 500000, CommittedJitterNs: 100000},
		{PK: "link11", Code: "ams-internal-1", Status: "activated", LinkType: "LAN", SideAPK: "device6", SideZPK: "device7", SideAIfaceName: "Ethernet3", SideZIfaceName: "Ethernet3", Bandwidth: 100000000000, CommittedRTTNs: 500000, CommittedJitterNs: 100000},
		{PK: "link12", Code: "sin-internal-1", Status: "activated", LinkType: "LAN", SideAPK: "device8", SideZPK: "device9", SideAIfaceName: "Ethernet3", SideZIfaceName: "Ethernet3", Bandwidth: 100000000000, CommittedRTTNs: 500000, CommittedJitterNs: 100000},
	}
	var linkSchema serviceability.LinkSchema
	err = linkDS.WriteBatch(ctx, conn, len(links), func(i int) ([]any, error) {
		return linkSchema.ToRow(links[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		SnapshotTS: now,
		OpID:       testOpID(),
	})
	require.NoError(t, err)

	// Latency samples for some links (not the shortest path)
	latencyDS, err := dztelemlatency.NewDeviceLinkLatencyDataset(log)
	require.NoError(t, err)
	ingestedAt := now
	latencySamples := []struct {
		time           time.Time
		epoch          int64
		sampleIndex    int32
		originDevicePK string
		targetDevicePK string
		linkPK         string
		rttUs          uint32
		loss           bool
		ipdvUs         *int64
	}{
		// TYO-SIN: ~65ms measured (Path 2)
		{now.Add(-10 * time.Minute), 100, 1, "device2", "device8", "link3", 63000, false, int64Ptr(2000)},
		{now.Add(-9 * time.Minute), 100, 2, "device2", "device8", "link3", 65000, false, int64Ptr(2500)},
		{now.Add(-8 * time.Minute), 100, 3, "device2", "device8", "link3", 67000, false, int64Ptr(3000)},
		{now.Add(-7 * time.Minute), 100, 4, "device2", "device8", "link3", 64000, false, int64Ptr(2200)},
		{now.Add(-6 * time.Minute), 100, 5, "device2", "device8", "link3", 66000, false, int64Ptr(2800)},

		// SIN-FRA: ~145ms measured (Path 2)
		{now.Add(-10 * time.Minute), 100, 1, "device8", "device5", "link4", 143000, false, int64Ptr(5000)},
		{now.Add(-9 * time.Minute), 100, 2, "device8", "device5", "link4", 145000, false, int64Ptr(5500)},
		{now.Add(-8 * time.Minute), 100, 3, "device8", "device5", "link4", 147000, false, int64Ptr(6000)},
		{now.Add(-7 * time.Minute), 100, 4, "device8", "device5", "link4", 144000, false, int64Ptr(5200)},
		{now.Add(-6 * time.Minute), 100, 5, "device8", "device5", "link4", 146000, false, int64Ptr(5800)},

		// LON-AMS: ~8ms measured (Path 3)
		{now.Add(-10 * time.Minute), 100, 1, "device10", "device6", "link7", 7500, false, int64Ptr(300)},
		{now.Add(-9 * time.Minute), 100, 2, "device10", "device6", "link7", 8000, false, int64Ptr(400)},
		{now.Add(-8 * time.Minute), 100, 3, "device10", "device6", "link7", 8500, false, int64Ptr(500)},
		{now.Add(-7 * time.Minute), 100, 4, "device10", "device6", "link7", 7800, false, int64Ptr(350)},
		{now.Add(-6 * time.Minute), 100, 5, "device10", "device6", "link7", 8200, false, int64Ptr(450)},

		// No telemetry for shortest path links
	}
	err = latencyDS.WriteBatch(ctx, conn, len(latencySamples), func(i int) ([]any, error) {
		s := latencySamples[i]
		return []any{
			s.time.UTC(),     // event_ts
			ingestedAt,       // ingested_at
			s.epoch,          // epoch
			s.sampleIndex,    // sample_index
			s.originDevicePK, // origin_device_pk
			s.targetDevicePK, // target_device_pk
			s.linkPK,         // link_pk
			int64(s.rttUs),   // rtt_us
			s.loss,           // loss
			s.ipdvUs,         // ipdv_us
		}, nil
	})
	require.NoError(t, err)
}

func validateMultiHopLatencyQuery(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	// Verify there are NO direct TYO-AMS links
	directQuery := `
SELECT l.code
FROM dz_links_current l
JOIN dz_devices_current da ON l.side_a_pk = da.pk
JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
JOIN dz_metros_current ma ON da.metro_pk = ma.pk
JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
WHERE (ma.code = 'tyo' AND mz.code = 'ams') OR (ma.code = 'ams' AND mz.code = 'tyo')
`
	directResult, err := dataset.Query(ctx, conn, directQuery, nil)
	require.NoError(t, err)
	require.Equal(t, 0, directResult.Count, "Should have NO direct links between TYO and AMS")

	// Verify multiple devices per metro (key for this test)
	deviceCountQuery := `
SELECT m.code, COUNT(*) as device_count
FROM dz_devices_current d
JOIN dz_metros_current m ON d.metro_pk = m.pk
GROUP BY m.code
ORDER BY m.code
`
	deviceCountResult, err := dataset.Query(ctx, conn, deviceCountQuery, nil)
	require.NoError(t, err)
	t.Logf("Devices per metro: %d rows", deviceCountResult.Count)
	require.GreaterOrEqual(t, deviceCountResult.Count, 3, "Should have at least 3 metros with devices")
	foundTokyo := false
	for _, row := range deviceCountResult.Rows {
		metro, ok := row["code"].(string)
		if !ok {
			// Try m.code in case the alias wasn't applied
			metro, _ = row["m.code"].(string)
		}
		count, _ := row["device_count"].(uint64)
		t.Logf("  %s: %d devices", metro, count)
		if metro == "tyo" {
			require.Equal(t, uint64(3), count, "Tokyo should have 3 devices")
			foundTokyo = true
		}
	}
	require.True(t, foundTokyo, "Tokyo metro should exist in device counts")

	// Verify we have latency data for some links (not the shortest path)
	latencyQuery := `
SELECT l.code, COUNT(*) as samples
FROM fact_dz_device_link_latency f
JOIN dz_links_current l ON f.link_pk = l.pk
WHERE f.event_ts >= now() - INTERVAL 24 HOUR
GROUP BY l.code
ORDER BY l.code
`
	latencyResult, err := dataset.Query(ctx, conn, latencyQuery, nil)
	require.NoError(t, err)
	require.Equal(t, 3, latencyResult.Count, "Should have latency data for 3 links")

	// Verify NO latency data exists for the shortest path links
	answerPathQuery := `
SELECT COUNT(*) as cnt
FROM fact_dz_device_link_latency f
JOIN dz_links_current l ON f.link_pk = l.pk
WHERE l.code IN ('tyo-fra-direct', 'fra-ams-direct')
`
	answerPathResult, err := dataset.Query(ctx, conn, answerPathQuery, nil)
	require.NoError(t, err)
	cnt := answerPathResult.Rows[0]["cnt"].(uint64)
	require.Equal(t, uint64(0), cnt, "Should have NO latency data for shortest path links")

	t.Log("Database validation passed")
}

// seedMultiHopLatencyGraphData seeds Neo4j topology matching ClickHouse.
func seedMultiHopLatencyGraphData(t *testing.T, ctx context.Context, client neo4j.Client) {
	metros := []graphMetro{
		{PK: "metro1", Code: "tyo", Name: "Tokyo"},
		{PK: "metro2", Code: "fra", Name: "Frankfurt"},
		{PK: "metro3", Code: "ams", Name: "Amsterdam"},
		{PK: "metro4", Code: "sin", Name: "Singapore"},
		{PK: "metro5", Code: "lon", Name: "London"},
	}
	devices := []graphDevice{
		// Tokyo - 3 devices
		{PK: "device1", Code: "tyo-dzd1", Status: "activated", MetroPK: "metro1", MetroCode: "tyo"},
		{PK: "device2", Code: "tyo-dzd2", Status: "activated", MetroPK: "metro1", MetroCode: "tyo"},
		{PK: "device3", Code: "tyo-dzd3", Status: "activated", MetroPK: "metro1", MetroCode: "tyo"},
		// Frankfurt - 2 devices
		{PK: "device4", Code: "fra-dzd1", Status: "activated", MetroPK: "metro2", MetroCode: "fra"},
		{PK: "device5", Code: "fra-dzd2", Status: "activated", MetroPK: "metro2", MetroCode: "fra"},
		// Amsterdam - 2 devices
		{PK: "device6", Code: "ams-dzd1", Status: "activated", MetroPK: "metro3", MetroCode: "ams"},
		{PK: "device7", Code: "ams-dzd2", Status: "activated", MetroPK: "metro3", MetroCode: "ams"},
		// Singapore - 2 devices
		{PK: "device8", Code: "sin-dzd1", Status: "activated", MetroPK: "metro4", MetroCode: "sin"},
		{PK: "device9", Code: "sin-dzd2", Status: "activated", MetroPK: "metro4", MetroCode: "sin"},
		// London - 1 device
		{PK: "device10", Code: "lon-dzd1", Status: "activated", MetroPK: "metro5", MetroCode: "lon"},
	}
	links := []graphLink{
		// TYO-FRA-AMS path (140ms)
		{PK: "link1", Code: "tyo-fra-direct", Status: "activated", SideAPK: "device1", SideZPK: "device4", CommittedRTTNs: 130000000},
		{PK: "link2", Code: "fra-ams-direct", Status: "activated", SideAPK: "device4", SideZPK: "device6", CommittedRTTNs: 10000000},

		// TYO-SIN-FRA-AMS path (220ms)
		{PK: "link3", Code: "tyo-sin-1", Status: "activated", SideAPK: "device2", SideZPK: "device8", CommittedRTTNs: 70000000},
		{PK: "link4", Code: "sin-fra-1", Status: "activated", SideAPK: "device8", SideZPK: "device5", CommittedRTTNs: 140000000},
		{PK: "link5", Code: "fra-ams-via-sin", Status: "activated", SideAPK: "device5", SideZPK: "device7", CommittedRTTNs: 10000000},

		// TYO-LON-AMS path (160ms)
		{PK: "link6", Code: "tyo-lon-1", Status: "activated", SideAPK: "device3", SideZPK: "device10", CommittedRTTNs: 150000000},
		{PK: "link7", Code: "lon-ams-1", Status: "activated", SideAPK: "device10", SideZPK: "device6", CommittedRTTNs: 10000000},

		// Intra-metro links
		{PK: "link8", Code: "tyo-internal-1", Status: "activated", SideAPK: "device1", SideZPK: "device2", CommittedRTTNs: 500000},
		{PK: "link9", Code: "tyo-internal-2", Status: "activated", SideAPK: "device2", SideZPK: "device3", CommittedRTTNs: 500000},
		{PK: "link10", Code: "fra-internal-1", Status: "activated", SideAPK: "device4", SideZPK: "device5", CommittedRTTNs: 500000},
		{PK: "link11", Code: "ams-internal-1", Status: "activated", SideAPK: "device6", SideZPK: "device7", CommittedRTTNs: 500000},
		{PK: "link12", Code: "sin-internal-1", Status: "activated", SideAPK: "device8", SideZPK: "device9", CommittedRTTNs: 500000},
	}

	seedGraphData(t, ctx, client, metros, devices, links)
}
