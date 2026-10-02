//go:build evals

package evals_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
	serviceability "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	"github.com/stretchr/testify/require"
)

func TestLake_Agent_Evals_Anthropic_MulticastDeviceCapacity(t *testing.T) {
	t.Parallel()
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY not set, skipping eval test")
	}

	runTest_MulticastDeviceCapacity(t, newAnthropicLLMClient)
}

func runTest_MulticastDeviceCapacity(t *testing.T, llmFactory LLMClientFactory) {
	ctx := context.Background()

	debugLevel, debug := getDebugLevel()

	clientInfo := testClientInfo(t)

	conn, err := clientInfo.Client.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	seedMulticastDeviceCapacityData(t, ctx, conn)

	validateMulticastDeviceCapacityQuery(t, ctx, conn)

	if testing.Short() {
		t.Log("Skipping workflow execution in short mode")
		return
	}

	p := setupWorkflow(t, ctx, clientInfo, llmFactory, debug, debugLevel)

	question := "which DZDs have capacity for new multicast subscribers?"
	if debug {
		if debugLevel == 1 {
			t.Logf("=== Query: '%s' ===\n", question)
		} else {
			t.Logf("=== Starting workflow query: '%s' ===\n", question)
		}
	}
	result, err := p.Run(ctx, question)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Answer)

	response := result.Answer
	if debug {
		if debugLevel == 1 {
			t.Logf("=== Response ===\n%s\n", response)
		} else {
			t.Logf("\n=== Final Workflow Response ===\n%s\n", response)
		}
	} else {
		t.Logf("Workflow response:\n%s", response)
	}

	expectations := []Expectation{
		{
			Description:   "Devices with capacity identified",
			ExpectedValue: "nyc-dzd-a and nyc-dzd-c are reported as having capacity for new multicast subscribers",
			Rationale:     "nyc-dzd-a has max 5 with 2 current subscribers (3 available); nyc-dzd-c has max 10 with 0 current subscribers (10 available)",
		},
		{
			Description:   "Full device excluded",
			ExpectedValue: "nyc-dzd-b is NOT reported as having capacity (it is full)",
			Rationale:     "nyc-dzd-b has max 3 with 3 current subscribers, so 0 available",
		},
		{
			Description:   "Counts derived from live users, not stale on-chain counts",
			ExpectedValue: "the current subscriber counts reflect the attached multicast users (a=2, c=0), not the device's on-chain multicast_subscribers_count fields (a=0, c=8)",
			Rationale:     "the on-chain device counts are stale; current subscribers must come from dz_users_current",
		},
	}
	isCorrect, err := evaluateResponse(t, ctx, question, response, expectations...)
	require.NoError(t, err, "Evaluation must be available")
	require.True(t, isCorrect, "Evaluation indicates the response does not correctly identify device multicast subscriber capacity")
}

// seedMulticastDeviceCapacityData seeds data for the device multicast capacity test.
// Scenario (1 metro, 3 devices, 1 multicast group):
//   - nyc-dzd-a: max_multicast_subscribers=5, 2 live subscribers -> 3 available (HAS capacity).
//     On-chain multicast_subscribers_count is left stale at 0.
//   - nyc-dzd-b: max_multicast_subscribers=3, 3 live subscribers -> 0 available (FULL).
//   - nyc-dzd-c: max_multicast_subscribers=10, 0 live subscribers -> 10 available (HAS capacity).
//     On-chain multicast_subscribers_count is set to a stale, misleadingly-high 8.
//
// The stale on-chain counts (a=0, c=8) deliberately disagree with the live user
// data so the eval fails if the agent reads the device count columns instead of
// counting attached users.
func seedMulticastDeviceCapacityData(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	now := testTime()
	opID := uuid.New()

	metros := []serviceability.Metro{
		{PK: "metro1", Code: "nyc", Name: "New York"},
	}
	seedMetros(t, ctx, conn, metros, now, now)

	devices := []serviceability.Device{
		{PK: "device-a", Code: "nyc-dzd-a", Status: "activated", MetroPK: "metro1", DeviceType: "DZD", MaxMulticastSubscribers: 5, MulticastSubscribersCount: 0},
		{PK: "device-b", Code: "nyc-dzd-b", Status: "activated", MetroPK: "metro1", DeviceType: "DZD", MaxMulticastSubscribers: 3, MulticastSubscribersCount: 0},
		{PK: "device-c", Code: "nyc-dzd-c", Status: "activated", MetroPK: "metro1", DeviceType: "DZD", MaxMulticastSubscribers: 10, MulticastSubscribersCount: 8},
	}
	seedDevices(t, ctx, conn, devices, now, now)

	groupPK := "group1"
	multicastGroups := []serviceability.MulticastGroup{
		{PK: groupPK, OwnerPubkey: "group-owner1", Code: "solana-shreds", MulticastIP: net.ParseIP("239.1.1.1"), MaxBandwidth: 1000000000, Status: "activated", PublisherCount: 0, SubscriberCount: 5},
	}
	seedMulticastGroups(t, ctx, conn, multicastGroups, now, opID)

	users := []serviceability.User{
		// device-a: 2 subscribers
		{PK: "ua1", OwnerPubkey: "owner-a1", Status: "activated", DevicePK: "device-a", DZIP: net.ParseIP("10.1.1.1"), ClientIP: net.ParseIP("203.0.113.1"), Kind: "multicast", TunnelID: 101, Subscribers: []string{groupPK}},
		{PK: "ua2", OwnerPubkey: "owner-a2", Status: "activated", DevicePK: "device-a", DZIP: net.ParseIP("10.1.1.2"), ClientIP: net.ParseIP("203.0.113.2"), Kind: "multicast", TunnelID: 102, Subscribers: []string{groupPK}},
		// device-b: 3 subscribers
		{PK: "ub1", OwnerPubkey: "owner-b1", Status: "activated", DevicePK: "device-b", DZIP: net.ParseIP("10.1.2.1"), ClientIP: net.ParseIP("203.0.113.3"), Kind: "multicast", TunnelID: 103, Subscribers: []string{groupPK}},
		{PK: "ub2", OwnerPubkey: "owner-b2", Status: "activated", DevicePK: "device-b", DZIP: net.ParseIP("10.1.2.2"), ClientIP: net.ParseIP("203.0.113.4"), Kind: "multicast", TunnelID: 104, Subscribers: []string{groupPK}},
		{PK: "ub3", OwnerPubkey: "owner-b3", Status: "activated", DevicePK: "device-b", DZIP: net.ParseIP("10.1.2.3"), ClientIP: net.ParseIP("203.0.113.5"), Kind: "multicast", TunnelID: 105, Subscribers: []string{groupPK}},
		// device-c: 0 subscribers (no multicast users attached)
	}
	seedUsers(t, ctx, conn, users, now, now, opID)
}

func validateMulticastDeviceCapacityQuery(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	query := `
WITH subs AS (
    SELECT device_pk, countIf(JSONLength(subscribers) > 0) AS current_subscribers
    FROM dz_users_current
    WHERE status = 'activated' AND kind = 'multicast'
    GROUP BY device_pk
)
SELECT
    d.code AS code,
    d.max_multicast_subscribers AS max_subscribers,
    COALESCE(s.current_subscribers, 0) AS current_subscribers,
    toInt64(d.max_multicast_subscribers) - toInt64(COALESCE(s.current_subscribers, 0)) AS available
FROM dz_devices_current d
LEFT JOIN subs s ON s.device_pk = d.pk
WHERE d.max_multicast_subscribers > 0
ORDER BY d.code
`
	result, err := dataset.Query(ctx, conn, query, nil)
	require.NoError(t, err, "Failed to execute device multicast capacity query")
	require.Equal(t, 3, result.Count, "Should have exactly 3 devices with a configured subscriber max")

	want := map[string]struct {
		current, available int64
	}{
		"nyc-dzd-a": {current: 2, available: 3},
		"nyc-dzd-b": {current: 3, available: 0},
		"nyc-dzd-c": {current: 0, available: 10},
	}
	for _, row := range result.Rows {
		code := row["code"].(string)
		current := int64(row["current_subscribers"].(uint64))
		available := row["available"].(int64)
		exp, ok := want[code]
		require.True(t, ok, "unexpected device %s", code)
		require.Equal(t, exp.current, current, "%s current subscribers", code)
		require.Equal(t, exp.available, available, "%s available capacity", code)
	}

	t.Logf("Database validation passed: nyc-dzd-a (3 avail), nyc-dzd-b (0 avail), nyc-dzd-c (10 avail)")
}
