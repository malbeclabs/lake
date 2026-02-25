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

func TestLake_Agent_Evals_Anthropic_MulticastGroupSummary(t *testing.T) {
	t.Parallel()
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY not set, skipping eval test")
	}

	runTest_MulticastGroupSummary(t, newAnthropicLLMClient)
}

func runTest_MulticastGroupSummary(t *testing.T, llmFactory LLMClientFactory) {
	ctx := context.Background()

	debugLevel, debug := getDebugLevel()

	clientInfo := testClientInfo(t)

	conn, err := clientInfo.Client.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	seedMulticastGroupSummaryData(t, ctx, conn)

	validateMulticastGroupSummaryQuery(t, ctx, conn)

	if testing.Short() {
		t.Log("Skipping workflow execution in short mode")
		return
	}

	p := setupWorkflow(t, ctx, clientInfo, llmFactory, debug, debugLevel)

	question := "how many publishers and subscribers are in each multicast group?"
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
			Description:   "Both multicast groups listed",
			ExpectedValue: "solana-shreds and validator-gossip both appear in the response",
			Rationale:     "There are 2 multicast groups seeded",
		},
		{
			Description:   "solana-shreds publisher and subscriber counts",
			ExpectedValue: "solana-shreds has 2 publishers and 2 subscribers",
			Rationale:     "user1 publishes to shreds, user2 publishes to both; user3 subscribes to both, user4 subscribes to shreds only",
		},
		{
			Description:   "validator-gossip publisher and subscriber counts",
			ExpectedValue: "validator-gossip has 1 publisher and 2 subscribers",
			Rationale:     "user2 publishes to both; user1 subscribes to gossip, user3 subscribes to both",
		},
	}
	isCorrect, err := evaluateResponse(t, ctx, question, response, expectations...)
	require.NoError(t, err, "Evaluation must be available")
	require.True(t, isCorrect, "Evaluation indicates the response does not correctly summarize multicast group membership")
}

// seedMulticastGroupSummaryData seeds data for multicast group summary test.
// Scenario:
// - 1 metro, 1 device
// - 2 multicast groups: solana-shreds and validator-gossip
// - 4 multicast users with varied membership:
//   - user1: publishes to shreds, subscribes to gossip
//   - user2: publishes to both
//   - user3: subscribes to both
//   - user4: subscribes to shreds only
//
// Expected counts:
//   - solana-shreds: 2 publishers (user1, user2), 2 subscribers (user3, user4)
//   - validator-gossip: 1 publisher (user2), 2 subscribers (user1, user3)
func seedMulticastGroupSummaryData(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	now := testTime()
	opID := uuid.New()

	metros := []serviceability.Metro{
		{PK: "metro1", Code: "nyc", Name: "New York"},
	}
	seedMetros(t, ctx, conn, metros, now, now)

	devices := []serviceability.Device{
		{PK: "device1", Code: "nyc-dzd1", Status: "activated", MetroPK: "metro1", DeviceType: "DZD"},
	}
	seedDevices(t, ctx, conn, devices, now, now)

	shredsPK := "group-shreds"
	gossipPK := "group-gossip"
	multicastGroups := []serviceability.MulticastGroup{
		{PK: shredsPK, OwnerPubkey: "group-owner1", Code: "solana-shreds", MulticastIP: net.ParseIP("239.1.1.1"), MaxBandwidth: 1000000000, Status: "activated", PublisherCount: 2, SubscriberCount: 2},
		{PK: gossipPK, OwnerPubkey: "group-owner2", Code: "validator-gossip", MulticastIP: net.ParseIP("239.1.1.2"), MaxBandwidth: 500000000, Status: "activated", PublisherCount: 1, SubscriberCount: 2},
	}
	seedMulticastGroups(t, ctx, conn, multicastGroups, now, opID)

	users := []serviceability.User{
		// user1: publishes to shreds, subscribes to gossip
		{PK: "user1", OwnerPubkey: "owner1", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.1"), ClientIP: net.ParseIP("203.0.113.1"), Kind: "multicast", TunnelID: 101, Publishers: []string{shredsPK}, Subscribers: []string{gossipPK}},
		// user2: publishes to both
		{PK: "user2", OwnerPubkey: "owner2", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.2"), ClientIP: net.ParseIP("203.0.113.2"), Kind: "multicast", TunnelID: 102, Publishers: []string{shredsPK, gossipPK}, Subscribers: []string{}},
		// user3: subscribes to both
		{PK: "user3", OwnerPubkey: "owner3", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.3"), ClientIP: net.ParseIP("203.0.113.3"), Kind: "multicast", TunnelID: 103, Publishers: []string{}, Subscribers: []string{shredsPK, gossipPK}},
		// user4: subscribes to shreds only
		{PK: "user4", OwnerPubkey: "owner4", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.4"), ClientIP: net.ParseIP("203.0.113.4"), Kind: "multicast", TunnelID: 104, Publishers: []string{}, Subscribers: []string{shredsPK}},
	}
	seedUsers(t, ctx, conn, users, now, now, opID)
}

func validateMulticastGroupSummaryQuery(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	query := `
SELECT
    g.code AS group_code,
    countIf(mode = 'P') AS pub_count,
    countIf(mode = 'S') AS sub_count
FROM (
    SELECT
        arrayJoin(JSONExtract(u.publishers, 'Array(String)')) AS group_pk,
        'P' AS mode
    FROM dz_users_current u
    WHERE u.status = 'activated' AND u.kind = 'multicast' AND JSONLength(u.publishers) > 0
    UNION ALL
    SELECT
        arrayJoin(JSONExtract(u.subscribers, 'Array(String)')) AS group_pk,
        'S' AS mode
    FROM dz_users_current u
    WHERE u.status = 'activated' AND u.kind = 'multicast' AND JSONLength(u.subscribers) > 0
) AS memberships
JOIN dz_multicast_groups_current g ON memberships.group_pk = g.pk
GROUP BY g.code
ORDER BY g.code
`
	result, err := dataset.Query(ctx, conn, query, nil)
	require.NoError(t, err, "Failed to execute multicast group summary query")
	require.Equal(t, 2, result.Count, "Should have exactly 2 groups")

	for _, row := range result.Rows {
		code := row["group_code"].(string)
		pubCount := row["pub_count"].(uint64)
		subCount := row["sub_count"].(uint64)
		switch code {
		case "solana-shreds":
			require.Equal(t, uint64(2), pubCount, "solana-shreds should have 2 publishers")
			require.Equal(t, uint64(2), subCount, "solana-shreds should have 2 subscribers")
		case "validator-gossip":
			require.Equal(t, uint64(1), pubCount, "validator-gossip should have 1 publisher")
			require.Equal(t, uint64(2), subCount, "validator-gossip should have 2 subscribers")
		}
	}

	t.Logf("Database validation passed: solana-shreds (2 pub, 2 sub), validator-gossip (1 pub, 2 sub)")
}
