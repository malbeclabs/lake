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

func TestLake_Agent_Evals_Anthropic_MulticastGroupMembers(t *testing.T) {
	t.Parallel()
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY not set, skipping eval test")
	}

	runTest_MulticastGroupMembers(t, newAnthropicLLMClient)
}

func runTest_MulticastGroupMembers(t *testing.T, llmFactory LLMClientFactory) {
	ctx := context.Background()

	debugLevel, debug := getDebugLevel()

	clientInfo := testClientInfo(t)

	conn, err := clientInfo.Client.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	seedMulticastGroupMembersData(t, ctx, conn)

	validateMulticastGroupMembersQuery(t, ctx, conn)

	if testing.Short() {
		t.Log("Skipping workflow execution in short mode")
		return
	}

	p := setupWorkflow(t, ctx, clientInfo, llmFactory, debug, debugLevel)

	question := "which validators are publishers in multicast group solana-shreds and what metros are they in?"
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
			Description:   "Three publishers identified",
			ExpectedValue: "3 publishers found: owner1/node1, owner2/node2, owner3/node3",
			Rationale:     "There are 3 users who publish to solana-shreds, each with a matching gossip node and vote account",
		},
		{
			Description:   "NYC metro has 2 publishers",
			ExpectedValue: "New York (nyc) has 2 publishers",
			Rationale:     "user1 and user2 are publishers on device1 in NYC metro",
		},
		{
			Description:   "London metro has 1 publisher",
			ExpectedValue: "London (lon) has 1 publisher",
			Rationale:     "user3 is a publisher on device2 in London metro",
		},
	}
	isCorrect, err := evaluateResponse(t, ctx, question, response, expectations...)
	require.NoError(t, err, "Evaluation must be available")
	require.True(t, isCorrect, "Evaluation indicates the response does not correctly identify multicast group publishers")
}

// seedMulticastGroupMembersData seeds data for multicast group members test.
// Scenario:
// - 2 metros: NYC, London
// - 2 devices: one per metro
// - 1 multicast group: solana-shreds
// - 5 multicast users:
//   - user1: publisher only (NYC)
//   - user2: publisher+subscriber (NYC)
//   - user3: publisher only (LON)
//   - user4: subscriber only (LON) — should NOT appear
//   - user5: subscriber only (NYC) — should NOT appear
// - 3 gossip nodes matching publisher client_ips
// - 3 vote accounts for those nodes
func seedMulticastGroupMembersData(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	now := testTime()
	opID := uuid.New()

	metros := []serviceability.Metro{
		{PK: "metro1", Code: "nyc", Name: "New York"},
		{PK: "metro2", Code: "lon", Name: "London"},
	}
	seedMetros(t, ctx, conn, metros, now, now)

	devices := []serviceability.Device{
		{PK: "device1", Code: "nyc-dzd1", Status: "activated", MetroPK: "metro1", DeviceType: "DZD"},
		{PK: "device2", Code: "lon-dzd1", Status: "activated", MetroPK: "metro2", DeviceType: "DZD"},
	}
	seedDevices(t, ctx, conn, devices, now, now)

	group1PK := "group1"
	multicastGroups := []serviceability.MulticastGroup{
		{PK: group1PK, OwnerPubkey: "group-owner1", Code: "solana-shreds", MulticastIP: net.ParseIP("239.1.1.1"), MaxBandwidth: 1000000000, Status: "activated", PublisherCount: 3, SubscriberCount: 3},
	}
	seedMulticastGroups(t, ctx, conn, multicastGroups, now, opID)

	users := []serviceability.User{
		// Publishers (should appear in results)
		{PK: "user1", OwnerPubkey: "owner1", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.1"), ClientIP: net.ParseIP("203.0.113.1"), Kind: "multicast", TunnelID: 101, Publishers: []string{group1PK}, Subscribers: []string{}},
		{PK: "user2", OwnerPubkey: "owner2", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.2"), ClientIP: net.ParseIP("203.0.113.2"), Kind: "multicast", TunnelID: 102, Publishers: []string{group1PK}, Subscribers: []string{group1PK}},
		{PK: "user3", OwnerPubkey: "owner3", Status: "activated", DevicePK: "device2", DZIP: net.ParseIP("10.2.1.1"), ClientIP: net.ParseIP("198.51.100.1"), Kind: "multicast", TunnelID: 201, Publishers: []string{group1PK}, Subscribers: []string{}},
		// Subscribers only (should NOT appear)
		{PK: "user4", OwnerPubkey: "owner4", Status: "activated", DevicePK: "device2", DZIP: net.ParseIP("10.2.1.2"), ClientIP: net.ParseIP("198.51.100.2"), Kind: "multicast", TunnelID: 202, Publishers: []string{}, Subscribers: []string{group1PK}},
		{PK: "user5", OwnerPubkey: "owner5", Status: "activated", DevicePK: "device1", DZIP: net.ParseIP("10.1.1.3"), ClientIP: net.ParseIP("203.0.113.3"), Kind: "multicast", TunnelID: 103, Publishers: []string{}, Subscribers: []string{group1PK}},
	}
	seedUsers(t, ctx, conn, users, now, now, opID)

	gossipNodes := []*testGossipNode{
		{Pubkey: "node1", GossipIP: net.ParseIP("203.0.113.1"), GossipPort: 8001, TPUQUICIP: net.ParseIP("203.0.113.1"), TPUQUICPort: 8003, Version: "1.18.0", Epoch: 500},
		{Pubkey: "node2", GossipIP: net.ParseIP("203.0.113.2"), GossipPort: 8001, TPUQUICIP: net.ParseIP("203.0.113.2"), TPUQUICPort: 8003, Version: "1.18.0", Epoch: 500},
		{Pubkey: "node3", GossipIP: net.ParseIP("198.51.100.1"), GossipPort: 8001, TPUQUICIP: net.ParseIP("198.51.100.1"), TPUQUICPort: 8003, Version: "1.18.0", Epoch: 500},
	}
	seedGossipNodes(t, ctx, conn, gossipNodes, now, now, opID)

	voteAccounts := []testVoteAccount{
		{VotePubkey: "vote1", NodePubkey: "node1", EpochVoteAccount: true, Epoch: 500, ActivatedStake: 1000000000000, Commission: 5},
		{VotePubkey: "vote2", NodePubkey: "node2", EpochVoteAccount: true, Epoch: 500, ActivatedStake: 800000000000, Commission: 5},
		{VotePubkey: "vote3", NodePubkey: "node3", EpochVoteAccount: true, Epoch: 500, ActivatedStake: 1200000000000, Commission: 10},
	}
	seedVoteAccounts(t, ctx, conn, voteAccounts, now, now, opID)
}

func validateMulticastGroupMembersQuery(t *testing.T, ctx context.Context, conn clickhouse.Connection) {
	// Verify publishers for solana-shreds group with metro info
	query := `
SELECT
    u.owner_pubkey,
    gn.pubkey AS node_pubkey,
    va.vote_pubkey,
    m.code AS metro_code
FROM dz_users_current u
JOIN dz_devices_current d ON u.device_pk = d.pk
JOIN dz_metros_current m ON d.metro_pk = m.pk
JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
WHERE u.status = 'activated'
  AND u.kind = 'multicast'
  AND has(JSONExtract(u.publishers, 'Array(String)'), 'group1')
ORDER BY u.owner_pubkey
`
	result, err := dataset.Query(ctx, conn, query, nil)
	require.NoError(t, err, "Failed to execute multicast group members query")
	require.Equal(t, 3, result.Count, "Should have exactly 3 publishers")

	// Verify specific publishers and their metros
	nycCount := 0
	lonCount := 0
	for _, row := range result.Rows {
		metro := row["metro_code"].(string)
		switch metro {
		case "nyc":
			nycCount++
		case "lon":
			lonCount++
		}
	}
	require.Equal(t, 2, nycCount, "NYC should have 2 publishers")
	require.Equal(t, 1, lonCount, "London should have 1 publisher")

	t.Logf("Database validation passed: 3 publishers (nyc=2, lon=1)")
}
