package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createPublisherShredStatsTable creates the publisher_shred_stats table
// in the configured shredder database (normally created by shredder migrations, not lake migrations).
func createPublisherShredStatsTable(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	err := config.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", config.ShredderDB))
	require.NoError(t, err)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.publisher_shred_stats (
			event_ts DateTime64(3),
			ingested_at DateTime64(3),
			host String,
			publisher_ip String,
			client_ip String,
			node_pubkey String,
			vote_pubkey String,
			activated_stake UInt64,
			dz_user_pubkey String,
			dz_device_code String,
			dz_metro_code String,
			epoch UInt64,
			slot UInt64,
			total_packets UInt64,
			unique_shreds UInt64,
			data_shreds UInt64,
			coding_shreds UInt64,
			max_data_index Int64,
			needs_repair Bool,
			first_seen_ns Int64,
			last_seen_ns Int64,
			is_scheduled_leader Bool
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toYYYYMM(event_ts)
		ORDER BY (host, slot, publisher_ip)
	`, "`"+config.ShredderDB+"`"))
	require.NoError(t, err)
}

func insertPublisherCheckTestData(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	createPublisherShredStatsTable(t)

	// Create the bebop multicast group
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('bebop-group', now(), now(), generateUUIDv4(), 0, 1,
			 '31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj', '', 'bebop', '233.84.178.1', 100000000, 'activated', 0, 0)
	`)
	require.NoError(t, err)

	// Create devices and metros
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users)
		VALUES
			('dev-ams', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-ams', 'up', 'edge', 'ams1', '', '', 'metro-ams', 0),
			('dev-nyc', now(), now(), generateUUIDv4(), 0, 2,
			 'dev-nyc', 'up', 'edge', 'nyc1', '', '', 'metro-nyc', 0)
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, code, name, latitude, longitude)
		VALUES
			('metro-ams', now(), now(), generateUUIDv4(), 0, 1,
			 'metro-ams', 'AMS', 'Amsterdam', 52.37, 4.89),
			('metro-nyc', now(), now(), generateUUIDv4(), 0, 2,
			 'metro-nyc', 'NYC', 'New York', 40.71, -74.01)
	`)
	require.NoError(t, err)

	// Create three bebop publisher users:
	// - dzuser1: publishing (has shred stats), on ams1
	// - dzuser2: publishing (has shred stats), on nyc1
	// - dzuser3: connected but NOT publishing (no shred stats), on ams1
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('dzuser1', now(), now(), generateUUIDv4(), 0, 1,
			 'dzuser1', 'owner1', 'activated', 'multicast', '203.0.113.1', '10.0.0.1', 'dev-ams', '', 501, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]'),
			('dzuser2', now(), now(), generateUUIDv4(), 0, 2,
			 'dzuser2', 'owner2', 'activated', 'multicast', '203.0.113.2', '10.0.0.2', 'dev-nyc', '', 502, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]'),
			('dzuser3', now(), now(), generateUUIDv4(), 0, 3,
			 'dzuser3', 'owner3', 'activated', 'multicast', '203.0.113.3', '10.0.0.3', 'dev-ams', '', 503, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]')
	`)
	require.NoError(t, err)

	// Gossip nodes: map client IPs to node pubkeys
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
			('nodepub1', now(), now(), generateUUIDv4(), 0, 1,
			 'nodepub1', 800, '203.0.113.1', 8001, '', 0, ''),
			('nodepub2', now(), now(), generateUUIDv4(), 0, 2,
			 'nodepub2', 800, '203.0.113.2', 8001, '', 0, ''),
			('nodepub3', now(), now(), generateUUIDv4(), 0, 3,
			 'nodepub3', 800, '203.0.113.3', 8001, '', 0, '')
	`)
	require.NoError(t, err)

	// Vote accounts: map node pubkeys to vote pubkeys and stake
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
			('votepub1', now(), now(), generateUUIDv4(), 0, 1,
			 'votepub1', 800, 'nodepub1', 50000000000, 'true', 0),
			('votepub2', now(), now(), generateUUIDv4(), 0, 2,
			 'votepub2', 800, 'nodepub2', 10000000000, 'true', 0),
			('votepub3', now(), now(), generateUUIDv4(), 0, 3,
			 'votepub3', 800, 'nodepub3', 5000000000, 'true', 0)
	`)
	require.NoError(t, err)

	// Validators.app: client name/version lookup by vote account
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_validatorsapp_validators_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 account, name, vote_account, software_version, software_client, software_client_id,
			 jito, jito_commission, is_active, is_dz, active_stake, commission, delinquent,
			 epoch, epoch_credits, skipped_slot_percent, total_score,
			 data_center_key, autonomous_system_number, latitude, longitude, ip, stake_pools_list)
		VALUES
			('nodepub1', now(), now(), generateUUIDv4(), 0, 1,
			 'nodepub1', 'Validator 1', 'votepub1', '2.2.3', 'Jito', 2,
			 1, 0, 1, 1, 50000000000, 0, 0,
			 800, 1000, '0.5', 100,
			 'US-NY', 0, '', '', '', ''),
			('nodepub3', now(), now(), generateUUIDv4(), 0, 2,
			 'nodepub3', 'Validator 3', 'votepub3', '2.1.0', 'Agave', 1,
			 0, 0, 1, 1, 5000000000, 0, 0,
			 800, 500, '1.0', 50,
			 'EU-AMS', 0, '', '', '', '')
	`)
	require.NoError(t, err)

	// Shred stats: only dzuser1 and dzuser2 are publishing (dzuser3 is NOT)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.publisher_shred_stats`, "`"+config.ShredderDB+"`")+`
			(event_ts, ingested_at, host, publisher_ip, client_ip, node_pubkey,
			 vote_pubkey, activated_stake, dz_user_pubkey, dz_device_code, dz_metro_code,
			 epoch, slot, total_packets, unique_shreds, data_shreds, coding_shreds,
			 max_data_index, needs_repair, first_seen_ns, last_seen_ns, is_scheduled_leader)
		VALUES
			(now(), now(), 'shredder-1', '10.0.0.1', '203.0.113.1', 'nodepub1',
			 'votepub1', 50000000000, 'dzuser1', 'ams1', 'AMS',
			 800, 1001, 100, 64, 32, 32, 31, false, 0, 0, true),
			(now(), now(), 'shredder-1', '10.0.0.1', '203.0.113.1', 'nodepub1',
			 'votepub1', 50000000000, 'dzuser1', 'ams1', 'AMS',
			 800, 1002, 100, 64, 32, 32, 31, true, 0, 0, false),
			(now(), now(), 'shredder-1', '10.0.0.2', '203.0.113.2', 'nodepub2',
			 'votepub2', 10000000000, 'dzuser2', 'nyc1', 'NYC',
			 800, 1001, 50, 32, 16, 16, 15, false, 0, 0, false)
	`)
	require.NoError(t, err)
}

// insertBulkShredStats inserts N leader slots and M retransmit slots for a publisher.
func insertBulkShredStats(t *testing.T, dzUserPubkey string, publisherIP string, epoch, startSlot uint64, leaderSlots, retransmitSlots int) {
	t.Helper()
	ctx := t.Context()
	slot := startSlot
	for range leaderSlots {
		err := config.DB.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.publisher_shred_stats`, "`"+config.ShredderDB+"`")+`
				(event_ts, ingested_at, host, publisher_ip, client_ip, node_pubkey,
				 vote_pubkey, activated_stake, dz_user_pubkey, dz_device_code, dz_metro_code,
				 epoch, slot, total_packets, unique_shreds, data_shreds, coding_shreds,
				 max_data_index, needs_repair, first_seen_ns, last_seen_ns, is_scheduled_leader)
			VALUES
				(now(), now(), 'shredder-1', ?, '', '',
				 '', 0, ?, '', '',
				 ?, ?, 100, 64, 32, 32, 31, false, 0, 0, true)
		`, publisherIP, dzUserPubkey, epoch, slot)
		require.NoError(t, err)
		slot++
	}
	for range retransmitSlots {
		err := config.DB.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.publisher_shred_stats`, "`"+config.ShredderDB+"`")+`
				(event_ts, ingested_at, host, publisher_ip, client_ip, node_pubkey,
				 vote_pubkey, activated_stake, dz_user_pubkey, dz_device_code, dz_metro_code,
				 epoch, slot, total_packets, unique_shreds, data_shreds, coding_shreds,
				 max_data_index, needs_repair, first_seen_ns, last_seen_ns, is_scheduled_leader)
			VALUES
				(now(), now(), 'shredder-1', ?, '', '',
				 '', 0, ?, '', '',
				 ?, ?, 100, 64, 32, 32, 31, false, 0, 0, false)
		`, publisherIP, dzUserPubkey, epoch, slot)
		require.NoError(t, err)
		slot++
	}
}

func TestGetPublisherCheck_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createPublisherShredStatsTable(t)

	// Create bebop group but no users
	ctx := t.Context()
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('bebop-group', now(), now(), generateUUIDv4(), 0, 1,
			 '31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj', '', 'bebop', '233.84.178.1', 100000000, 'activated', 0, 0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Empty(t, resp.Publishers)
}

func TestGetPublisherCheck_AllPublishers(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, uint64(800), resp.Epoch)
	assert.Len(t, resp.Publishers, 3) // 2 publishing + 1 connected-not-publishing

	// First publisher (highest stake, 50B lamports) — publishing leader + retransmit
	pub1 := resp.Publishers[0]
	assert.Equal(t, "10.0.0.1", pub1.PublisherIP)
	assert.Equal(t, "dzuser1", pub1.DZUserPubkey)
	assert.True(t, pub1.MulticastConnected)
	assert.True(t, pub1.PublishingLeaderShreds)
	assert.False(t, pub1.PublishingRetransmitted, "1 retransmit slot is below the 50-slot threshold")
	assert.Equal(t, uint64(1), pub1.LeaderSlots)
	assert.Equal(t, uint64(2), pub1.TotalSlots)
	assert.Equal(t, "Validator 1", pub1.ValidatorName)
	assert.Equal(t, "Jito", pub1.ValidatorClient)
	assert.Equal(t, "2.2.3", pub1.ValidatorVersion)
	assert.True(t, pub1.ValidatorVersionOk)

	// Second publisher (10B lamports) — publishing retransmit only
	pub2 := resp.Publishers[1]
	assert.Equal(t, "10.0.0.2", pub2.PublisherIP)
	assert.Equal(t, "dzuser2", pub2.DZUserPubkey)
	assert.True(t, pub2.MulticastConnected)
	assert.False(t, pub2.PublishingLeaderShreds)
	assert.False(t, pub2.PublishingRetransmitted, "1 retransmit slot is below the 50-slot threshold")
	assert.Equal(t, "", pub2.ValidatorName)
	assert.Equal(t, uint64(1), pub2.TotalSlots)

	// Third publisher (5B lamports) — connected but NOT publishing
	pub3 := resp.Publishers[2]
	assert.Equal(t, "10.0.0.3", pub3.PublisherIP)
	assert.Equal(t, "dzuser3", pub3.DZUserPubkey)
	assert.True(t, pub3.MulticastConnected)
	assert.False(t, pub3.PublishingLeaderShreds)
	assert.False(t, pub3.PublishingRetransmitted)
	assert.Equal(t, uint64(0), pub3.TotalSlots)
	assert.Equal(t, uint64(0), pub3.LeaderSlots)
	assert.Equal(t, uint64(0), pub3.TotalUniqueShreds)
	assert.Equal(t, "AMS", pub3.DZMetroCode)
	assert.Equal(t, "Validator 3", pub3.ValidatorName)
	assert.Equal(t, "Agave", pub3.ValidatorClient)
	assert.Equal(t, "2.1.0", pub3.ValidatorVersion)
	assert.True(t, pub3.ValidatorVersionOk)
}

func TestGetPublisherCheck_FilterByIP(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?q=10.0.0.1", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 1)
	assert.Equal(t, "10.0.0.1", resp.Publishers[0].PublisherIP)
}

func TestGetPublisherCheck_FilterByClientIP(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?q=203.0.113.2", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 1)
	assert.Equal(t, "10.0.0.2", resp.Publishers[0].PublisherIP)
}

func TestGetPublisherCheck_EpochsParam(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	// Default (epochs=2) should work the same as before
	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 3)

	// epochs=1 should also work (single epoch)
	req = httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?epochs=1", nil)
	rr = httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 3)

	// Invalid epochs param should use default
	req = httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?epochs=abc", nil)
	rr = httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 3)
}

// TestGetPublisherCheck_MultiHost verifies that slot counts are not inflated
// when multiple shredder hosts report stats for the same (publisher, slot) pair.
// The publisher_shred_stats table is keyed by (host, slot, publisher_ip), so each
// shredder instance writes its own row per slot. The query must deduplicate by slot.
func TestGetPublisherCheck_MultiHost(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	// Insert duplicate rows from a second shredder host for dzuser1's slots.
	ctx := t.Context()
	err := config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.publisher_shred_stats`, "`"+config.ShredderDB+"`")+`
			(event_ts, ingested_at, host, publisher_ip, client_ip, node_pubkey,
			 vote_pubkey, activated_stake, dz_user_pubkey, dz_device_code, dz_metro_code,
			 epoch, slot, total_packets, unique_shreds, data_shreds, coding_shreds,
			 max_data_index, needs_repair, first_seen_ns, last_seen_ns, is_scheduled_leader)
		VALUES
			(now(), now(), 'shredder-2', '10.0.0.1', '203.0.113.1', 'nodepub1',
			 'votepub1', 50000000000, 'dzuser1', 'ams1', 'AMS',
			 800, 1001, 100, 64, 32, 32, 31, false, 0, 0, true),
			(now(), now(), 'shredder-2', '10.0.0.1', '203.0.113.1', 'nodepub1',
			 'votepub1', 50000000000, 'dzuser1', 'ams1', 'AMS',
			 800, 1002, 100, 64, 32, 32, 31, true, 0, 0, false),
			(now(), now(), 'shredder-2', '10.0.0.2', '203.0.113.2', 'nodepub2',
			 'votepub2', 10000000000, 'dzuser2', 'nyc1', 'NYC',
			 800, 1001, 50, 32, 16, 16, 15, false, 0, 0, false)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?q=dzuser1", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Publishers, 1)

	pub := resp.Publishers[0]
	// With 2 shredder hosts, each slot has 2 rows. Slot counts must NOT be doubled.
	assert.Equal(t, uint64(1), pub.LeaderSlots, "leader_slots should count distinct slots, not rows")
	assert.Equal(t, uint64(2), pub.TotalSlots, "total_slots should count distinct slots, not rows")
	assert.Equal(t, uint64(128), pub.TotalUniqueShreds, "unique_shreds should use max per slot, not sum across hosts")
}

func TestGetPublisherCheck_MaxSlotInResponse(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	// Test data has slots 1001 and 1002
	assert.Equal(t, uint64(1002), resp.MaxSlot)
}

func TestGetPublisherCheck_SlotsParam(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	// Insert an old slot (slot 1) that should be excluded when querying last 500 slots
	ctx := t.Context()
	err := config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.publisher_shred_stats`, "`"+config.ShredderDB+"`")+`
			(event_ts, ingested_at, host, publisher_ip, client_ip, node_pubkey,
			 vote_pubkey, activated_stake, dz_user_pubkey, dz_device_code, dz_metro_code,
			 epoch, slot, total_packets, unique_shreds, data_shreds, coding_shreds,
			 max_data_index, needs_repair, first_seen_ns, last_seen_ns, is_scheduled_leader)
		VALUES
			(now(), now(), 'shredder-1', '10.0.0.2', '203.0.113.2', 'nodepub2',
			 'votepub2', 10000000000, 'dzuser2', 'nyc1', 'NYC',
			 799, 1, 50, 32, 16, 16, 15, false, 0, 0, true)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?slots=500&q=dzuser2", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Publishers, 1)

	pub := resp.Publishers[0]
	assert.Equal(t, uint64(1), pub.TotalSlots)
	assert.False(t, pub.PublishingLeaderShreds, "old leader slot should be excluded by slot window")
	assert.False(t, pub.PublishingRetransmitted, "1 retransmit slot is below the 50-slot threshold")
}

func TestGetPublisherCheck_SlotsDefault(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?slots=999", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 3)
	// Verify slot-mode was activated (max_slot should be set)
	assert.Equal(t, uint64(1002), resp.MaxSlot, "slot-mode should be active, reporting max_slot")
}

func TestGetPublisherCheck_SlotsAndEpochsMutuallyExclusive(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?slots=500&epochs=5", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 3)
	// Verify slot-mode was used (max_slot confirms slot-based query ran)
	assert.Equal(t, uint64(1002), resp.MaxSlot, "slots should take precedence over epochs")
}

func TestGetPublisherCheck_FilterByDZID(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertPublisherCheckTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?q=dzuser1", nil)
	rr := httptest.NewRecorder()
	handlers.GetPublisherCheck(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.PublisherCheckResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Len(t, resp.Publishers, 1)
	assert.Equal(t, "dzuser1", resp.Publishers[0].DZUserPubkey)
}

func TestGetPublisherCheck_RetransmitThreshold(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createPublisherShredStatsTable(t)

	ctx := t.Context()

	// Create bebop group
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('bebop-group', now(), now(), generateUUIDv4(), 0, 1,
			 '31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj', '', 'bebop', '233.84.178.1', 100000000, 'activated', 0, 0)
	`)
	require.NoError(t, err)

	// Create four publishers to test each threshold scenario
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
			('below-abs', now(), now(), generateUUIDv4(), 0, 1,
			 'below-abs', '', 'activated', 'multicast', '', '10.0.1.1', '', '', 0, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]'),
			('above-abs-below-ratio', now(), now(), generateUUIDv4(), 0, 2,
			 'above-abs-below-ratio', '', 'activated', 'multicast', '', '10.0.1.2', '', '', 0, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]'),
			('above-both', now(), now(), generateUUIDv4(), 0, 3,
			 'above-both', '', 'activated', 'multicast', '', '10.0.1.3', '', '', 0, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]'),
			('boundary', now(), now(), generateUUIDv4(), 0, 4,
			 'boundary', '', 'activated', 'multicast', '', '10.0.1.4', '', '', 0, '["31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"]', '[]')
	`)
	require.NoError(t, err)

	// Scenario 1: 10 retransmit / 100 total (10% ratio, but only 10 retransmit < 50 min)
	insertBulkShredStats(t, "below-abs", "10.0.1.1", 800, 2000, 90, 10)

	// Scenario 2: 60 retransmit / 2000 total (3% ratio < 5% min, but 60 > 50 abs min)
	insertBulkShredStats(t, "above-abs-below-ratio", "10.0.1.2", 800, 5000, 1940, 60)

	// Scenario 3: 200 retransmit / 400 total (50% ratio, 200 > 50) — clearly flagged
	insertBulkShredStats(t, "above-both", "10.0.1.3", 800, 8000, 200, 200)

	// Scenario 4: exactly 50 retransmit / 1000 total (exactly 5%, exactly 50) — boundary, flagged
	insertBulkShredStats(t, "boundary", "10.0.1.4", 800, 10000, 950, 50)

	tests := []struct {
		name     string
		dzUser   string
		wantFlag bool
	}{
		{"below absolute minimum", "below-abs", false},
		{"above absolute but below ratio", "above-abs-below-ratio", false},
		{"above both thresholds", "above-both", true},
		{"boundary - exactly at both thresholds", "boundary", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/dz/publisher-check?q="+tt.dzUser, nil)
			rr := httptest.NewRecorder()
			handlers.GetPublisherCheck(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)

			var resp handlers.PublisherCheckResponse
			err := json.NewDecoder(rr.Body).Decode(&resp)
			require.NoError(t, err)
			require.Len(t, resp.Publishers, 1)
			assert.Equal(t, tt.wantFlag, resp.Publishers[0].PublishingRetransmitted,
				"publisher %s: retransmit flag mismatch", tt.dzUser)
		})
	}
}
