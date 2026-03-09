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
			 'bebop-pk', '', 'bebop', '233.84.178.1', 100000000, 'activated', 0, 0)
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
			 'dzuser1', 'owner1', 'activated', 'multicast', '203.0.113.1', '10.0.0.1', 'dev-ams', '', 501, '["bebop-pk"]', '[]'),
			('dzuser2', now(), now(), generateUUIDv4(), 0, 2,
			 'dzuser2', 'owner2', 'activated', 'multicast', '203.0.113.2', '10.0.0.2', 'dev-nyc', '', 502, '["bebop-pk"]', '[]'),
			('dzuser3', now(), now(), generateUUIDv4(), 0, 3,
			 'dzuser3', 'owner3', 'activated', 'multicast', '203.0.113.3', '10.0.0.3', 'dev-ams', '', 503, '["bebop-pk"]', '[]')
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
			 'bebop-pk', '', 'bebop', '233.84.178.1', 100000000, 'activated', 0, 0)
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
	assert.True(t, pub1.PublishingRetransmitted)
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
	assert.True(t, pub2.PublishingRetransmitted)
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
