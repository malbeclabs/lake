package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertShredsRewardsTestData seeds two validators (node-A, node-B) across
// three subscription epochs (100, 101, 102) so that the earnings math is exact:
//
//	tokens_received_2z (the 2Z journal pool) = 10000 per epoch.
//	leader_slots: A=60, B=40 per epoch → total_leader_slots=100.
//	client_proportion = 3500 (35% to client), so 65% to validator (10000-3500=6500).
//	earned_2z(A) per epoch = 10000 * 60 * 6500 / (100 * 10000) = 3900.
//	earned_2z(B) per epoch = 10000 * 40 * 6500 / (100 * 10000) = 2600.
//
// Across three epochs:
//
//	total(A) = 3*3900 = 11700, total(B) = 3*2600 = 7800.
//
// is_claimable=1 is only set for epoch=102 (newest) for both validators, so
// the per-epoch claimable amount is 3900 / 2600 respectively (the rest is
// not claimable).
func insertShredsRewardsTestData(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	// Leaves: (subscription_epoch, node_id, leader_slots, client_id).
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES
		('100-A', now(), now(), generateUUIDv4(), 0, 1, 100, 10, 'node-A', 60, 1, 0),
		('100-B', now(), now(), generateUUIDv4(), 0, 2, 100, 10, 'node-B', 40, 1, 1),
		('101-A', now(), now(), generateUUIDv4(), 0, 3, 101, 11, 'node-A', 60, 1, 0),
		('101-B', now(), now(), generateUUIDv4(), 0, 4, 101, 11, 'node-B', 40, 1, 1),
		('102-A', now(), now(), generateUUIDv4(), 0, 5, 102, 12, 'node-A', 60, 1, 0),
		('102-B', now(), now(), generateUUIDv4(), 0, 6, 102, 12, 'node-B', 40, 1, 1)
	`)
	require.NoError(t, err)

	// Distributions: one row per subscription_epoch with associated_dz_epoch
	// and distributed_validator_2z_amount = 10000.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, subscription_epoch, associated_dz_epoch, device_count, client_seat_count,
		 validator_rewards_proportion, total_publishing_validators,
		 collected_usdc_payments, collected_2z_converted_from_usdc,
		 distributed_validator_rewards_count, distributed_contributor_rewards_count,
		 distributed_validator_2z_amount, distributed_contributor_2z_amount,
		 burned_2z_amount)
		VALUES
		('d-100', now(), now(), generateUUIDv4(), 0, 1, 'd-100', 100, 10, 0, 0, 6500, 2, 0, 0, 0, 0, 10000, 0, 0),
		('d-101', now(), now(), generateUUIDv4(), 0, 2, 'd-101', 101, 11, 0, 0, 6500, 2, 0, 0, 0, 0, 10000, 0, 0),
		('d-102', now(), now(), generateUUIDv4(), 0, 3, 'd-102', 102, 12, 0, 0, 6500, 2, 0, 0, 0, 0, 10000, 0, 0)
	`)
	require.NoError(t, err)

	// 2Z reward pool per subscription_epoch (the journal's post-swap 2Z). This
	// is the numerator for the earnings formula: tokens_received_2z=10000 per
	// epoch keeps the expected earned_2z values identical to the doc comment.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES
		('epoch-100', now(), now(), generateUUIDv4(), 0, 1, 100, 10000),
		('epoch-101', now(), now(), generateUUIDv4(), 0, 2, 101, 10000),
		('epoch-102', now(), now(), generateUUIDv4(), 0, 3, 102, 10000)
	`)
	require.NoError(t, err)

	// Client proportions: client_id=1, proportion=3500 (35%) for each epoch.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES
		('p-100-1', now(), now(), generateUUIDv4(), 0, 1, 100, 1, 3500, 3500),
		('p-101-1', now(), now(), generateUUIDv4(), 0, 2, 101, 1, 3500, 3500),
		('p-102-1', now(), now(), generateUUIDv4(), 0, 3, 102, 1, 3500, 3500)
	`)
	require.NoError(t, err)

	// Distribution status: is_claimable=1 only for epoch=102 (newest).
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_leaf_distribution_status_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, node_id, client_id, is_claimable, journal_mint_key)
		VALUES
		('s-100-A', now(), now(), generateUUIDv4(), 0, 1, 100, 'node-A', 1, 0, ''),
		('s-100-B', now(), now(), generateUUIDv4(), 0, 2, 100, 'node-B', 1, 0, ''),
		('s-101-A', now(), now(), generateUUIDv4(), 0, 3, 101, 'node-A', 1, 0, ''),
		('s-101-B', now(), now(), generateUUIDv4(), 0, 4, 101, 'node-B', 1, 0, ''),
		('s-102-A', now(), now(), generateUUIDv4(), 0, 5, 102, 'node-A', 1, 1, 'journal-A'),
		('s-102-B', now(), now(), generateUUIDv4(), 0, 6, 102, 'node-B', 1, 1, 'journal-B')
	`)
	require.NoError(t, err)

	// Solana vote accounts for node lookups (name + activated stake + vote pubkey).
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		('vote-A', now(), now(), generateUUIDv4(), 0, 1, 'vote-A', 12, 'node-A', 5000000000, 'true', 0),
		('vote-B', now(), now(), generateUUIDv4(), 0, 2, 'vote-B', 12, 'node-B', 1000000000, 'true', 0)
	`)
	require.NoError(t, err)

	// Validators.app: human-readable names for each vote account.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_validatorsapp_validators_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 account, name, vote_account, software_version, software_client, software_client_id,
		 jito, jito_commission, is_active, is_dz, active_stake, commission, delinquent,
		 epoch, epoch_credits, skipped_slot_percent, total_score,
		 data_center_key, autonomous_system_number, latitude, longitude, ip, stake_pools_list)
		VALUES
		('node-A', now(), now(), generateUUIDv4(), 0, 1,
		 'node-A', 'Alpha Validator', 'vote-A', '2.2.0', 'Jito', 2,
		 1, 0, 1, 1, 5000000000, 0, 0,
		 12, 100, '0.5', 100,
		 'US-NY', 0, '', '', '', ''),
		('node-B', now(), now(), generateUUIDv4(), 0, 2,
		 'node-B', 'Bravo Validator', 'vote-B', '2.2.0', 'Agave', 1,
		 0, 0, 1, 1, 1000000000, 0, 0,
		 12, 100, '0.5', 100,
		 'EU-FRA', 0, '', '', '', '')
	`)
	require.NoError(t, err)

	// Gossip + DZ users so the DZUserIP field gets populated for node-A.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
		('node-A', now(), now(), generateUUIDv4(), 0, 1,
		 'node-A', 12, '203.0.113.10', 8001, '', 0, '')
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
		('user-A', now(), now(), generateUUIDv4(), 0, 1,
		 'user-A', 'owner-A', 'activated', 'multicast', '203.0.113.10', '10.0.0.10', '', '', 100, '[]', '[]')
	`)
	require.NoError(t, err)
}

func decodeShredsRewards(t *testing.T, body []byte) handlers.ShredsRewardsResponse {
	t.Helper()
	var resp handlers.ShredsRewardsResponse
	require.NoError(t, json.NewDecoder(strings.NewReader(string(body))).Decode(&resp))
	return resp
}

func TestGetShredsRewards_GoldenPath(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	// EpochColumns is the 10 newest Solana epochs (we only have 3), newest first.
	// subscription_epoch == Solana epoch (the program creates one ShredDistribution
	// per Solana epoch from its launch).
	assert.Equal(t, []uint64{102, 101, 100}, resp.EpochColumns)
	assert.Equal(t, uint64(102), resp.LatestFinalizedEpoch)
	assert.Equal(t, uint64(12), resp.CurrentSolanaEpoch)

	require.Len(t, resp.Validators, 2)
	// Default sort is total_earned_2z DESC: node-A (11700) before node-B (7800).
	a := resp.Validators[0]
	b := resp.Validators[1]
	assert.Equal(t, "node-A", a.NodeID)
	assert.Equal(t, "Alpha Validator", a.ValidatorName)
	assert.Equal(t, "vote-A", a.VotePubkey)
	assert.Equal(t, uint64(5000000000), a.ActivatedStake)
	assert.Equal(t, "203.0.113.10", a.DZUserIP)
	assert.InDelta(t, 11700.0, a.TotalEarned2Z, 1e-6)
	assert.InDelta(t, 3900.0, a.ImmediatelyClaimable2Z, 1e-6)
	assert.Greater(t, a.TotalEarned2Z, b.TotalEarned2Z)

	assert.Equal(t, "node-B", b.NodeID)
	assert.Equal(t, "Bravo Validator", b.ValidatorName)
	assert.Empty(t, b.DZUserIP, "node-B has no dz_users row")
	assert.InDelta(t, 7800.0, b.TotalEarned2Z, 1e-6)
	assert.InDelta(t, 2600.0, b.ImmediatelyClaimable2Z, 1e-6)

	// Per-epoch earnings map covers all 3 Solana epochs (== subscription_epochs).
	require.Len(t, a.EpochEarnings, 3)
	for _, e := range []uint64{100, 101, 102} {
		assert.InDelta(t, 3900.0, a.EpochEarnings[e], 1e-6, "node-A epoch %d", e)
		assert.InDelta(t, 2600.0, b.EpochEarnings[e], 1e-6, "node-B epoch %d", e)
	}
}

func TestGetShredsRewards_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	assert.Empty(t, resp.Validators)
	assert.Empty(t, resp.EpochColumns)
}

func TestGetShredsRewards_SearchByNode(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?search=node:node-A", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 1)
	assert.Equal(t, "node-A", resp.Validators[0].NodeID)
}

func TestGetShredsRewards_SearchFreeText(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	// "alpha" matches "Alpha Validator" (validator name) for node-A.
	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?search=alpha", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 1)
	assert.Equal(t, "node-A", resp.Validators[0].NodeID)

	// "vote-B" matches the vote pubkey for node-B.
	req = httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?search=vote-B", nil)
	rr = httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp = decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 1)
	assert.Equal(t, "node-B", resp.Validators[0].NodeID)
}

func TestGetShredsRewards_SortAscending(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?sort=total_earned_2z&order=asc", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 2)
	// Ascending: node-B (7800) first, then node-A (11700).
	assert.Equal(t, "node-B", resp.Validators[0].NodeID)
	assert.Equal(t, "node-A", resp.Validators[1].NodeID)
}

func TestGetShredsRewards_LimitOffset(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?limit=1&offset=0", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 1)
	assert.Equal(t, "node-A", resp.Validators[0].NodeID)

	req = httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?limit=1&offset=1", nil)
	rr = httptest.NewRecorder()
	api.GetShredsRewards(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp = decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 1)
	assert.Equal(t, "node-B", resp.Validators[0].NodeID)
}

func TestGetShredsRewards_DeduplicatesValidators(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)
	ctx := t.Context()

	// Seed a SECOND activated DZ user sharing node-A's gossip IP
	// (203.0.113.10). The list query joins dz_users on client_ip = gossip_ip,
	// so without a per-node dedup this fans node-A into two identical rows.
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers)
		VALUES
		('user-A2', now(), now(), generateUUIDv4(), 0, 9,
		 'user-A2', 'owner-A2', 'activated', 'multicast', '203.0.113.10', '10.0.0.11', '', '', 101, '[]', '[]')
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	// Each validator must appear exactly once despite the fan-out.
	require.Len(t, resp.Validators, 2)
	seen := map[string]int{}
	for _, v := range resp.Validators {
		seen[v.NodeID]++
	}
	assert.Equal(t, 1, seen["node-A"], "node-A must not be duplicated by the dz_users fan-out")
	assert.Equal(t, 1, seen["node-B"])
}

// TestGetShredsRewards_MultiClientValidator is the regression test for the bug
// where a validator publishing under multiple software clients in a single
// epoch was collapsed to one leaf row, understating its leader slots and
// earnings. node-X publishes under client 1 (30 slots) and client 2 (70 slots)
// in epoch 200; both leaves must be counted.
//
//	pool = 10000, total_leader_slots(200) = 30 + 70 = 100, client proportion 3500
//	(→ 65% to validator). earned(client1) = 10000*30*6500/(100*10000) = 1950,
//	earned(client2) = 10000*70*6500/(100*10000) = 4550, total = 6500.
//
// Only client 1 is claimable, so immediately_claimable = 1950 (not 6500) —
// proving the per-client claimable grain too.
func TestGetShredsRewards_MultiClientValidator(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES
		('200-X-c1', now(), now(), generateUUIDv4(), 0, 1, 200, 20, 'node-X', 30, 1, 0),
		('200-X-c2', now(), now(), generateUUIDv4(), 0, 2, 200, 20, 'node-X', 70, 2, 1)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES ('epoch-200', now(), now(), generateUUIDv4(), 0, 1, 200, 10000)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES
		('p-200-1', now(), now(), generateUUIDv4(), 0, 1, 200, 1, 3500, 3500),
		('p-200-2', now(), now(), generateUUIDv4(), 0, 2, 200, 2, 3500, 3500)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_leaf_distribution_status_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, node_id, client_id, is_claimable, journal_mint_key)
		VALUES
		('s-200-X-c1', now(), now(), generateUUIDv4(), 0, 1, 200, 'node-X', 1, 1, 'journal-X'),
		('s-200-X-c2', now(), now(), generateUUIDv4(), 0, 2, 200, 'node-X', 2, 0, 'journal-X')
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Validators, 1, "node-X must appear exactly once")
	x := resp.Validators[0]
	assert.Equal(t, "node-X", x.NodeID)
	// Both client leaves counted: 1950 + 4550 = 6500.
	assert.InDelta(t, 6500.0, x.TotalEarned2Z, 1e-6, "earnings must sum across both clients")
	// Only client 1 is claimable: 1950, not the full 6500.
	assert.InDelta(t, 1950.0, x.ImmediatelyClaimable2Z, 1e-6, "only client 1's earnings are claimable")
	// Per-epoch earnings for epoch 200 is the node's full 6500.
	assert.InDelta(t, 6500.0, x.EpochEarnings[200], 1e-6)
}

// TestGetShredsRewards_ProportionDefaultsWhenUnset is the regression test for
// the earnings overstatement: the per-client proportion is stored as 0 when
// unset, with the real fallback in default_proportion, and a missing client
// row also reads as 0 (ClickHouse fills LEFT JOIN columns with 0, not NULL).
// The validator share must use default_proportion (3500 → 65%), NOT treat the
// 0 as a literal proportion (which gave a 100% share and overstated earnings).
//
//	pool = 10000, total_leader_slots = 100, validator share = 10000-3500 = 6500
//	earned = 10000 * 100 * 6500 / (100 * 10000) = 6500   (NOT 10000)
func TestGetShredsRewards_ProportionDefaultsWhenUnset(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES ('300-Z', now(), now(), generateUUIDv4(), 0, 1, 300, 30, 'node-Z', 100, 1, 0)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES ('epoch-300', now(), now(), generateUUIDv4(), 0, 1, 300, 10000)
	`))
	// proportion stored as 0 (unset), real fallback in default_proportion.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES ('p-300-1', now(), now(), generateUUIDv4(), 0, 1, 300, 1, 0, 3500)
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Validators, 1)
	// 6500 (65% share via default_proportion), NOT 10000 (the 0-proportion bug).
	assert.InDelta(t, 6500.0, resp.Validators[0].TotalEarned2Z, 1e-6,
		"unset proportion must fall through to default_proportion, not a 100% share")
}

// TestGetShredsRewards_UsesJournalTotalLeaderSlots verifies earnings divide by
// the journal's authoritative total_leader_slots (stored on the pool row), not
// the sum of indexed leaves. When some validators' leaves are missing, the
// summed-leaves denominator is too small and over-credits everyone; the stored
// value is the true denominator.
//
//	pool = 10000, node-D leader_slots = 100, but the journal's total = 200
//	(another 100 slots belong to a validator whose leaf isn't indexed).
//	earned = 10000 * 100 * 6500 / (200 * 10000) = 3250   (NOT 6500)
func TestGetShredsRewards_UsesJournalTotalLeaderSlots(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES ('400-D', now(), now(), generateUUIDv4(), 0, 1, 400, 40, 'node-D', 100, 1, 0)
	`))
	// Pool carries the authoritative denominator (200) — larger than the lone
	// indexed leaf's 100 slots, simulating a missing validator's leaves.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z, total_leader_slots)
		VALUES ('epoch-400', now(), now(), generateUUIDv4(), 0, 1, 400, 10000, 200)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES ('p-400-1', now(), now(), generateUUIDv4(), 0, 1, 400, 1, 3500, 3500)
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Validators, 1)
	// 3250 (denominator 200), NOT 6500 (summed-leaves denominator 100).
	assert.InDelta(t, 3250.0, resp.Validators[0].TotalEarned2Z, 1e-6,
		"must divide by the journal's total_leader_slots, not the summed indexed leaves")
}

// withChiNodeIDParam installs the {nodeId} URL param onto the request context
// so the handler can read it via chi.URLParam.
func withChiNodeIDParam(req *http.Request, nodeID string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("nodeId", nodeID)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func decodeShredsRewardsDetail(t *testing.T, body []byte) handlers.ShredsRewardsDetailResponse {
	t.Helper()
	var resp handlers.ShredsRewardsDetailResponse
	require.NoError(t, json.NewDecoder(strings.NewReader(string(body))).Decode(&resp))
	return resp
}

func TestGetShredsRewardsDetail_FullHistory(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-A", nil)
	req = withChiNodeIDParam(req, "node-A")
	rr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewardsDetail(t, rr.Body.Bytes())

	// Header fields should be populated from the seeded vote/validators/gossip rows.
	assert.Equal(t, "node-A", resp.NodeID)
	assert.Equal(t, "vote-A", resp.VotePubkey)
	assert.Equal(t, "Alpha Validator", resp.ValidatorName)
	assert.Equal(t, uint64(5000000000), resp.ActivatedStake)
	assert.Equal(t, "203.0.113.10", resp.DZUserIP)

	// Fixture seeds three epochs: subscription_epoch (== Solana epoch) 100, 101, 102.
	require.Len(t, resp.Epochs, 3)

	// Newest epoch first.
	assert.Equal(t, uint64(102), resp.Epochs[0].SolanaEpoch)
	assert.Equal(t, uint64(101), resp.Epochs[1].SolanaEpoch)
	assert.Equal(t, uint64(100), resp.Epochs[2].SolanaEpoch)

	// Per-row fields use the shared earnings formula: 3900 per epoch for node-A.
	for i, e := range resp.Epochs {
		assert.Equal(t, uint32(60), e.LeaderSlots, "epoch index %d", i)
		assert.Equal(t, uint16(1), e.ClientID, "epoch index %d", i)
		assert.InDelta(t, 3900.0, e.Earned2Z, 1e-6, "epoch index %d", i)
	}

	// All three epochs have status rows in the fixture, so IsClaimable is non-nil.
	// is_claimable=1 only for the newest epoch (102); the older two are 0/false.
	require.NotNil(t, resp.Epochs[0].IsClaimable)
	require.NotNil(t, resp.Epochs[1].IsClaimable)
	require.NotNil(t, resp.Epochs[2].IsClaimable)
	assert.True(t, *resp.Epochs[0].IsClaimable, "epoch 102 is claimable")
	assert.False(t, *resp.Epochs[1].IsClaimable, "epoch 101 is not claimable")
	assert.False(t, *resp.Epochs[2].IsClaimable, "epoch 100 is not claimable")

	// Derived state mirrors the live-journal bits: 102 claimable, 100/101 have
	// live rows with the bit cleared -> distributed (accumulated then paid).
	assert.Equal(t, handlers.ClaimStateClaimable, resp.Epochs[0].State, "epoch 102")
	assert.Equal(t, handlers.ClaimStateDistributed, resp.Epochs[1].State, "epoch 101")
	assert.Equal(t, handlers.ClaimStateDistributed, resp.Epochs[2].State, "epoch 100")

	// SolanaEpoch equals SubscriptionEpoch by construction.
	assert.Equal(t, uint64(102), resp.Epochs[0].SubscriptionEpoch)
	assert.Equal(t, uint64(101), resp.Epochs[1].SubscriptionEpoch)
	assert.Equal(t, uint64(100), resp.Epochs[2].SubscriptionEpoch)
}

func TestGetShredsRewardsDetail_UnknownNode(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-ZZZ", nil)
	req = withChiNodeIDParam(req, "node-ZZZ")
	rr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewardsDetail(t, rr.Body.Bytes())

	assert.Equal(t, "node-ZZZ", resp.NodeID)
	assert.Empty(t, resp.VotePubkey)
	assert.Empty(t, resp.ValidatorName)
	assert.Equal(t, uint64(0), resp.ActivatedStake)
	assert.Empty(t, resp.DZUserIP)
	assert.NotNil(t, resp.Epochs, "epochs should be a non-nil empty array")
	assert.Empty(t, resp.Epochs)
}

func TestGetShredsRewardsDetail_IncludesEpochsOutsideRecentWindow(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)
	ctx := t.Context()

	// Seed an older epoch (subscription_epoch=99, associated_dz_epoch=9) for
	// node-A with a leaf and a distribution, but NO status row. This simulates
	// an epoch that has rolled out of the on-chain 12-epoch tracking window.
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES
		('099-A', now(), now(), generateUUIDv4(), 0, 7, 99, 9, 'node-A', 60, 1, 0),
		('099-B', now(), now(), generateUUIDv4(), 0, 8, 99, 9, 'node-B', 40, 1, 1)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, subscription_epoch, associated_dz_epoch, device_count, client_seat_count,
		 validator_rewards_proportion, total_publishing_validators,
		 collected_usdc_payments, collected_2z_converted_from_usdc,
		 distributed_validator_rewards_count, distributed_contributor_rewards_count,
		 distributed_validator_2z_amount, distributed_contributor_2z_amount,
		 burned_2z_amount)
		VALUES
		('d-099', now(), now(), generateUUIDv4(), 0, 4, 'd-099', 99, 9, 0, 0, 6500, 2, 0, 0, 0, 0, 10000, 0, 0)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES
		('epoch-99', now(), now(), generateUUIDv4(), 0, 4, 99, 10000)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES
		('p-099-1', now(), now(), generateUUIDv4(), 0, 4, 99, 1, 3500, 3500)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-A", nil)
	req = withChiNodeIDParam(req, "node-A")
	rr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewardsDetail(t, rr.Body.Bytes())

	// All four Solana epochs (99, 100, 101, 102) are returned, newest first.
	require.Len(t, resp.Epochs, 4)
	assert.Equal(t, uint64(102), resp.Epochs[0].SolanaEpoch)
	assert.Equal(t, uint64(101), resp.Epochs[1].SolanaEpoch)
	assert.Equal(t, uint64(100), resp.Epochs[2].SolanaEpoch)
	assert.Equal(t, uint64(99), resp.Epochs[3].SolanaEpoch)

	// The oldest epoch (99) has no status row → IsClaimable must be nil.
	assert.Nil(t, resp.Epochs[3].IsClaimable, "epoch outside the tracking window has nil is_claimable")
	// The other three epochs do have status rows.
	assert.NotNil(t, resp.Epochs[0].IsClaimable, "epoch 102 has a status row")
	assert.NotNil(t, resp.Epochs[1].IsClaimable, "epoch 101 has a status row")
	assert.NotNil(t, resp.Epochs[2].IsClaimable, "epoch 100 has a status row")

	// Epoch 99 has a funded pool but no status row at all (its journal was
	// swept before we ever tracked it), so its per-leaf claim state is
	// unrecoverable — reported as unknown, NOT assumed paid.
	assert.Equal(t, handlers.ClaimStateUnknown, resp.Epochs[3].State, "epoch 99 has no recoverable status")
	// 100/101 are live-journal rows with the bit cleared -> distributed (paid),
	// 102 is claimable.
	assert.Equal(t, handlers.ClaimStateClaimable, resp.Epochs[0].State, "epoch 102")
	assert.Equal(t, handlers.ClaimStateDistributed, resp.Epochs[1].State, "epoch 101")
	assert.Equal(t, handlers.ClaimStateDistributed, resp.Epochs[2].State, "epoch 100")

	// And the earnings math still works for the older epoch.
	assert.InDelta(t, 3900.0, resp.Epochs[3].Earned2Z, 1e-6)
}

// TestGetShredsRewardsDetail_PendingState covers the fourth claim state: an
// epoch with a pool row but zero tokens (distribution not finalized) and no
// status row anywhere must report `pending`, not `distributed`.
func TestGetShredsRewardsDetail_PendingState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES ('200-P', now(), now(), generateUUIDv4(), 0, 1, 200, 20, 'node-P', 50, 1, 0)
	`))
	// Pool row exists (so the INNER JOIN keeps the epoch) but tokens=0 →
	// distribution not finalized. No status rows seeded anywhere.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, subscription_epoch, tokens_received_2z)
		VALUES ('epoch-200', now(), now(), generateUUIDv4(), 0, 1, 200, 0)
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-P", nil)
	req = withChiNodeIDParam(req, "node-P")
	rr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewardsDetail(t, rr.Body.Bytes())

	require.Len(t, resp.Epochs, 1)
	assert.Equal(t, handlers.ClaimStatePending, resp.Epochs[0].State, "unfunded epoch is pending, not distributed")
	assert.Nil(t, resp.Epochs[0].IsClaimable, "no live journal row → is_claimable nil")
}
