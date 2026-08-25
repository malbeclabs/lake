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
// three subscription epochs (100, 101, 102) so that the earnings math is exact.
// Pools are in base units (2Z = 8 decimals); 1e12 base units = 10000 whole 2Z.
// Earnings are returned in WHOLE tokens (the API divides by the token decimals)
// and include the 10% burn (×0.9):
//
//	pool = 1e12 base units (10000 whole 2Z) per epoch.
//	leader_slots: A=60, B=40 per epoch → summed-leaf denominator = 100.
//	client_proportion = 3500 (35% to client) → 65% to validator (10000-3500=6500).
//	earned(A) per epoch = 1e12 * 60 * 6500 * 0.9 / (100 * 10000) / 1e8 = 3510 whole 2Z.
//	earned(B) per epoch = 1e12 * 40 * 6500 * 0.9 / (100 * 10000) / 1e8 = 2340 whole 2Z.
//
// Across three epochs:
//
//	total(A) = 3*3510 = 10530, total(B) = 3*2340 = 7020.
//
// is_claimable=1 is only set for epoch=102 (newest) for both validators, so
// the per-epoch claimable amount is 3510 / 2340 respectively (the rest is
// not claimable). journal_mint_key is left empty → the leaf resolves to 2Z.
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

	// Reward pool per subscription_epoch (the journal's post-swap balance, in
	// base units). 1e12 base units = 10000 whole 2Z. This is the numerator for
	// the earnings formula. reward_mint/accumulated_slots_scaled are left at
	// their defaults (''/0), exercising the legacy fallback: '' → 2Z, 0 → the
	// summed-leaves denominator.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES
		('epoch-100', now(), now(), generateUUIDv4(), 0, 1, 100, 1000000000000),
		('epoch-101', now(), now(), generateUUIDv4(), 0, 2, 101, 1000000000000),
		('epoch-102', now(), now(), generateUUIDv4(), 0, 3, 102, 1000000000000)
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
		('s-102-A', now(), now(), generateUUIDv4(), 0, 5, 102, 'node-A', 1, 1, ''),
		('s-102-B', now(), now(), generateUUIDv4(), 0, 6, 102, 'node-B', 1, 1, '')
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
	assert.Equal(t, 2, resp.Total, "Total reflects the full count query, not the page size")
	// Default sort is total_earned_2z DESC: node-A (10530) before node-B (7020).
	a := resp.Validators[0]
	b := resp.Validators[1]
	assert.Equal(t, "node-A", a.NodeID)
	assert.Equal(t, "Alpha Validator", a.ValidatorName)
	assert.Equal(t, "vote-A", a.VotePubkey)
	assert.Equal(t, uint64(5000000000), a.ActivatedStake)
	assert.Equal(t, "203.0.113.10", a.DZUserIP)
	assert.InDelta(t, 10530.0, a.TotalEarned2Z, 1e-6)
	assert.InDelta(t, 3510.0, a.ImmediatelyClaimable2Z, 1e-6)
	assert.Greater(t, a.TotalEarned2Z, b.TotalEarned2Z)

	assert.Equal(t, "node-B", b.NodeID)
	assert.Equal(t, "Bravo Validator", b.ValidatorName)
	assert.Empty(t, b.DZUserIP, "node-B has no dz_users row")
	assert.InDelta(t, 7020.0, b.TotalEarned2Z, 1e-6)
	assert.InDelta(t, 2340.0, b.ImmediatelyClaimable2Z, 1e-6)

	// Per-epoch earnings map covers all 3 Solana epochs (== subscription_epochs),
	// each tagged with the 2Z token symbol.
	require.Len(t, a.EpochEarnings, 3)
	for _, e := range []uint64{100, 101, 102} {
		assert.InDelta(t, 3510.0, a.EpochEarnings[e], 1e-6, "node-A epoch %d", e)
		assert.InDelta(t, 2340.0, b.EpochEarnings[e], 1e-6, "node-B epoch %d", e)
		assert.Equal(t, "2Z", a.EpochTokens[e], "node-A epoch %d token", e)
		assert.Equal(t, "2Z", b.EpochTokens[e], "node-B epoch %d token", e)
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
//	pool = 1e12 base units (10000 whole 2Z), denominator = 30 + 70 = 100, client
//	proportion 3500 (→ 65% to validator), 10% burn (×0.9). In whole 2Z:
//	earned(client1) = 10000*30/100*0.65*0.9 = 1755,
//	earned(client2) = 10000*70/100*0.65*0.9 = 4095, total = 5850.
//
// Only client 1 is claimable, so immediately_claimable = 1755 (not 5850) —
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
		VALUES ('epoch-200', now(), now(), generateUUIDv4(), 0, 1, 200, 1000000000000)
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
		('s-200-X-c1', now(), now(), generateUUIDv4(), 0, 1, 200, 'node-X', 1, 1, ''),
		('s-200-X-c2', now(), now(), generateUUIDv4(), 0, 2, 200, 'node-X', 2, 0, '')
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Validators, 1, "node-X must appear exactly once")
	x := resp.Validators[0]
	assert.Equal(t, "node-X", x.NodeID)
	// Both client leaves counted: 1755 + 4095 = 5850.
	assert.InDelta(t, 5850.0, x.TotalEarned2Z, 1e-6, "earnings must sum across both clients")
	// Only client 1 is claimable: 1755, not the full 5850.
	assert.InDelta(t, 1755.0, x.ImmediatelyClaimable2Z, 1e-6, "only client 1's earnings are claimable")
	// Per-epoch earnings for epoch 200 is the node's full 5850.
	assert.InDelta(t, 5850.0, x.EpochEarnings[200], 1e-6)
}

// TestGetShredsRewards_ProportionDefaultsWhenUnset is the regression test for
// the earnings overstatement: the per-client proportion is stored as 0 when
// unset, with the real fallback in default_proportion, and a missing client
// row also reads as 0 (ClickHouse fills LEFT JOIN columns with 0, not NULL).
// The validator share must use default_proportion (3500 → 65%), NOT treat the
// 0 as a literal proportion (which gave a 100% share and overstated earnings).
//
//	pool = 1e12 base units (10000 whole 2Z), denominator = 100, validator share
//	= 10000-3500 = 6500, 10% burn (×0.9). earned = 10000 * 0.65 * 0.9 = 5850 whole
//	2Z   (NOT 9000, the burn-applied 0-proportion bug, nor 10000)
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
		VALUES ('epoch-300', now(), now(), generateUUIDv4(), 0, 1, 300, 1000000000000)
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
	// 5850 (65% share via default_proportion, ×0.9 burn), NOT 9000 (the
	// 0-proportion bug giving a 100% share, then burned).
	assert.InDelta(t, 5850.0, resp.Validators[0].TotalEarned2Z, 1e-6,
		"unset proportion must fall through to default_proportion, not a 100% share")
}

// TestGetShredsRewards_UsesJournalTotalLeaderSlots verifies earnings divide by
// the journal's authoritative total_leader_slots (stored on the pool row), not
// the sum of indexed leaves. When some validators' leaves are missing, the
// summed-leaves denominator is too small and over-credits everyone; the stored
// value is the true denominator.
//
//	pool = 1e12 base units (10000 whole 2Z), node-D leader_slots = 100, but the
//	journal's total_leader_slots = 200 (another 100 slots belong to a validator
//	whose leaf isn't indexed), with the 10% burn (×0.9). accumulated_slots_scaled
//	is unset (0) so the denominator falls back to total_leader_slots × 10000.
//	earned = 10000 * 100/200 * 0.65 * 0.9 = 2925 whole 2Z   (NOT 5850)
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
		VALUES ('epoch-400', now(), now(), generateUUIDv4(), 0, 1, 400, 1000000000000, 200)
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
	// 2925 (denominator 200), NOT 5850 (summed-leaves denominator 100).
	assert.InDelta(t, 2925.0, resp.Validators[0].TotalEarned2Z, 1e-6,
		"must divide by the journal's total_leader_slots, not the summed indexed leaves")
}

// TestGetShredsRewards_MultiTokenUSDC covers the multi-token era: a validator
// rewarded in USDC. Its leaf is attributed to the USDC mint via the status row's
// journal_mint_key, joined to the USDC pool, and split over the USDC journal's
// accumulated_slots_scaled denominator (not the epoch total), with USDC's 6 decimals.
//
//	USDC is not burned and its journal holds no swapped balance
//	(tokens_received_2z = 0); the pool comes from distributed_amount = 9e8 base
//	units (900 whole USDC). accumulated_slots_scaled = 50 × 10000 = 500000 (so the
//	journal owns 50 slots), node-U has all 50 slots, client proportion defaults to
//	3500 (→ 65%), NO burn.
//	earned = 9e8 * 50 * 6500 / 500000 / 1e6 = 585 whole USDC.
//
// The 2Z headline total stays 0 (cross-token sums are not meaningful), but the
// per-epoch cell carries the USDC amount and symbol.
func TestGetShredsRewards_MultiTokenUSDC(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	const usdcMint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES ('500-U', now(), now(), generateUUIDv4(), 0, 1, 500, 50, 'node-U', 50, 1, 0)
	`))
	// USDC pool: tokens_received_2z=0 (no swapped balance), reward drawn from
	// distributed_amount, with the per-token scaled-slot denominator.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z, total_leader_slots, reward_mint, accumulated_slots_scaled, distributed_amount)
		VALUES ('epoch-500:usdc', now(), now(), generateUUIDv4(), 0, 1, 500, 0, 0, '`+usdcMint+`', 500000, 900000000)
	`))
	// Status row attributes node-U's leaf to the USDC journal.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_leaf_distribution_status_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, node_id, client_id, is_claimable, journal_mint_key)
		VALUES ('s-500-U', now(), now(), generateUUIDv4(), 0, 1, 500, 'node-U', 1, 1, '`+usdcMint+`')
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Validators, 1)
	u := resp.Validators[0]
	assert.Equal(t, "node-U", u.NodeID)
	// USDC-only validator: no 2Z, so the 2Z headline total is 0.
	assert.InDelta(t, 0.0, u.TotalEarned2Z, 1e-6, "USDC earnings must not count toward the 2Z total")
	// The per-epoch cell carries the USDC amount and symbol.
	assert.InDelta(t, 585.0, u.EpochEarnings[500], 1e-6, "USDC reward split over the journal's slots, with burn")
	assert.Equal(t, "USDC", u.EpochTokens[500])

	// Detail endpoint surfaces the same per-token amount and symbol.
	dreq := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-U", nil)
	dreq = withChiNodeIDParam(dreq, "node-U")
	drr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(drr, dreq)
	assert.Equal(t, http.StatusOK, drr.Code, "body=%s", drr.Body.String())
	dresp := decodeShredsRewardsDetail(t, drr.Body.Bytes())
	require.Len(t, dresp.Epochs, 1)
	assert.Equal(t, "USDC", dresp.Epochs[0].TokenSymbol)
	assert.InDelta(t, 585.0, dresp.Epochs[0].Earned, 1e-6)
	assert.Equal(t, handlers.ClaimStateClaimable, dresp.Epochs[0].State)
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

	// Per-row fields use the shared earnings formula: 3510 whole 2Z per epoch
	// for node-A (3900 × 0.9 burn), tagged with the 2Z symbol.
	for i, e := range resp.Epochs {
		assert.Equal(t, uint32(60), e.LeaderSlots, "epoch index %d", i)
		assert.Equal(t, uint16(1), e.ClientID, "epoch index %d", i)
		assert.InDelta(t, 3510.0, e.Earned, 1e-6, "epoch index %d", i)
		assert.Equal(t, "2Z", e.TokenSymbol, "epoch index %d", i)
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
		('epoch-99', now(), now(), generateUUIDv4(), 0, 4, 99, 1000000000000)
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

	// And the earnings math still works for the older epoch (3900 × 0.9 burn).
	assert.InDelta(t, 3510.0, resp.Epochs[3].Earned, 1e-6)
	assert.Equal(t, "2Z", resp.Epochs[3].TokenSymbol)
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

// insertClientRegistry adds display names for the given client ids.
func insertClientRegistry(t *testing.T, api *handlers.API, rows string) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_validator_client_rewards_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, client_id, manager_key, short_description)
		VALUES `+rows))
}

// insertMultiClientLeaves seeds one validator splitting its slots across two
// clients in one epoch: 30 slots on client 1 and 70 on client 2, against a
// 1e12-base-unit (10000 whole 2Z) pool with a 3500 client proportion. Earnings
// work out to 1755 for client 1 and 4095 for client 2, summing to the 5850 the
// node earns in total — the same numbers TestGetShredsRewards_MultiClientValidator
// pins from the validator side.
func insertMultiClientLeaves(t *testing.T, api *handlers.API) {
	t.Helper()
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
		VALUES ('epoch-200', now(), now(), generateUUIDv4(), 0, 1, 200, 1000000000000)
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
		('s-200-X-c1', now(), now(), generateUUIDv4(), 0, 1, 200, 'node-X', 1, 1, ''),
		('s-200-X-c2', now(), now(), generateUUIDv4(), 0, 2, 200, 'node-X', 2, 0, '')
	`))
}

// group=client regroups the same leaves by client team instead of by node, so a
// validator publishing under two clients is split rather than collapsed. This is
// the point of the whole feature (lake#784).
func TestGetShredsRewards_GroupByClient(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMultiClientLeaves(t, api)
	insertClientRegistry(t, api, `
		('c-1', now(), now(), generateUUIDv4(), 0, 1, 'client-pk-1', 1, 'mgr-1', 'Agave'),
		('c-2', now(), now(), generateUUIDv4(), 0, 2, 'client-pk-2', 2, 'mgr-2', 'Firedancer')`)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?group=client", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	assert.Empty(t, resp.Validators, "client mode must not also return validator rows")
	require.Len(t, resp.Clients, 2)

	// Ordered by all-time earnings descending, so client 2 leads on 70 slots.
	fd, agave := resp.Clients[0], resp.Clients[1]
	assert.Equal(t, uint16(2), fd.ClientID)
	assert.Equal(t, "Firedancer", fd.ClientName)
	// 4095 was the validators' 65% share of client 2's leaf; the client team's own
	// share is the complementary 35%: 4095 * 3500/6500.
	assert.InDelta(t, 4095.0*3500/6500, fd.TotalEarned2Z, 1e-6)
	assert.Equal(t, uint64(1), fd.Validators)

	assert.Equal(t, uint16(1), agave.ClientID)
	assert.Equal(t, "Agave", agave.ClientName)
	assert.InDelta(t, 1755.0*3500/6500, agave.TotalEarned2Z, 1e-6)
	assert.Equal(t, uint64(1), agave.Validators)
}

// The two groupings are complementary shares of one pool, not the same number
// regrouped: onchain a leaf is weighted by slots * (MAX - client_proportion) for
// the validator and slots * client_proportion for the client team. So their
// all-time totals must stand in exactly that ratio over the same leaves — at the
// flat 3500 proportion, 35/65.
//
// This replaces an earlier assertion that the two totals were equal, which was
// only true while the client view was showing the validators' share regrouped.
func TestGetShredsRewards_GroupByClientIsComplementaryShare(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMultiClientLeaves(t, api)
	insertClientRegistry(t, api, `
		('c-1', now(), now(), generateUUIDv4(), 0, 1, 'client-pk-1', 1, 'mgr-1', 'Agave'),
		('c-2', now(), now(), generateUUIDv4(), 0, 2, 'client-pk-2', 2, 'mgr-2', 'Firedancer')`)

	get := func(query string) handlers.ShredsRewardsResponse {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards"+query, nil)
		rr := httptest.NewRecorder()
		api.GetShredsRewards(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
		return decodeShredsRewards(t, rr.Body.Bytes())
	}

	var validatorTotal, clientTotal float64
	for _, v := range get("").Validators {
		validatorTotal += v.TotalEarned2Z
	}
	for _, c := range get("?group=client").Clients {
		clientTotal += c.TotalEarned2Z
	}

	require.NotZero(t, validatorTotal, "fixture must produce earnings for the ratio to mean anything")
	assert.InDelta(t, validatorTotal*3500/6500, clientTotal, 1e-6,
		"the client teams' share must be the complement of the validators' over the same leaves")
	assert.Less(t, clientTotal, validatorTotal,
		"at a 3500 proportion the client teams receive less than their validators")
}

// A client's leaves can land before its registry row is indexed. Such a team
// must still appear with its earnings rather than vanish or render blank.
func TestGetShredsRewards_GroupByClientUnregisteredFallback(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMultiClientLeaves(t, api)
	// Register client 1 only; client 2 has no registry row.
	insertClientRegistry(t, api, `
		('c-1', now(), now(), generateUUIDv4(), 0, 1, 'client-pk-1', 1, 'mgr-1', 'Agave')`)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?group=client", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Clients, 2, "an unregistered client must still be listed")
	unregistered := resp.Clients[0]
	assert.Equal(t, uint16(2), unregistered.ClientID)
	assert.Equal(t, "Client 2", unregistered.ClientName, "falls back to the id, never a blank label")
	assert.InDelta(t, 4095.0*3500/6500, unregistered.TotalEarned2Z, 1e-6)
}

// An empty result must serialise as [], not null.
func TestGetShredsRewards_GroupByClientEmpty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?group=client", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), `"clients":[]`)
}

// The default grouping is unchanged, so every existing caller keeps its shape.
func TestGetShredsRewards_DefaultGroupingStillValidators(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	assert.NotEmpty(t, resp.Validators)
	assert.Empty(t, resp.Clients, "validator mode must not populate the client rows")
}

// The Validators column counts nodes publishing under a client in the newest
// funded epoch, not every node that ever did. A validator runs one client at a
// time and switches between them, so a lifetime count reads as a current
// headcount while being much larger — in production Jito Labs has 432 nodes
// all-time against 68 in the latest epoch — and it counts a switcher under every
// client it ever used, so the column would sum to about twice the number of
// validators that exist.
//
// All-time earnings stay all-time, which is the other half of the contract: a
// client whose validators have all left still earned what it earned. The two
// clients here separate the halves — client 2 has no current validators and must
// still report its past earnings.
//
// Fixture: epoch 300 has one 50-slot leaf on each client, so its 10000-token
// pool splits evenly at 2925 each (10000 * 0.65 * 0.9 * 50/100). Epoch 301, the
// newest funded epoch, has only client 1's leaf, which takes the whole 5850.
func TestGetShredsRewards_GroupByClientCountsCurrentValidatorsOnly(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES
		('300-gone',  now(), now(), generateUUIDv4(), 0, 1, 300, 30, 'node-gone', 50, 2, 0),
		('300-stays', now(), now(), generateUUIDv4(), 0, 2, 300, 30, 'node-stays', 50, 1, 1),
		('301-stays', now(), now(), generateUUIDv4(), 0, 3, 301, 31, 'node-stays', 50, 1, 0)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES
		('epoch-300', now(), now(), generateUUIDv4(), 0, 1, 300, 1000000000000),
		('epoch-301', now(), now(), generateUUIDv4(), 0, 2, 301, 1000000000000)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES
		('p-300-1', now(), now(), generateUUIDv4(), 0, 1, 300, 1, 3500, 3500),
		('p-300-2', now(), now(), generateUUIDv4(), 0, 2, 300, 2, 3500, 3500),
		('p-301-1', now(), now(), generateUUIDv4(), 0, 3, 301, 1, 3500, 3500)
	`))
	insertClientRegistry(t, api, `
		('c-1', now(), now(), generateUUIDv4(), 0, 1, 'client-pk-1', 1, 'mgr-1', 'Agave'),
		('c-2', now(), now(), generateUUIDv4(), 0, 2, 'client-pk-2', 2, 'mgr-2', 'Firedancer')`)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?group=client", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	byName := map[string]handlers.ShredsClientRewardsRow{}
	for _, c := range resp.Clients {
		byName[c.ClientName] = c
	}
	require.Len(t, byName, 2)

	current := byName["Agave"]
	assert.Equal(t, uint64(1), current.Validators, "node-stays published in epoch 301")
	assert.InDelta(t, (2925.0+5850.0)*3500/6500, current.TotalEarned2Z, 1e-6)

	departed := byName["Firedancer"]
	assert.Equal(t, uint64(0), departed.Validators,
		"node-gone last published in epoch 300, so it is not a current validator")
	assert.InDelta(t, 2925.0*3500/6500, departed.TotalEarned2Z, 1e-6,
		"a client with no current validators still reports what it earned")
}
