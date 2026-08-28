package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	// Total comes from count() OVER (), which counts the query's rows — so a
	// fan-out that got past the identity joins would inflate it in step with the
	// row count and stay invisible to the completeness check the page cache runs.
	// This is the assertion that catches it.
	assert.Equal(t, 2, resp.Total, "the total counts validators, not joined rows")
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

	assert.Equal(t, "node-A", resp.NodeID)

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

// insertShredsRewardsManyEpochs seeds one validator (node-R) across `epochs`
// consecutive funded epochs starting at `firstEpoch`, each with an identical pool
// and the same 100 leader slots, so every epoch's earnings are the same 5850 whole
// 2Z. The OLDEST epoch is the only claimable one — the reverse of the usual shape,
// so a recency filter applied to the claimable total is immediately visible.
func insertShredsRewardsManyEpochs(t *testing.T, api *handlers.API, firstEpoch, epochs int) {
	t.Helper()
	var leaves, pools, props, status []string
	for i := range epochs {
		e := firstEpoch + i
		leaves = append(leaves, fmt.Sprintf(
			`('l-%[1]d', now(), now(), generateUUIDv4(), 0, %[1]d, %[1]d, %[1]d, 'node-R', 100, 1, 0)`, e))
		pools = append(pools, fmt.Sprintf(
			`('pool-%[1]d', now(), now(), generateUUIDv4(), 0, %[1]d, %[1]d, 1000000000000)`, e))
		props = append(props, fmt.Sprintf(
			`('p-%[1]d', now(), now(), generateUUIDv4(), 0, %[1]d, %[1]d, 1, 3500, 3500)`, e))
		// Only the oldest epoch's leaf is still claimable.
		claimable := 0
		if i == 0 {
			claimable = 1
		}
		status = append(status, fmt.Sprintf(
			`('s-%[1]d', now(), now(), generateUUIDv4(), 0, %[1]d, %[1]d, 'node-R', 1, %[2]d, '')`, e, claimable))
	}
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES `+strings.Join(leaves, ",")))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES `+strings.Join(pools, ",")))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES `+strings.Join(props, ",")))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_validator_leaf_distribution_status_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, node_id, client_id, is_claimable, journal_mint_key)
		VALUES `+strings.Join(status, ",")))
}

// TestGetShredsRewards_PerEpochMapCoversOnlyEpochColumns pins the is_recent
// filter. It was expressed as `if(R.solana_epoch IS NULL, 0, 1)` over a LEFT JOIN
// to a `LIMIT 10` CTE, which is always 1 — ClickHouse fills an unmatched LEFT JOIN
// column with the type's default (0), never NULL. Every epoch a validator ever
// earned in landed in the per-epoch maps, which on mainnet made them ~9× the size
// they were meant to be and dominated the list payload.
func TestGetShredsRewards_PerEpochMapCoversOnlyEpochColumns(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsRewardsManyEpochs(t, api, 600, 14)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	// 14 funded epochs, but the header carries the newest 10.
	require.Len(t, resp.EpochColumns, 10)
	assert.Equal(t, []uint64{613, 612, 611, 610, 609, 608, 607, 606, 605, 604}, resp.EpochColumns)

	require.Len(t, resp.Validators, 1)
	r := resp.Validators[0]
	// The maps are keyed to the columns, so they hold exactly those 10 epochs —
	// not all 14.
	assert.Len(t, r.EpochEarnings, 10, "per-epoch earnings must not run past the epoch columns")
	assert.Len(t, r.EpochTokens, 10)
	for _, e := range resp.EpochColumns {
		assert.InDelta(t, 5850.0, r.EpochEarnings[e], 1e-6, "epoch %d", e)
	}
	assert.NotContains(t, r.EpochEarnings, uint64(603), "epoch 603 is outside the columns")
	assert.NotContains(t, r.EpochEarnings, uint64(600))

	// All 14 epochs still count toward the all-time total, which is not windowed.
	assert.InDelta(t, 14*5850.0, r.TotalEarned2Z, 1e-6)
}

// TestGetShredsRewards_ClaimableCountsEpochsOutsideTheColumns fixes the boundary
// the per-epoch window must NOT be applied to. A leaf stays claimable until it is
// distributed however old its epoch is — on mainnet 11,539 of 14,326 claimable
// leaves sit outside the 10-epoch window — and the detail page derives its own
// claimable figure from per-epoch state with no window at all, so windowing this
// one would make the two pages disagree about the same validator.
func TestGetShredsRewards_ClaimableCountsEpochsOutsideTheColumns(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	// Only epoch 600 — the oldest of 14, four epochs before the columns start — is
	// claimable.
	insertShredsRewardsManyEpochs(t, api, 600, 14)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewards(t, rr.Body.Bytes())

	require.Len(t, resp.Validators, 1)
	assert.InDelta(t, 5850.0, resp.Validators[0].ImmediatelyClaimable2Z, 1e-6,
		"a claimable leaf older than the epoch columns is still claimable")

	// And the detail page agrees, which is the point of not windowing it.
	dreq := withChiNodeIDParam(
		httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-R", nil), "node-R")
	drr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(drr, dreq)
	require.Equal(t, http.StatusOK, drr.Code, "body=%s", drr.Body.String())
	dresp := decodeShredsRewardsDetail(t, drr.Body.Bytes())

	// 2Z only: the list column is 2Z-denominated, while the detail page breaks its
	// total down per token, so a mixed-token validator's two figures are in
	// different denominations by design. Summing the detail's 2Z leaves is the
	// like-for-like comparison.
	var detailClaimable2Z float64
	for _, e := range dresp.Epochs {
		if e.State == handlers.ClaimStateClaimable && e.TokenSymbol == "2Z" {
			detailClaimable2Z += e.Earned
		}
	}
	assert.InDelta(t, resp.Validators[0].ImmediatelyClaimable2Z, detailClaimable2Z, 1e-6,
		"list and detail must report the same 2Z claimable total for one validator")
}

// TestGetShredsRewardsDetail_MultiClientEpochRows covers the row grain the page
// renders. An epoch a validator switched software clients in produces one row per
// client; both must come back, in a stable order, each naming its own client.
func TestGetShredsRewardsDetail_MultiClientEpochRows(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMultiClientLeaves(t, api)
	insertClientRegistry(t, api, `
		('c-1', now(), now(), generateUUIDv4(), 0, 1, 'client-pk-1', 1, 'mgr-1', 'Agave'),
		('c-2', now(), now(), generateUUIDv4(), 0, 2, 'client-pk-2', 2, 'mgr-2', 'Firedancer')`)

	req := withChiNodeIDParam(
		httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-X", nil), "node-X")
	rr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewardsDetail(t, rr.Body.Bytes())

	require.Len(t, resp.Epochs, 2, "one row per (epoch, client), not per epoch")
	// Ordered by epoch DESC then client_id ASC, so the two rows of one epoch keep
	// a fixed order across the page's refetches.
	assert.Equal(t, uint64(200), resp.Epochs[0].SubscriptionEpoch)
	assert.Equal(t, uint64(200), resp.Epochs[1].SubscriptionEpoch)
	assert.Equal(t, uint16(1), resp.Epochs[0].ClientID)
	assert.Equal(t, uint16(2), resp.Epochs[1].ClientID)
	// Each row names its own client rather than showing a bare id.
	assert.Equal(t, "Agave", resp.Epochs[0].ClientName)
	assert.Equal(t, "Firedancer", resp.Epochs[1].ClientName)
	// (epoch, client) is unique, which is what lets the page key its rows on it.
	assert.NotEqual(t, resp.Epochs[0].ClientID, resp.Epochs[1].ClientID)
}

// TestGetShredsRewardsDetail_UnregisteredClientFallsBackToID: a leaf can land
// before its client's registry row is indexed, and the page must still name the
// column something rather than rendering a blank.
func TestGetShredsRewardsDetail_UnregisteredClientFallsBackToID(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMultiClientLeaves(t, api)

	req := withChiNodeIDParam(
		httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards/node-X", nil), "node-X")
	rr := httptest.NewRecorder()
	api.GetShredsRewardsDetail(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	resp := decodeShredsRewardsDetail(t, rr.Body.Bytes())
	require.Len(t, resp.Epochs, 2)
	assert.Equal(t, "Client 1", resp.Epochs[0].ClientName)
	assert.Equal(t, "Client 2", resp.Epochs[1].ClientName)
}

// insertShredsRewardsManyValidators seeds `n` validators over a single funded
// epoch, each with a different slot count so total_earned_2z is strictly ordered,
// and a name/vote/stake set that orders differently from the earnings — enough for
// every sortable column to have something to say.
//
// Every validator's claimable amount is 0, which is the shape that matters for
// paging: hundreds of rows tied on one sort key is the normal state of this list,
// and it is exactly where an order without a tiebreaker starts dropping or
// repeating rows between pages.
func insertShredsRewardsManyValidators(t *testing.T, api *handlers.API, n int) {
	t.Helper()
	var leaves, votes, names []string
	for i := range n {
		node := fmt.Sprintf("node-%03d", i)
		vote := fmt.Sprintf("vote-%03d", i)
		leaves = append(leaves, fmt.Sprintf(
			`('l-%[1]s', now(), now(), generateUUIDv4(), 0, %[2]d, 700, 70, '%[1]s', %[3]d, 1, %[2]d)`,
			node, i+1, i+1))
		votes = append(votes, fmt.Sprintf(
			`('%[1]s', now(), now(), generateUUIDv4(), 0, %[3]d, '%[1]s', 700, '%[2]s', %[4]d, 'true', 0)`,
			vote, node, i+1, (n-i)*1000000000))
		// Half the validators have no validators.app row, so validator_name sorting
		// has to order a block of empty strings too. The name counts DOWN as the
		// earnings count up, so name order and earnings order disagree and a sort
		// on one cannot pass by accidentally reproducing the other.
		if i%2 == 0 {
			label := fmt.Sprintf("Validator %03d", n-i)
			names = append(names, fmt.Sprintf(
				`('%[1]s', now(), now(), generateUUIDv4(), 0, %[2]d, '%[1]s', '%[3]s', '%[1]s')`,
				vote, i+1, label))
		}
	}
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_validator_rewards_leaves_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, associated_dz_epoch, node_id, leader_slots, client_id, leaf_index)
		VALUES `+strings.Join(leaves, ",")))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_distribution_2z_pool_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, tokens_received_2z)
		VALUES ('pool-700', now(), now(), generateUUIDv4(), 0, 1, 700, 1000000000000)`))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_distribution_client_proportions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 subscription_epoch, client_id, proportion, default_proportion)
		VALUES ('p-700-1', now(), now(), generateUUIDv4(), 0, 1, 700, 1, 3500, 3500)`))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES `+strings.Join(votes, ",")))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_validatorsapp_validators_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 account, name, vote_account)
		VALUES `+strings.Join(names, ",")))
}

// shredsRewardsCacheMu serialises the tests that exercise the page cache.
//
// Every test in the package shares ONE Postgres database — SetupPostgresForTest
// hands out a pool onto the same container DB — so page_cache is shared state
// even though each test gets its own ClickHouse database. Two parallel tests
// writing ShredsRewardsPageCacheKey would read each other's blob against their
// own ClickHouse data. They still run in parallel with the rest of the suite;
// only the handful that touch this key take turns.
var shredsRewardsCacheMu sync.Mutex

// seedShredsRewardsCache computes the complete set the way the page-cache worker
// does and stores it under the real key, holding the cache lock for the rest of
// the test. Pass nil to assert on the entry being ABSENT instead.
func seedShredsRewardsCache(t *testing.T, api *handlers.API, complete *handlers.ShredsRewardsResponse) {
	t.Helper()
	shredsRewardsCacheMu.Lock()
	t.Cleanup(shredsRewardsCacheMu.Unlock)

	if complete == nil {
		_, err := api.PgPool.Exec(t.Context(),
			`DELETE FROM page_cache WHERE key = $1`, handlers.ShredsRewardsPageCacheKey)
		require.NoError(t, err)
		return
	}
	blob, err := json.Marshal(complete)
	require.NoError(t, err)
	_, err = api.PgPool.Exec(t.Context(),
		`INSERT INTO page_cache (key, data, updated_at) VALUES ($1, $2, now())
		 ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
		handlers.ShredsRewardsPageCacheKey, blob)
	require.NoError(t, err)
}

// TestGetShredsRewards_CachedPageMatchesLive is the invariant behind serving every
// unfiltered page out of one cached complete set: for each sortable column,
// direction and page, what the handler slices out of the cache must be exactly
// what the live ClickHouse query would have returned.
//
// The two are separate implementations of one order — buildShredsRewardsSort in
// SQL, sortShredsRewardsRows in Go — so nothing but this test stops them drifting.
func TestGetShredsRewards_CachedPageMatchesLive(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	const validators = 25
	insertShredsRewardsManyValidators(t, api, validators)

	complete, err := api.FetchShredsRewardsData(t.Context())
	require.NoError(t, err)
	require.Len(t, complete.Validators, validators)
	require.Equal(t, validators, complete.Total)

	// Assert the fixture is the shape the sort cases need before trusting them: a
	// block of distinct names and a block of empty ones. An earlier revision of the
	// seeder emitted one identical name for every named validator, which left the
	// validator_name cases passing on the tiebreaker alone and testing nothing.
	// The seeder names every even-indexed validator, so of 25 there are 13 named
	// and 12 empty, and the map holds one key per distinct name plus the empty one.
	const named, unnamed = (validators + 1) / 2, validators / 2
	names := map[string]int{}
	for _, v := range complete.Validators {
		names[v.ValidatorName]++
	}
	require.Equal(t, unnamed, names[""], "the unnamed half has no validators.app row")
	require.Len(t, names, named+1, "every named validator must have a distinct name")

	seedShredsRewardsCache(t, api, complete)

	get := func(t *testing.T, query string) (handlers.ShredsRewardsResponse, string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?"+query, nil)
		rr := httptest.NewRecorder()
		api.GetShredsRewards(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
		return decodeShredsRewards(t, rr.Body.Bytes()), rr.Header().Get("X-Cache")
	}

	for _, sortField := range []string{
		"", "total_earned_2z", "validator_name", "activated_stake", "immediately_claimable_2z",
	} {
		for _, order := range []string{"desc", "asc"} {
			for _, offset := range []int{0, 10, 20, 30} {
				name := fmt.Sprintf("%s/%s/offset=%d", sortField, order, offset)
				t.Run(name, func(t *testing.T) {
					q := fmt.Sprintf("sort=%s&order=%s&limit=10&offset=%d", sortField, order, offset)
					cached, hdr := get(t, q)
					require.Equal(t, "HIT", hdr, "unfiltered pages must be served from the cache")

					// The same request with a search term the data cannot match would
					// take the live path but return nothing, so compare against the
					// live computation directly instead.
					live, err := api.ExportComputeShredsRewards(
						t.Context(), "", sortField, order, 10, offset)
					require.NoError(t, err)

					require.Equal(t, len(live.Validators), len(cached.Validators), "page length")
					for i := range live.Validators {
						assert.Equal(t, live.Validators[i].NodeID, cached.Validators[i].NodeID,
							"row %d of the page", i)
					}
					// Including offset=30, which is past the end of a 25-row set: the
					// cached path knows the size without asking, and the live path
					// recovers it rather than reporting the 0 its window aggregate
					// had no row to ride on.
					assert.Equal(t, live.Total, cached.Total)
					assert.Equal(t, validators, cached.Total)
				})
			}
		}
	}
}

// TestGetShredsRewards_CachedPagesPartitionTheSet: paging through the cached set
// must visit every validator exactly once. A sort with no total order lets ties
// land differently in the queries that fetch two adjacent pages, which shows up as
// a validator appearing on both pages or on neither — and this list's claimable
// column is 0 for nearly every row.
func TestGetShredsRewards_CachedPagesPartitionTheSet(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	const validators = 25
	insertShredsRewardsManyValidators(t, api, validators)

	complete, err := api.FetchShredsRewardsData(t.Context())
	require.NoError(t, err)
	seedShredsRewardsCache(t, api, complete)

	// Every row is tied at 0 here, so this is the worst case for the tiebreaker.
	seen := map[string]int{}
	for offset := 0; offset < validators; offset += 10 {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
			"/api/dz/shreds/rewards?sort=immediately_claimable_2z&order=desc&limit=10&offset=%d", offset), nil)
		rr := httptest.NewRecorder()
		api.GetShredsRewards(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
		for _, v := range decodeShredsRewards(t, rr.Body.Bytes()).Validators {
			seen[v.NodeID]++
		}
	}
	require.Len(t, seen, validators, "every validator appears across the pages")
	for node, times := range seen {
		assert.Equal(t, 1, times, "%s appeared on more than one page", node)
	}
}

// TestGetShredsRewards_SearchServedFromCache: a search selects from exactly the
// set the cached entry holds, so it is answered from the cache rather than by the
// slowest query the page can make. Live, the search WHERE sits on top of
// shredsRewardsIdentityJoins and overran the handler's 15s budget, so search
// returned 500 and showed no data at all.
func TestGetShredsRewards_SearchServedFromCache(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	insertShredsRewardsManyValidators(t, api, 25)

	complete, err := api.FetchShredsRewardsData(t.Context())
	require.NoError(t, err)
	seedShredsRewardsCache(t, api, complete)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?search=node:node-007", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Equal(t, "HIT", rr.Header().Get("X-Cache"))

	resp := decodeShredsRewards(t, rr.Body.Bytes())
	require.Len(t, resp.Validators, 1)
	assert.Equal(t, "node-007", resp.Validators[0].NodeID)
	assert.Equal(t, 1, resp.Total, "the total describes the filtered set, which is what the pager reads")
}

// TestGetShredsRewards_CachedSearchMatchesLive is the invariant behind answering a
// search from the cache: for every search shape the page can produce, what
// matchShredsRewardsSearch selects in Go must be exactly what the SQL WHERE
// selects in ClickHouse.
//
// The two are one predicate written twice — buildShredsRewardsSearch in SQL,
// matchShredsRewardsSearch in Go — so nothing but this test stops them drifting.
// Both paths stay reachable at runtime (a cache miss still serves live), so a
// drift would show the same query returning different rows depending on cache
// state.
func TestGetShredsRewards_CachedSearchMatchesLive(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	const validators = 25
	insertShredsRewardsManyValidators(t, api, validators)

	complete, err := api.FetchShredsRewardsData(t.Context())
	require.NoError(t, err)
	require.Len(t, complete.Validators, validators)

	// The blob the cache would hold, compared against the query directly. Neither
	// side goes through page_cache or the handler: seedShredsRewardsCache holds a
	// mutex until the test ends, so seeding twice to reach both paths deadlocks.
	blob, err := json.Marshal(complete)
	require.NoError(t, err)

	ids := func(t *testing.T, resp *handlers.ShredsRewardsResponse) []string {
		t.Helper()
		out := make([]string, 0, len(resp.Validators))
		for _, v := range resp.Validators {
			out = append(out, v.NodeID)
		}
		return out
	}

	// Field-scoped, free text, case folding, multiple ANDed filters, a term that
	// matches nothing, one that matches everything, and a bare colon.
	for _, search := range []string{
		"node:node-007", "node:NODE-007", "name:Validator", "name:validator",
		"vote:vote-00", "node-01", "NODE-01",
		"node:node-0,name:Validator", "node:node-007,name:nope",
		"zzz-matches-nothing", "node:node", "unknownfield:node-007",
	} {
		t.Run(search, func(t *testing.T) {
			live, err := api.ExportComputeShredsRewards(t.Context(), search, "", "", 500, 0)
			require.NoError(t, err)

			cached, ok := handlers.ExportSliceCachedShredsRewards(blob, search, "", "", 500, 0)
			require.True(t, ok, "the seeded payload is the complete set")

			require.Equal(t, live.Total, cached.Total, "search %q: totals disagree", search)
			require.Equal(t, ids(t, live), ids(t, cached), "search %q: matched sets disagree", search)
		})
	}
}

// TestGetShredsRewards_MissingCacheFallsBackToLive: the key is a NEW one, so the
// entry is absent on the deploy that introduces it and stays absent until the
// refresh chain first reaches it — and page_cache survives a pod restart, so this
// is the state the page ships in. It must serve live, not empty.
func TestGetShredsRewards_MissingCacheFallsBackToLive(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	insertShredsRewardsManyValidators(t, api, 12)
	seedShredsRewardsCache(t, api, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?limit=10&offset=10", nil)
	rr := httptest.NewRecorder()
	api.GetShredsRewards(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Equal(t, "MISS", rr.Header().Get("X-Cache"))
	resp := decodeShredsRewards(t, rr.Body.Bytes())
	assert.Len(t, resp.Validators, 2)
	assert.Equal(t, 12, resp.Total)
}

// TestGetShredsRewards_PagePastEndStillReportsTotal covers the one shape where
// count() OVER () has no row to ride on. The pager reads `total` to decide how
// many pages exist, so reporting 0 for a page past the end collapses the footer
// and leaves the reader an empty table with nothing saying the set is simply
// shorter than the page they asked for.
//
// Both paths are checked because they recover it differently: the cached one from
// the stored set, the live one by re-asking its own query for a single row.
func TestGetShredsRewards_PagePastEndStillReportsTotal(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	const validators = 12
	insertShredsRewardsManyValidators(t, api, validators)

	t.Run("live", func(t *testing.T) {
		// A search is the only shape that reaches the live path, and node-0 matches
		// every one of node-000..node-011.
		resp, err := api.ExportComputeShredsRewards(t.Context(), "node:node-0", "", "", 10, 100)
		require.NoError(t, err)
		assert.Empty(t, resp.Validators, "offset 100 is past the end")
		assert.Equal(t, validators, resp.Total,
			"the total describes the matching set, not the empty page")
	})

	t.Run("live/genuinely empty", func(t *testing.T) {
		// No recovery to do here: nothing matches at any offset, so 0 is the answer
		// rather than a lost count.
		resp, err := api.ExportComputeShredsRewards(t.Context(), "node:nothing-matches", "", "", 10, 100)
		require.NoError(t, err)
		assert.Empty(t, resp.Validators)
		assert.Equal(t, 0, resp.Total)
	})

	t.Run("cached", func(t *testing.T) {
		complete, err := api.FetchShredsRewardsData(t.Context())
		require.NoError(t, err)
		seedShredsRewardsCache(t, api, complete)

		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/rewards?limit=10&offset=100", nil)
		rr := httptest.NewRecorder()
		api.GetShredsRewards(rr, req)

		require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
		assert.Equal(t, "HIT", rr.Header().Get("X-Cache"))
		resp := decodeShredsRewards(t, rr.Body.Bytes())
		assert.Empty(t, resp.Validators)
		assert.Equal(t, validators, resp.Total)
	})
}
