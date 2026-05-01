package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDZDPLocationStateTable creates the dzdp database and location_state
// table used by geo concentration and geo validators handlers.
func createDZDPLocationStateTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	err := api.DB.Exec(ctx, "CREATE DATABASE IF NOT EXISTS dzdp")
	require.NoError(t, err)
	err = api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dzdp.location_state (
			snapshot_at DateTime64(9),
			target_ip String,
			winning_probe_pubkey String,
			lat Float64,
			lng Float64,
			best_rtt_ns UInt64,
			state LowCardinality(String),
			decided_at DateTime64(9)
		) ENGINE = ReplacingMergeTree(snapshot_at)
		ORDER BY target_ip
	`)
	require.NoError(t, err)
}

// seedGeoTestData inserts dimension and DZDP data shared by both
// geo_concentration and geo_validators tests.
func seedGeoTestData(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	// Seed vote accounts
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 vote_pubkey, epoch, node_pubkey, activated_stake_lamports,
			 epoch_vote_account, commission_percentage)
		VALUES
			('vote-1', now(), now(), generateUUIDv4(), 0, 1,
			 'vote-1', 100, 'node-1', 100000000000, 'true', 5),
			('vote-2', now(), now(), generateUUIDv4(), 0, 1,
			 'vote-2', 100, 'node-2', 200000000000, 'true', 10),
			('vote-3', now(), now(), generateUUIDv4(), 0, 1,
			 'vote-3', 100, 'node-3', 50000000000, 'true', 3)
	`)
	require.NoError(t, err)

	// Seed gossip nodes
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
			('node-1', now(), now(), generateUUIDv4(), 0, 1,
			 'node-1', 100, '10.0.0.1', 8001, '', 0, '1.18.0'),
			('node-2', now(), now(), generateUUIDv4(), 0, 1,
			 'node-2', 100, '10.0.0.2', 8001, '', 0, '1.18.0'),
			('node-3', now(), now(), generateUUIDv4(), 0, 1,
			 'node-3', 100, '10.0.0.3', 8001, '', 0, '1.18.0')
	`)
	require.NoError(t, err)

	// Seed geoip records
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_geoip_records_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 ip, country_code, country, region, city, city_id, metro_name,
			 latitude, longitude, postal_code, time_zone, accuracy_radius,
			 asn, asn_org, is_anycast, is_anonymous_proxy, is_satellite_provider)
		VALUES
			('10.0.0.1', now(), now(), generateUUIDv4(), 0, 1,
			 '10.0.0.1', 'NL', 'Netherlands', 'NH', 'Amsterdam', 1, 'Amsterdam',
			 52.37, 4.89, '1000', 'Europe/Amsterdam', 100,
			 13335, 'Cloudflare Inc', false, false, false),
			('10.0.0.2', now(), now(), generateUUIDv4(), 0, 1,
			 '10.0.0.2', 'US', 'United States', 'NY', 'New York', 2, 'New York',
			 40.71, -74.01, '10001', 'America/New_York', 100,
			 13335, 'Cloudflare Inc', false, false, false),
			('10.0.0.3', now(), now(), generateUUIDv4(), 0, 1,
			 '10.0.0.3', 'DE', 'Germany', 'HE', 'Frankfurt', 3, 'Frankfurt',
			 52.37, 4.89, '60306', 'Europe/Berlin', 100,
			 16509, 'Amazon.com Inc', false, false, false)
	`)
	require.NoError(t, err)

	// Seed DZ metros
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, code, name, longitude, latitude)
		VALUES
			('ams', now(), now(), generateUUIDv4(), 0, 1,
			 'ams', 'ams', 'Amsterdam', 4.89, 52.37),
			('nyc', now(), now(), generateUUIDv4(), 0, 1,
			 'nyc', 'nyc', 'New York', -74.01, 40.71)
	`)
	require.NoError(t, err)

	// Seed validatorsapp validators
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_validatorsapp_validators_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 account, name, vote_account, software_version, software_client,
			 software_client_id, jito, jito_commission, is_active, is_dz,
			 active_stake, commission, delinquent, epoch, epoch_credits,
			 skipped_slot_percent, total_score, data_center_key,
			 autonomous_system_number, latitude, longitude, ip, stake_pools_list)
		VALUES
			('node-1', now(), now(), generateUUIDv4(), 0, 1,
			 'node-1', 'Validator One', 'vote-1', '1.18.0', 'solana-labs',
			 1, 0, 0, 1, 1,
			 100000000000, 5, 0, 100, 1000,
			 '0.5', 100, 'NL-Amsterdam',
			 13335, '52.37', '4.89', '10.0.0.1', ''),
			('node-2', now(), now(), generateUUIDv4(), 0, 1,
			 'node-2', 'Validator Two', 'vote-2', '1.18.0', 'solana-labs',
			 1, 0, 0, 1, 0,
			 200000000000, 10, 0, 100, 2000,
			 '0.3', 200, 'US-New York',
			 13335, '40.71', '-74.01', '10.0.0.2', ''),
			('node-3', now(), now(), generateUUIDv4(), 0, 1,
			 'node-3', 'Validator Three', 'vote-3', '1.18.0', 'solana-labs',
			 1, 0, 0, 1, 1,
			 50000000000, 3, 0, 100, 500,
			 '0.8', 50, 'DE-Frankfurt',
			 16509, '52.37', '4.89', '10.0.0.3', '')
	`)
	require.NoError(t, err)

	// Seed DZDP location_state
	err = api.DB.Exec(ctx, `
		INSERT INTO dzdp.location_state
			(snapshot_at, target_ip, winning_probe_pubkey, lat, lng,
			 best_rtt_ns, state, decided_at)
		VALUES
			(now64(9), '10.0.0.1', 'probe-1', 52.37, 4.89,
			 1000000, 'decided', now64(9)),
			(now64(9), '10.0.0.2', 'probe-2', 40.71, -74.01,
			 2000000, 'decided', now64(9)),
			(now64(9), '10.0.0.3', 'probe-3', 52.37, 4.89,
			 1500000, 'decided', now64(9))
	`)
	require.NoError(t, err)
}

func TestGetGeoConcentration(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	createDZDPLocationStateTable(t, api)

	t.Run("empty response", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geo/concentration", nil)
		w := httptest.NewRecorder()
		api.GetGeoConcentration(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoConcentrationResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, 0, resp.HeroStats.ValidatorsMeasured)
		assert.Empty(t, resp.Metros)
		assert.Empty(t, resp.Countries)
		assert.Empty(t, resp.ASNs)
	})

	seedGeoTestData(t, api)

	t.Run("returns aggregated data", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geo/concentration", nil)
		w := httptest.NewRecorder()
		api.GetGeoConcentration(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoConcentrationResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		// 3 validators measured
		assert.Equal(t, 3, resp.HeroStats.ValidatorsMeasured)

		// 2 anchor points (AMS + NYC metros)
		assert.Equal(t, 2, resp.HeroStats.AnchorPoints)

		// 2 metros: AMS (~150 SOL from vote-1 + vote-3) and NYC (~200 SOL from vote-2)
		assert.Len(t, resp.Metros, 2)

		// Metros should be sorted by stake descending
		// NYC: 200 SOL > AMS: 150 SOL
		var amsMetro, nycMetro *handlers.GeoConcentrationMetro
		for i := range resp.Metros {
			switch resp.Metros[i].MetroCode {
			case "ams":
				amsMetro = &resp.Metros[i]
			case "nyc":
				nycMetro = &resp.Metros[i]
			}
		}
		require.NotNil(t, amsMetro, "expected to find AMS metro")
		require.NotNil(t, nycMetro, "expected to find NYC metro")

		assert.Equal(t, 2, amsMetro.Validators)
		assert.InDelta(t, 150.0, amsMetro.StakeSol, 1.0)

		assert.Equal(t, 1, nycMetro.Validators)
		assert.InDelta(t, 200.0, nycMetro.StakeSol, 1.0)

		// Countries: NL, US, DE
		assert.Len(t, resp.Countries, 3)

		// 2 ASNs: 13335 (Cloudflare) and 16509 (Amazon)
		assert.Len(t, resp.ASNs, 2)

		var cfASN, awsASN *handlers.GeoConcentrationASN
		for i := range resp.ASNs {
			switch resp.ASNs[i].ASN {
			case 13335:
				cfASN = &resp.ASNs[i]
			case 16509:
				awsASN = &resp.ASNs[i]
			}
		}
		require.NotNil(t, cfASN, "expected to find ASN 13335")
		require.NotNil(t, awsASN, "expected to find ASN 16509")

		assert.Equal(t, 2, cfASN.Validators)
		assert.InDelta(t, 300.0, cfASN.StakeSol, 1.0)
		assert.Equal(t, 1, awsASN.Validators)
		assert.InDelta(t, 50.0, awsASN.StakeSol, 1.0)

		// Stake percentages should add up to ~100
		var totalMetroPct float64
		for _, m := range resp.Metros {
			totalMetroPct += m.StakePct
		}
		assert.InDelta(t, 100.0, totalMetroPct, 1.0)

		var totalASNPct float64
		for _, a := range resp.ASNs {
			totalASNPct += a.StakePct
		}
		assert.InDelta(t, 100.0, totalASNPct, 1.0)
	})
}
