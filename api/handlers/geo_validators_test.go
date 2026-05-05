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

func TestGetGeoValidators(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	createDZDPLocationStateTable(t, api)

	t.Run("empty response", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/validators", nil)
		w := httptest.NewRecorder()
		api.GetGeoValidators(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoValidatorsResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, 0, resp.TotalValidators)
		assert.Empty(t, resp.Validators)
		assert.Empty(t, resp.MetroBreakdown)
	})

	seedGeoTestData(t, api)

	t.Run("returns all validators", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/validators", nil)
		w := httptest.NewRecorder()
		api.GetGeoValidators(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoValidatorsResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		assert.Equal(t, 3, resp.TotalValidators)
		assert.Len(t, resp.Validators, 3)

		// Validators should be sorted by stake descending: vote-2 (200), vote-1 (100), vote-3 (50)
		assert.Equal(t, "vote-2", resp.Validators[0].VotePubkey)
		assert.Equal(t, "vote-1", resp.Validators[1].VotePubkey)
		assert.Equal(t, "vote-3", resp.Validators[2].VotePubkey)

		assert.InDelta(t, 200.0, resp.Validators[0].StakeSol, 1.0)
		assert.InDelta(t, 100.0, resp.Validators[1].StakeSol, 1.0)
		assert.InDelta(t, 50.0, resp.Validators[2].StakeSol, 1.0)

		// Tier distribution should have 3 tiers
		assert.Len(t, resp.TierDistribution, 3)

		// Metro breakdown should have 2 metros
		assert.Len(t, resp.MetroBreakdown, 2)

		var amsBreakdown, nycBreakdown *handlers.GeoMetroBreakdown
		for i := range resp.MetroBreakdown {
			switch resp.MetroBreakdown[i].MetroCode {
			case "ams":
				amsBreakdown = &resp.MetroBreakdown[i]
			case "nyc":
				nycBreakdown = &resp.MetroBreakdown[i]
			}
		}
		require.NotNil(t, amsBreakdown, "expected AMS metro breakdown")
		require.NotNil(t, nycBreakdown, "expected NYC metro breakdown")

		assert.Equal(t, 2, amsBreakdown.Validators)
		assert.InDelta(t, 150.0, amsBreakdown.StakeSol, 1.0)
		assert.Equal(t, 1, nycBreakdown.Validators)
		assert.InDelta(t, 200.0, nycBreakdown.StakeSol, 1.0)
	})

	t.Run("filter by metro", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/validators?metro=ams", nil)
		w := httptest.NewRecorder()
		api.GetGeoValidators(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoValidatorsResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		assert.Equal(t, 2, resp.TotalValidators)
		assert.Len(t, resp.Validators, 2)

		// Both should be AMS validators: vote-1 and vote-3
		pubkeys := make(map[string]bool)
		for _, v := range resp.Validators {
			pubkeys[v.VotePubkey] = true
		}
		assert.True(t, pubkeys["vote-1"], "expected vote-1 in AMS metro")
		assert.True(t, pubkeys["vote-3"], "expected vote-3 in AMS metro")
	})

	t.Run("filter by dz_filter on", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/validators?dz_filter=on", nil)
		w := httptest.NewRecorder()
		api.GetGeoValidators(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoValidatorsResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		// Only is_dz validators: vote-1 and vote-3
		assert.Equal(t, 2, resp.TotalValidators)
		assert.Len(t, resp.Validators, 2)

		pubkeys := make(map[string]bool)
		for _, v := range resp.Validators {
			pubkeys[v.VotePubkey] = true
			assert.True(t, v.IsDZ, "expected all validators to be DZ")
		}
		assert.True(t, pubkeys["vote-1"], "expected vote-1 with dz_filter=on")
		assert.True(t, pubkeys["vote-3"], "expected vote-3 with dz_filter=on")
	})

	t.Run("filter by dz_filter off", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/validators?dz_filter=off", nil)
		w := httptest.NewRecorder()
		api.GetGeoValidators(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp handlers.GeoValidatorsResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		// Only non-DZ validator: vote-2
		assert.Equal(t, 1, resp.TotalValidators)
		assert.Len(t, resp.Validators, 1)
		assert.Equal(t, "vote-2", resp.Validators[0].VotePubkey)
		assert.False(t, resp.Validators[0].IsDZ, "expected vote-2 to not be DZ")
	})
}
