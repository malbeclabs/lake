package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createLocationOffsetsTable creates the location_offsets table in the
// mainnet-beta database, matching the env-named database the handler queries.
func createLocationOffsetsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	err := api.DB.Exec(ctx, "CREATE DATABASE IF NOT EXISTS `mainnet-beta`")
	require.NoError(t, err)
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.location_offsets (
			received_at DateTime64(3),
			source_addr String,
			authority_pubkey String,
			sender_pubkey String,
			measurement_slot UInt64,
			lat Float64,
			lng Float64,
			measured_rtt_ns UInt64,
			rtt_ns UInt64,
			target_ip String,
			num_references UInt8,
			signature_valid Bool,
			signature_error String,
			raw_offset String,
			ref_authority_pubkeys Array(String),
			ref_sender_pubkeys Array(String),
			ref_measured_rtt_ns Array(UInt64),
			ref_rtt_ns Array(UInt64)
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(received_at)
		ORDER BY (received_at, sender_pubkey)
	`, "`mainnet-beta`"))
	require.NoError(t, err)
}

func setupGeolocExplorerTestData(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	// Insert probe dimension data (backs the geoloc_probes_current view)
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_geoloc_probes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner, exchange_pk, public_ip, location_offset_port, metrics_publisher_pk,
			 reference_count, code, parent_devices, target_update_count)
		VALUES
			('probe-1', now(), now(), generateUUIDv4(), 0, 1,
			 'sender-pk-1', 'owner1', '', '1.2.3.4', 9100, '', 3, 'probe-ams', '', 5)
	`)
	require.NoError(t, err)

	// Insert location_offsets data in the env-named database
	err = api.DB.Exec(ctx, "INSERT INTO `mainnet-beta`.location_offsets"+`
		(received_at, source_addr, authority_pubkey, sender_pubkey, measurement_slot,
		 lat, lng, measured_rtt_ns, rtt_ns, target_ip, num_references,
		 signature_valid, signature_error, raw_offset,
		 ref_authority_pubkeys, ref_sender_pubkeys, ref_measured_rtt_ns, ref_rtt_ns)
		VALUES
			(now(), '10.0.0.1', 'auth1', 'sender-pk-1', 100,
			 52.37, 4.89, 5000000, 6000000, '8.8.8.8', 3,
			 true, '', '{}',
			 ['auth-ref-1', 'auth-ref-2'], ['sender-ref-1', 'sender-ref-2'], [4000000, 4500000], [5000000, 5500000]),
			(now(), '10.0.0.2', 'auth2', 'sender-pk-2', 101,
			 40.71, -74.01, 8000000, 9000000, '1.1.1.1', 2,
			 true, '', '{}',
			 ['auth-ref-3'], ['sender-ref-3'], [7000000], [8000000])
	`)
	require.NoError(t, err)
}

func TestGetGeolocExplorer(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	// Map the default env to the test database so joins resolve correctly
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	createLocationOffsetsTable(t, api)

	t.Run("empty response", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/explorer", nil)
		w := httptest.NewRecorder()
		api.GetGeolocExplorer(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		var resp handlers.GeolocExplorerResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Offsets)
	})

	setupGeolocExplorerTestData(t, api)

	t.Run("returns offsets with probe code", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/explorer", nil)
		w := httptest.NewRecorder()
		api.GetGeolocExplorer(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		var resp handlers.GeolocExplorerResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Offsets)
		found := false
		for _, o := range resp.Offsets {
			if o.SenderPubkey == "sender-pk-1" {
				found = true
				assert.Equal(t, "probe-ams", o.ProbeCode)
				assert.InDelta(t, 52.37, o.Lat, 0.01)
				assert.InDelta(t, 4.89, o.Lng, 0.01)
				assert.NotEmpty(t, o.RefMeasuredRttNs)
				assert.NotEmpty(t, o.RefRttNs)
			}
		}
		assert.True(t, found, "expected to find offset with sender-pk-1")
	})

	t.Run("custom hours param", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/explorer?hours=24", nil)
		w := httptest.NewRecorder()
		api.GetGeolocExplorer(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		var resp handlers.GeolocExplorerResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Offsets)
	})
}
