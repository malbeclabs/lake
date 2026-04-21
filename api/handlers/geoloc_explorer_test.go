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

	// Insert location_offsets data in the env-named database.
	// Two rows for sender-pk-1 to test min aggregation.
	err = api.DB.Exec(ctx, "INSERT INTO `mainnet-beta`.location_offsets"+`
		(received_at, source_addr, authority_pubkey, sender_pubkey, measurement_slot,
		 lat, lng, measured_rtt_ns, rtt_ns, target_ip, num_references,
		 signature_valid, signature_error, raw_offset,
		 ref_authority_pubkeys, ref_sender_pubkeys, ref_measured_rtt_ns, ref_rtt_ns)
		VALUES
			(now(), '10.0.0.1', 'auth1', 'sender-pk-1', 100,
			 52.37, 4.89, 8000000, 6000000, '8.8.8.8', 3,
			 true, '', '{}',
			 ['auth-ref-1'], ['sender-ref-1'], [5000000], [5500000]),
			(now(), '10.0.0.1', 'auth1', 'sender-pk-1', 101,
			 52.37, 4.89, 6000000, 5000000, '8.8.8.8', 3,
			 true, '', '{}',
			 ['auth-ref-1'], ['sender-ref-1'], [4000000], [4500000]),
			(now(), '10.0.0.2', 'auth2', 'sender-pk-2', 102,
			 40.71, -74.01, 9000000, 8000000, '1.1.1.1', 2,
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
		assert.Empty(t, resp.Devices)
		assert.Empty(t, resp.Targets)
	})

	setupGeolocExplorerTestData(t, api)

	t.Run("returns aggregated devices and targets", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/explorer", nil)
		w := httptest.NewRecorder()
		api.GetGeolocExplorer(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		var resp handlers.GeolocExplorerResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		// Should have 2 devices (sender-pk-1 and sender-pk-2)
		assert.Len(t, resp.Devices, 2)

		// Find sender-pk-1 and verify aggregation
		var dev1 *handlers.GeolocExplorerDevice
		for i := range resp.Devices {
			if resp.Devices[i].SenderPubkey == "sender-pk-1" {
				dev1 = &resp.Devices[i]
			}
		}
		require.NotNil(t, dev1, "expected to find device with sender-pk-1")
		assert.Equal(t, "probe-ams", dev1.ProbeCode)
		assert.InDelta(t, 52.37, dev1.Lat, 0.01)
		assert.InDelta(t, 4.89, dev1.Lng, 0.01)
		// min of 5000000 and 4000000 = 4000000
		assert.Equal(t, uint64(4000000), dev1.MinRefMeasuredRttNs)

		// Should have 2 targets (sender-pk-1/8.8.8.8 and sender-pk-2/1.1.1.1)
		assert.Len(t, resp.Targets, 2)

		// Find the target for sender-pk-1
		var tgt1 *handlers.GeolocExplorerTarget
		for i := range resp.Targets {
			if resp.Targets[i].SenderPubkey == "sender-pk-1" {
				tgt1 = &resp.Targets[i]
			}
		}
		require.NotNil(t, tgt1, "expected to find target for sender-pk-1")
		assert.Equal(t, "8.8.8.8", tgt1.TargetIP)
		// min of 8000000 and 6000000 = 6000000
		assert.Equal(t, uint64(6000000), tgt1.MinMeasuredRttNs)
	})

	t.Run("custom hours param", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/explorer?hours=1", nil)
		w := httptest.NewRecorder()
		api.GetGeolocExplorer(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		var resp handlers.GeolocExplorerResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Devices)
	})
}
