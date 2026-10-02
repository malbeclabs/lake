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

// insertFacilityMulticastData inserts a facility with one device (via location_pk)
// whose on-chain multicast counts are left at 0, plus activated multicast users
// that subscribe to / publish to groups. The facility aggregate must report the
// live per-device counts, not the stale on-chain zeros (#650).
func insertFacilityMulticastData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_facilities_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner, lat, lng, loc_id, status, code, name, country, reference_count) VALUES
		('fac-mc', now(), now(), generateUUIDv4(), 0, 1, 'fac-mc', '', 40.0, -74.0, 1, 'activated', 'FAC-MC', 'Multicast Facility', 'US', 1)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, contributor_pk, metro_pk, location_pk, public_ip, max_users) VALUES
		('fdev-1', now(), now(), generateUUIDv4(), 0, 1, 'fdev-1', 'F-DEV-01', 'up', 'switch', '', '', 'fac-mc', '10.7.0.1', 100)
	`)
	require.NoError(t, err)

	// 3 subscriber-only, 2 publisher-only, 1 both-roles, 1 pending (excluded).
	// Live totals: subscribers 4, publishers 3.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id, publishers, subscribers) VALUES
		('f-s1',   now(), now(), generateUUIDv4(), 0, 1, 'f-s1',   'activated', 'fdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g1"]'),
		('f-s2',   now(), now(), generateUUIDv4(), 0, 2, 'f-s2',   'activated', 'fdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g1"]'),
		('f-s3',   now(), now(), generateUUIDv4(), 0, 3, 'f-s3',   'activated', 'fdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g2"]'),
		('f-p1',   now(), now(), generateUUIDv4(), 0, 4, 'f-p1',   'activated', 'fdev-1', 'multicast', 'p', '', '', 0, '["g1"]', '[]'),
		('f-p2',   now(), now(), generateUUIDv4(), 0, 5, 'f-p2',   'activated', 'fdev-1', 'multicast', 'p', '', '', 0, '["g1"]', '[]'),
		('f-b1',   now(), now(), generateUUIDv4(), 0, 6, 'f-b1',   'activated', 'fdev-1', 'multicast', 'p', '', '', 0, '["g2"]', '["g2"]'),
		('f-pend', now(), now(), generateUUIDv4(), 0, 7, 'f-pend', 'pending',   'fdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g1"]')
	`)
	require.NoError(t, err)
}

func TestGetFacilities_MulticastCountsFromLiveUsers(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertFacilityMulticastData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/facilities", nil)
	rr := httptest.NewRecorder()
	api.GetFacilities(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.FacilityListItem]
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))

	var f *handlers.FacilityListItem
	for i := range response.Items {
		if response.Items[i].PK == "fac-mc" {
			f = &response.Items[i]
			break
		}
	}
	require.NotNil(t, f)
	assert.Equal(t, uint64(4), f.MulticastSubscribersCount)
	assert.Equal(t, uint64(3), f.MulticastPublishersCount)
}
