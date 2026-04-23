package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	v1 "github.com/malbeclabs/lake/api/v1"
)

// v1ValidatorsMetadataContractFields is the authoritative JSON keys for each
// item in the /api/v1/solana/validators-metadata response. A mismatch means the
// public contract has changed — bump the API version.
var v1ValidatorsMetadataContractFields = []string{
	"ip",
	"active_stake",
	"vote_account",
	"software_client",
	"software_version",
}

func seedValidatorsAppData(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_validatorsapp_validators_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 account, name, vote_account, software_version, software_client, software_client_id,
			 jito, jito_commission, is_active, is_dz, active_stake, commission, delinquent,
			 epoch, epoch_credits, skipped_slot_percent, total_score,
			 data_center_key, autonomous_system_number, latitude, longitude, ip, stake_pools_list)
		VALUES
			('n1', now(), now(), generateUUIDv4(), 0, 1,
			 'n1', 'Validator 1', 'vote1', '2.2.3', 'Jito', 2,
			 1, 0, 1, 1, 50000000000, 0, 0,
			 800, 1000, '0.5', 100,
			 'US-NY', 0, '', '', '1.2.3.4', ''),
			('n2', now(), now(), generateUUIDv4(), 0, 2,
			 'n2', 'Validator 2', 'vote2', '2.1.0', 'Agave', 1,
			 0, 0, 1, 1, 10000000000, 0, 0,
			 800, 500, '1.0', 50,
			 'EU-AMS', 0, '', '', '5.6.7.8', ''),
			('n3', now(), now(), generateUUIDv4(), 0, 3,
			 'n3', 'Inactive Validator', 'vote3', '2.0.0', 'Agave', 1,
			 0, 0, 0, 0, 0, 0, 1,
			 800, 0, '0', 0,
			 '', 0, '', '', '9.9.9.9', '')
	`))
}

func TestV1ValidatorsMetadata_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/solana/validators-metadata", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var items []v1.ValidatorMetadata
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	assert.Empty(t, items)
}

func TestV1ValidatorsMetadata_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedValidatorsAppData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/solana/validators-metadata", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw []map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	require.NotEmpty(t, raw)
	for i, item := range raw {
		assertJSONKeys(t, item, v1ValidatorsMetadataContractFields, "items[i]")
		_ = i
	}
}

func TestV1ValidatorsMetadata_ActiveOnly(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedValidatorsAppData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/solana/validators-metadata", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var items []v1.ValidatorMetadata
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))

	// Only is_active = 1 rows, ordered by active_stake DESC.
	require.Len(t, items, 2)
	assert.Equal(t, "1.2.3.4", items[0].IP)
	assert.Equal(t, int64(50000000000), items[0].ActiveStake)
	assert.Equal(t, "vote1", items[0].VoteAccount)
	assert.Equal(t, "Jito", items[0].SoftwareClient)
	assert.Equal(t, "2.2.3", items[0].SoftwareVersion)

	assert.Equal(t, "5.6.7.8", items[1].IP)
	assert.Equal(t, int64(10000000000), items[1].ActiveStake)
}

// TestV1ValidatorsMetadata_LegacyRedirect verifies the pre-namespace path
// still resolves (via 308) to the canonical /solana/validators-metadata
// path, so existing consumers keep working during migration.
func TestV1ValidatorsMetadata_LegacyRedirect(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/validators-metadata", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusPermanentRedirect, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "/api/v1/solana/validators-metadata", rr.Header().Get("Location"))
}
