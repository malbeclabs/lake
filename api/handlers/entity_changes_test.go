package handlers_test

import (
	"testing"

	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEntityChanges_BgpStatusTransition verifies that a user whose only change
// between two history rows is bgp_status surfaces in entity_changes_v as an
// 'updated' event carrying changed_fields=['bgp_status'] — not the contentless
// "updated (0 fields)" it would produce if bgp_status weren't tracked.
func TestEntityChanges_BgpStatusTransition(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Two snapshots for the same user, identical except bgp_status up -> down.
	// Distinct attrs_hash so the SCD delta fires; the earliest row is the
	// baseline (excluded by the view's min_ts guard), leaving one 'updated'.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES
			('ec-user', now() - INTERVAL 2 MINUTE, now(), generateUUIDv4(), 0, 1, 'ec-user', 'o', 'activated', 'multicast', '10.0.0.1', '10.0.0.2', 'ec-dev', 't1', 505, '[]', '[]', 'up'),
			('ec-user', now() - INTERVAL 1 MINUTE, now(), generateUUIDv4(), 0, 2, 'ec-user', 'o', 'activated', 'multicast', '10.0.0.1', '10.0.0.2', 'ec-dev', 't1', 505, '[]', '[]', 'down')`))

	rows, err := api.DB.Query(ctx, `
		SELECT change_type, changed_fields
		FROM entity_changes_v
		WHERE entity_type = 'user' AND entity_pk = 'ec-user'
		ORDER BY snapshot_ts`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		changeType    string
		changedFields []string
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.changeType, &r.changedFields))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, got, 1, "only the bgp_status transition should surface (baseline is excluded)")
	assert.Equal(t, "updated", got[0].changeType)
	assert.Equal(t, []string{"bgp_status"}, got[0].changedFields,
		"a BGP transition must render as changed_fields=['bgp_status'], not an empty list")
}
