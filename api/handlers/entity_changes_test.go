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

// TestEntityChanges_FeedPksTransition is the feed-seat equivalent: a user whose
// only change is the set of feeds it holds seats on must surface as
// changed_fields=['feed_pks'].
func TestEntityChanges_FeedPksTransition(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Two snapshots for the same user, identical except feed_pks gaining a seat.
	// Distinct attrs_hash so the SCD delta fires; the earliest row is the
	// baseline (excluded by the view's min_ts guard), leaving one 'updated'.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, tunnel_id, publishers, subscribers, bgp_status, feed_pks)
		VALUES
			('ec-feed-user', now() - INTERVAL 2 MINUTE, now(), generateUUIDv4(), 0, 1, 'ec-feed-user', 'o', 'activated', 'multicast', '10.0.0.1', '10.0.0.2', 'ec-dev', 't1', 505, '[]', '[]', 'up', '[]'),
			('ec-feed-user', now() - INTERVAL 1 MINUTE, now(), generateUUIDv4(), 0, 2, 'ec-feed-user', 'o', 'activated', 'multicast', '10.0.0.1', '10.0.0.2', 'ec-dev', 't1', 505, '[]', '[]', 'up', '["feedA"]')`))

	rows, err := api.DB.Query(ctx, `
		SELECT change_type, changed_fields
		FROM entity_changes_v
		WHERE entity_type = 'user' AND entity_pk = 'ec-feed-user'
		ORDER BY snapshot_ts`)
	require.NoError(t, err)
	defer rows.Close()

	var got [][]string
	var changeType string
	for rows.Next() {
		var fields []string
		require.NoError(t, rows.Scan(&changeType, &fields))
		got = append(got, fields)
	}
	require.NoError(t, rows.Err())

	require.Len(t, got, 1, "only the feed_pks transition should surface (baseline is excluded)")
	assert.Equal(t, "updated", changeType)
	assert.Equal(t, []string{"feed_pks"}, got[0],
		"a feed-seat change must render as changed_fields=['feed_pks'], not an empty list")
}

// TestEntityChanges_FeedEntity verifies the 'feed' section of entity_changes_v:
// Feed accounts surface as first-class timeline entities, with metro_code
// resolved through the metro join.
func TestEntityChanges_FeedEntity(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// The metro the feed is scoped to, so the branch's LEFT JOIN against
	// dz_metros_current resolves metro_code.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, longitude, latitude)
		VALUES
			('ec-metro', now() - INTERVAL 3 MINUTE, now(), generateUUIDv4(), 0, 1, 'ec-metro', 'ams', 'Amsterdam', 4.9, 52.4)`))

	// Two snapshots for the same feed, identical except the multicast groups it
	// offers. Each test gets its own database, so this is the only writer to
	// dim_dz_feeds_history: the earliest row is the baseline the min_ts guard
	// excludes, leaving exactly one 'updated'.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
			('ec-feed', now() - INTERVAL 2 MINUTE, now(), generateUUIDv4(), 0, 1, 'ec-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'ec-metro', '[]'),
			('ec-feed', now() - INTERVAL 1 MINUTE, now(), generateUUIDv4(), 0, 2, 'ec-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'ec-metro', '["groupA"]')`))

	rows, err := api.DB.Query(ctx, `
		SELECT entity_code, change_type, changed_fields, new_status, contributor_code, metro_code
		FROM entity_changes_v
		WHERE entity_type = 'feed' AND entity_pk = 'ec-feed'
		ORDER BY snapshot_ts`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		entityCode      string
		changeType      string
		changedFields   []string
		newStatus       string
		contributorCode string
		metroCode       string
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.entityCode, &r.changeType, &r.changedFields, &r.newStatus, &r.contributorCode, &r.metroCode))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, got, 1, "only the groups change should surface (baseline is excluded)")
	assert.Equal(t, "feed-ams", got[0].entityCode)
	assert.Equal(t, "updated", got[0].changeType)
	assert.Equal(t, []string{"groups"}, got[0].changedFields)
	assert.Equal(t, "ams", got[0].metroCode, "metro_code should resolve through metro_pk")
	assert.Empty(t, got[0].newStatus, "feeds have no status")
	assert.Empty(t, got[0].contributorCode, "feeds have no contributor")
}
