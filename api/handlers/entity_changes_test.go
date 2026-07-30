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

	require.Len(t, got, 1, "only the feed_pks transition should surface (baseline is excluded)")
	assert.Equal(t, "updated", got[0].changeType)
	assert.Equal(t, []string{"feed_pks"}, got[0].changedFields,
		"a feed-seat change must render as changed_fields=['feed_pks'], not an empty list")
}

// TestEntityChanges_FeedEntity verifies the 'feed' section of entity_changes_v:
// Feed accounts surface as first-class timeline entities across all three
// change types, with metro_code resolved through the metro join.
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
			('ec-metro', now() - INTERVAL 5 MINUTE, now(), generateUUIDv4(), 0, 1, 'ec-metro', 'ams', 'Amsterdam', 4.9, 52.4)`))

	// Each test gets its own database, so this is the only writer to
	// dim_dz_feeds_history and the min_ts guard excludes exactly the earliest
	// row here. Expected: ec-feed's first snapshot is the excluded baseline,
	// then a groups change ('updated'), then a tombstone ('deleted'); ec-feed-2
	// arrives after min_ts so its first snapshot is a real 'created'. ec-feed-2
	// points at a metro that does not exist, exercising the join's '' fallback.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
			('ec-feed', now() - INTERVAL 4 MINUTE, now(), generateUUIDv4(), 0, 1, 'ec-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'ec-metro', '[]'),
			('ec-feed', now() - INTERVAL 3 MINUTE, now(), generateUUIDv4(), 0, 2, 'ec-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'ec-metro', '["groupA"]'),
			('ec-feed', now() - INTERVAL 2 MINUTE, now(), generateUUIDv4(), 1, 3, 'ec-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'ec-metro', '["groupA"]'),
			('ec-feed-2', now() - INTERVAL 1 MINUTE, now(), generateUUIDv4(), 0, 4, 'ec-feed-2', 'o', 'feed-fra', 'Frankfurt Feed', 'ec-no-such-metro', '[]')`))

	rows, err := api.DB.Query(ctx, `
		SELECT entity_code, change_type, changed_fields, new_status, contributor_code, metro_code
		FROM entity_changes_v
		WHERE entity_type = 'feed'
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

	require.Len(t, got, 3, "the ec-feed baseline is excluded; updated, deleted and created remain")

	assert.Equal(t, "feed-ams", got[0].entityCode)
	assert.Equal(t, "updated", got[0].changeType)
	assert.Equal(t, []string{"groups"}, got[0].changedFields)
	assert.Equal(t, "ams", got[0].metroCode, "metro_code should resolve through metro_pk")
	assert.Empty(t, got[0].newStatus, "feeds have no status")
	assert.Empty(t, got[0].contributorCode, "feeds have no contributor")

	assert.Equal(t, "deleted", got[1].changeType)

	assert.Equal(t, "feed-fra", got[2].entityCode)
	assert.Equal(t, "created", got[2].changeType)
	assert.Empty(t, got[2].metroCode, "an unresolvable metro_pk should render as empty, not NULL")
}
