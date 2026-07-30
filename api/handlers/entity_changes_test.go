package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
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

// TestTimeline_FeedRoundTrip exercises the full handler path for feed events:
// GET /api/timeline with entity_type=feed must return the feed change with its
// details fetched — the UI sends this exact allowlist on every request, so a
// feed section that only existed in the view would be filtered out here.
func TestTimeline_FeedRoundTrip(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	t1 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, longitude, latitude)
		VALUES
			('rt-metro', ?, now(), generateUUIDv4(), 0, 1, 'rt-metro', 'ams', 'Amsterdam', 4.9, 52.4)`, t1))

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
			('rt-feed', ?, now(), generateUUIDv4(), 0, 1, 'rt-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'rt-metro', '[]'),
			('rt-feed', ?, now(), generateUUIDv4(), 0, 2, 'rt-feed', 'o', 'feed-ams', 'Amsterdam Feed', 'rt-metro', '["groupA"]')`, t1, t2))

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=feed",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	api.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	require.Len(t, resp.Events, 1, "the feed update must survive the entity_type=feed filter")
	e := resp.Events[0]
	assert.Equal(t, "feed", e.EntityType)
	assert.Equal(t, "feed-ams", e.EntityCode)
	assert.Equal(t, "Feed feed-ams groups changed", e.Title)

	details, ok := e.Details.(map[string]any)
	require.True(t, ok, "feed events must carry EntityChangeDetails, got %T", e.Details)
	assert.Equal(t, "updated", details["change_type"])

	changes, ok := details["changes"].([]any)
	require.True(t, ok, "feed updates must carry field diffs, got %T", details["changes"])
	require.Len(t, changes, 1)
	change := changes[0].(map[string]any)
	assert.Equal(t, "groups", change["field"])
	assert.Equal(t, "[]", change["old_value"])
	assert.Equal(t, `["groupA"]`, change["new_value"])

	entity, ok := details["entity"].(map[string]any)
	require.True(t, ok, "feed events must carry the feed entity, got %T", details["entity"])
	assert.Equal(t, "ams", entity["metro_code"], "metro_code must be resolved through metro_pk")
	assert.Equal(t, "Amsterdam Feed", entity["name"])
}

// TestEntityChanges_PayloadColumnsTracked pins the PayloadColumns() <->
// changed_fields contract for every entity type in entity_changes_v. Every
// payload column is hashed into attrs_hash, so changing any of them fires an
// SCD delta; a column the view does not enumerate then renders as a
// contentless "updated (0 fields)" timeline event — the bug shipped twice
// already (bgp_status, then feed_pks) because nothing failed when a schema
// gained a payload column. Skip-listed columns are known, deliberate gaps and
// are pinned as empty: closing one in the view forces removing it here.
func TestEntityChanges_PayloadColumnsTracked(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	arms := []struct {
		entityType string
		table      string
		schema     interface{ PayloadColumns() []string }
		overrides  map[string][2]string // column -> {base, changed} SQL literals when type defaults don't fit
		skip       map[string]string    // column -> why it is not in changed_fields
	}{
		{
			entityType: "device",
			table:      "dim_dz_devices_history",
			schema:     &dzsvc.DeviceSchema{},
			skip: map[string]string{
				"code":                        "not enumerated in the view",
				"location_pk":                 "not enumerated in the view",
				"max_unicast_users":           "not enumerated in the view",
				"max_multicast_subscribers":   "not enumerated in the view",
				"max_multicast_publishers":    "not enumerated in the view",
				"unicast_users_count":         "not enumerated in the view",
				"multicast_subscribers_count": "not enumerated in the view",
				"reserved_seats":              "not enumerated in the view",
				"multicast_publishers_count":  "not enumerated in the view",
				"interfaces":                  "not enumerated in the view",
			},
		},
		{
			entityType: "link",
			table:      "dim_dz_links_history",
			schema:     &dzsvc.LinkSchema{},
			skip: map[string]string{
				"code":              "not enumerated in the view",
				"side_a_iface_name": "not enumerated in the view",
				"side_z_iface_name": "not enumerated in the view",
				"side_a_ip":         "not enumerated in the view",
				"side_z_ip":         "not enumerated in the view",
				"link_topologies":   "not enumerated in the view",
				"unicast_drained":   "not enumerated in the view",
			},
		},
		{
			entityType: "metro",
			table:      "dim_dz_metros_history",
			schema:     &dzsvc.MetroSchema{},
			skip: map[string]string{
				"code": "not enumerated in the view",
			},
		},
		{
			entityType: "contributor",
			table:      "dim_dz_contributors_history",
			schema:     &dzsvc.ContributorSchema{},
		},
		{
			entityType: "user",
			table:      "dim_dz_users_history",
			schema:     &dzsvc.UserSchema{},
			overrides: map[string][2]string{
				// the view excludes validator/gossip_only users entirely
				"kind": {"'multicast'", "'ibrl'"},
			},
			skip: map[string]string{
				"owner_pubkey": "deferred, see malbeclabs/infra#2092 follow-up",
				"tenant_pk":    "deferred, see malbeclabs/infra#2092 follow-up",
				"publishers":   "deferred, see malbeclabs/infra#2092 follow-up",
				"subscribers":  "deferred, see malbeclabs/infra#2092 follow-up",
			},
		},
		{
			entityType: "feed",
			table:      "dim_dz_feeds_history",
			schema:     &dzsvc.FeedSchema{},
		},
	}

	valueFor := func(typ string, changed bool) string {
		switch typ {
		case "INTEGER", "BIGINT":
			if changed {
				return "2"
			}
			return "1"
		case "DOUBLE":
			if changed {
				return "2.5"
			}
			return "1.5"
		case "Boolean": // stored as UInt8
			if changed {
				return "1"
			}
			return "0"
		default: // VARCHAR
			if changed {
				return "'v2'"
			}
			return "'v1'"
		}
	}

	for _, arm := range arms {
		cols := arm.schema.PayloadColumns()
		names := make([]string, len(cols))
		types := make([]string, len(cols))
		for i, c := range cols {
			name, typ, ok := strings.Cut(c, ":")
			require.True(t, ok, "payload column %q has no type suffix", c)
			names[i], types[i] = name, typ
		}

		// One entity per payload column; two snapshots differing only in that
		// column (attrs_hash 1 -> 2 makes the SCD delta fire either way). All
		// baselines share the table-wide min snapshot_ts, so the view's min_ts
		// guard excludes exactly them, leaving one 'updated' per entity.
		var rowsSQL []string
		for i, target := range names {
			eid := fmt.Sprintf("inv-%s-%s", arm.entityType, target)
			for _, changed := range []bool{false, true} {
				vals := make([]string, len(names))
				for j, typ := range types {
					flip := changed && j == i
					if ov, has := arm.overrides[names[j]]; has {
						if flip {
							vals[j] = ov[1]
						} else {
							vals[j] = ov[0]
						}
					} else {
						vals[j] = valueFor(typ, flip)
					}
				}
				ts, hash := "'2025-06-01 00:00:00'", 1
				if changed {
					ts, hash = "'2025-06-01 01:00:00'", 2
				}
				rowsSQL = append(rowsSQL, fmt.Sprintf("('%s', %s, now(), generateUUIDv4(), 0, %d, '%s', %s)",
					eid, ts, hash, eid, strings.Join(vals, ", ")))
			}
		}
		insert := fmt.Sprintf("INSERT INTO %s (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, %s) VALUES %s",
			arm.table, strings.Join(names, ", "), strings.Join(rowsSQL, ", "))
		require.NoError(t, api.DB.Exec(ctx, insert), "arm %s", arm.entityType)

		dbRows, err := api.DB.Query(ctx,
			"SELECT entity_id, changed_fields FROM entity_changes_v WHERE entity_type = ?", arm.entityType)
		require.NoError(t, err)
		byEntity := map[string][]string{}
		for dbRows.Next() {
			var eid string
			var fields []string
			require.NoError(t, dbRows.Scan(&eid, &fields))
			byEntity[eid] = fields
		}
		require.NoError(t, dbRows.Err())
		dbRows.Close()

		for _, name := range names {
			eid := fmt.Sprintf("inv-%s-%s", arm.entityType, name)
			fields, ok := byEntity[eid]
			require.True(t, ok, "%s: no 'updated' event for a %s-only change even though attrs_hash changed", arm.entityType, name)
			if reason, skipped := arm.skip[name]; skipped {
				assert.Empty(t, fields,
					"%s.%s is skip-listed (%s) but now appears tracked — remove it from the skip list", arm.entityType, name, reason)
			} else {
				assert.NotEmpty(t, fields,
					"%s.%s is in attrs_hash but renders as 'updated (0 fields)' — enumerate it in entity_changes_v or add it to this skip list", arm.entityType, name)
			}
		}
	}
}
