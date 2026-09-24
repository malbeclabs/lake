package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
)

// feedSeatJSON builds one feed_seats entry the way the indexer writes it.
func feedSeatJSON(feedPK string, maxUsers, maxFutureUsers, currentUsers, anniversaryDay int, windowEnd, terminatesAt time.Time) string {
	return fmt.Sprintf(
		`{"feed_pk":%q,"max_users":%d,"max_future_users":%d,"current_users":%d,"anniversary_day":%d,"window_end":%d,"terminates_at":%d}`,
		feedPK, maxUsers, maxFutureUsers, currentUsers, anniversaryDay, windowEnd.Unix(), terminatesAt.Unix(),
	)
}

// seedSubscriptions writes three metros, four feeds and five access passes.
//
// Shreds feed seats, one row each on the page:
//
//	pass-1 payer-1  fra, live for another month, one user connected  → active
//	pass-1 payer-1  ams, same pass, second seat, nobody connected    → pending
//	pass-2 payer-2  fra, past its window but not yet terminated      → expired
//	pass-3 payer-3  ams, past termination, still on the pass         → expired
//
// pass-2 is the case that pins the status cut: its termination is still in the
// future, and that does not hold the term open.
//
// And three that must never appear: a kalshi seat (wrong product), a seat on a
// feed with no dimension row (unlabelled, so not provably shreds), and a deleted
// pass holding a live fra seat.
func seedSubscriptions(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, name, longitude, latitude)
		VALUES
		('metro-fra', now(), now(), generateUUIDv4(), 0, 1, 'metro-fra', 'fra', 'Frankfurt', 0, 0),
		('metro-ams', now(), now(), generateUUIDv4(), 0, 2, 'metro-ams', 'ams', 'Amsterdam', 0, 0)
	`))

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
		('feed-fra', now(), now(), generateUUIDv4(), 0, 1, 'feed-fra', 'owner', 'solana-shreds-full', 'solana-shreds-full-fra', 'metro-fra', ''),
		('feed-ams', now(), now(), generateUUIDv4(), 0, 2, 'feed-ams', 'owner', 'solana-shreds-full', 'solana-shreds-full-ams', 'metro-ams', ''),
		('feed-kal', now(), now(), generateUUIDv4(), 0, 3, 'feed-kal', 'owner', 'kalshi-sports-mbp', 'kalshi-sports-mbp-fra', 'metro-fra', '')
	`))

	now := time.Now().UTC()
	liveWindow := now.Add(30 * 24 * time.Hour)
	pastWindow := now.Add(-2 * 24 * time.Hour)
	futureTermination := now.Add(5 * 24 * time.Hour)
	pastTermination := now.Add(-1 * 24 * time.Hour)

	// pass-1 is written twice. The earlier snapshot is what "Since" reads; the
	// later one is the state every other column reports, and it adds the ams
	// seat, so the two snapshots must not both produce rows.
	firstSeen := "2026-08-01 00:00:00"
	insert := fmt.Sprintf(`
		INSERT INTO dim_dz_access_passes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, type_tag, associated_pubkey, others_type_name, others_key,
		 client_ip, user_payer, last_access_epoch, connection_count, status,
		 mgroup_pub_allowlist, mgroup_sub_allowlist, flags, feed_seats)
		VALUES
		('pass-1', '%[1]s', now(), generateUUIDv4(), 0, 1,
		 'pass-1', 'owner', 'edge_seat', '', '', '', '0.0.0.0', 'payer-1', 951, 1, 'connected', '', '', 0,
		 '[%[2]s]'),
		('pass-1', now(), now(), generateUUIDv4(), 0, 2,
		 'pass-1', 'owner', 'edge_seat', '', '', '', '0.0.0.0', 'payer-1', 951, 2, 'connected', '', '', 0,
		 '[%[2]s,%[3]s]'),
		('pass-2', now(), now(), generateUUIDv4(), 0, 3,
		 'pass-2', 'owner', 'edge_seat', '', '', '', '0.0.0.0', 'payer-2', 951, 1, 'connected', '', '', 0,
		 '[%[4]s]'),
		('pass-3', now(), now(), generateUUIDv4(), 0, 4,
		 'pass-3', 'owner', 'edge_seat', '', '', '', '0.0.0.0', 'payer-3', 951, 0, 'disconnected', '', '', 0,
		 '[%[5]s]'),
		('pass-4', now(), now(), generateUUIDv4(), 0, 5,
		 'pass-4', 'owner', 'edge_seat', '', '', '', '0.0.0.0', 'payer-4', 951, 1, 'connected', '', '', 0,
		 '[%[6]s,%[7]s]'),
		('pass-5', now(), now(), generateUUIDv4(), 1, 6,
		 'pass-5', 'owner', 'edge_seat', '', '', '', '0.0.0.0', 'payer-5', 951, 1, 'connected', '', '', 0,
		 '[%[2]s]')
	`,
		firstSeen,
		feedSeatJSON("feed-fra", 2, 2, 1, 15, liveWindow, liveWindow),
		feedSeatJSON("feed-ams", 1, 0, 0, 15, liveWindow, liveWindow),
		feedSeatJSON("feed-fra", 1, 1, 1, 3, pastWindow, futureTermination),
		feedSeatJSON("feed-ams", 1, 1, 0, 3, pastTermination, pastTermination),
		feedSeatJSON("feed-kal", 1, 1, 1, 3, liveWindow, liveWindow),
		feedSeatJSON("feed-gone", 1, 1, 1, 3, liveWindow, liveWindow),
	)
	require.NoError(t, api.DB.Exec(ctx, insert))
}

// seedSubscriptionUsers writes the devices and DZ user accounts the User and
// Device columns resolve through.
//
//	payer-1 fra  activated multicast on dev-fra, feed-fra   → shows
//	payer-1 ams  activated multicast on dev-ams, feed-ams   → shows
//	payer-1 fra  activated IBRL on dev-fra, no feed         → must not show
//	payer-2 fra  pending multicast on dev-fra, feed-fra     → must not show
func seedSubscriptionUsers(t *testing.T, api *handlers.API) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, metro_pk, status)
		VALUES
		('dev-fra', now(), now(), generateUUIDv4(), 0, 1, 'dev-fra', 'fra-dz01', 'metro-fra', 'activated'),
		('dev-ams', now(), now(), generateUUIDv4(), 0, 2, 'dev-ams', 'ams-dz01', 'metro-ams', 'activated')
	`))
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk,
		 tunnel_id, publishers, subscribers, bgp_status, feed_pks)
		VALUES
		('user-1', now(), now(), generateUUIDv4(), 0, 1, 'user-1', 'payer-1', 'activated', 'multicast',
		 '10.2.0.1', '10.2.0.1', 'dev-fra', '', 1, '', '', 'up', '["feed-fra"]'),
		('user-2', now(), now(), generateUUIDv4(), 0, 2, 'user-2', 'payer-1', 'activated', 'multicast',
		 '10.2.0.2', '10.2.0.2', 'dev-ams', '', 2, '', '', 'down', '["feed-ams"]'),
		('user-3', now(), now(), generateUUIDv4(), 0, 3, 'user-3', 'payer-1', 'activated', 'ibrl',
		 '10.2.0.3', '10.2.0.3', 'dev-fra', '', 3, '', '', 'up', '[]'),
		('user-4', now(), now(), generateUUIDv4(), 0, 4, 'user-4', 'payer-2', 'pending', 'multicast',
		 '10.2.0.4', '10.2.0.4', 'dev-fra', '', 4, '', '', 'up', '["feed-fra"]')
	`))
}

func fetchSubscriptions(t *testing.T, api *handlers.API, query string) handlers.PaginatedResponse[handlers.ShredSubscriptionItem] {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/subscriptions"+query, nil)
	rr := httptest.NewRecorder()
	api.GetShredSubscriptions(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var response handlers.PaginatedResponse[handlers.ShredSubscriptionItem]
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	return response
}

func findSubscription(items []handlers.ShredSubscriptionItem, passPK, feedPK string) *handlers.ShredSubscriptionItem {
	for i := range items {
		if items[i].PassPK == passPK && items[i].FeedPK == feedPK {
			return &items[i]
		}
	}
	return nil
}

func TestGetShredSubscriptions_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	response := fetchSubscriptions(t, api, "")
	assert.Empty(t, response.Items)
	assert.Equal(t, 0, response.Total)
}

// One row per (pass, feed), with the feed and metro joins resolved and every
// figure taken from the seat entry rather than from the pass.
func TestGetShredSubscriptions_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)

	response := fetchSubscriptions(t, api, "")
	require.Equal(t, 4, response.Total)
	require.Len(t, response.Items, 4)

	fra := findSubscription(response.Items, "pass-1", "feed-fra")
	require.NotNil(t, fra)
	assert.Equal(t, "payer-1", fra.Payer)
	assert.Equal(t, "connected", fra.PassStatus)
	assert.Equal(t, "solana-shreds-full", fra.FeedCode)
	assert.Equal(t, "solana-shreds-full-fra", fra.FeedName)
	assert.Equal(t, "metro-fra", fra.MetroPK)
	assert.Equal(t, "fra", fra.MetroCode)
	assert.Equal(t, uint8(2), fra.MaxUsers)
	assert.Equal(t, uint8(1), fra.CurrentUsers)
	assert.Equal(t, uint8(15), fra.AnniversaryDay)

	// The second seat comes from the same pass and carries its own caps, which
	// is the whole reason the row is the seat and not the pass.
	ams := findSubscription(response.Items, "pass-1", "feed-ams")
	require.NotNil(t, ams)
	assert.Equal(t, "ams", ams.MetroCode)
	assert.Equal(t, uint8(1), ams.MaxUsers)
	assert.Equal(t, uint8(0), ams.MaxFutureUsers)
	assert.Equal(t, uint8(0), ams.CurrentUsers)
}

// The page is scoped to the shreds feeds, and a feed the dimension has no row
// for cannot be shown to be one. A deleted pass is gone whatever it held.
func TestGetShredSubscriptions_ExcludesOtherProductsAndDeletedPasses(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)

	response := fetchSubscriptions(t, api, "")
	for _, item := range response.Items {
		assert.Equal(t, "solana-shreds-full", item.FeedCode, "pass %s", item.PassPK)
		assert.NotEqual(t, "pass-4", item.PassPK, "kalshi and unlabelled seats are not shreds subscriptions")
		assert.NotEqual(t, "pass-5", item.PassPK, "a deleted pass holds nothing")
	}
}

// The three states partition the seats, so the chips add back up to the
// unfiltered total — no seat is counted twice by two chips, and none falls
// through all three.
func TestGetShredSubscriptions_StatusFilter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)

	// pass-1's fra seat has a user connected; its ams seat does not, so the two
	// split across active and pending. pass-2 and pass-3 are both past their term
	// date, which is the only thing expired asks about — pass-2's termination is
	// still in the future and that does not hold it open.
	tests := []struct {
		status string
		want   int
	}{
		{"active", 1},  // pass-1 fra: inside its window, one user connected
		{"pending", 1}, // pass-1 ams: inside its window, nobody connected
		{"expired", 2}, // pass-2 and pass-3: both past window_end
		{"active,pending", 2},
		{"active,pending,expired", 4},
		{"", 4},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			query := ""
			if tt.status != "" {
				query = "?status=" + tt.status
			}
			assert.Equal(t, tt.want, fetchSubscriptions(t, api, query).Total)
		})
	}
}

// A status filter naming nothing this build serves matches nothing, rather than
// falling through to every row. A browser one deploy ahead of the API asks for a
// state the API has not got, and answering a narrowing request with the whole
// table puts that number under a single chip.
func TestGetShredSubscriptions_UnknownStatusMatchesNothing(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)

	require.Equal(t, 4, fetchSubscriptions(t, api, "").Total, "no status asked for returns every seat")
	assert.Equal(t, 0, fetchSubscriptions(t, api, "?status=nonesuch").Total)
	assert.Equal(t, 0, fetchSubscriptions(t, api, "?status=expiring").Total, "a state this build dropped")

	// A recognized state alongside an unrecognized one still returns its own.
	assert.Equal(t, 1, fetchSubscriptions(t, api, "?status=active,nonesuch").Total)
}

// "Since" is the first snapshot the seat was seen on its pass, not the snapshot
// every other column is read from. Without that the column would report the
// last time anything on the pass changed.
func TestGetShredSubscriptions_StartedAtIsFirstObservation(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)

	response := fetchSubscriptions(t, api, "")
	fra := findSubscription(response.Items, "pass-1", "feed-fra")
	require.NotNil(t, fra)

	started, err := time.Parse(time.RFC3339, fra.StartedAt)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), started)

	// The ams seat was added on the later snapshot, so it starts there.
	ams := findSubscription(response.Items, "pass-1", "feed-ams")
	require.NotNil(t, ams)
	amsStarted, err := time.Parse(time.RFC3339, ams.StartedAt)
	require.NoError(t, err)
	assert.True(t, amsStarted.After(started), "the second seat started after the first")
}

// The User and Device columns resolve through dz_users_current.feed_pks, which
// names the feeds an account is attached to. Two accounts must stay out: an IBRL
// user of the same payer on the same device, which is a unicast tunnel and not a
// feed consumer, and a user that has not activated.
func TestGetShredSubscriptions_ResolvesConnectedUsersAndDevices(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)
	seedSubscriptionUsers(t, api)

	response := fetchSubscriptions(t, api, "")

	fra := findSubscription(response.Items, "pass-1", "feed-fra")
	require.NotNil(t, fra)
	require.Len(t, fra.Users, 1, "the IBRL user on the same device is not a feed consumer")
	assert.Equal(t, "user-1", fra.Users[0].PK)
	assert.Equal(t, "dev-fra", fra.Users[0].DevicePK)
	assert.Equal(t, "fra-dz01", fra.Users[0].DeviceCode)
	assert.Equal(t, "up", fra.Users[0].BGPStatus)

	// A session reading down does not stop the account being attached: the
	// column shows the user and marks the status beside it.
	ams := findSubscription(response.Items, "pass-1", "feed-ams")
	require.NotNil(t, ams)
	require.Len(t, ams.Users, 1)
	assert.Equal(t, "user-2", ams.Users[0].PK)
	assert.Equal(t, "ams-dz01", ams.Users[0].DeviceCode)
	assert.Equal(t, "down", ams.Users[0].BGPStatus)

	// pass-2 holds a fra seat whose only candidate user has not activated.
	pending := findSubscription(response.Items, "pass-2", "feed-fra")
	require.NotNil(t, pending)
	assert.Empty(t, pending.Users, "a pending account is not connected")
}

// device: and user: match against every user on the seat, and a seat nobody has
// connected to matches neither.
func TestGetShredSubscriptions_DeviceAndUserFilters(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)
	seedSubscriptionUsers(t, api)

	byDevice := fetchSubscriptions(t, api, "?filters=device:fra-dz01")
	require.Equal(t, 1, byDevice.Total)
	assert.Equal(t, "pass-1", byDevice.Items[0].PassPK)
	assert.Equal(t, "feed-fra", byDevice.Items[0].FeedPK)

	byUser := fetchSubscriptions(t, api, "?filters=user:user-2")
	require.Equal(t, 1, byUser.Total)
	assert.Equal(t, "feed-ams", byUser.Items[0].FeedPK)

	// The IBRL user is on dev-fra too, but it reaches no seat through a feed.
	assert.Equal(t, 0, fetchSubscriptions(t, api, "?filters=user:user-3").Total)
	assert.Equal(t, 0, fetchSubscriptions(t, api, "?filters=device:nope").Total)
}

func TestGetShredSubscriptions_FiltersAndSort(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedSubscriptions(t, api)

	byPayer := fetchSubscriptions(t, api, "?filters=payer:payer-1")
	assert.Equal(t, 2, byPayer.Total)

	byMetro := fetchSubscriptions(t, api, "?filters=metro:ams")
	require.Equal(t, 2, byMetro.Total)
	for _, item := range byMetro.Items {
		assert.Equal(t, "ams", item.MetroCode)
	}

	// The feed filter reads the per-metro name, which is what distinguishes one
	// shreds feed from another — every one of them shares a code.
	byFeed := fetchSubscriptions(t, api, "?filters=feed:solana-shreds-full-fra")
	assert.Equal(t, 2, byFeed.Total)

	// current_users is numeric, so it takes an operator.
	idle := fetchSubscriptions(t, api, "?filters=users:=0")
	require.Equal(t, 2, idle.Total)
	for _, item := range idle.Items {
		assert.Equal(t, uint8(0), item.CurrentUsers)
	}

	sorted := fetchSubscriptions(t, api, "?sort_by=payer&sort_dir=asc")
	require.Len(t, sorted.Items, 4)
	assert.Equal(t, "payer-1", sorted.Items[0].Payer)
	assert.Equal(t, "payer-3", sorted.Items[len(sorted.Items)-1].Payer)
}
