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
)

// An empty table returns an empty JSON array, not null. The page hides its
// section on a zero-length array, and `null` would break that check.
func TestGetShredFeedRevenue_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/feed-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredFeedRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.JSONEq(t, `[]`, rr.Body.String())
}

// Rows carry the feed's label from dz_feeds_current, convert base units to
// dollars, and sort newest month first then largest collected first.
func TestGetShredFeedRevenue_JoinsLabelAndSorts(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_feed_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, feed_key, year, month, collected_usdc)
		VALUES
		('fd-a-09', now(), now(), generateUUIDv4(), 0, 1, 'fd-a-09', 'feed-a', 2026, 9, 2419354841),
		('fd-b-09', now(), now(), generateUUIDv4(), 0, 2, 'fd-b-09', 'feed-b', 2026, 9,  464516130),
		('fd-a-08', now(), now(), generateUUIDv4(), 0, 3, 'fd-a-08', 'feed-a', 2026, 8, 2080645159)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
		('feed-a', now(), now(), generateUUIDv4(), 0, 1, 'feed-a', 'owner-a', 'xlax', 'LA feed', 'metro-1', '')
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/feed-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredFeedRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var items []handlers.ShredFeedRevenueItem
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	require.Len(t, items, 3)

	// Newest month first, largest collected first within the month.
	assert.Equal(t, "feed-a", items[0].FeedKey)
	assert.Equal(t, uint8(9), items[0].Month)
	assert.Equal(t, "xlax", items[0].Code)
	assert.Equal(t, "LA feed", items[0].Name)
	assert.InDelta(t, 2419.354841, items[0].CollectedDollars, 1e-9)

	assert.Equal(t, "feed-b", items[1].FeedKey)
	assert.Equal(t, uint8(9), items[1].Month)

	assert.Equal(t, uint8(8), items[2].Month)
	assert.Equal(t, uint16(2026), items[2].Year)
}

// code_prefix keeps another feed product out of the shreds page's totals. The
// feed-subscription program is shared by every DoubleZero feed, so an
// unfiltered read folds kalshi revenue into a shreds tile. An unlabelled feed
// survives the filter: dropping it would hide revenue behind a late label.
func TestGetShredFeedRevenue_FiltersByCodePrefix(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_feed_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, feed_key, year, month, collected_usdc)
		VALUES
		('fd-shreds', now(), now(), generateUUIDv4(), 0, 1, 'fd-shreds', 'feed-shreds', 2026, 8, 4500000000),
		('fd-kalshi', now(), now(), generateUUIDv4(), 0, 2, 'fd-kalshi', 'feed-kalshi', 2026, 8,    1290322),
		('fd-orphan', now(), now(), generateUUIDv4(), 0, 3, 'fd-orphan', 'feed-orphan', 2026, 8,    9000000)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
		('feed-shreds', now(), now(), generateUUIDv4(), 0, 1, 'feed-shreds', 'owner-a', 'solana-shreds-full', 'solana-shreds-full-ams', 'metro-1', ''),
		('feed-kalshi', now(), now(), generateUUIDv4(), 0, 2, 'feed-kalshi', 'owner-b', 'kalshi-sports-mbp', 'kalshi-sports-mbp-ams', 'metro-1', '')
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/feed-revenue?code_prefix="+handlers.ShredsFeedCodePrefix, nil)
	rr := httptest.NewRecorder()
	api.GetShredFeedRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var items []handlers.ShredFeedRevenueItem
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))

	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.FeedKey)
	}
	assert.ElementsMatch(t, []string{"feed-shreds", "feed-orphan"}, keys)

	// No prefix still returns every product, so the endpoint stays usable for a
	// feed-wide view.
	req = httptest.NewRequest(http.MethodGet, "/api/dz/shreds/feed-revenue", nil)
	rr = httptest.NewRecorder()
	api.GetShredFeedRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	items = nil
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	assert.Len(t, items, 3)
}

// A database without the dimension answers 200 with an empty array instead of
// 500. The migration ships with the indexer, so an API pod rolled out ahead of
// it queries a table that is not there yet; that window is a deploy race, and a
// 500 would log at ERROR on every page load and page on-call.
func TestGetShredFeedRevenue_MissingTableIsEmptyNotError(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/feed-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredFeedRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.JSONEq(t, `[]`, rr.Body.String())
}

// A feed with no row in dz_feeds_current still appears, with empty label
// fields. A serviceability snapshot that has not caught up must not hide
// revenue that was really collected.
func TestGetShredFeedRevenue_KeepsUnlabelledFeed(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_feed_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, feed_key, year, month, collected_usdc)
		VALUES
		('fd-orphan', now(), now(), generateUUIDv4(), 0, 1, 'fd-orphan', 'feed-unknown', 2026, 9, 1000000)
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/feed-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredFeedRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var items []handlers.ShredFeedRevenueItem
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	require.Len(t, items, 1)
	assert.Equal(t, "feed-unknown", items[0].FeedKey)
	assert.Equal(t, "", items[0].Code)
	assert.InDelta(t, 1.0, items[0].CollectedDollars, 1e-9)
}
