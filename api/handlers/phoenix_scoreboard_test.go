package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertPhoenixWindow writes one rollup row the way the indexer does, with the t-digest state
// built from the given signed leads.
func insertPhoenixWindow(t *testing.T, api *handlers.API, start time.Time, dz, venue uint64, leads []float64) {
	t.Helper()
	arr := "["
	for i, l := range leads {
		if i > 0 {
			arr += ","
		}
		arr += fmt.Sprintf("%g", l)
	}
	arr += "]"
	err := api.DB.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO phoenix_race_rollup_15m
		SELECT
			toDateTime(%[1]d, 'UTC'), now64(3), %[2]d, %[3]d, %[4]d,
			ifNotFinite(q[1], 0), ifNotFinite(q[2], 0), ifNotFinite(q[3], 0), s
		FROM (
			SELECT
				quantilesTDigestState(0.5, 0.95, 0.99)(x) AS s,
				finalizeAggregation(s) AS q
			FROM (SELECT arrayJoin(CAST(%[5]s AS Array(Float64))) AS x)
		)`, start.Unix(), len(leads), dz, venue, arr))
	require.NoError(t, err)
}

func getPhoenixScoreboard(t *testing.T, api *handlers.API) handlers.PhoenixScoreboardResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/dz/phoenix/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetPhoenixScoreboard(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp handlers.PhoenixScoreboardResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return resp
}

func TestGetPhoenixScoreboard_EmptyTable(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	resp := getPhoenixScoreboard(t, api)
	assert.Zero(t, resp.Races)
	assert.NotNil(t, resp.Buckets, "an empty board still carries an array")
	assert.Empty(t, resp.Buckets)
	assert.Nil(t, resp.AsOf, "nothing written yet")
}

func TestGetPhoenixScoreboard_AnIdleDayStillReportsFreshness(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	newest := time.Date(2026, 9, 28, 19, 45, 0, 0, time.UTC)
	insertPhoenixWindow(t, api, newest.Add(-15*time.Minute), 0, 0, nil)
	insertPhoenixWindow(t, api, newest, 0, 0, nil)

	resp := getPhoenixScoreboard(t, api)
	assert.Zero(t, resp.Races)
	assert.Empty(t, resp.Buckets)
	require.NotNil(t, resp.AsOf, "a quiet feed must not read as a dead rollup")
	require.NotNil(t, resp.NextRefreshAt)
}

func TestGetPhoenixScoreboard_TotalsAndBucketsShareOneWindow(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	newest := time.Date(2026, 9, 28, 19, 45, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		insertPhoenixWindow(t, api, newest.Add(-time.Duration(i)*15*time.Minute), 2, 0, []float64{100, 200})
	}

	resp := getPhoenixScoreboard(t, api)
	require.Len(t, resp.Buckets, 96)
	var races uint64
	for _, b := range resp.Buckets {
		races += b.Races
	}
	assert.Equal(t, resp.Races, races, "the headline counts exactly the windows the chart draws")
	assert.Equal(t, resp.Buckets[0].Start, resp.WindowStart)
}

func TestGetPhoenixScoreboard_TotalsMergeTheWindows(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	newest := time.Date(2026, 9, 28, 19, 45, 0, 0, time.UTC)
	insertPhoenixWindow(t, api, newest.Add(-15*time.Minute), 5, 0, []float64{100, 150, 200, 250, 300})
	insertPhoenixWindow(t, api, newest, 1, 0, []float64{1000})
	insertPhoenixWindow(t, api, newest.Add(-30*time.Minute), 0, 0, nil)
	insertPhoenixWindow(t, api, newest.Add(-24*time.Hour), 0, 1, []float64{-40})

	resp := getPhoenixScoreboard(t, api)

	assert.EqualValues(t, 6, resp.Races)
	assert.EqualValues(t, 6, resp.DZWins)
	assert.Zero(t, resp.VenueWins, "the venue's win is outside the window")
	assert.InDelta(t, 100, resp.All.WinPct, 1e-9)
	assert.InDelta(t, 200, resp.All.P50Ms, 60, "the merged median, not a mean of window medians")

	require.Len(t, resp.Buckets, 2, "the idle window is dropped")
	assert.Equal(t, newest.Add(-15*time.Minute), resp.Buckets[0].Start)
	assert.EqualValues(t, 5, resp.Buckets[0].Races)
	assert.Equal(t, newest, resp.Buckets[1].Start)
	assert.Equal(t, newest.Add(15*time.Minute), resp.WindowEnd)
	require.NotNil(t, resp.AsOf)
	require.NotNil(t, resp.NextRefreshAt)
	assert.True(t, resp.NextRefreshAt.After(*resp.AsOf))
}

func TestGetPhoenixScoreboard_IdleWritesAdvanceAsOf(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	newest := time.Date(2026, 9, 28, 19, 45, 0, 0, time.UTC)
	insertPhoenixWindow(t, api, newest.Add(-15*time.Minute), 3, 0, []float64{100, 200, 300})
	before := getPhoenixScoreboard(t, api).AsOf
	require.NotNil(t, before)

	time.Sleep(20 * time.Millisecond)
	insertPhoenixWindow(t, api, newest, 0, 0, nil)

	resp := getPhoenixScoreboard(t, api)
	require.NotNil(t, resp.AsOf)
	assert.True(t, resp.AsOf.After(*before), "an idle window is still a refresh")
	assert.Len(t, resp.Buckets, 1)
}

func TestGetPhoenixScoreboard_AQuietTailStaysInTheWindow(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	newest := time.Date(2026, 9, 28, 19, 45, 0, 0, time.UTC)
	insertPhoenixWindow(t, api, newest.Add(-6*time.Hour), 2, 0, []float64{100, 200})
	insertPhoenixWindow(t, api, newest, 0, 0, nil)

	resp := getPhoenixScoreboard(t, api)
	require.Len(t, resp.Buckets, 1)
	assert.Equal(t, newest.Add(15*time.Minute), resp.WindowEnd, "the chart runs to the newest window, raced or not")
	assert.Equal(t, newest.Add(15*time.Minute-24*time.Hour), resp.WindowStart)
}
