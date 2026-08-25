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

// insertLevelAt inserts one market-by-price wire message from a named vantage, `daysAgo` days
// back. The day is what this view groups by, and the vantage is the axis it must not double
// count, so both are parameters here where insertLevel (coverage tests) fixes them.
func insertLevelAt(t *testing.T, api *handlers.API, node, source string, channelID, instrumentID uint32, msgType, statusAfter string, daysAgo int) {
	t.Helper()
	insertLevelAtHour(t, api, node, source, channelID, instrumentID, msgType, statusAfter, daysAgo, 12)
}

// insertLevelAtHour also fixes the hour, which the span tests need: whether one recorder's
// record covers a book's whole life is a question about where its first and last rows sit.
func insertLevelAtHour(t *testing.T, api *handlers.API, node, source string, channelID, instrumentID uint32, msgType, statusAfter string, daysAgo, hour int) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.kalshi_mbp_levels
		(capture_run_id, measurement_node_id, host, location_code, source, symbol, channel_id,
		 instrument_id, source_id, msg_type, source_ts_ns, source_ts_kind, recv_ts_ns,
		 recv_ts_kind, book_levels_after, status_after)
		VALUES ('run1', '%s', '%s', 'cmh', '%s', 'KXNFLGAME', %d, %d, 3, '%s', 0, 'venue',
		        toUInt64(toUnixTimestamp64Nano(toDateTime64(toStartOfDay(now64(9)) - toIntervalDay(%d) + toIntervalHour(%d), 9))),
		        'kernel_udp_software', 4, '%s')
	`, db, node, node, source, channelID, instrumentID, msgType, daysAgo, hour, statusAfter)))
}

func dayOf(t *testing.T, resp handlers.KalshiL2CompletenessResponse, i int) handlers.KalshiL2Day {
	t.Helper()
	require.Greater(t, len(resp.Days), i, "expected at least %d day(s)", i+1)
	return resp.Days[i]
}

func getCompleteness(t *testing.T, api *handlers.API) handlers.KalshiL2CompletenessResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/dz/kalshi/l2-completeness", nil)
	rr := httptest.NewRecorder()
	api.GetKalshiL2Completeness(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp handlers.KalshiL2CompletenessResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	return resp
}

// An absent proxy table must degrade to an empty 200. That is local dev and any environment
// whose ClickHouse has no capture tables; prod has recorded levels since 2026-08-18.
func TestKalshiL2Completeness_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)

	resp := getCompleteness(t, api)
	assert.Empty(t, resp.Days)
}

// A day whose books all stayed anchored and all carried a snapshot is clean: both fault counts
// are zero and nothing is named as a gap lane.
func TestKalshiL2Completeness_CleanDay(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "level_update", "ready", 0)

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 1, day.Lanes)
	assert.EqualValues(t, 1, day.Instruments)
	assert.EqualValues(t, 2, day.Messages)
	assert.EqualValues(t, 0, day.GappedInstruments)
	assert.EqualValues(t, 0, day.UnanchoredInstruments)
	assert.Empty(t, day.GapLanes)
}

// A book that ran un-anchored, and a second book with no snapshot in the day, are the two
// reasons a day is not sellable. They are counted separately because they need different
// answers: one is loss, the other is a replay that cannot start inside the day.
func TestKalshiL2Completeness_GapsAndMissingSnapshots(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// Book 100: gapped, but re-anchored inside the day.
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "level_update", "gap", 0)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
	// Book 200: never snapshotted in this day.
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 200, "level_update", "ready", 0)

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 2, day.Instruments)
	assert.EqualValues(t, 1, day.GappedInstruments)
	assert.EqualValues(t, 1, day.UnanchoredInstruments)
	assert.Equal(t, []string{"Perpetual Futures"}, day.GapLanes)
}

// Two recorders of one lane are two observations of one publisher, not twice the coverage. The
// vantage roll-up takes the widest, so the day must report the instruments once.
func TestKalshiL2Completeness_VantagesAreNotSummed(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	for _, node := range []string{"cmh-rec1", "cmh-rec2"} {
		insertLevelAt(t, api, node, "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
		insertLevelAt(t, api, node, "mbp_edge_kalshi_perps", 1, 200, "snapshot_end", "ready", 0)
	}

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 1, day.Lanes)
	assert.EqualValues(t, 2, day.Instruments)
	assert.EqualValues(t, 2, day.Messages)
}

// Different channels carry different instrument sets, so those DO sum. This is the axis the
// vantage rule must not be applied to.
func TestKalshiL2Completeness_ChannelsAreSummed(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 2, 100, "snapshot_end", "ready", 0)

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 2, day.Lanes)
	assert.EqualValues(t, 2, day.Instruments)
}

// Days are separate rows, newest first, and a day outside the window is not reported.
func TestKalshiL2Completeness_DayOrderAndWindow(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 2)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 90)

	resp := getCompleteness(t, api)
	require.Len(t, resp.Days, 2)
	assert.Greater(t, resp.Days[0].Day, resp.Days[1].Day)
}

// A lossy redundant recorder must not mark a lane incomplete. Book 100 gapped on rec2 and
// stayed anchored on rec1, and book 200 was snapshotted only by rec2 — a replay takes each book
// from whichever recorder held it, so the day is clean. Collapsing the fault counts at the lane
// grain instead of per book would report 1 gapped and 1 unanchored here and call a sellable day
// unsellable.
func TestKalshiL2Completeness_LossyVantageDoesNotFailTheLane(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
	insertLevelAt(t, api, "cmh-rec2", "mbp_edge_kalshi_perps", 1, 100, "level_update", "gap", 0)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 200, "level_update", "ready", 0)
	insertLevelAt(t, api, "cmh-rec2", "mbp_edge_kalshi_perps", 1, 200, "snapshot_end", "ready", 0)

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 2, day.Instruments)
	assert.EqualValues(t, 0, day.GappedInstruments)
	assert.EqualValues(t, 0, day.UnanchoredInstruments)
	assert.Empty(t, day.GapLanes)
}

// The other direction: a book no recorder held is counted once, not once per recorder.
func TestKalshiL2Completeness_BookGappedInEveryVantageCountsOnce(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	for _, node := range []string{"cmh-rec1", "cmh-rec2"} {
		insertLevelAt(t, api, node, "mbp_edge_kalshi_perps", 1, 100, "level_update", "gap", 0)
	}

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 1, day.Instruments)
	assert.EqualValues(t, 1, day.GappedInstruments)
	assert.EqualValues(t, 1, day.UnanchoredInstruments)
	assert.Equal(t, []string{"Perpetual Futures"}, day.GapLanes)
}

// A recorder that was only up for part of the day must not clear another recorder's gap. rec1
// gapped book 100 across the whole day; rec2 holds it cleanly, but only from 18:00, so it has
// no record of the hours rec1 marked. Nothing here holds the book end to end, and taking rec2's
// clean word for the day would sell a hole.
func TestKalshiL2Completeness_PartialVantageDoesNotClearAGap(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAtHour(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0, 1)
	insertLevelAtHour(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "level_update", "gap", 0, 20)
	insertLevelAtHour(t, api, "cmh-rec2", "mbp_edge_kalshi_perps", 1, 100, "level_update", "ready", 0, 18)
	insertLevelAtHour(t, api, "cmh-rec2", "mbp_edge_kalshi_perps", 1, 100, "level_update", "ready", 0, 20)

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 1, day.Instruments)
	assert.EqualValues(t, 1, day.GappedInstruments)
	assert.Equal(t, []string{"Perpetual Futures"}, day.GapLanes)
}

// The same shape with the clean recorder covering the whole book reads clean. rec2 spans rec1's
// first and last rows, so it holds a continuous record of the book and the gap is filled.
func TestKalshiL2Completeness_CoveringVantageClearsAGap(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAtHour(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0, 1)
	insertLevelAtHour(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "level_update", "gap", 0, 20)
	insertLevelAtHour(t, api, "cmh-rec2", "mbp_edge_kalshi_perps", 1, 100, "level_update", "ready", 0, 1)
	insertLevelAtHour(t, api, "cmh-rec2", "mbp_edge_kalshi_perps", 1, 100, "level_update", "ready", 0, 20)

	day := dayOf(t, getCompleteness(t, api), 0)
	assert.EqualValues(t, 1, day.Instruments)
	assert.EqualValues(t, 0, day.GappedInstruments)
	assert.Empty(t, day.GapLanes)
}

// The scan width is a parameter so one query serves both the full and the partial refresh. A
// three-day scan must not return a day outside it, and it still reports the view's window rather
// than its own width — the partial payload is an argument to the merge, not an answer.
func TestKalshiL2Completeness_ScanWidthBoundsTheDays(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 0)
	insertLevelAt(t, api, "cmh-rec1", "mbp_edge_kalshi_perps", 1, 100, "snapshot_end", "ready", 5)

	full, err := api.FetchKalshiL2Completeness(t.Context(), 7)
	require.NoError(t, err)
	require.Len(t, full.Days, 2)

	partial, err := api.FetchKalshiL2Completeness(t.Context(), 3)
	require.NoError(t, err)
	require.Len(t, partial.Days, 1, "the day five back is outside a three-day scan")
	assert.Equal(t, full.DayCount, partial.DayCount, "DayCount is the view's window, not the scan's")
}
