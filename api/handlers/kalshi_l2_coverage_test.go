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

// createKalshiMbpLevelsTable creates kalshi_mbp_levels with the columns the coverage query
// reads. The real table (the capture's 20260805000001 migration) is much wider; the payload
// columns it omits here are all Nullable and unread by this handler.
func createKalshiMbpLevelsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.kalshi_mbp_levels (
			capture_run_id String,
			measurement_node_id String,
			host String,
			location_code LowCardinality(String),
			source LowCardinality(String),
			symbol LowCardinality(String),
			channel_id UInt8,
			instrument_id UInt32,
			source_id UInt16,
			msg_type LowCardinality(String),
			source_ts_ns UInt64,
			source_ts_kind LowCardinality(String),
			recv_ts_ns UInt64,
			recv_ts_kind LowCardinality(String),
			book_levels_after UInt32,
			status_after LowCardinality(String)
		) ENGINE = MergeTree
		PARTITION BY toDate(fromUnixTimestamp64Nano(toInt64(recv_ts_ns)))
		ORDER BY (measurement_node_id, source, channel_id, symbol, instrument_id, recv_ts_ns)
	`, db)))
}

// insertLevel inserts one market-by-price wire message, recorded `agoSecs` ago.
func insertLevel(t *testing.T, api *handlers.API, source string, channelID, instrumentID uint32, msgType string, depth uint32, statusAfter string, agoSecs int) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.kalshi_mbp_levels
		(capture_run_id, measurement_node_id, host, location_code, source, symbol, channel_id,
		 instrument_id, source_id, msg_type, source_ts_ns, source_ts_kind, recv_ts_ns,
		 recv_ts_kind, book_levels_after, status_after)
		VALUES ('run1', 'cmh-rec1', 'cmh-rec1', 'cmh', '%s', 'KXNFLGAME', %d, %d, 3, '%s', 0,
		        'venue', toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalSecond(%d))),
		        'kernel_udp_software', %d, '%s')
	`, db, source, channelID, instrumentID, msgType, agoSecs, depth, statusAfter)))
}

func TestGetKalshiL2Coverage_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	// Do NOT create the table -> handler must degrade to empty 200, not 500.

	req := httptest.NewRequest(http.MethodGet, "/api/dz/kalshi/l2-coverage", nil)
	rr := httptest.NewRecorder()
	api.GetKalshiL2Coverage(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.KalshiL2CoverageResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Lanes)
}

func TestKalshiL2Coverage_Empty(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.Lanes)
	assert.Equal(t, 15, resp.WindowMinutes)
}

func TestKalshiL2Coverage_LaneStats(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// Three level updates with depths 4, 6, 8, plus one reset, one clear, one snapshot_end,
	// and one gap-flagged message.
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "level_update", 4, "ready", 10)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "level_update", 6, "ready", 9)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "level_update", 8, "ready", 8)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "instrument_reset", 0, "awaiting_snapshot", 7)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "book_clear", 0, "ready", 6)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "snapshot_end", 8, "ready", 5)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "level_update", 8, "gap", 4)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Lanes, 1)

	l := resp.Lanes[0]
	assert.Equal(t, "mbp_sports_nfl", l.Source)
	assert.Equal(t, "NFL", l.Label)
	assert.Equal(t, "Football", l.Category)
	assert.Equal(t, "cmh", l.LocationCode)
	assert.EqualValues(t, 1, l.Instruments)
	assert.EqualValues(t, 1, l.Resets)
	assert.EqualValues(t, 1, l.Clears)
	assert.EqualValues(t, 1, l.SnapshotCycles)
	assert.EqualValues(t, 1, l.Gaps)

	// 7 messages / 4 of them level updates, over a 15-minute (900s) window.
	assert.InDelta(t, 7.0/900.0, l.MessagesPerSec, 1e-9)
	assert.InDelta(t, 4.0/900.0, l.LevelUpdatesPerSec, 1e-9)

	// Depth stats come only from level-bearing messages (4, 6, 8, 8) — the reset and clear
	// carry depth 0 and must not drag the median down.
	assert.InDelta(t, 8.0, l.DepthMax, 0.001)
	assert.GreaterOrEqual(t, l.DepthP50, 6.0)
}

// instrument_id is unique only WITHIN a channel_id, and prod's two publisher arms share a
// multicast group and differ only by channel. Collapsing them would merge two independent
// delta streams and undercount instruments.
func TestKalshiL2Coverage_SeparatesChannels(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// The same instrument_id on two channels of one source: two lanes, one instrument each.
	insertLevel(t, api, "mbp_lashay_2", 1, 7, "level_update", 5, "ready", 10)
	insertLevel(t, api, "mbp_lashay_2", 101, 7, "level_update", 5, "ready", 10)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Lanes, 2, "the two arms must be reported separately")

	for _, l := range resp.Lanes {
		assert.Equal(t, "mbp_lashay_2", l.Source)
		assert.EqualValues(t, 1, l.Instruments)
	}
	// Sorted by channel id within a lane.
	assert.EqualValues(t, 1, resp.Lanes[0].ChannelID)
	assert.EqualValues(t, 101, resp.Lanes[1].ChannelID)
}

// A lane the publisher adds without a matching entry in kalshiL2Lanes must still be reported —
// a page that hides unknown lanes under-reports exactly when someone is checking a new one.
func TestKalshiL2Coverage_ReportsUnknownLane(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevel(t, api, "mbp_sports_pickleball", 1, 1, "level_update", 3, "ready", 10)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Lanes, 1)
	assert.Equal(t, "mbp_sports_pickleball", resp.Lanes[0].Label, "unknown lanes fall back to the raw source id")
	assert.Equal(t, "Other", resp.Lanes[0].Category)
}

// Rows older than the coverage window must not contribute; the window is what keeps this scan
// bounded over a table with no TTL.
func TestKalshiL2Coverage_ExcludesOutsideWindow(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "level_update", 5, "ready", 10)
	insertLevel(t, api, "mbp_sports_nfl", 1, 100, "level_update", 5, "ready", 60*60) // an hour ago

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Lanes, 1)
	assert.InDelta(t, 1.0/900.0, resp.Lanes[0].LevelUpdatesPerSec, 1e-9)
}
