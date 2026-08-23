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
			status_after LowCardinality(String),
			publisher_source_ip LowCardinality(String)
		) ENGINE = MergeTree
		PARTITION BY toDate(fromUnixTimestamp64Nano(toInt64(recv_ts_ns)))
		ORDER BY (measurement_node_id, source, channel_id, symbol, instrument_id, recv_ts_ns)
	`, db)))
}

// insertLevel inserts one market-by-price wire message, recorded `agoSecs` ago, from the default
// publisher address.
func insertLevel(t *testing.T, api *handlers.API, source string, channelID, instrumentID uint32, msgType string, depth uint32, statusAfter string, agoSecs int) {
	t.Helper()
	insertLevelFrom(t, api, "148.51.121.69", source, channelID, instrumentID, msgType, depth, statusAfter, agoSecs)
}

// insertLevelFrom is insertLevel with the publishing address spelled out. The address is what makes
// a row a channel instance rather than a channel, so every test that cares about two publishers on
// one channel goes through this.
func insertLevelFrom(t *testing.T, api *handlers.API, publisherSourceIP, source string, channelID, instrumentID uint32, msgType string, depth uint32, statusAfter string, agoSecs int) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.kalshi_mbp_levels
		(capture_run_id, measurement_node_id, host, location_code, source, symbol, channel_id,
		 instrument_id, source_id, msg_type, source_ts_ns, source_ts_kind, recv_ts_ns,
		 recv_ts_kind, book_levels_after, status_after, publisher_source_ip)
		VALUES ('run1', 'cmh-rec1', 'cmh-rec1', 'cmh', '%s', 'KXNFLGAME', %d, %d, 3, '%s', 0,
		        'venue', toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalSecond(%d))),
		        'kernel_udp_software', %d, '%s', '%s')
	`, db, source, channelID, instrumentID, msgType, agoSecs, depth, statusAfter, publisherSourceIP)))
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
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 4, "ready", 10)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 6, "ready", 9)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 8, "ready", 8)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "instrument_reset", 0, "awaiting_snapshot", 7)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "book_clear", 0, "ready", 6)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "snapshot_end", 8, "ready", 5)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 8, "gap", 4)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)

	// The response also carries the silent lanes of the configured roster; select the one
	// this test published to.
	l := lanesBySource(resp.Lanes)["mbp_edge_kalshi_sports_nfl"]
	assert.True(t, l.Seen)
	assert.Equal(t, "mbp_edge_kalshi_sports_nfl", l.Source)
	assert.Equal(t, "NFL", l.Label)
	assert.Equal(t, "Football", l.Category)
	assert.Equal(t, "cmh", l.LocationCode)
	assert.EqualValues(t, 1, l.Instruments)
	assert.EqualValues(t, 1, l.Resets)
	assert.EqualValues(t, 1, l.Clears)
	assert.EqualValues(t, 1, l.SnapshotCycles)
	// One message arrived gap-flagged, on one instrument: the duration measure and the
	// book count agree here because the fixture has a single un-anchored message. They
	// diverge on real data, which is the whole reason both exist — see KalshiL2Lane.
	assert.EqualValues(t, 1, l.GapMessages)
	assert.EqualValues(t, 1, l.GapBooks)

	// 7 messages / 4 of them level updates, over a 15-minute (900s) window. Messages is the
	// count the rate is derived from and the denominator of the un-anchored share.
	assert.EqualValues(t, 7, l.Messages)
	assert.InDelta(t, 7.0/900.0, l.MessagesPerSec, 1e-9)
	assert.InDelta(t, 4.0/900.0, l.LevelUpdatesPerSec, 1e-9)

	// Depth stats come only from level-bearing messages (4, 6, 8, 8) — the reset and clear
	// carry depth 0 and must not drag the median down.
	assert.InDelta(t, 8.0, l.DepthMax, 0.001)
	assert.GreaterOrEqual(t, l.DepthP50, 6.0)
}

// A long recovery on ONE book must not read as many faults: gap_messages grows with the
// message rate and the recovery duration rather than with the number of things that went
// wrong. See KalshiL2Lane for the production measurement behind the split.
func TestKalshiL2Coverage_GapBooksCountsBooksNotMessages(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// Instrument 100 spends six messages un-anchored; instrument 101 spends one. Two books
	// gapped, seven messages arrived gapped.
	for i := 0; i < 6; i++ {
		insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 4, "gap", 10)
	}
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 101, "level_update", 4, "gap", 9)
	// And a third book that never gapped, so it must not be counted.
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 102, "level_update", 4, "ready", 8)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	l := lanesBySource(resp.Lanes)["mbp_edge_kalshi_sports_nfl"]

	assert.EqualValues(t, 7, l.GapMessages, "duration measure: every message that arrived un-anchored")
	assert.EqualValues(t, 2, l.GapBooks, "fault count: distinct books affected, not messages")
	// The point of the pair: they must not be interchangeable.
	assert.NotEqual(t, l.GapMessages, l.GapBooks)
	// And the share the page renders is gap_messages over this total, computed with no
	// reference to the window length.
	assert.EqualValues(t, 8, l.Messages)
}

// instrument_id is unique only WITHIN a channel_id, and prod's two publisher arms share a
// multicast group and differ only by channel. Collapsing them would merge two independent
// delta streams and undercount instruments.
func TestKalshiL2Coverage_SeparatesChannels(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// The same instrument_id on two channels of one source: two lanes, one instrument each.
	insertLevel(t, api, "mbp_edge_kalshi_perps", 1, 7, "level_update", 5, "ready", 10)
	insertLevel(t, api, "mbp_edge_kalshi_perps", 101, 7, "level_update", 5, "ready", 10)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	arms := []handlers.KalshiL2Lane{}
	for _, l := range resp.Lanes {
		if l.Source == "mbp_edge_kalshi_perps" && l.Seen {
			arms = append(arms, l)
		}
	}
	require.Len(t, arms, 2, "the two arms must be reported separately")
	for _, l := range arms {
		assert.EqualValues(t, 1, l.Instruments)
	}
	// Sorted by channel id within a lane.
	assert.EqualValues(t, 1, arms[0].ChannelID)
	assert.EqualValues(t, 101, arms[1].ChannelID)
}

// A lane the publisher adds without a matching entry in kalshiL2Lanes must still be reported —
// a page that hides unknown lanes under-reports exactly when someone is checking a new one.
func TestKalshiL2Coverage_ReportsUnknownLane(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevel(t, api, "mbp_edge_kalshi_sports_pickleball", 1, 1, "level_update", 3, "ready", 10)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	seen := lanesBySource(resp.Lanes)
	require.Contains(t, seen, "mbp_edge_kalshi_sports_pickleball")
	assert.Equal(t, "mbp_edge_kalshi_sports_pickleball", seen["mbp_edge_kalshi_sports_pickleball"].Label,
		"unknown lanes fall back to the raw source id")
	assert.Equal(t, "Other", seen["mbp_edge_kalshi_sports_pickleball"].Category)
}

func lanesBySource(lanes []handlers.KalshiL2Lane) map[string]handlers.KalshiL2Lane {
	m := map[string]handlers.KalshiL2Lane{}
	for _, l := range lanes {
		m[l.Source] = l
	}
	return m
}

// A lane that stops publishing must stay on the page. Lanes are discovered from rows inside
// the window, so without the roster merge a silent lane simply vanishes — and the page then
// looks healthy in exactly the failure mode it exists to catch.
func TestKalshiL2Coverage_KeepsSilentLaneVisible(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	// Only NFL is publishing; every other configured lane is silent.
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 4, "ready", 10)

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	bySource := lanesBySource(resp.Lanes)

	require.Contains(t, bySource, "mbp_edge_kalshi_sports_nba")
	nba := bySource["mbp_edge_kalshi_sports_nba"]
	assert.False(t, nba.Seen, "a silent configured lane is reported, not omitted")
	assert.Zero(t, nba.MessagesPerSec)
	assert.True(t, nba.LastSeen.IsZero())
	assert.Equal(t, "NBA", nba.Label)

	assert.True(t, bySource["mbp_edge_kalshi_sports_nfl"].Seen)
}

// Rows older than the coverage window must not contribute; the window is what keeps this scan
// bounded over a table with no TTL.
func TestKalshiL2Coverage_ExcludesOutsideWindow(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiMbpLevelsTable(t, api)

	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 5, "ready", 10)
	insertLevel(t, api, "mbp_edge_kalshi_sports_nfl", 1, 100, "level_update", 5, "ready", 60*60) // an hour ago

	resp, err := api.FetchKalshiL2Coverage(t.Context())
	require.NoError(t, err)
	nfl := lanesBySource(resp.Lanes)["mbp_edge_kalshi_sports_nfl"]
	assert.InDelta(t, 1.0/900.0, nfl.LevelUpdatesPerSec, 1e-9)
}
