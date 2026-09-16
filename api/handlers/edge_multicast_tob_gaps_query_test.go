package handlers_test

import (
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorded-gap query itself, against a real table.
//
// The merge tests seed the payload directly, which exercises the folding and none of the read —
// the same gap `edge_multicast_observations_query_test.go` opens by describing, and the same
// consequence: a wrong column name, a wrong gap predicate or a window filter that excludes
// everything is invisible from the page. The refresher WARNs, the leg writes an empty payload,
// and every top-of-book row quietly falls back to the staleness-only reading this leg exists to
// replace — which looks exactly like a feed the recorder is not covering yet.

func createKalshiEdgeBookTopTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.kalshi_edge_book_top (
			site LowCardinality(String),
			recorder LowCardinality(String),
			env LowCardinality(String),
			feed LowCardinality(String),
			observation LowCardinality(String),
			source_addr IPv4,
			channel_id UInt8,
			dst_addr IPv4,
			dst_port UInt16,
			source_id UInt16,
			instrument_id UInt32,
			sequence_number UInt64,
			reset_count UInt8,
			symbol LowCardinality(String),
			book_certain Bool,
			uncertain_reason LowCardinality(String),
			recv_ts DateTime64(9, 'UTC')
		) ENGINE = MergeTree
		PARTITION BY toDate(recv_ts)
		ORDER BY (site, recorder, feed, source_addr, channel_id, instrument_id, recv_ts)
	`, db)))
}

// insertEdgeBookTop records one top as `recorder` derived it, `agoSecs` ago.
func insertEdgeBookTop(t *testing.T, api *handlers.API, site, recorder, dstAddr, sourceAddr string,
	channelID uint8, instrumentID uint32, resetCount uint8, uncertainReason string, agoSecs int) {
	t.Helper()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %s.kalshi_edge_book_top
		(site, recorder, env, feed, observation, source_addr, channel_id, dst_addr, dst_port,
		 source_id, instrument_id, sequence_number, reset_count, symbol, book_certain,
		 uncertain_reason, recv_ts)
		VALUES ('%s', '%s', 'production', 'perps-tob', 'edge-%s', toIPv4('%s'), %d,
		        toIPv4('%s'), 31000, 3, %d, 1, %d, 'KXBTCPERP', %t, '%s',
		        now64(9) - toIntervalSecond(%d))
	`, db, site, recorder, site, sourceAddr, channelID, dstAddr, instrumentID, resetCount,
		uncertainReason == "", uncertainReason, agoSecs)))
}

func tobGapSeriesByKey(series []handlers.EdgeMulticastTOBGapSeries) map[string]handlers.EdgeMulticastTOBGapSeries {
	out := map[string]handlers.EdgeMulticastTOBGapSeries{}
	for _, s := range series {
		out[fmt.Sprintf("%s|%s|%d|%s", s.MulticastGroup, s.PublisherSourceIP, s.ChannelID, s.Node)] = s
	}
	return out
}

// **The counters the page reads, off a real table.** Two instruments gapped out of three, on one
// of two paths, with a row outside the window and a pre-rollout row that names no group.
func TestFetchEdgeMulticastTOBGapsCountsTheMarkerAndNotTheSpan(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	createKalshiEdgeBookTopTable(t, api)

	const group = "233.84.178.3"
	const gapped, clean = "gap", ""

	// One path, one recorder: three instruments, two of them lowered by a sequence hole. The
	// third is clean, so gap_books must read 2 and not the instrument count.
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.121.69", 1, 11, 0, gapped, 30)
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.121.69", 1, 11, 0, gapped, 29)
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.121.69", 1, 22, 0, gapped, 29)
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.121.69", 1, 33, 0, clean, 28)

	// The redundant path at the same node, clean, and its era advanced once.
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.120.6", 101, 11, 0, clean, 30)
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.120.6", 101, 11, 1, clean, 20)

	// Outside the fifteen-minute window: must not be counted.
	insertEdgeBookTop(t, api, "cmh", "aws-cmh-mn-recorder1", group, "148.51.121.69", 1, 11, 0, gapped, 60*60)

	// Written before the rollout that added dst_addr: the type default names no group, and the
	// query excludes it rather than letting it fold as a series attributed to nothing.
	insertEdgeBookTop(t, api, "was", "aws-was-mn-recorder1", "0.0.0.0", "148.51.121.69", 1, 11, 0, gapped, 30)

	got, err := api.FetchEdgeMulticastTOBGaps(t.Context())
	require.NoError(t, err)
	require.NotNil(t, got)

	byKey := tobGapSeriesByKey(got.Series)
	require.Len(t, byKey, 2, "two paths at one node, and nothing from the address that names no group")

	gappedPath := byKey[fmt.Sprintf("%s|148.51.121.69|1|aws-cmh-mn-recorder1", group)]
	assert.Equal(t, uint64(4), gappedPath.Messages, "the out-of-window row is not in the denominator")
	assert.Equal(t, uint64(3), gappedPath.GapMessages)
	assert.Equal(t, uint64(2), gappedPath.GapBooks, "two instruments gapped, not the three that were seen")
	assert.Equal(t, "cmh", gappedPath.LocationCode)
	assert.False(t, gappedPath.LastSeen.IsZero(), "the stamp converts")
	// Two whole seconds carried a gap-marked top, and they are adjacent.
	require.Len(t, gappedPath.GapSeconds, 2)

	cleanPath := byKey[fmt.Sprintf("%s|148.51.120.6|101|aws-cmh-mn-recorder1", group)]
	assert.Equal(t, uint64(2), cleanPath.Messages)
	assert.Equal(t, uint64(0), cleanPath.GapMessages, "a clean path reads zero, and it is a reading")
	assert.Equal(t, uint64(0), cleanPath.GapBooks)
	assert.Empty(t, cleanPath.GapSeconds)
	assert.Equal(t, uint64(1), cleanPath.Resets, "the era advanced once across the window")
}

// An absent table leaves the rest of the page intact: the leg yields an empty payload rather
// than an error, the same rule every other probe-guarded leg follows.
func TestFetchEdgeMulticastTOBGapsWithoutTheTableIsEmptyAndNotAnError(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	got, err := api.FetchEdgeMulticastTOBGaps(t.Context())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, got.Series)
}
