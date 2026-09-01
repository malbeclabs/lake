package handlers_test

import (
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorder-rows query itself, against the real migrated tables.
//
// The fold tests seed a payload directly, which exercises the arithmetic and none of the read. That
// would leave the column names, the two-stage port fold, the ReplacingMergeTree collapse and the
// manifest lookup unverified — and a break in any of them is invisible from the page: the refresher
// WARNs, the entry is never written, and the Sequence column quietly goes back to reporting the
// level-grain leg's unattributed number as if nothing had changed. The other two legs have a
// live-query fixture for the same reason.

const (
	recorderQueryGroup   = "233.84.178.3"
	recorderQueryPath    = "148.51.121.69"
	recorderQueryPortMkt = 31000
	recorderQueryPortRef = 31001
)

// insertRecorderCoverage writes one archive segment for one channel instance. datagramCount is what
// the recorder kept and captureDropTotal the CUMULATIVE ring counter as it stood at the end of the
// segment — never a rate, and only its delta across a window says anything.
func insertRecorderCoverage(t *testing.T, api *handlers.API, site, recorder, env string, channelID uint8, dstPort int, segmentSeq, datagramCount, captureDropTotal uint64, dropScope string, agoSecs int) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO fact_edge_recorder_segment_coverage
			(start_ts, ingested_at, end_ts, site, recorder, env, feed,
			 source_addr, channel_id, dst_port, segment_seq, first_seq, last_seq, datagram_count,
			 reset_counts_seen, capture_drop_total, interface_drop_total, drop_scope,
			 roles_joined.role, roles_joined.group_addr, roles_joined.port,
			 object_key, object_sha256, build_version, build_commit, config_hash)
		VALUES (now64(9) - toIntervalSecond(%[1]d), now64(3), now64(9) - toIntervalSecond(%[2]d),
			'%[3]s', '%[4]s', '%[5]s', 'kalshi-perps-mbp',
			'%[6]s', %[7]d, %[8]d, %[9]d, 1, 2, %[10]d,
			[0], %[11]d, 0, '%[12]s',
			['mktdata','refdata'], ['%[13]s','%[13]s'], [%[14]d,%[15]d],
			'obj', 'sha', 'v1', 'c1', 'h1')`,
		agoSecs+60, agoSecs, site, recorder, env,
		recorderQueryPath, channelID, dstPort, segmentSeq, datagramCount,
		captureDropTotal, dropScope,
		recorderQueryGroup, recorderQueryPortMkt, recorderQueryPortRef)))
}

// insertRecorderGap writes one contiguous run of missing sequence numbers, with the residue the
// deriver left after subtracting what the recorder admits.
func insertRecorderGap(t *testing.T, api *handlers.API, site, recorder, env string, channelID uint8, dstPort int, missingFrom, missingCount, unexplained, referenceSeqs uint64, admittedScope, verdict string, agoSecs int) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO fact_edge_recorder_sequence_gap
			(before_ts, ingested_at, after_ts, site, recorder, env, feed, port_role,
			 group_addr, source_addr, channel_id, dst_port, reset_count, era_index,
			 missing_from, missing_to, missing_count, reference_seqs,
			 sent_from_ts, sent_to_ts, admitted_recorder, admitted_scope, unexplained_count,
			 interface_drops, seen_elsewhere, on_redundant_path, verdict, object_key)
		VALUES (now64(9) - toIntervalSecond(%[1]d), now64(3), now64(9) - toIntervalSecond(%[1]d),
			'%[2]s', '%[3]s', '%[4]s', 'kalshi-perps-mbp', 'mktdata',
			'%[5]s', '%[6]s', %[7]d, %[8]d, 0, 1,
			%[9]d, %[10]d, %[11]d, %[12]d,
			NULL, NULL, %[13]d, '%[14]s', %[15]d,
			0, 0, 0, '%[16]s', 'obj')`,
		agoSecs, site, recorder, env, recorderQueryGroup, recorderQueryPath,
		channelID, dstPort, missingFrom, missingFrom+missingCount-1, missingCount, referenceSeqs,
		missingCount-unexplained, admittedScope, unexplained, verdict)))
}

func recorderSeriesByKey(series []handlers.EdgeMulticastRecorderSequenceSeries) map[string]handlers.EdgeMulticastRecorderSequenceSeries {
	out := map[string]handlers.EdgeMulticastRecorderSequenceSeries{}
	for _, s := range series {
		out[fmt.Sprintf("%s|%d|%s|%s", s.PublisherSourceIP, s.ChannelID, s.Node, s.Env)] = s
	}
	return out
}

// Absent tables are the local-dev state and the state of every environment whose recorder analysis
// tier is not loading yet. It costs this leg and nothing else — never the refresh, never the page.
func TestFetchEdgeMulticastRecorderSequence_MissingTables(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	// Deliberately no migrations, so neither recorder table exists.

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.Series)
	assert.Equal(t, 15, resp.WindowMinutes)
}

// The grain, and the two folds it makes. The destination port IS folded — a sequence number is
// minted per port role, so loss in each of a channel's ports is loss — and the recording site is
// NOT, because two vantages of one instance are two observations.
func TestFetchEdgeMulticastRecorderSequence_Grain(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	// One instance at cmh, two port roles, each with its own segment and its own gap.
	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 1, 400_000, 0, "port-role", 30)
	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortRef, 1, 1_000, 0, "port-role", 30)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 500, 40, 30, 400_000, "port-role", "publisher", 60)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortRef, 900, 5, 5, 1_000, "port-role", "publisher", 55)

	// The same instance at a second vantage, clean.
	insertRecorderCoverage(t, api, "was", "aws-was", "mainnet", 1, recorderQueryPortMkt, 1, 400_040, 0, "port-role", 30)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 2, "two vantages of one instance are two rows, never one")

	by := recorderSeriesByKey(resp.Series)

	cmh := by[recorderQueryPath+"|1|aws-cmh|mainnet"]
	assert.EqualValues(t, 45, cmh.Missing, "the ports are summed: 40 on mktdata and 5 on refdata")
	assert.EqualValues(t, 35, cmh.Unexplained, "and so is the residue, which is what the column reports")
	assert.EqualValues(t, 401_000, cmh.ReferenceSeqs, "each port's own reference, summed across the ports")
	assert.EqualValues(t, 401_000, cmh.Datagrams)
	assert.EqualValues(t, 2, cmh.Gaps, "one discontinuity per run")
	assert.EqualValues(t, 40, cmh.MaxRun, "the worst single run, in sequence values")
	assert.Equal(t, recorderQueryGroup, cmh.MulticastGroup)
	assert.Equal(t, "cmh", cmh.LocationCode)
	assert.Equal(t, "port-role", cmh.DropScope)
	assert.False(t, cmh.LastSeen.IsZero(), "the newest segment end is what the staleness grade reads")

	was := by[recorderQueryPath+"|1|aws-was|mainnet"]
	assert.EqualValues(t, 0, was.Missing, "clean, and it says so rather than being absent")
	assert.EqualValues(t, 400_040, was.Datagrams)
}

// reference_seqs is a per-window total repeated on every gap row of that window. Summing it within
// a port multiplies the denominator by however many gaps there were, which divides the loss rate by
// the same number: the worse the window, the healthier it reads.
func TestFetchEdgeMulticastRecorderSequence_ReferenceIsNotSummedWithinAPort(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 1, 99_900, 0, "port-role", 30)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 500, 40, 40, 100_000, "port-role", "publisher", 60)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 900, 60, 60, 100_000, "port-role", "publisher", 50)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 1)
	assert.EqualValues(t, 100_000, resp.Series[0].ReferenceSeqs,
		"one window's reference, not one per gap row")
	assert.EqualValues(t, 100, resp.Series[0].Unexplained, "the losses themselves ARE additive")
}

// Both tables are ReplacingMergeTree on ingested_at and reprocessing after an analyser fix writes
// the row again rather than mutating it. Collapsing happens at merge time only, so without FINAL a
// window between a reprocess and its merge reports the same loss twice.
func TestFetchEdgeMulticastRecorderSequence_ReprocessedRowIsNotCountedTwice(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 1, 99_900, 0, "port-role", 30)
	// The same gap — same instance, same era, same first missing sequence — derived twice. The
	// second run subtracted a drop the first had not loaded yet.
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 500, 40, 40, 100_000, "port-role", "publisher", 60)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 500, 40, 10, 100_000, "port-role", "recorder", 60)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 1)
	assert.EqualValues(t, 1, resp.Series[0].Gaps, "one gap, derived twice, is still one gap")
	assert.EqualValues(t, 10, resp.Series[0].Unexplained, "and the newer derivation is the one that stands")
}

// A gap in a window this recorder has no coverage row for is the tables' own `unverifiable` case,
// and it must produce no row at all: the column then falls through to the other legs instead of
// inheriting a claim from a window nobody can vouch for.
func TestFetchEdgeMulticastRecorderSequence_NoCoverageMeansNoOpinion(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 500, 40, 40, 100_000, "port-role", "publisher", 60)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	assert.Empty(t, resp.Series, "no coverage row is no standing to report anything about this window")
}

// At capture-handle scope the ring's drops belong to the handle and to no instance, so the figure
// the fold gates on has to be handle-wide: the delta of the cumulative counter across every
// coverage row of one (site, recorder, env), and never this instance's own rows. A drop caused by
// one channel rides on the next datagram of another that gets through.
func TestFetchEdgeMulticastRecorderSequence_HandleDropIsHandleWide(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	// Channel 1 saw the ring counter move; channel 101 under the same handle saw none of it.
	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 1, 50_000, 1_000, "capture-handle", 300)
	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 2, 50_000, 1_040, "capture-handle", 30)
	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 101, recorderQueryPortMkt, 1, 50_000, 1_040, "capture-handle", 30)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 101, recorderQueryPortMkt, 500, 40, 40, 100_000, "capture-handle", "unverifiable", 60)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 2)

	by := recorderSeriesByKey(resp.Series)
	for _, key := range []string{recorderQueryPath + "|1|aws-cmh|mainnet", recorderQueryPath + "|101|aws-cmh|mainnet"} {
		assert.EqualValues(t, 40, by[key].HandleAdmitted, "%s: the handle's delta, carried on every instance under it", key)
	}
	assert.EqualValues(t, 40, by[recorderQueryPath+"|101|aws-cmh|mainnet"].Unexplained,
		"the residue reaches the payload intact; it is the FOLD that withholds it, on this handle figure")
}

// A recorder that admitted nothing over the whole window turns every gap under it into someone
// else's with evidence, whatever mode it was capturing in.
func TestFetchEdgeMulticastRecorderSequence_HandleThatAdmittedNothingExoneratesItself(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 1, 50_000, 7, "capture-handle", 300)
	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 2, 50_000, 7, "capture-handle", 30)
	insertRecorderGap(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortMkt, 500, 40, 40, 100_000, "capture-handle", "publisher", 60)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 1)
	assert.EqualValues(t, 0, resp.Series[0].HandleAdmitted,
		"a cumulative counter that did not move admitted nothing, whatever its absolute value")
}

// An instance that lost nothing has no gap row, so its destination address has to come out of the
// manifest — matched to the segment's own port role, because one recorder joins several groups.
// Without it a clean instance could never be placed on a group and the column could never say a
// feed is clean.
func TestFetchEdgeMulticastRecorderSequence_GroupAddressFromTheManifest(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	insertRecorderCoverage(t, api, "cmh", "aws-cmh", "mainnet", 1, recorderQueryPortRef, 1, 1_234, 0, "port-role", 30)

	resp, err := api.FetchEdgeMulticastRecorderSequence(t.Context())
	require.NoError(t, err)
	require.Len(t, resp.Series, 1)
	assert.Equal(t, recorderQueryGroup, resp.Series[0].MulticastGroup,
		"the roles the recorder was asked to join carry the address it was asked to join it on")
	assert.EqualValues(t, 1_234, resp.Series[0].Datagrams)
}
