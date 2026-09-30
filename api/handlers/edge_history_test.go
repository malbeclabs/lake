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

type pcapFile struct {
	group, recorder, ip, key, manifest string
	first, last, offload               time.Time
	size                               uint64
	empty                              bool // no packets captured
}

func insertPcapFiles(t *testing.T, api *handlers.API, files ...pcapFile) {
	t.Helper()
	for _, f := range files {
		offload := f.offload
		if offload.IsZero() {
			offload = f.last.Add(time.Minute)
		}
		manifest := f.manifest
		if manifest == "" {
			manifest = "manifest-" + f.key
		}
		packets := 100
		if f.empty {
			packets = 0
		}
		err := api.DB.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO fact_dz_edge_pcap_file
			SELECT fromUnixTimestamp64Micro(%[1]d), now64(3), '%[3]s', '%[4]s',
			       toStartOfHour(fromUnixTimestamp64Micro(%[1]d)), '%[5]s', '%[6]s', 0,
			       %[7]d, %[10]d, fromUnixTimestamp64Micro(%[2]d), '', '%[8]s',
			       fromUnixTimestamp64Micro(%[9]d)`,
			f.first.UnixMicro(), f.last.UnixMicro(), f.recorder, f.ip, f.key, f.group, f.size, manifest, offload.UnixMicro(), packets))
		require.NoError(t, err)
	}
}

func getEdgeHistory(t *testing.T, api *handlers.API, rng string) handlers.EdgeHistoryResponse {
	t.Helper()
	target := "/api/dz/edge/history"
	if rng != "" {
		target += "?range=" + rng
	}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rr := httptest.NewRecorder()
	api.GetEdgeHistory(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp handlers.EdgeHistoryResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return resp
}

func TestGetEdgeHistory_EmptyTable(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	resp := getEdgeHistory(t, api, "")
	assert.Equal(t, "all", resp.Range)
	assert.NotNil(t, resp.Feeds)
	assert.Empty(t, resp.Feeds)
	assert.Nil(t, resp.AsOf)
}

func TestGetEdgeHistory_RejectsUnknownRange(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/history?range=1y", nil)
	rr := httptest.NewRecorder()
	api.GetEdgeHistory(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// A capture that stops for ten minutes and resumes reports one gap of that length; the
// holes between abutting files, and a sub-minute one on a quiet feed, report nothing.
func TestGetEdgeHistory_GapsAndCoverage(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	h := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	const g, rec, ip = "233.84.178.15", "aws-cmh-mn-recorder1", "3.151.138.124"
	insertPcapFiles(t, api,
		pcapFile{group: g, recorder: rec, ip: ip, key: "a1", first: h, last: h.Add(20 * time.Minute), size: 50},
		pcapFile{group: g, recorder: rec, ip: ip, key: "a2", first: h.Add(20 * time.Minute), last: h.Add(40 * time.Minute), size: 50},
		// ten minutes missing
		pcapFile{group: g, recorder: rec, ip: ip, key: "a3", first: h.Add(50 * time.Minute), last: h.Add(59 * time.Minute), size: 50},
		// eight seconds between files: a quiet feed, not a gap
		pcapFile{group: g, recorder: rec, ip: ip, key: "a4", first: h.Add(59*time.Minute + 8*time.Second), last: h.Add(time.Hour), size: 50},
	)

	resp := getEdgeHistory(t, api, "7d")
	require.Len(t, resp.Feeds, 1)
	feed := resp.Feeds[0]
	assert.Equal(t, g, feed.MulticastGroup)
	assert.Equal(t, uint64(4), feed.Files)
	assert.Equal(t, uint64(200), feed.Bytes)
	require.Len(t, feed.Recorders, 1)
	r := feed.Recorders[0]
	assert.Equal(t, "cmh", r.Site)
	assert.Equal(t, 1, r.GapCount)
	assert.InDelta(t, 600, r.GapSeconds, 0.001)
	require.Len(t, r.Gaps, 1)
	assert.True(t, r.Gaps[0].Start.Equal(h.Add(40*time.Minute)))
	assert.False(t, r.Live, "last packet is three hours old")
	assert.InDelta(t, 100*50.0/60.0, r.CoveragePct, 0.01, "50 of the 60 minutes between first and last packet")

	// Hourly buckets: the captured hour reads 50/60, the hours around it are outside
	// the recorder's span.
	i := int(h.Sub(resp.WindowStart) / time.Hour)
	require.Len(t, r.BucketCoverage, len(r.BucketBytes))
	assert.InDelta(t, 50.0/60.0, r.BucketCoverage[i], 0.001)
	assert.Equal(t, uint64(200), r.BucketBytes[i])
	assert.Equal(t, -1.0, r.BucketCoverage[i-1])
	assert.Equal(t, -1.0, r.BucketCoverage[i+1])
}

// A recorder rebuilt on a new address is one history with the rebuild as its gap, and
// a key an offload rewrote reads as its newest file, with the loss counted.
func TestGetEdgeHistory_InstancesAndOverwrites(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	h := time.Now().UTC().Truncate(time.Hour).Add(-48 * time.Hour)
	const g, rec = "233.84.178.20", "aws-tyo-mn-recorder1"
	insertPcapFiles(t, api,
		pcapFile{group: g, recorder: rec, ip: "54.168.241.102", key: "old/1", first: h, last: h.Add(time.Hour), size: 10},
		pcapFile{group: g, recorder: rec, ip: "13.114.28.108", key: "new/1", first: h.Add(5 * time.Hour), last: h.Add(6 * time.Hour), size: 10},
		// The same key written twice: a restart at +30m replaced the file that began at
		// +6h, so only +6h30..+7h is still held and the half hour before it is lost.
		pcapFile{group: g, recorder: rec, ip: "13.114.28.108", key: "new/2", manifest: "m1", first: h.Add(6 * time.Hour), last: h.Add(6*time.Hour + 29*time.Minute), size: 10},
		pcapFile{group: g, recorder: rec, ip: "13.114.28.108", key: "new/2", manifest: "m2", first: h.Add(6*time.Hour + 30*time.Minute), last: h.Add(7 * time.Hour), size: 7},
	)

	resp := getEdgeHistory(t, api, "all")
	require.Len(t, resp.Feeds, 1)
	r := resp.Feeds[0].Recorders[0]
	require.Len(t, r.Instances, 2)
	assert.Equal(t, "54.168.241.102", r.Instances[0].IP, "instances oldest first")
	assert.Equal(t, uint64(3), r.Files, "the rewritten key is one file")
	assert.Equal(t, uint64(27), r.Bytes, "and it is the newer one")
	assert.Equal(t, uint64(1), r.Overwritten)
	assert.Equal(t, uint64(1), resp.Overwritten)
	assert.Equal(t, 2, r.GapCount, "the rebuild, and the overwritten half hour")
	assert.InDelta(t, 4*3600+30*60, r.GapSeconds, 0.001)
	assert.Equal(t, time.Hour.Seconds()*24, float64(resp.BucketSeconds))
}

// A window that opens on an outage: the recorder captured before the window, stopped,
// and resumed inside it. The outage belongs to its span — it is not "not recording" —
// so the coverage reads the resumed share, not zero.
func TestGetEdgeHistory_WindowOpensOnAnOutage(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	now := time.Now().UTC()
	const g, rec, ip = "233.84.178.4", "aws-dub-mn-recorder1", "54.76.116.123"
	before := now.Add(-8 * 24 * time.Hour).Truncate(time.Hour)
	resumed := now.Add(-2 * 24 * time.Hour).Truncate(time.Hour)
	insertPcapFiles(t, api,
		pcapFile{group: g, recorder: rec, ip: ip, key: "before", first: before, last: before.Add(time.Hour), size: 1},
		pcapFile{group: g, recorder: rec, ip: ip, key: "resumed", first: resumed, last: now.Add(-time.Minute), size: 1},
	)

	resp := getEdgeHistory(t, api, "7d")
	require.Len(t, resp.Feeds, 1)
	r := resp.Feeds[0].Recorders[0]
	require.Equal(t, 1, r.GapCount)
	assert.True(t, r.Gaps[0].Start.Equal(resp.WindowStart), "the gap is clipped to the window")
	span := now.Sub(resp.WindowStart).Seconds()
	assert.InDelta(t, 100*(span-r.GapSeconds)/span, r.CoveragePct, 0.05)
	assert.Greater(t, r.CoveragePct, 20.0, "about two days of seven, not 0%")
	assert.Equal(t, 0.0, r.BucketCoverage[0], "the window's first hour is missing, not outside the span")
}

// However long ago the last packet before the window was, an outage that opens the
// window is found: here the recorder stopped 45 days ago and resumed 10 days ago, and
// the 30-day window opens on 20 days of it.
func TestGetEdgeHistory_OpeningOutageOfAnyLength(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	now := time.Now().UTC()
	const g, rec, ip = "233.84.178.8", "aws-fra-mn-recorder1", "63.178.8.5"
	stopped := now.Add(-45 * 24 * time.Hour).Truncate(time.Hour)
	resumed := now.Add(-10 * 24 * time.Hour).Truncate(time.Hour)
	insertPcapFiles(t, api,
		pcapFile{group: g, recorder: rec, ip: ip, key: "old", first: stopped.Add(-time.Hour), last: stopped, size: 1},
		pcapFile{group: g, recorder: rec, ip: ip, key: "new", first: resumed, last: now.Add(-time.Minute), size: 1},
	)

	resp := getEdgeHistory(t, api, "30d")
	require.Len(t, resp.Feeds, 1)
	r := resp.Feeds[0].Recorders[0]
	require.Equal(t, 1, r.GapCount)
	assert.True(t, r.Gaps[0].Start.Equal(resp.WindowStart))
	assert.True(t, r.Gaps[0].End.Equal(resumed))
	assert.Equal(t, 0.0, r.BucketCoverage[0], "the window's first day is missing, not outside the span")
}

// A zero-packet file carries no packet times, so it neither stretches the span nor
// makes a stopped recorder read as live; it still counts as a file.
func TestGetEdgeHistory_EmptyFilesDoNotDefineTheSpan(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	now := time.Now().UTC()
	const g, rec, ip = "233.84.178.30", "aws-was-mn-recorder1", "18.232.43.98"
	h := now.Add(-10 * time.Hour).Truncate(time.Hour)
	recent := now.Truncate(time.Hour)
	insertPcapFiles(t, api,
		pcapFile{group: g, recorder: rec, ip: ip, key: "data", first: h, last: h.Add(time.Hour), size: 5},
		pcapFile{group: g, recorder: rec, ip: ip, key: "empty", first: recent, last: recent, size: 24, empty: true},
	)

	resp := getEdgeHistory(t, api, "7d")
	require.Len(t, resp.Feeds, 1)
	r := resp.Feeds[0].Recorders[0]
	assert.Equal(t, uint64(2), r.Files)
	assert.True(t, r.LastTS.Equal(h.Add(time.Hour)), "the span ends at the last captured packet")
	assert.False(t, r.Live)
	assert.Equal(t, 100.0, r.CoveragePct)
}
