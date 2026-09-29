package rollup

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/malbeclabs/doublezero/config"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhoenixRaceWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 28, 14, 3, 0, 0, time.UTC)
	// 14:03 less the 20-minute settle is 13:43, so the newest closed window is 13:15-13:30.
	wantEnd := time.Date(2026, 9, 28, 13, 30, 0, 0, time.UTC)

	cases := []struct {
		name      string
		newest    time.Time
		wantStart time.Time
	}{
		{"nothing stored: the whole board", time.Time{}, wantEnd.Add(-24 * time.Hour)},
		{"a pass last hour: heal behind the newest window",
			time.Date(2026, 9, 28, 12, 15, 0, 0, time.UTC), time.Date(2026, 9, 28, 9, 15, 0, 0, time.UTC)},
		{"a long outage: never more than the board shows",
			time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC), wantEnd.Add(-24 * time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start, end, ok := phoenixRaceWindow(now, tc.newest)
			require.True(t, ok)
			assert.Equal(t, wantEnd, end)
			assert.Equal(t, tc.wantStart, start)
			assert.Zero(t, end.Sub(start)%phoenixBucket, "whole windows only")
		})
	}

	// A non-UTC clock must not shift the alignment.
	start, end, _ := phoenixRaceWindow(now.In(time.FixedZone("PDT", -7*3600)), time.Time{})
	assert.Equal(t, wantEnd, end)
	assert.Equal(t, wantEnd.Add(-24*time.Hour), start)
}

// The scan runs inline in the live loop, like the competitor pass, so the same brackets hold.
func TestPhoenixTimeouts_BracketTheScan(t *testing.T) {
	t.Parallel()

	if phoenixRollupActivityTimeout <= phoenixScanTimeout {
		t.Errorf("activity timeout %s does not cover the scan %s", phoenixRollupActivityTimeout, phoenixScanTimeout)
	}
	if phoenixHeartbeatTimeout <= competitorHeartbeatInterval {
		t.Errorf("heartbeat timeout %s does not exceed the heartbeat interval %s",
			phoenixHeartbeatTimeout, competitorHeartbeatInterval)
	}
	if len(phoenixSites) > phoenixMaxSites {
		t.Errorf("%d sites, but the activity timeout covers %d scans", len(phoenixSites), phoenixMaxSites)
	}
	if phoenixRollupActivityTimeout >= rollupWindow {
		t.Errorf("a pass may take %s, which is not inside rollupWindow %s", phoenixRollupActivityTimeout, rollupWindow)
	}
	if phoenixScanSpillBytes >= phoenixScanMaxMemoryBytes {
		t.Errorf("the scan spills at %d bytes, which is not under its %d-byte cap", phoenixScanSpillBytes, phoenixScanMaxMemoryBytes)
	}
	cycle := 2*liveActivityAttempts*liveActivityTimeout + competitorRollupActivityTimeout + phoenixRollupActivityTimeout
	if cycle >= rollupWindow {
		t.Errorf("one live cycle may take %s, which is not inside rollupWindow %s", cycle, rollupWindow)
	}
}

// Off unless configured, and off means no query at all: a nil connection would panic.
func TestRollupPhoenixRace_OffWithoutARecorderDatabase(t *testing.T) {
	t.Parallel()
	a := &Activities{Log: laketesting.NewLogger()}
	n, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: time.Now()})
	require.NoError(t, err)
	assert.Zero(t, n)
}

// setupPhoenixBookTops creates tables shaped like recorder.venue_book_top and recorder.book_top,
// restricted to the columns the rollup reads.
func setupPhoenixBookTops(t *testing.T) (clickhouse.Conn, string) {
	t.Helper()
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn := openRawConn(t, sharedDB, info.Database)
	for _, ddl := range []string{`
		CREATE TABLE venue_book_top (
			observation LowCardinality(String),
			feed        LowCardinality(String),
			symbol      LowCardinality(String),
			book_key    UInt64,
			recv_ts     DateTime64(9, 'UTC'),
			price_exp   Int8,
			qty_exp     Int8
		) ENGINE = MergeTree ORDER BY (observation, feed, symbol, recv_ts)`, `
		CREATE TABLE book_top (
			observation LowCardinality(String),
			feed        LowCardinality(String),
			symbol      LowCardinality(String),
			book_key    UInt64,
			recv_ts     DateTime64(9, 'UTC'),
			price_exp   Int8,
			qty_exp     Int8,
			from_anchor UInt8
		) ENGINE = MergeTree ORDER BY (recv_ts, observation)`,
	} {
		require.NoError(t, conn.Exec(t.Context(), ddl))
	}
	return conn, info.Database
}

var cmh = phoenixSites[0]

func cmhDue(t *testing.T, a *Activities, now time.Time) bool {
	t.Helper()
	stored, err := a.phoenixRaceStored(t.Context())
	require.NoError(t, err)
	last, ok := stored[cmh.Code]
	return !ok || last.lastWrite.Before(now.UTC().Truncate(phoenixCadence))
}

type phoenixObs struct {
	obs, feed, symbol string
	book              uint64
	at                time.Time
	priceExp          int8
	fromAnchor        uint8
}

var phoenixBooks atomic.Uint64

func book() uint64 { return phoenixBooks.Add(1) }

func seen(b uint64, obs string, at time.Time) phoenixObs {
	return phoenixObs{obs: obs, feed: phoenixFeed, symbol: "SOL-PERP", book: b, at: at, priceExp: -2}
}

func raceOf(b uint64, first, second string, at time.Time, leadMs float64) []phoenixObs {
	return []phoenixObs{seen(b, first, at), seen(b, second, at.Add(time.Duration(leadMs*float64(time.Millisecond))))}
}

func race(first, second string, at time.Time, leadMs float64) []phoenixObs {
	return raceOf(book(), first, second, at, leadMs)
}

func insertPhoenixObs(t *testing.T, conn clickhouse.Conn, groups ...[]phoenixObs) {
	t.Helper()
	venue, err := conn.PrepareBatch(t.Context(), `INSERT INTO venue_book_top`)
	require.NoError(t, err)
	dz, err := conn.PrepareBatch(t.Context(), `INSERT INTO book_top`)
	require.NoError(t, err)
	for _, g := range groups {
		for _, r := range g {
			if strings.HasPrefix(r.obs, "venue-") {
				require.NoError(t, venue.Append(r.obs, r.feed, r.symbol, r.book, r.at, r.priceExp, int8(-3)))
			} else {
				require.NoError(t, dz.Append(r.obs, r.feed, r.symbol, r.book, r.at, r.priceExp, int8(-3), r.fromAnchor))
			}
		}
	}
	require.NoError(t, venue.Send())
	require.NoError(t, dz.Send())
}

func withFeed(g []phoenixObs, feed string) []phoenixObs {
	for i := range g {
		g[i].feed = feed
	}
	return g
}

type phoenixWindowRow struct {
	paired, dz, venue uint64
	p50, p95, p99     float64
}

func readPhoenixRollup(t *testing.T, conn clickhouse.Conn, site string) map[time.Time]phoenixWindowRow {
	t.Helper()
	rows, err := conn.Query(t.Context(), `
		SELECT bucket_ts, paired, dz_wins, venue_wins, signed_lead_p50_ms, signed_lead_p95_ms, signed_lead_p99_ms
		FROM phoenix_race_rollup_15m FINAL WHERE site = ?`, site)
	require.NoError(t, err)
	defer rows.Close()
	out := map[time.Time]phoenixWindowRow{}
	for rows.Next() {
		var ts time.Time
		var s phoenixWindowRow
		require.NoError(t, rows.Scan(&ts, &s.paired, &s.dz, &s.venue, &s.p50, &s.p95, &s.p99))
		out[ts.UTC()] = s
	}
	require.NoError(t, rows.Err())
	return out
}

func TestRollupPhoenixRace_WritesEveryWindowAndOnlyRaces(t *testing.T) {
	t.Parallel()
	conn, db := setupPhoenixBookTops(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger(), RecorderDatabase: db, phoenixSites: []phoenixSite{cmh}}

	now := time.Now().UTC()
	_, end, _ := phoenixRaceWindow(now, time.Time{})
	bucket := end.Add(-2 * phoenixBucket)
	in := bucket.Add(time.Minute)

	disagree := race(cmh.DZRecorder, cmh.VenueRecorder, in, 100)
	disagree[1].priceExp = -4
	padded := race(cmh.DZRecorder, cmh.VenueRecorder, in, 100)
	padded[1].symbol = " sol-perp "
	anchored := race(cmh.DZRecorder, cmh.VenueRecorder, in, 100)
	anchored[0].fromAnchor = 1
	keyless := raceOf(0, cmh.DZRecorder, cmh.VenueRecorder, in, 100)
	unseen := book()
	again := book()
	insertPhoenixObs(t, conn,
		race(cmh.DZRecorder, cmh.VenueRecorder, in, 100),
		race(cmh.DZRecorder, cmh.VenueRecorder, in, 200),
		race(cmh.DZRecorder, cmh.VenueRecorder, in, 300),
		race(cmh.VenueRecorder, cmh.DZRecorder, in, 50),
		race(cmh.DZRecorder, cmh.VenueRecorder, in, 0),
		padded,
		withFeed(race(cmh.DZRecorder, cmh.VenueRecorder, in, 100), "tob_edge_other"),
		race("chi/aws-chi-mn-recorder1", cmh.VenueRecorder, in, 100),
		race(cmh.DZRecorder, "chi/aws-chi-mn-recorder1", in, 100),
		[]phoenixObs{seen(unseen, cmh.DZRecorder, in)},
		raceOf(again, cmh.DZRecorder, cmh.VenueRecorder, in.Add(-3*24*time.Hour), 100),
		raceOf(again, cmh.DZRecorder, cmh.VenueRecorder, in, 100),
		anchored,
		keyless,
		disagree,
		race(cmh.DZRecorder, cmh.VenueRecorder, in, phoenixMaxLeadMs+1),
		race(cmh.DZRecorder, cmh.VenueRecorder, end.Add(time.Minute), 100),
	)

	n, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now})
	require.NoError(t, err)
	assert.Equal(t, int(phoenixLookback/phoenixBucket), n)

	stored := readPhoenixRollup(t, conn, cmh.Code)
	require.Len(t, stored, n, "every window is written, raced or not")

	got := stored[bucket]
	assert.EqualValues(t, 6, got.paired, "a tie is a race")
	assert.EqualValues(t, 4, got.dz, "a tie is a win for neither side")
	assert.EqualValues(t, 1, got.venue)
	// Signed: the venue's win is a negative lead, so the median sits between 100 and 200.
	assert.Greater(t, got.p50, 50.0)
	assert.Less(t, got.p50, 250.0)
	assert.InDelta(t, 300, got.p99, 1)

	empty := stored[bucket.Add(-phoenixBucket)]
	assert.Zero(t, empty.paired, "an idle window is a zero row")
	assert.Zero(t, empty.p50, "not NaN: the quantiles of nothing are written as 0")
	_, unsettled := stored[end]
	assert.False(t, unsettled, "the window inside the settle period is not written")
}

// The gate is what makes this hourly. It must close after a pass that found nothing, or an
// idle feed re-runs the 24h scan on every 30s cycle.
func TestRollupPhoenixRace_RunsOncePerHourEvenWhenIdle(t *testing.T) {
	t.Parallel()
	conn, db := setupPhoenixBookTops(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger(), RecorderDatabase: db}
	now := time.Now().UTC()

	n, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now})
	require.NoError(t, err)
	require.Positive(t, n)

	n, err = a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now})
	require.NoError(t, err)
	assert.Zero(t, n, "second pass in the same hour")

	assert.True(t, cmhDue(t, a, now.Add(phoenixCadence)), "due again in the next hour")
}

// A later pass recomputes the heal window. The table is a ReplacingMergeTree on bucket_ts, so
// a straggler updates its window instead of adding a second row for it.
func TestRollupPhoenixRace_HealReplacesAWindow(t *testing.T) {
	t.Parallel()
	conn, db := setupPhoenixBookTops(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger(), RecorderDatabase: db}

	now := time.Now().UTC()
	start, end, _ := phoenixRaceWindow(now, time.Time{})
	bucket := end.Add(-phoenixBucket)
	insertPhoenixObs(t, conn, race(cmh.DZRecorder, cmh.VenueRecorder, bucket.Add(time.Minute), 100))
	_, err := a.writePhoenixRaceWindows(t.Context(), cmh, start, end, now)
	require.NoError(t, err)

	insertPhoenixObs(t, conn, race(cmh.VenueRecorder, cmh.DZRecorder, bucket.Add(2*time.Minute), 40))
	healStart, _, _ := phoenixRaceWindow(now, bucket)
	_, err = a.writePhoenixRaceWindows(t.Context(), cmh, healStart, end, now.Add(time.Second))
	require.NoError(t, err)

	var copies uint64
	require.NoError(t, conn.QueryRow(t.Context(),
		fmt.Sprintf(`SELECT count() FROM phoenix_race_rollup_15m FINAL WHERE bucket_ts = toDateTime(%d, 'UTC')`, bucket.Unix()),
	).Scan(&copies))
	assert.EqualValues(t, 1, copies)

	got := readPhoenixRollup(t, conn, cmh.Code)[bucket]
	assert.EqualValues(t, 2, got.paired)
	assert.EqualValues(t, 1, got.venue)
}

func TestRecorderDatabaseForNetwork_MainnetOnly(t *testing.T) {
	t.Setenv("CLICKHOUSE_RECORDER_DB", "recorder")
	for network, want := range map[string]string{
		"":                    "recorder",
		config.EnvMainnetBeta: "recorder",
		"testnet":             "",
		"devnet":              "",
	} {
		assert.Equal(t, want, recorderDatabaseForNetwork(network), "network %q", network)
	}
}

func TestRollupPhoenixRace_BacksOffAfterAFailedScan(t *testing.T) {
	t.Parallel()
	conn, db := setupPhoenixBookTops(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger(), RecorderDatabase: "no_such_database"}
	now := time.Now().UTC()

	n, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now})
	require.NoError(t, err, "the activity escalates its own failures and never fails the workflow step")
	assert.Zero(t, n)

	a.RecorderDatabase = db
	n, err = a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now.Add(time.Minute)})
	require.NoError(t, err)
	assert.Zero(t, n, "no rescan inside the backoff")
	assert.Empty(t, readPhoenixRollup(t, conn, cmh.Code))

	n, err = a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now.Add(phoenixRetryAfter + time.Minute)})
	require.NoError(t, err)
	assert.Positive(t, n)
}

func TestRollupPhoenixRace_StampsTheWriteWithThePassClock(t *testing.T) {
	t.Parallel()
	conn, db := setupPhoenixBookTops(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger(), RecorderDatabase: db}
	now := time.Now().UTC().Truncate(time.Hour).Add(-10 * time.Second)

	_, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now})
	require.NoError(t, err)

	var lastWrite time.Time
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT max(ingested_at) FROM phoenix_race_rollup_15m`).Scan(&lastWrite))
	assert.Equal(t, now.Truncate(time.Millisecond), lastWrite.UTC())

	assert.True(t, cmhDue(t, a, now.Add(20*time.Second)), "the next hour is still due")
}

func TestRollupPhoenixRace_KeepsSitesApart(t *testing.T) {
	t.Parallel()
	conn, db := setupPhoenixBookTops(t)
	tyo := phoenixSite{Code: "tyo", DZRecorder: "tyo/test-recorder1", VenueRecorder: "venue-test-tyo-recorder1"}
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger(), RecorderDatabase: db, phoenixSites: []phoenixSite{cmh, tyo}}

	now := time.Now().UTC()
	_, end, _ := phoenixRaceWindow(now, time.Time{})
	in := end.Add(-phoenixBucket).Add(time.Minute)

	everywhere := book()
	insertPhoenixObs(t, conn,
		race(tyo.VenueRecorder, tyo.DZRecorder, in, 50),
		[]phoenixObs{
			seen(everywhere, tyo.DZRecorder, in),
			seen(everywhere, cmh.DZRecorder, in.Add(30*time.Millisecond)),
			seen(everywhere, cmh.VenueRecorder, in.Add(130*time.Millisecond)),
			seen(everywhere, tyo.VenueRecorder, in.Add(900*time.Millisecond)),
			seen(everywhere, "fra/aws-fra-mn-recorder1", in.Add(2*time.Second)),
		},
		race(cmh.DZRecorder, tyo.VenueRecorder, in, 100),
	)

	n, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: now})
	require.NoError(t, err)
	assert.Equal(t, 2*int(phoenixLookback/phoenixBucket), n, "every window, for each site")

	bucket := end.Add(-phoenixBucket)
	gotCMH := readPhoenixRollup(t, conn, cmh.Code)[bucket]
	gotTYO := readPhoenixRollup(t, conn, tyo.Code)[bucket]

	assert.EqualValues(t, 1, gotCMH.paired, "an update every site saw is a race at each site, and a cross-site pair is a race at none")
	assert.EqualValues(t, 1, gotCMH.dz)
	assert.InDelta(t, 100, gotCMH.p50, 1, "the lead is between this site's two recorders, not the first and last anywhere")

	assert.EqualValues(t, 2, gotTYO.paired)
	assert.EqualValues(t, 1, gotTYO.dz)
	assert.EqualValues(t, 1, gotTYO.venue)
}

func TestRollupPhoenixRace_AFailedDueCheckWaitsAndPagesAfterErrorAfter(t *testing.T) {
	t.Parallel()
	conn, _ := setupPhoenixBookTops(t)
	require.NoError(t, conn.Exec(t.Context(), `DROP TABLE phoenix_race_rollup_15m`))
	require.NoError(t, conn.Exec(t.Context(), `
		CREATE VIEW phoenix_race_rollup_15m AS
		SELECT 'cmh' AS site, now64(3) AS ingested_at, now() AS bucket_ts
		FROM numbers(1)
		WHERE throwIf(number = 0, 'due check failed') = 0`))
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := &Activities{ClickHouse: conn, Log: log, RecorderDatabase: "recorder"}

	start := time.Now().UTC()
	for at := time.Duration(0); at < phoenixErrorAfter; at += rollupInterval {
		n, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: start.Add(at)})
		require.NoError(t, err)
		assert.Zero(t, n)
	}
	assert.Equal(t, int(phoenixErrorAfter/phoenixRetryAfter), strings.Count(buf.String(), "phoenix race rollup failed"),
		"one due check per %s, not one per cycle", phoenixRetryAfter)
	assert.NotContains(t, buf.String(), "level=ERROR")

	_, err := a.RollupPhoenixRace(t.Context(), PhoenixRaceInput{Now: start.Add(phoenixErrorAfter)})
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(buf.String(), "level=ERROR"), "one page, not one per site")
}

func TestPhoenixFailures_EscalateOnTheirOwnClock(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var f phoenixFailures
	err := errors.New("code: 497, message: Not enough privileges")

	start := time.Now()
	f.observe(log, cmh.Code, start, err)
	f.observe(log, cmh.Code, start.Add(phoenixErrorAfter/2), err)
	assert.NotContains(t, buf.String(), "level=ERROR")

	f.observe(log, cmh.Code, start.Add(phoenixErrorAfter), err)
	assert.Equal(t, 1, strings.Count(buf.String(), "level=ERROR"), "three failures, under the count, but %s apart on the pass clock", phoenixErrorAfter)
}

func TestPhoenixFailures_EscalateOnlyAfterErrorAfter(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var f phoenixFailures
	err := errors.New("code: 497, message: Not enough privileges")

	start := time.Now()
	passes := int(phoenixErrorAfter / phoenixRetryAfter)
	for i := 0; i < passes; i++ {
		f.observe(log, cmh.Code, start.Add(time.Duration(i)*phoenixRetryAfter), err)
	}
	assert.NotContains(t, buf.String(), "level=ERROR", "%d failures %s apart are under %s", passes, phoenixRetryAfter, phoenixErrorAfter)

	f.observe(log, cmh.Code, start.Add(phoenixErrorAfter), err)
	assert.Equal(t, 1, strings.Count(buf.String(), "level=ERROR"))
}
