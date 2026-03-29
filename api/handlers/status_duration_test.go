package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIssueDurationTables(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String,
			code String,
			status String,
			link_type String,
			bandwidth_bps Nullable(Int64),
			side_a_pk Nullable(String),
			side_z_pk Nullable(String),
			contributor_pk Nullable(String),
			side_a_iface_name Nullable(String),
			side_z_iface_name Nullable(String),
			committed_rtt_ns Int64 DEFAULT 0,
			committed_jitter_ns Int64 DEFAULT 0,
			isis_delay_override_ns Int64 DEFAULT 0
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fact_dz_device_link_latency (
			event_ts DateTime,
			link_pk String,
			rtt_us Float64,
			loss UInt8
		) ENGINE = Memory
	`)
	require.NoError(t, err)
}

// issueStartQuery is the query from fetchStatusData that computes when a
// continuous issue started. It uses a 15-minute coalesce gap (3 consecutive
// healthy 5-minute buckets) before considering an issue resolved.
const issueStartQuery = `
	WITH buckets AS (
		SELECT
			l.code,
			toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
			countIf(loss OR rtt_us = 0) * 100.0 / count(*) as loss_pct
		FROM fact_dz_device_link_latency lat
		JOIN dz_links_current l ON lat.link_pk = l.pk
		WHERE lat.event_ts > now() - INTERVAL 7 DAY
		  AND l.code IN (?)
		GROUP BY l.code, bucket
		HAVING count(*) >= 3
	),
	last_healthy AS (
		SELECT b1.code as code, max(b3.bucket) as last_good_bucket
		FROM buckets b1
		JOIN buckets b2 ON b1.code = b2.code
			AND b2.bucket = b1.bucket + INTERVAL 5 MINUTE AND b2.loss_pct < ?
		JOIN buckets b3 ON b1.code = b3.code
			AND b3.bucket = b1.bucket + INTERVAL 10 MINUTE AND b3.loss_pct < ?
		WHERE b1.loss_pct < ?
		GROUP BY b1.code
	),
	earliest_issue AS (
		SELECT code, min(bucket) as first_issue_bucket
		FROM buckets
		WHERE loss_pct >= ?
		GROUP BY code
	)
	SELECT
		ei.code,
		if(lh.code != '',
		   lh.last_good_bucket + INTERVAL 5 MINUTE,
		   ei.first_issue_bucket) as issue_start
	FROM earliest_issue ei
	LEFT JOIN last_healthy lh ON ei.code = lh.code
`

const lossThreshold = 1.0

// insertLatencySamples inserts n latency samples in a 5-minute bucket starting at bucketStart.
// If lossy is true, all samples have loss=1; otherwise loss=0 with normal RTT.
func insertLatencySamples(t *testing.T, api *handlers.API, linkPK string, bucketStart time.Time, n int, lossy bool) {
	t.Helper()
	ctx := t.Context()
	for i := range n {
		ts := bucketStart.Add(time.Duration(i) * 10 * time.Second)
		var rtt float64
		var loss uint8
		if lossy {
			rtt = 0
			loss = 1
		} else {
			rtt = 5000
			loss = 0
		}
		err := api.DB.Exec(ctx,
			`INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, rtt_us, loss) VALUES (?, ?, ?, ?)`,
			ts, linkPK, rtt, loss)
		require.NoError(t, err)
	}
}

func queryIssueStart(t *testing.T, api *handlers.API, linkCode string) (time.Time, bool) {
	t.Helper()
	ctx := t.Context()
	rows, err := api.DB.Query(ctx, issueStartQuery,
		[]string{linkCode}, lossThreshold, lossThreshold, lossThreshold, lossThreshold)
	require.NoError(t, err)
	defer rows.Close()

	if rows.Next() {
		var code string
		var issueStart time.Time
		require.NoError(t, rows.Scan(&code, &issueStart))
		return issueStart, true
	}
	return time.Time{}, false
}

func TestIssueDuration_BriefHealthyBucketDoesNotResetDuration(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupIssueDurationTables(t, api)
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN')
	`)
	require.NoError(t, err)

	// Timeline (each bucket = 5 min):
	// T-60m: healthy
	// T-55m: healthy
	// T-50m: healthy  ← last sustained healthy period ends here
	// T-45m: lossy
	// T-40m: lossy
	// T-35m: healthy  ← brief dip, should NOT reset duration
	// T-30m: lossy
	// T-25m: lossy
	// T-20m: lossy
	// T-15m: lossy
	// T-10m: lossy
	// T-5m:  lossy
	now := time.Now().UTC().Truncate(5 * time.Minute)

	insertLatencySamples(t, api, "link-1", now.Add(-60*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-55*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-50*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-45*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-40*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-35*time.Minute), 5, false) // healthy (brief)
	insertLatencySamples(t, api, "link-1", now.Add(-30*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-25*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-20*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-15*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-10*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-5*time.Minute), 5, true)   // lossy

	issueStart, found := queryIssueStart(t, api, "NYC-LAX-001")
	require.True(t, found, "should find an issue")

	// The last sustained healthy period (3 consecutive healthy buckets) ends at T-50m.
	// So issue should start at T-50m + 5min = T-45m.
	expectedStart := now.Add(-45 * time.Minute)
	assert.Equal(t, expectedStart.UTC(), issueStart.UTC(),
		"issue start should be after the last sustained healthy period, not after the brief healthy blip")
}

func TestIssueDuration_SustainedHealthyPeriodResetsCorrectly(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupIssueDurationTables(t, api)
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN')
	`)
	require.NoError(t, err)

	// Timeline:
	// T-60m: lossy
	// T-55m: lossy
	// T-50m: healthy
	// T-45m: healthy
	// T-40m: healthy  ← 3 consecutive healthy = sustained healthy period
	// T-35m: lossy    ← new issue starts here
	// T-30m: lossy
	// T-25m: lossy
	now := time.Now().UTC().Truncate(5 * time.Minute)

	insertLatencySamples(t, api, "link-1", now.Add(-60*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-55*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-50*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-45*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-40*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-35*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-30*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-25*time.Minute), 5, true)  // lossy

	issueStart, found := queryIssueStart(t, api, "NYC-LAX-001")
	require.True(t, found, "should find an issue")

	// Last sustained healthy ends at T-40m, issue starts at T-40m + 5min = T-35m
	expectedStart := now.Add(-35 * time.Minute)
	assert.Equal(t, expectedStart.UTC(), issueStart.UTC(),
		"issue start should be after the sustained healthy period")
}

func TestIssueDuration_TwoHealthyBucketsNotEnoughToReset(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupIssueDurationTables(t, api)
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN')
	`)
	require.NoError(t, err)

	// Timeline:
	// T-60m: healthy
	// T-55m: healthy
	// T-50m: healthy  ← last sustained healthy
	// T-45m: lossy
	// T-40m: healthy  ← only 2 consecutive healthy (not enough)
	// T-35m: healthy
	// T-30m: lossy
	// T-25m: lossy
	now := time.Now().UTC().Truncate(5 * time.Minute)

	insertLatencySamples(t, api, "link-1", now.Add(-60*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-55*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-50*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-45*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-40*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-35*time.Minute), 5, false) // healthy
	insertLatencySamples(t, api, "link-1", now.Add(-30*time.Minute), 5, true)  // lossy
	insertLatencySamples(t, api, "link-1", now.Add(-25*time.Minute), 5, true)  // lossy

	issueStart, found := queryIssueStart(t, api, "NYC-LAX-001")
	require.True(t, found, "should find an issue")

	// Only 2 consecutive healthy buckets at T-40m and T-35m — not enough to reset.
	// The last sustained healthy period (3 consecutive) ends at T-50m.
	// Issue starts at T-50m + 5min = T-45m.
	expectedStart := now.Add(-45 * time.Minute)
	assert.Equal(t, expectedStart.UTC(), issueStart.UTC(),
		"2 consecutive healthy buckets should not be enough to reset the duration")
}

func TestIssueDuration_NoHealthyPeriodFallsBackToEarliestIssue(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupIssueDurationTables(t, api)
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN')
	`)
	require.NoError(t, err)

	// All buckets are lossy — no healthy period at all
	now := time.Now().UTC().Truncate(5 * time.Minute)

	insertLatencySamples(t, api, "link-1", now.Add(-30*time.Minute), 5, true)
	insertLatencySamples(t, api, "link-1", now.Add(-25*time.Minute), 5, true)
	insertLatencySamples(t, api, "link-1", now.Add(-20*time.Minute), 5, true)
	insertLatencySamples(t, api, "link-1", now.Add(-15*time.Minute), 5, true)
	insertLatencySamples(t, api, "link-1", now.Add(-10*time.Minute), 5, true)

	issueStart, found := queryIssueStart(t, api, "NYC-LAX-001")
	require.True(t, found, "should find an issue")

	// Falls back to earliest issue bucket
	expectedStart := now.Add(-30 * time.Minute)
	assert.Equal(t, expectedStart.UTC(), issueStart.UTC(),
		"should fall back to earliest issue bucket when no healthy period exists")
}

func TestIssueDuration_NoIssueReturnsNoResults(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupIssueDurationTables(t, api)
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN')
	`)
	require.NoError(t, err)

	// All buckets healthy
	now := time.Now().UTC().Truncate(5 * time.Minute)

	insertLatencySamples(t, api, "link-1", now.Add(-20*time.Minute), 5, false)
	insertLatencySamples(t, api, "link-1", now.Add(-15*time.Minute), 5, false)
	insertLatencySamples(t, api, "link-1", now.Add(-10*time.Minute), 5, false)
	insertLatencySamples(t, api, "link-1", now.Add(-5*time.Minute), 5, false)

	_, found := queryIssueStart(t, api, "NYC-LAX-001")
	assert.False(t, found, "should not find any issue when all buckets are healthy")
}
