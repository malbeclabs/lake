package mroute

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/malbeclabs/lake/indexer/pkg/dz/statecollect"
)

// TestS3Source_FetchLatest_MinIO is layer C: round-trip the source against a
// real S3-compatible backend. It mirrors the exact key layout the
// telemetry state-ingest server writes
// (telemetry/state-ingest/pkg/server/handler.go:302) and verifies:
//
//   - Device subprefixes under snapshots/ip-mroute/ are enumerated correctly.
//   - For each device, the alphabetically-greatest key within the lookback
//     window is selected.
//   - The dump's DevicePubkey, FileName, and RawJSON match what we wrote.
//   - SnapshotTS parses out of the `YYYYMMDDTHHMMSSZ.json` filename.
func TestS3Source_FetchLatest_MinIO(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}

	ctx := t.Context()

	ctr, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	require.NoError(t, err)
	t.Cleanup(func() {
		termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = ctr.Terminate(termCtx)
	})

	endpoint, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)

	// Build a raw S3 client pointed at MinIO so we can pre-populate the bucket.
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(awscreds.NewStaticCredentialsProvider(ctr.Username, ctr.Password, "")),
	)
	require.NoError(t, err)
	rawS3 := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	})

	const bucket = "doublezero-mainnet-beta-state-collect"
	_, err = rawS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Use today's UTC date so the source's 3-day lookback finds everything.
	now := time.Now().UTC()
	earlier := now.Add(-30 * time.Minute)
	yesterday := now.AddDate(0, 0, -1)

	// devA has two snapshots today (later one should win) and one yesterday.
	// devB has a single snapshot today. Body is the raw Arista JSON; the
	// fixture wraps it in the StateSnapshot envelope before upload, matching
	// what controlplane/telemetry/internal/state/collector.go writes.
	type fixture struct {
		pubkey string
		ts     time.Time
		body   string
	}
	const devAPK = "DzPkA111111111111111111111111111111111111111"
	const devBPK = "DzPkB222222222222222222222222222222222222222"

	devALatestBody := `{"vrfs":{"default":{"sparseMode":{"groups":{"233.84.178.1":{"groupSources":{"10.0.0.1":{"sourceAddress":"10.0.0.1","creationTime":1779000000.0,"routeFlags":"SMP","registerInOifList":false,"rpfInterface":"Port-Channel1000","oifList":["Port-Channel3000"]}}}}},"bidirectional":{"groups":{}}}}}`
	fixtures := []fixture{
		{devAPK, yesterday, `{"vrfs":{"default":{"sparseMode":{"groups":{}},"bidirectional":{"groups":{}}}}}`},
		{devAPK, earlier, `{"vrfs":{"default":{"sparseMode":{"groups":{}},"bidirectional":{"groups":{}}}}}`},
		{devAPK, now, devALatestBody},
		{devBPK, earlier, `{"vrfs":{"default":{"sparseMode":{"groups":{"233.84.178.10":{"groupSources":{"10.0.0.5":{"sourceAddress":"10.0.0.5","creationTime":1779000000.0,"routeFlags":"ME","registerInOifList":false,"rpfInterface":"Null0","oifList":[]}}}}},"bidirectional":{"groups":{}}}}}`},
	}

	for _, f := range fixtures {
		key := buildStateIngestKey(f.pubkey, f.ts)
		body, err := json.Marshal(statecollect.Envelope{
			Metadata: statecollect.Metadata{
				Kind:      SnapshotKind,
				Timestamp: f.ts.UTC().Format(time.RFC3339),
				Device:    f.pubkey,
			},
			Data: json.RawMessage(f.body),
		})
		require.NoError(t, err)
		_, err = rawS3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})
		require.NoError(t, err, "put %s", key)
	}

	// Construct the S3Source pointed at MinIO. The Source uses the default
	// AWS credential chain; t.Setenv injects static creds so it picks them up.
	t.Setenv("AWS_ACCESS_KEY_ID", ctr.Username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", ctr.Password)
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")

	src, err := NewS3Source(ctx, S3SourceConfig{
		Bucket:       bucket,
		Region:       "us-east-1",
		EndpointURL:  "http://" + endpoint,
		LookbackDays: 3,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })

	dumps, err := src.FetchLatest(ctx)
	require.NoError(t, err)
	require.Len(t, dumps, 2, "expected one latest dump per device")

	byPK := map[string]*Dump{}
	for _, d := range dumps {
		byPK[d.DevicePubkey] = d
	}

	t.Run("devA: latest body across days", func(t *testing.T) {
		d := byPK[devAPK]
		require.NotNil(t, d)
		assert.Equal(t, devALatestBody, string(d.RawJSON), "should return the latest snapshot, not yesterday's")
		assert.Contains(t, d.FileName, "device="+devAPK)
		assert.Contains(t, d.FileName, "date="+now.Format("2006-01-02"))
		// SnapshotTS round-trips through the filename to second precision.
		expected := now.Truncate(time.Second)
		assert.WithinDuration(t, expected, d.SnapshotTS, time.Second)
	})

	t.Run("devB: single snapshot picked", func(t *testing.T) {
		d := byPK[devBPK]
		require.NotNil(t, d)
		assert.Contains(t, d.FileName, "device="+devBPK)
		// Confirm Parse can round-trip what FetchLatest returned.
		entries, err := Parse(d.RawJSON)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "233.84.178.10", entries[0].GroupAddress)
		assert.Equal(t, "10.0.0.5", entries[0].SourceAddress)
		assert.Equal(t, "ME", entries[0].RouteFlags)
	})
}

// buildStateIngestKey reproduces the key pattern from the telemetry
// state-ingest server's buildSnapshotKey (handler.go:302) for the mroute
// kind, with no BucketPathPrefix.
func buildStateIngestKey(pubkey string, ts time.Time) string {
	ts = ts.UTC()
	return "snapshots/" + SnapshotKind +
		"/device=" + pubkey +
		"/date=" + ts.Format("2006-01-02") +
		"/hour=" + ts.Format("15") +
		"/" + ts.Format("20060102T150405Z") + ".json"
}
