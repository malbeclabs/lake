package msdp

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

// TestS3Source_FetchLatest_MinIO is layer C for MSDP: round-trip the
// source against a real S3-compatible backend with envelope-wrapped
// objects matching what doublezero-telemetry writes. Exercises all
// three MSDP kinds in a single device, plus a second device
// contributing only the summary kind, to verify per-kind device
// enumeration is independent.
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

	now := time.Now().UTC()
	const devAPK = "DzPkA111111111111111111111111111111111111111"
	const devBPK = "DzPkB222222222222222222222222222222222222222"

	devASummary := `{"peerList":[{"peerIpAddress":"172.16.0.1","state":"established","sessionStartTime":1779369727.0,"saCount":5,"resetCount":2}]}`
	devAPim := `{"saCache":[{"sourceGroupPair":{"sourceAddress":"148.51.120.1","groupAddress":"233.84.178.1"},"rpAddress":"10.0.0.0"}]}`
	devASA := `{"acceptedSaMsg":[{"sourceGroupPair":{"sourceAddress":"148.51.120.16","groupAddress":"233.84.178.1"},"rpAddress":"10.0.0.0","remoteAddress":"172.16.1.6"}],"rejectedSaMsg":[]}`
	devBSummary := `{"peerList":[{"peerIpAddress":"172.16.0.2","state":"connecting","saCount":0,"resetCount":0}]}`

	uploads := []struct {
		pubkey string
		kind   string
		body   string
	}{
		{devAPK, SnapshotKindSummary, devASummary},
		{devAPK, SnapshotKindPimSACache, devAPim},
		{devAPK, SnapshotKindSACacheRejected, devASA},
		{devBPK, SnapshotKindSummary, devBSummary},
	}

	for _, u := range uploads {
		key := buildStateIngestKey(u.kind, u.pubkey, now)
		body, err := json.Marshal(statecollect.Envelope{
			Metadata: statecollect.Metadata{
				Kind:      u.kind,
				Timestamp: now.UTC().Format(time.RFC3339),
				Device:    u.pubkey,
			},
			Data: json.RawMessage(u.body),
		})
		require.NoError(t, err)
		_, err = rawS3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})
		require.NoError(t, err, "put %s", key)
	}

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

	dumpsByKind, err := src.FetchLatest(ctx)
	require.NoError(t, err)

	t.Run("summary kind has both devices", func(t *testing.T) {
		require.Len(t, dumpsByKind[SnapshotKindSummary], 2)
	})

	t.Run("pim and sa-cache only have devA", func(t *testing.T) {
		require.Len(t, dumpsByKind[SnapshotKindPimSACache], 1)
		require.Len(t, dumpsByKind[SnapshotKindSACacheRejected], 1)
		assert.Equal(t, devAPK, dumpsByKind[SnapshotKindPimSACache][0].DevicePubkey)
		assert.Equal(t, devAPK, dumpsByKind[SnapshotKindSACacheRejected][0].DevicePubkey)
	})

	t.Run("envelope is unwrapped — Parse* consume Dump.RawJSON directly", func(t *testing.T) {
		// Summary
		var devASumDump *Dump
		for _, d := range dumpsByKind[SnapshotKindSummary] {
			if d.DevicePubkey == devAPK {
				devASumDump = d
				break
			}
		}
		require.NotNil(t, devASumDump)
		peers, err := ParseSummary(devASumDump.RawJSON)
		require.NoError(t, err)
		require.Len(t, peers, 1)
		assert.Equal(t, "172.16.0.1", peers[0].PeerIPAddress)
		assert.Equal(t, int64(5), peers[0].SACount)

		// PIM SA cache
		entries, err := ParsePimSACache(dumpsByKind[SnapshotKindPimSACache][0].RawJSON)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "148.51.120.1", entries[0].SourceAddress)

		// SA-cache rejected (single accepted, zero rejected)
		sa, err := ParseSACacheRejected(dumpsByKind[SnapshotKindSACacheRejected][0].RawJSON)
		require.NoError(t, err)
		require.Len(t, sa, 1)
		assert.Equal(t, SACacheStatusAccepted, sa[0].Status)
	})
}

// buildStateIngestKey reproduces the key pattern from the telemetry
// state-ingest server's buildSnapshotKey for a given kind, with no
// BucketPathPrefix.
func buildStateIngestKey(kind, pubkey string, ts time.Time) string {
	ts = ts.UTC()
	return "snapshots/" + kind +
		"/device=" + pubkey +
		"/date=" + ts.Format("2006-01-02") +
		"/hour=" + ts.Format("15") +
		"/" + ts.Format("20060102T150405Z") + ".json"
}
