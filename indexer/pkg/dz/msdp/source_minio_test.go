package msdp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
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
	mio := startMinIO(t)

	const bucket = "doublezero-mainnet-beta-state-collect"
	_, err := mio.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
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
		mio.putSnapshot(t, bucket, u.kind, u.pubkey, now, u.body)
	}

	t.Setenv("AWS_ACCESS_KEY_ID", mio.username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", mio.password)
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")

	src, err := NewS3Source(ctx, S3SourceConfig{
		Bucket:       bucket,
		Region:       "us-east-1",
		EndpointURL:  mio.endpoint,
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

// TestS3Source_FetchLatest_FanOut covers the parallel per-device fan-out against
// MinIO: result ordering, the bounded retry of transient failures, and the
// all-or-nothing abort that protects the MissingMeansDeleted contract in
// Store.Sync.
func TestS3Source_FetchLatest_FanOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}

	ctx := t.Context()
	mio := startMinIO(t)

	const bucket = "doublezero-fanout-state-collect"
	_, err := mio.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Enough devices that the fan-out saturates maxConcurrentFetches. Pubkeys are
	// zero-padded so generation order is also lexicographic (= S3 listing order).
	const deviceCount = 25
	body := `{"peerList":[{"peerIpAddress":"172.16.0.1","state":"established","saCount":1,"resetCount":0}]}`
	now := time.Now().UTC()
	pubkeys := make([]string, 0, deviceCount)
	for i := range deviceCount {
		pk := fmt.Sprintf("DzPkFanOut%02d111111111111111111111111111111", i)
		pubkeys = append(pubkeys, pk)
		mio.putSnapshot(t, bucket, SnapshotKindSummary, pk, now, body)
	}

	t.Run("device set and ordering follow the listing, work runs concurrently", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport}
		src := mio.newSource(t, bucket, ft)

		dumpsByKind, err := src.FetchLatest(ctx)
		require.NoError(t, err)

		got := make([]string, 0, deviceCount)
		for _, d := range dumpsByKind[SnapshotKindSummary] {
			got = append(got, d.DevicePubkey)
		}
		// Ordering is what the serial walk produced: S3 listing order.
		assert.Equal(t, pubkeys, got)

		peak := ft.peakInflight()
		assert.Greater(t, peak, 1, "per-device reads should overlap")
		assert.LessOrEqual(t, peak, maxConcurrentFetches, "fan-out must stay under the limit")
	})

	t.Run("transient mid-walk failure is retried and the walk completes", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, target: pubkeys[7], failFirst: 1}
		src := mio.newSource(t, bucket, ft)

		dumpsByKind, err := src.FetchLatest(ctx)
		require.NoError(t, err)
		require.Len(t, dumpsByKind[SnapshotKindSummary], deviceCount)
		assert.Greater(t, ft.matched(), 1, "the failed device should have been retried")
	})

	t.Run("persistent device failure aborts the whole fetch", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, target: pubkeys[7], failAll: true}
		src := mio.newSource(t, bucket, ft)

		dumpsByKind, err := src.FetchLatest(ctx)
		require.Error(t, err, "a partial device set would tombstone the missing devices")
		assert.Nil(t, dumpsByKind)
		assert.Equal(t, deviceFetchRetry.MaxAttempts, ft.matched(), "retries must be bounded")
	})

	t.Run("cancelled context aborts without exhausting retries", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		ft := &faultTransport{
			base:    http.DefaultTransport,
			target:  pubkeys[7],
			failAll: true,
			onFail:  cancel,
		}
		src := mio.newSource(t, bucket, ft)

		dumpsByKind, err := src.FetchLatest(cancelCtx)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, dumpsByKind)
		assert.Equal(t, 1, ft.matched(), "retrying a cancelled context is pointless")
	})
}

// faultTransport injects failures into the S3 calls whose URL mentions a target
// device pubkey, so tests can drive the per-device retry path deterministically.
// It also records peak concurrent requests.
type faultTransport struct {
	base http.RoundTripper

	// target is the device pubkey whose requests fail; empty fails nothing.
	target    string
	failFirst int  // fail this many matching requests, then pass through
	failAll   bool // fail every matching request
	onFail    func()

	mu       sync.Mutex
	inflight int
	peak     int
	matches  int
}

func (f *faultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.inflight++
	f.peak = max(f.peak, f.inflight)
	fail := false
	if f.target != "" && strings.Contains(req.URL.String(), f.target) {
		f.matches++
		fail = f.failAll || f.matches <= f.failFirst
	}
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.inflight--
		f.mu.Unlock()
	}()

	if fail {
		if f.onFail != nil {
			f.onFail()
		}
		// dberror classifies this as connectivity, i.e. transient.
		return nil, errors.New("connection reset by peer")
	}
	return f.base.RoundTrip(req)
}

func (f *faultTransport) peakInflight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

// matched returns how many requests for the target device the transport saw.
func (f *faultTransport) matched() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.matches
}

// minioBackend is a MinIO container plus a raw S3 client for populating it.
type minioBackend struct {
	endpoint string
	username string
	password string
	client   *s3.Client
}

// startMinIO boots a MinIO container for the calling test.
func startMinIO(t *testing.T) *minioBackend {
	t.Helper()
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

	mio := &minioBackend{
		endpoint: "http://" + endpoint,
		username: ctr.Username,
		password: ctr.Password,
	}
	mio.client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(mio.endpoint)
		o.UsePathStyle = true
	})
	return mio
}

// putSnapshot uploads one envelope-wrapped snapshot at the key layout the
// state-ingest server writes.
func (m *minioBackend) putSnapshot(t *testing.T, bucket, kind, pubkey string, ts time.Time, rawJSON string) {
	t.Helper()
	body, err := json.Marshal(statecollect.Envelope{
		Metadata: statecollect.Metadata{
			Kind:      kind,
			Timestamp: ts.UTC().Format(time.RFC3339),
			Device:    pubkey,
		},
		Data: json.RawMessage(rawJSON),
	})
	require.NoError(t, err)

	key := buildStateIngestKey(kind, pubkey, ts)
	_, err = m.client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err, "put %s", key)
}

// newSource builds an S3Source whose HTTP calls go through rt. The AWS SDK's own
// retryer is disabled so request counts reflect only the source's retry layer;
// in production the SDK retryer sits underneath and can only help.
func (m *minioBackend) newSource(t *testing.T, bucket string, rt http.RoundTripper) *S3Source {
	t.Helper()
	awsCfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(awscreds.NewStaticCredentialsProvider(m.username, m.password, "")),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(m.endpoint)
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: rt}
		o.Retryer = aws.NopRetryer{}
	})
	return &S3Source{client: client, bucket: bucket, lookbackDays: 3}
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
