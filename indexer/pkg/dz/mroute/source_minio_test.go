package mroute

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
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
	mio := startMinIO(t)

	const bucket = "doublezero-mainnet-beta-state-collect"
	_, err := mio.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
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
		mio.putSnapshot(t, bucket, f.pubkey, f.ts, f.body)
	}

	// Construct the S3Source pointed at MinIO. The Source uses the default
	// AWS credential chain; t.Setenv injects static creds so it picks them up.
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
	body := `{"vrfs":{"default":{"sparseMode":{"groups":{}},"bidirectional":{"groups":{}}}}}`
	now := time.Now().UTC()
	pubkeys := make([]string, 0, deviceCount)
	for i := range deviceCount {
		pk := fmt.Sprintf("DzPkFanOut%02d111111111111111111111111111111", i)
		pubkeys = append(pubkeys, pk)
		mio.putSnapshot(t, bucket, pk, now, body)
	}

	t.Run("device set and ordering follow the listing, work runs concurrently", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, devices: pubkeys}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		dumps, err := src.FetchLatest(ctx)
		require.NoError(t, err)

		got := make([]string, 0, deviceCount)
		for _, d := range dumps {
			got = append(got, d.DevicePubkey)
		}
		// Ordering is what the serial walk produced: S3 listing order.
		assert.Equal(t, pubkeys, got)

		peak := ft.peakInflight()
		assert.Greater(t, peak, 1, "per-device reads should overlap")
		assert.LessOrEqual(t, peak, maxConcurrentFetches, "fan-out must stay under the limit")
	})

	t.Run("transient mid-walk failure is retried and the walk completes", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, devices: pubkeys, target: pubkeys[7], failFirst: 1}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		dumps, err := src.FetchLatest(ctx)
		require.NoError(t, err)
		require.Len(t, dumps, deviceCount)
		assert.Greater(t, ft.matched(), 1, "the failed device should have been retried")
	})

	t.Run("transient failure listing devices is retried and the walk completes", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, devices: pubkeys, failListFirst: 1}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		dumps, err := src.FetchLatest(ctx)
		require.NoError(t, err, "a blip listing devices must not abort the cycle")
		require.Len(t, dumps, deviceCount)
		assert.Greater(t, ft.listAttempts(), 1, "the device listing should have been retried")
	})

	t.Run("persistent listing failure aborts with bounded retries", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, devices: pubkeys, failListFirst: math.MaxInt}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		dumps, err := src.FetchLatest(ctx)
		require.Error(t, err)
		assert.Nil(t, dumps)
		assert.Equal(t, deviceFetchRetry.MaxAttempts, ft.listAttempts(), "retries must be bounded")
	})

	t.Run("persistent device failure aborts the whole fetch", func(t *testing.T) {
		ft := &faultTransport{base: http.DefaultTransport, devices: pubkeys, target: pubkeys[7], failAll: true}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		dumps, err := src.FetchLatest(ctx)
		require.Error(t, err, "a partial device set would tombstone the missing devices")
		assert.Nil(t, dumps)
		assert.Equal(t, deviceFetchRetry.MaxAttempts, ft.matched(), "retries must be bounded")
	})

	t.Run("cancelled context aborts without exhausting retries", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		ft := &faultTransport{
			base:    http.DefaultTransport,
			devices: pubkeys,
			target:  pubkeys[7],
			failAll: true,
			onFail:  cancel,
		}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		dumps, err := src.FetchLatest(cancelCtx)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, dumps)
		assert.Equal(t, 1, ft.matched(), "retrying a cancelled context is pointless")
	})

	t.Run("no S3 calls are issued after the walk aborts", func(t *testing.T) {
		// pubkeys[0] is in the first batch, so it aborts the group while the other
		// in-flight devices are still held in delay and the rest have not started.
		ft := &faultTransport{
			base:    http.DefaultTransport,
			devices: pubkeys,
			target:  pubkeys[0],
			failAll: true,
			delay:   2 * time.Second,
		}
		src := mio.newSource(t, bucket, ft, laketesting.NewLogger())

		_, err := src.FetchLatest(ctx)
		require.Error(t, err)
		assert.LessOrEqual(t, ft.devicesReached(), maxConcurrentFetches,
			"devices that had not started must not reach S3 once the group is cancelled")
	})

	t.Run("device with no snapshot in the lookback window is dropped and logged", func(t *testing.T) {
		const staleBucket = "doublezero-stale-state-collect"
		_, err := mio.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(staleBucket)})
		require.NoError(t, err)

		const freshPK = "DzPkFresh11111111111111111111111111111111111"
		const stalePK = "DzPkStale22222222222222222222222222222222222"
		mio.putSnapshot(t, staleBucket, freshPK, now, body)
		mio.putSnapshot(t, staleBucket, stalePK, now.AddDate(0, 0, -5), body)

		var logs bytes.Buffer
		log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
		src := mio.newSource(t, staleBucket, http.DefaultTransport, log)

		dumps, err := src.FetchLatest(ctx)
		require.NoError(t, err)
		require.Len(t, dumps, 1)
		assert.Equal(t, freshPK, dumps[0].DevicePubkey)
		// The drop tombstones the device under MissingMeansDeleted, so it must
		// not be silent.
		assert.Contains(t, logs.String(), stalePK)
	})
}

// faultTransport attributes each S3 call to a device by pubkey and injects
// failures for one target device, so tests can drive the per-device retry path
// deterministically. It also records peak concurrency and which devices were
// reached at all.
type faultTransport struct {
	base    http.RoundTripper
	devices []string // pubkeys used to attribute a request to a device

	// target is the device pubkey whose requests fail; empty fails nothing.
	target    string
	failFirst int           // fail this many target requests, then pass through
	failAll   bool          // fail every target request
	delay     time.Duration // held on non-target requests, to keep the fan-out busy
	onFail    func()

	// failListFirst fails this many device-listing requests, then passes them
	// through. The listing runs before the fan-out, so no pubkey identifies it.
	failListFirst int

	mu        sync.Mutex
	inflight  int
	peak      int
	matches   int
	listCalls int
	seen      map[string]struct{}
}

func (f *faultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	url := req.URL.String()

	f.mu.Lock()
	f.inflight++
	f.peak = max(f.peak, f.inflight)
	var device string
	for _, pk := range f.devices {
		if strings.Contains(url, pk) {
			device = pk
			break
		}
	}
	if device != "" {
		if f.seen == nil {
			f.seen = map[string]struct{}{}
		}
		f.seen[device] = struct{}{}
	}
	fail := false
	if device != "" && device == f.target {
		f.matches++
		fail = f.failAll || f.matches <= f.failFirst
	}
	// Every request the source issues either names a device or is the
	// device-prefix listing: newSource pins region and credentials statically,
	// so the SDK makes no discovery calls of its own.
	if device == "" {
		f.listCalls++
		fail = f.listCalls <= f.failListFirst
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
	if f.delay > 0 && device != "" && device != f.target {
		select {
		case <-time.After(f.delay):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
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

// listAttempts returns how many device-listing requests the transport saw.
func (f *faultTransport) listAttempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

// devicesReached returns how many distinct devices issued at least one request.
func (f *faultTransport) devicesReached() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen)
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
func (m *minioBackend) putSnapshot(t *testing.T, bucket, pubkey string, ts time.Time, rawJSON string) {
	t.Helper()
	body, err := json.Marshal(statecollect.Envelope{
		Metadata: statecollect.Metadata{
			Kind:      SnapshotKind,
			Timestamp: ts.UTC().Format(time.RFC3339),
			Device:    pubkey,
		},
		Data: json.RawMessage(rawJSON),
	})
	require.NoError(t, err)

	key := buildStateIngestKey(pubkey, ts)
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
func (m *minioBackend) newSource(t *testing.T, bucket string, rt http.RoundTripper, log *slog.Logger) *S3Source {
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
	return &S3Source{client: client, log: log, bucket: bucket, lookbackDays: 3}
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
