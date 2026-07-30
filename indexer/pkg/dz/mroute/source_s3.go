package mroute

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/indexer/pkg/dz/statecollect"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// maxConcurrentFetches bounds in-flight per-device S3 reads, matching the limit
// used by the serviceability and shreds views.
const maxConcurrentFetches = 10

// deviceFetchRetry bounds per-device retries for transient S3 failures. The fetch
// is all-or-nothing (see Store.Sync), so without a retry one blip among the
// hundreds of calls a cycle makes aborts the whole thing. dberror.IsTransient
// treats context errors as non-transient, so a cancelled activity aborts
// immediately rather than retrying into its deadline.
var deviceFetchRetry = dberror.RetryConfig{
	MaxAttempts: 3,
	BaseBackoff: 100 * time.Millisecond,
	MaxBackoff:  time.Second,
}

// SnapshotKind is the state-collect kind name for mroute dumps. It matches the
// key used by the telemetry state-ingest server when it presigns the
// device-side PUT.
const SnapshotKind = "ip-mroute"

// S3SourceConfig configures the state-collect mroute snapshot source.
type S3SourceConfig struct {
	Bucket      string // S3 bucket name written to by the state-ingest server
	Region      string // AWS region
	KeyPrefix   string // Optional path prefix matching the state-ingest server's BucketPathPrefix
	EndpointURL string // Optional custom endpoint (for MinIO in tests)

	// LookbackDays controls how many UTC days back to scan for snapshots when
	// finding the latest per device. Three covers clock skew and the
	// midnight-UTC boundary; tests can shorten this.
	LookbackDays int
}

// S3Source implements Source against an S3 bucket populated by the telemetry
// state-ingest server. Snapshot keys follow the pattern
//
//	<KeyPrefix>/snapshots/<SnapshotKind>/device=<pubkey>/date=YYYY-MM-DD/hour=HH/<ts>.json
type S3Source struct {
	client       *s3.Client
	bucket       string
	keyPrefix    string // already stripped of trailing slash; empty when no prefix
	lookbackDays int
}

// NewS3Source constructs a state-collect S3 source using the default AWS
// credential chain (env / IAM role / profile). The bucket is expected to be
// the same one the telemetry state-ingest server writes to.
func NewS3Source(ctx context.Context, cfg S3SourceConfig) (*S3Source, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("mroute: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.LookbackDays <= 0 {
		cfg.LookbackDays = 3
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("mroute: load AWS config: %w", err)
	}

	clientOpts := []func(*s3.Options){
		func(o *s3.Options) { o.UsePathStyle = true },
	}
	if cfg.EndpointURL != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}

	return &S3Source{
		client:       s3.NewFromConfig(awsCfg, clientOpts...),
		bucket:       cfg.Bucket,
		keyPrefix:    strings.TrimRight(cfg.KeyPrefix, "/"),
		lookbackDays: cfg.LookbackDays,
	}, nil
}

// snapshotsPrefix returns the key prefix that contains all device subprefixes
// for this snapshot kind, with a trailing slash.
func (s *S3Source) snapshotsPrefix() string {
	if s.keyPrefix == "" {
		return "snapshots/" + SnapshotKind + "/"
	}
	return s.keyPrefix + "/snapshots/" + SnapshotKind + "/"
}

// FetchLatest enumerates device subprefixes under the snapshots/ip-mroute/
// path and returns the most recent dump per device within the lookback
// window. Each S3 object is a statecollect.Envelope; this method unwraps
// it so callers receive only the inner Arista JSON in Dump.RawJSON.
func (s *S3Source) FetchLatest(ctx context.Context) ([]*Dump, error) {
	devices, err := s.listDevicePubkeys(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	// Per-device reads are independent. Collect by device index so the result
	// order tracks the device listing regardless of completion order.
	perDevice := make([]*Dump, len(devices))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetches)
	for i, pubkey := range devices {
		g.Go(func() error {
			dump, err := s.fetchDevice(gctx, pubkey, now)
			if err != nil {
				return err
			}
			perDevice[i] = dump
			return nil
		})
	}
	// One failed device aborts the whole fetch: Store.Sync writes with
	// MissingMeansDeleted=true, so returning a partial device set would
	// tombstone the missing devices' prior state.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	dumps := make([]*Dump, 0, len(perDevice))
	for _, d := range perDevice {
		if d != nil {
			dumps = append(dumps, d)
		}
	}
	return dumps, nil
}

// fetchDevice reads the latest snapshot for one device, retrying transient S3
// failures. A nil Dump with a nil error means the device has no snapshot in the
// lookback window.
func (s *S3Source) fetchDevice(ctx context.Context, pubkey string, now time.Time) (*Dump, error) {
	return dberror.Retry(ctx, deviceFetchRetry, func() (*Dump, error) {
		key, keyTS, err := s.latestKeyForDevice(ctx, pubkey, now)
		if err != nil {
			return nil, err
		}
		if key == "" {
			return nil, nil
		}
		body, err := s.getObject(ctx, key)
		if err != nil {
			return nil, err
		}

		var env statecollect.Envelope
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("mroute: decode envelope %s: %w", key, err)
		}
		if env.Metadata.Kind != "" && env.Metadata.Kind != SnapshotKind {
			return nil, fmt.Errorf("mroute: envelope kind mismatch in %s: got %q, want %q",
				key, env.Metadata.Kind, SnapshotKind)
		}

		// Prefer the envelope's authoritative metadata; fall back to what
		// we parsed out of the S3 key when the envelope is missing fields.
		devicePK := pubkey
		if env.Metadata.Device != "" {
			devicePK = env.Metadata.Device
		}
		snapTS := keyTS
		if env.Metadata.Timestamp != "" {
			if parsed, err := time.Parse(time.RFC3339, env.Metadata.Timestamp); err == nil {
				snapTS = parsed.UTC()
			}
		}

		return &Dump{
			FetchedAt:    time.Now().UTC(),
			SnapshotTS:   snapTS,
			DevicePubkey: devicePK,
			RawJSON:      env.Data,
			FileName:     key,
		}, nil
	})
}

// listDevicePubkeys uses delimiter listing to enumerate the device=<pubkey>
// subprefixes under the snapshots/<kind>/ path. It does not page through
// individual snapshot objects.
func (s *S3Source) listDevicePubkeys(ctx context.Context) ([]string, error) {
	prefix := s.snapshotsPrefix()
	var pubkeys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("mroute: list device prefixes: %w", err)
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			// Extract pubkey from `<prefix>device=<pubkey>/`
			rest := strings.TrimPrefix(*cp.Prefix, prefix)
			rest = strings.TrimSuffix(rest, "/")
			if pk := strings.TrimPrefix(rest, "device="); pk != rest && pk != "" {
				pubkeys = append(pubkeys, pk)
			}
		}
	}
	return pubkeys, nil
}

// latestKeyForDevice scans the last LookbackDays date partitions for a single
// device and returns the alphabetically-greatest key, with the snapshot
// timestamp parsed from the filename. The filename format the state-ingest
// server writes is `YYYYMMDDTHHMMSSZ.json`.
func (s *S3Source) latestKeyForDevice(ctx context.Context, pubkey string, now time.Time) (string, time.Time, error) {
	base := s.snapshotsPrefix() + "device=" + pubkey + "/"
	var latestKey string
	for daysBack := 0; daysBack < s.lookbackDays; daysBack++ {
		date := now.AddDate(0, 0, -daysBack).Format("2006-01-02")
		prefix := base + "date=" + date + "/"
		key, err := s.latestKeyUnderPrefix(ctx, prefix)
		if err != nil {
			return "", time.Time{}, err
		}
		if key > latestKey {
			latestKey = key
		}
	}
	if latestKey == "" {
		return "", time.Time{}, nil
	}
	return latestKey, snapshotTSFromKey(latestKey), nil
}

func (s *S3Source) latestKeyUnderPrefix(ctx context.Context, prefix string) (string, error) {
	var latestKey string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("mroute: list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			if *obj.Key > latestKey {
				latestKey = *obj.Key
			}
		}
	}
	return latestKey, nil
}

func (s *S3Source) getObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("mroute: get %s: %w", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("mroute: read %s: %w", key, err)
	}
	return body, nil
}

// snapshotTSFromKey parses the `YYYYMMDDTHHMMSSZ.json` filename written by
// the state-ingest server back into a time.Time. Returns zero on any parse
// failure so callers can fall back to the dump's FetchedAt.
func snapshotTSFromKey(key string) time.Time {
	slash := strings.LastIndex(key, "/")
	name := key[slash+1:]
	name = strings.TrimSuffix(name, ".json")
	t, err := time.Parse("20060102T150405Z", name)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// Close releases resources. For S3Source, this is a no-op.
func (s *S3Source) Close() error {
	return nil
}
