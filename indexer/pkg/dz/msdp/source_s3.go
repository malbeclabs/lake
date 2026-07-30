package msdp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

// snapshotKinds lists the state-collect kinds this package consumes.
// `ip-msdp-sa-cache` is intentionally absent — the `*-rejected` variant
// returns the same accepted SAs plus rejected ones, so this is the
// single source of truth (see doublezero PR dropping the redundant
// command from defaults).
var snapshotKinds = []string{
	SnapshotKindSummary,
	SnapshotKindPimSACache,
	SnapshotKindSACacheRejected,
}

// S3SourceConfig configures the state-collect MSDP snapshot source.
type S3SourceConfig struct {
	Bucket       string
	Region       string
	KeyPrefix    string
	EndpointURL  string
	LookbackDays int

	// Logger is optional; it defaults to slog.Default().
	Logger *slog.Logger
}

// S3Source implements Source against an S3 bucket populated by the
// telemetry state-ingest server. The same bucket holds snapshots for
// every state-collect kind under their own subprefix; this Source walks
// each MSDP kind in turn.
type S3Source struct {
	client       *s3.Client
	log          *slog.Logger
	bucket       string
	keyPrefix    string
	lookbackDays int
}

// NewS3Source constructs a state-collect S3 source using the default
// AWS credential chain.
func NewS3Source(ctx context.Context, cfg S3SourceConfig) (*S3Source, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("msdp: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.LookbackDays <= 0 {
		cfg.LookbackDays = 3
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("msdp: load AWS config: %w", err)
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
		log:          cfg.Logger,
		bucket:       cfg.Bucket,
		keyPrefix:    strings.TrimRight(cfg.KeyPrefix, "/"),
		lookbackDays: cfg.LookbackDays,
	}, nil
}

// snapshotsPrefix returns the key prefix that contains all device
// subprefixes for the given kind, with a trailing slash.
func (s *S3Source) snapshotsPrefix(kind string) string {
	if s.keyPrefix == "" {
		return "snapshots/" + kind + "/"
	}
	return s.keyPrefix + "/snapshots/" + kind + "/"
}

// FetchLatest walks each MSDP kind's subprefix, returns the most recent
// envelope-wrapped dump per device for each kind. The returned
// Dump.RawJSON is the inner Arista JSON (envelope already unwrapped).
func (s *S3Source) FetchLatest(ctx context.Context) (map[string][]*Dump, error) {
	now := time.Now().UTC()
	out := make(map[string][]*Dump, len(snapshotKinds))
	for _, kind := range snapshotKinds {
		dumps, err := s.fetchLatestForKind(ctx, kind, now)
		if err != nil {
			return nil, err
		}
		out[kind] = dumps
	}
	return out, nil
}

func (s *S3Source) fetchLatestForKind(ctx context.Context, kind string, now time.Time) ([]*Dump, error) {
	devices, err := s.listDevicePubkeys(ctx, kind)
	if err != nil {
		return nil, err
	}
	// Per-device reads are independent. Collect by device index so the result
	// order tracks the device listing regardless of completion order.
	perDevice := make([]*Dump, len(devices))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentFetches)
	for i, pubkey := range devices {
		g.Go(func() error {
			// errgroup still invokes every function queued behind SetLimit
			// after the group context is cancelled. The AWS SDK rejects the
			// call on a cancelled context anyway; returning here makes the
			// skip explicit instead of relying on that.
			if err := gctx.Err(); err != nil {
				return err
			}
			dump, err := s.fetchDevice(gctx, kind, pubkey, now)
			if err != nil {
				return err
			}
			perDevice[i] = dump
			return nil
		})
	}
	// One failed device aborts the whole kind: Store.Sync writes with
	// MissingMeansDeleted=true, so returning a partial device set would
	// tombstone the devices whose reads failed.
	if err := g.Wait(); err != nil {
		return nil, err
	}

	dumps := make([]*Dump, 0, len(perDevice))
	var stale []string
	for i, d := range perDevice {
		if d == nil {
			stale = append(stale, devices[i])
			continue
		}
		dumps = append(dumps, d)
	}
	// A device whose prefix exists but holds no snapshot in the lookback window
	// is dropped here, and MissingMeansDeleted=true then tombstones its rows.
	// That is the intended outcome for a device that stopped uploading — its
	// state is no longer current — but it is the one path where rows disappear
	// without an error, so name the devices rather than let them vanish.
	if len(stale) > 0 {
		s.log.Warn("msdp: devices have no snapshot in the lookback window, their prior state will be tombstoned",
			"kind", kind, "lookback_days", s.lookbackDays, "count", len(stale), "devices", strings.Join(stale, ","))
	}
	return dumps, nil
}

// fetchDevice reads the latest snapshot for one device, retrying transient S3
// failures. A nil Dump with a nil error means the device has no snapshot in the
// lookback window.
func (s *S3Source) fetchDevice(ctx context.Context, kind, pubkey string, now time.Time) (*Dump, error) {
	return dberror.Retry(ctx, deviceFetchRetry, func() (*Dump, error) {
		key, keyTS, err := s.latestKeyForDevice(ctx, kind, pubkey, now)
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
			return nil, fmt.Errorf("msdp: decode envelope %s: %w", key, err)
		}
		if env.Metadata.Kind != "" && env.Metadata.Kind != kind {
			return nil, fmt.Errorf("msdp: envelope kind mismatch in %s: got %q, want %q",
				key, env.Metadata.Kind, kind)
		}

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
			Kind:         kind,
			FetchedAt:    time.Now().UTC(),
			SnapshotTS:   snapTS,
			DevicePubkey: devicePK,
			RawJSON:      env.Data,
			FileName:     key,
		}, nil
	})
}

func (s *S3Source) listDevicePubkeys(ctx context.Context, kind string) ([]string, error) {
	prefix := s.snapshotsPrefix(kind)
	var pubkeys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("msdp: list device prefixes (%s): %w", kind, err)
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			rest := strings.TrimPrefix(*cp.Prefix, prefix)
			rest = strings.TrimSuffix(rest, "/")
			if pk := strings.TrimPrefix(rest, "device="); pk != rest && pk != "" {
				pubkeys = append(pubkeys, pk)
			}
		}
	}
	return pubkeys, nil
}

func (s *S3Source) latestKeyForDevice(ctx context.Context, kind, pubkey string, now time.Time) (string, time.Time, error) {
	base := s.snapshotsPrefix(kind) + "device=" + pubkey + "/"
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
			return "", fmt.Errorf("msdp: list %s: %w", prefix, err)
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
		return nil, fmt.Errorf("msdp: get %s: %w", key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("msdp: read %s: %w", key, err)
	}
	return body, nil
}

// snapshotTSFromKey parses the `YYYYMMDDTHHMMSSZ.json` filename written
// by the state-ingest server back into a time.Time.
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
func (s *S3Source) Close() error { return nil }
