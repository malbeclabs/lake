package pcapwarehouse

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Warehouse reads the pcap warehouse bucket's layout:
//
//	<key prefix>/<host>-<public IP>/YYYY/MM/DD/HH/{capture_<group>_<n>.pcap, manifest_<ts>.yaml}
type Warehouse interface {
	// ListRecorders returns the recorder directory names under the key prefix.
	ListRecorders(ctx context.Context) ([]string, error)
	// ListHours returns the recorder's hour directories whose hour ends after
	// since, oldest first. A zero since returns every hour.
	ListHours(ctx context.Context, recorder string, since time.Time) ([]time.Time, error)
	// ListManifests returns the full keys of the manifests in one hour directory.
	ListManifests(ctx context.Context, recorder string, hour time.Time) ([]string, error)
	// GetObject reads one object.
	GetObject(ctx context.Context, key string) ([]byte, error)
}

// S3WarehouseConfig configures the warehouse bucket reader.
type S3WarehouseConfig struct {
	Bucket      string
	Region      string
	KeyPrefix   string // the network directory, e.g. mainnet-beta
	EndpointURL string
}

// S3Warehouse implements Warehouse against the bucket.
type S3Warehouse struct {
	client    *s3.Client
	bucket    string
	keyPrefix string
}

// NewS3Warehouse constructs a warehouse bucket reader using the default AWS credential chain.
func NewS3Warehouse(ctx context.Context, cfg S3WarehouseConfig) (*S3Warehouse, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("pcapwarehouse: bucket is required")
	}
	if cfg.KeyPrefix == "" {
		return nil, fmt.Errorf("pcapwarehouse: key prefix is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("pcapwarehouse: load AWS config: %w", err)
	}
	opts := []func(*s3.Options){
		func(o *s3.Options) { o.UsePathStyle = true },
	}
	if cfg.EndpointURL != "" {
		opts = append(opts, func(o *s3.Options) { o.BaseEndpoint = aws.String(cfg.EndpointURL) })
	}
	return &S3Warehouse{
		client:    s3.NewFromConfig(awsCfg, opts...),
		bucket:    cfg.Bucket,
		keyPrefix: strings.Trim(cfg.KeyPrefix, "/") + "/",
	}, nil
}

func (s *S3Warehouse) ListRecorders(ctx context.Context) ([]string, error) {
	return s.listChildren(ctx, s.keyPrefix)
}

// ListHours walks the recorder's YYYY/MM/DD/HH tree with delimiter listings, skipping
// any year, month or day that ends at or before since. A steady-state call therefore
// costs a handful of requests however much history the recorder holds, and a backfill
// never enumerates hours in a span where the recorder was not uploading.
func (s *S3Warehouse) ListHours(ctx context.Context, recorder string, since time.Time) ([]time.Time, error) {
	since = since.UTC()
	root := s.keyPrefix + recorder + "/"
	var hours []time.Time

	years, err := s.listNumeric(ctx, root)
	if err != nil {
		return nil, err
	}
	for _, y := range years {
		if !endsAfter(time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(1, 0, 0), since) {
			continue
		}
		yp := fmt.Sprintf("%s%04d/", root, y)
		months, err := s.listNumeric(ctx, yp)
		if err != nil {
			return nil, err
		}
		for _, m := range months {
			if !endsAfter(time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0), since) {
				continue
			}
			mp := fmt.Sprintf("%s%02d/", yp, m)
			days, err := s.listNumeric(ctx, mp)
			if err != nil {
				return nil, err
			}
			for _, d := range days {
				if !endsAfter(time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1), since) {
					continue
				}
				dp := fmt.Sprintf("%s%02d/", mp, d)
				hs, err := s.listNumeric(ctx, dp)
				if err != nil {
					return nil, err
				}
				for _, h := range hs {
					t := time.Date(y, time.Month(m), d, h, 0, 0, 0, time.UTC)
					if endsAfter(t.Add(time.Hour), since) {
						hours = append(hours, t)
					}
				}
			}
		}
	}
	slices.SortFunc(hours, func(a, b time.Time) int { return a.Compare(b) })
	return hours, nil
}

func endsAfter(end, since time.Time) bool {
	return since.IsZero() || end.After(since)
}

func (s *S3Warehouse) ListManifests(ctx context.Context, recorder string, hour time.Time) ([]string, error) {
	prefix := s.keyPrefix + recorder + "/" + hourDir(hour) + "/manifest_"
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("pcapwarehouse: list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if k := aws.ToString(obj.Key); strings.HasSuffix(k, ".yaml") {
				keys = append(keys, k)
			}
		}
	}
	slices.Sort(keys)
	return keys, nil
}

func (s *S3Warehouse) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("pcapwarehouse: get %s: %w", key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("pcapwarehouse: read %s: %w", key, err)
	}
	return data, nil
}

// listChildren returns the names of the immediate subdirectories of prefix.
func (s *S3Warehouse) listChildren(ctx context.Context, prefix string) ([]string, error) {
	var names []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("pcapwarehouse: list %s: %w", prefix, err)
		}
		for _, cp := range page.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(cp.Prefix), prefix), "/")
			if name != "" {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	return names, nil
}

// listNumeric returns the numeric subdirectories of prefix, ascending. Anything else
// under a date level is not part of the layout and is ignored.
func (s *S3Warehouse) listNumeric(ctx context.Context, prefix string) ([]int, error) {
	names, err := s.listChildren(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, len(names))
	for _, n := range names {
		if v, err := strconv.Atoi(n); err == nil {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out, nil
}
