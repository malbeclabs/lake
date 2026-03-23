package isis

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// DefaultBucket is the default S3 bucket for IS-IS dumps.
	DefaultBucket = "doublezero-mn-beta-isis-db"
	// DefaultRegion is the default AWS region for the IS-IS bucket.
	DefaultRegion = "us-east-1"
)

// S3SourceConfig configures the S3 source.
type S3SourceConfig struct {
	Bucket      string // S3 bucket name
	Region      string // AWS region
	EndpointURL string // Optional custom endpoint (for MinIO testing)
}

// S3Source implements Source using AWS S3 with anonymous access.
type S3Source struct {
	client *s3.Client
	bucket string
}

// NewS3Source creates a new S3 source with anonymous credentials.
func NewS3Source(ctx context.Context, cfg S3SourceConfig) (*S3Source, error) {
	if cfg.Bucket == "" {
		cfg.Bucket = DefaultBucket
	}
	if cfg.Region == "" {
		cfg.Region = DefaultRegion
	}

	// Configure AWS SDK with anonymous credentials for public bucket
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Build S3 client options
	clientOpts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = true // Required for MinIO compatibility
		},
	}

	// Add custom endpoint if specified (for testing with MinIO)
	if cfg.EndpointURL != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)

	return &S3Source{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// FetchLatest retrieves the most recent IS-IS dump from S3.
// Files are named with timestamp prefixes (YYYY-MM-DDTHH-MM-SSZ_upload_data.json),
// so the latest file is the alphabetically last one.
//
// To avoid listing the entire bucket (which can exceed the 1000-object page
// limit), we prefix the listing with today's date. If no files exist for
// today yet (e.g. just after midnight UTC), we fall back to yesterday.
func (s *S3Source) FetchLatest(ctx context.Context) (*Dump, error) {
	// List the last 3 days to handle clock skew, delayed uploads, and
	// the window around midnight UTC. Pick the latest key across all days.
	now := time.Now().UTC()
	var latestKey string
	for daysBack := 0; daysBack < 3; daysBack++ {
		prefix := now.AddDate(0, 0, -daysBack).Format("2006-01-02")
		key, err := s.latestKeyForDate(ctx, prefix)
		if err != nil {
			return nil, err
		}
		if key > latestKey {
			latestKey = key
		}
	}
	if latestKey == "" {
		return nil, fmt.Errorf("no objects found in bucket %s", s.bucket)
	}

	// Fetch the latest object
	getOutput, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(latestKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", latestKey, err)
	}
	defer getOutput.Body.Close()

	data, err := io.ReadAll(getOutput.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return &Dump{
		FetchedAt: time.Now(),
		RawJSON:   data,
		FileName:  latestKey,
	}, nil
}

// latestKeyForDate lists objects with the given date prefix (YYYY-MM-DD) and
// returns the alphabetically last key, or "" if no objects match.
func (s *S3Source) latestKeyForDate(ctx context.Context, datePrefix string) (string, error) {
	var latestKey string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(datePrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to list objects with prefix %s: %w", datePrefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && *obj.Key > latestKey {
				latestKey = *obj.Key
			}
		}
	}
	return latestKey, nil
}

// Close releases resources. For S3Source, this is a no-op.
func (s *S3Source) Close() error {
	return nil
}
