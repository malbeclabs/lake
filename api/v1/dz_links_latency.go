package v1

import (
	"context"
	"fmt"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// DZLinkLatencyBucket is a single time-bucketed latency measurement for one
// link. The bucket covers the response envelope's BucketSeconds starting at
// TS. Zero values for samples + RTT + jitter + loss indicate the bucket had
// no probe data.
type DZLinkLatencyBucket struct {
	TS string `json:"ts" doc:"RFC3339 bucket start timestamp (UTC)" example:"2026-05-12T00:00:00Z"`

	ASamples     uint64  `json:"a_samples" doc:"Probe samples observed on the A side"`
	ALossPct     float64 `json:"a_loss_pct" doc:"Packet loss percent on the A side"`
	AAvgRttUs    float64 `json:"a_avg_rtt_us" doc:"Average RTT, A side, microseconds"`
	AMinRttUs    float64 `json:"a_min_rtt_us" doc:"Min RTT, A side, microseconds"`
	AP50RttUs    float64 `json:"a_p50_rtt_us" doc:"P50 RTT, A side, microseconds"`
	AP90RttUs    float64 `json:"a_p90_rtt_us" doc:"P90 RTT, A side, microseconds"`
	AP95RttUs    float64 `json:"a_p95_rtt_us" doc:"P95 RTT, A side, microseconds"`
	AP99RttUs    float64 `json:"a_p99_rtt_us" doc:"P99 RTT, A side, microseconds"`
	AMaxRttUs    float64 `json:"a_max_rtt_us" doc:"Max RTT, A side, microseconds"`
	AAvgJitterUs float64 `json:"a_avg_jitter_us" doc:"Average jitter, A side, microseconds"`
	AMinJitterUs float64 `json:"a_min_jitter_us" doc:"Min jitter, A side, microseconds"`
	AP50JitterUs float64 `json:"a_p50_jitter_us" doc:"P50 jitter, A side, microseconds"`
	AP90JitterUs float64 `json:"a_p90_jitter_us" doc:"P90 jitter, A side, microseconds"`
	AP95JitterUs float64 `json:"a_p95_jitter_us" doc:"P95 jitter, A side, microseconds"`
	AP99JitterUs float64 `json:"a_p99_jitter_us" doc:"P99 jitter, A side, microseconds"`
	AMaxJitterUs float64 `json:"a_max_jitter_us" doc:"Max jitter, A side, microseconds"`

	ZSamples     uint64  `json:"z_samples" doc:"Probe samples observed on the Z side"`
	ZLossPct     float64 `json:"z_loss_pct" doc:"Packet loss percent on the Z side"`
	ZAvgRttUs    float64 `json:"z_avg_rtt_us" doc:"Average RTT, Z side, microseconds"`
	ZMinRttUs    float64 `json:"z_min_rtt_us" doc:"Min RTT, Z side, microseconds"`
	ZP50RttUs    float64 `json:"z_p50_rtt_us" doc:"P50 RTT, Z side, microseconds"`
	ZP90RttUs    float64 `json:"z_p90_rtt_us" doc:"P90 RTT, Z side, microseconds"`
	ZP95RttUs    float64 `json:"z_p95_rtt_us" doc:"P95 RTT, Z side, microseconds"`
	ZP99RttUs    float64 `json:"z_p99_rtt_us" doc:"P99 RTT, Z side, microseconds"`
	ZMaxRttUs    float64 `json:"z_max_rtt_us" doc:"Max RTT, Z side, microseconds"`
	ZAvgJitterUs float64 `json:"z_avg_jitter_us" doc:"Average jitter, Z side, microseconds"`
	ZMinJitterUs float64 `json:"z_min_jitter_us" doc:"Min jitter, Z side, microseconds"`
	ZP50JitterUs float64 `json:"z_p50_jitter_us" doc:"P50 jitter, Z side, microseconds"`
	ZP90JitterUs float64 `json:"z_p90_jitter_us" doc:"P90 jitter, Z side, microseconds"`
	ZP95JitterUs float64 `json:"z_p95_jitter_us" doc:"P95 jitter, Z side, microseconds"`
	ZP99JitterUs float64 `json:"z_p99_jitter_us" doc:"P99 jitter, Z side, microseconds"`
	ZMaxJitterUs float64 `json:"z_max_jitter_us" doc:"Max jitter, Z side, microseconds"`
}

// DZLinkLatency is the per-link entry in the latency listing response.
type DZLinkLatency struct {
	LinkPK               string                `json:"link_pk" doc:"Link pubkey (PDA)"`
	LinkCode             string                `json:"link_code" doc:"Link code"`
	LinkType             string                `json:"link_type" doc:"Link type (e.g. WAN, DZX)"`
	ContributorCode      string                `json:"contributor_code" doc:"Contributor code (A side)"`
	SideZContributorCode string                `json:"side_z_contributor_code" doc:"Contributor code on the Z side"`
	SideADevice          string                `json:"side_a_device" doc:"Device code on the A side"`
	SideZDevice          string                `json:"side_z_device" doc:"Device code on the Z side"`
	SideAMetro           string                `json:"side_a_metro" doc:"Metro code on the A side"`
	SideZMetro           string                `json:"side_z_metro" doc:"Metro code on the Z side"`
	CommittedRttUs       float64               `json:"committed_rtt_us" doc:"Committed RTT SLO, microseconds"`
	CommittedJitterUs    float64               `json:"committed_jitter_us" doc:"Committed jitter SLO, microseconds"`
	Buckets              []DZLinkLatencyBucket `json:"buckets" doc:"Time-ordered buckets (oldest first)"`
}

// DZLinkLatencyResponse is the body returned by the link latency endpoint.
type DZLinkLatencyResponse struct {
	TimeRange     string          `json:"time_range" doc:"Resolved time range preset, when not using an explicit window"`
	BucketSeconds int             `json:"bucket_seconds" doc:"Bucket width in seconds"`
	BucketCount   int             `json:"bucket_count" doc:"Number of buckets per link"`
	TotalLinks    int             `json:"total_links" doc:"Total links matching the filter (before pagination)"`
	LinkLimit     int             `json:"link_limit" doc:"Maximum links returned per page"`
	LinkOffset    int             `json:"link_offset" doc:"Offset into the filtered link set"`
	Links         []DZLinkLatency `json:"links" doc:"Links sorted by link_code"`
}

// DZLinkLatencyInput is the request for the link latency endpoint.
// Multi-value filters accept repeated query params, e.g.
// `?metro_code=NYC&metro_code=LAX`. Within a filter, values are OR'd; across
// filters, AND'd. Contributor and metro filters match either side of the link.
type DZLinkLatencyInput struct {
	LinkPK          []string `query:"link_pk,explode" doc:"Filter to specific link pubkeys"`
	LinkCode        []string `query:"link_code,explode" doc:"Filter by link code (case-insensitive exact)"`
	ContributorCode []string `query:"contributor_code,explode" doc:"Filter by contributor code (case-insensitive exact); matches either side"`
	MetroCode       []string `query:"metro_code,explode" doc:"Filter by metro code (case-insensitive exact); matches either side"`
	Range           string   `query:"range" enum:"1h,3h,6h,12h,24h,3d,7d,14d,30d" default:"24h" doc:"Time range preset. Ignored if start_time and end_time are both set."`
	StartTime       int64    `query:"start_time" minimum:"0" default:"0" doc:"Custom window start (unix seconds). If set together with end_time, overrides range."`
	EndTime         int64    `query:"end_time" minimum:"0" default:"0" doc:"Custom window end (unix seconds)."`
	Bucket          string   `query:"bucket" enum:"auto,10s,30s,1m,5m,10m,15m,30m,1h,4h,12h,1d" default:"auto" doc:"Bucket size. 'auto' picks granularity based on the window. Sub-5-minute buckets are capped to short windows."`
	LinkLimit       int      `query:"link_limit" minimum:"1" maximum:"500" default:"100" doc:"Maximum links returned per page. Each link includes the full bucket window."`
	LinkOffset      int      `query:"link_offset" minimum:"0" default:"0" doc:"Offset into the filtered, sorted link set"`
}

// DZLinkLatencyOutput wraps the response body for huma.
type DZLinkLatencyOutput struct {
	Body DZLinkLatencyResponse
}

func registerDZLinksLatency(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-dz-links-latency",
		Method:      "GET",
		Path:        "/dz/links/latency",
		Summary:     "List latency time-series for DZ network links",
		Description: "Returns per-bucket A-side and Z-side RTT percentiles, jitter percentiles, packet loss, and sample counts for DoubleZero network links. Use the filter query params to narrow to specific links, contributors, or metros — within a filter values are OR'd; across filters AND'd.",
		Tags:        []string{"DZ/Links"},
	}, func(ctx context.Context, input *DZLinkLatencyInput) (*DZLinkLatencyOutput, error) {
		opts := handlers.LinkLatencyOptions{
			TimeRange: input.Range,
			Bucket:    input.Bucket,
			Filter: handlers.LinkLatencyFilter{
				LinkPKs:          input.LinkPK,
				LinkCodes:        input.LinkCode,
				ContributorCodes: input.ContributorCode,
				MetroCodes:       input.MetroCode,
			},
		}

		var windowDur time.Duration
		if input.StartTime > 0 && input.EndTime > 0 {
			st := time.Unix(input.StartTime, 0).UTC()
			et := time.Unix(input.EndTime, 0).UTC()
			if !et.After(st) {
				return nil, huma.Error422UnprocessableEntity("end_time must be after start_time")
			}
			opts.StartTime = &st
			opts.EndTime = &et
			windowDur = et.Sub(st)
		} else {
			windowDur = rangePresetToDuration(input.Range)
		}

		// Sub-5-minute buckets query the raw fact table. We cap them at the
		// same windows `bucket=auto` would have chosen, so callers can't ask
		// for `bucket=10s&range=30d` and pull 259k buckets per link out of
		// raw facts.
		if err := validateRawBucketWindow(input.Bucket, windowDur); err != nil {
			return nil, huma.Error422UnprocessableEntity(err.Error())
		}

		result, err := api.FetchLinkLatency(ctx, opts)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch link latency", err)
		}

		total := len(result.Links)
		start := input.LinkOffset
		if start > total {
			start = total
		}
		end := start + input.LinkLimit
		if end > total {
			end = total
		}
		page := result.Links[start:end]

		links := make([]DZLinkLatency, len(page))
		for i, link := range page {
			links[i] = toDZLinkLatency(link)
		}

		return &DZLinkLatencyOutput{Body: DZLinkLatencyResponse{
			TimeRange:     result.TimeRange,
			BucketSeconds: result.BucketSeconds,
			BucketCount:   result.BucketCount,
			TotalLinks:    total,
			LinkLimit:     input.LinkLimit,
			LinkOffset:    input.LinkOffset,
			Links:         links,
		}}, nil
	})
}

func toDZLinkLatency(r *handlers.LinkMetricsResponse) DZLinkLatency {
	buckets := make([]DZLinkLatencyBucket, len(r.Buckets))
	for i, b := range r.Buckets {
		buckets[i] = DZLinkLatencyBucket{TS: b.TS}
		if b.Latency == nil {
			continue
		}
		buckets[i].ASamples = b.Latency.ASamples
		buckets[i].ALossPct = b.Latency.ALossPct
		buckets[i].AAvgRttUs = b.Latency.AAvgRttUs
		buckets[i].AMinRttUs = b.Latency.AMinRttUs
		buckets[i].AP50RttUs = b.Latency.AP50RttUs
		buckets[i].AP90RttUs = b.Latency.AP90RttUs
		buckets[i].AP95RttUs = b.Latency.AP95RttUs
		buckets[i].AP99RttUs = b.Latency.AP99RttUs
		buckets[i].AMaxRttUs = b.Latency.AMaxRttUs
		buckets[i].AAvgJitterUs = b.Latency.AAvgJitterUs
		buckets[i].AMinJitterUs = b.Latency.AMinJitterUs
		buckets[i].AP50JitterUs = b.Latency.AP50JitterUs
		buckets[i].AP90JitterUs = b.Latency.AP90JitterUs
		buckets[i].AP95JitterUs = b.Latency.AP95JitterUs
		buckets[i].AP99JitterUs = b.Latency.AP99JitterUs
		buckets[i].AMaxJitterUs = b.Latency.AMaxJitterUs

		buckets[i].ZSamples = b.Latency.ZSamples
		buckets[i].ZLossPct = b.Latency.ZLossPct
		buckets[i].ZAvgRttUs = b.Latency.ZAvgRttUs
		buckets[i].ZMinRttUs = b.Latency.ZMinRttUs
		buckets[i].ZP50RttUs = b.Latency.ZP50RttUs
		buckets[i].ZP90RttUs = b.Latency.ZP90RttUs
		buckets[i].ZP95RttUs = b.Latency.ZP95RttUs
		buckets[i].ZP99RttUs = b.Latency.ZP99RttUs
		buckets[i].ZMaxRttUs = b.Latency.ZMaxRttUs
		buckets[i].ZAvgJitterUs = b.Latency.ZAvgJitterUs
		buckets[i].ZMinJitterUs = b.Latency.ZMinJitterUs
		buckets[i].ZP50JitterUs = b.Latency.ZP50JitterUs
		buckets[i].ZP90JitterUs = b.Latency.ZP90JitterUs
		buckets[i].ZP95JitterUs = b.Latency.ZP95JitterUs
		buckets[i].ZP99JitterUs = b.Latency.ZP99JitterUs
		buckets[i].ZMaxJitterUs = b.Latency.ZMaxJitterUs
	}

	return DZLinkLatency{
		LinkPK:               r.LinkPK,
		LinkCode:             r.LinkCode,
		LinkType:             r.LinkType,
		ContributorCode:      r.ContributorCode,
		SideZContributorCode: r.SideZContributorCode,
		SideADevice:          r.SideADevice,
		SideZDevice:          r.SideZDevice,
		SideAMetro:           r.SideAMetro,
		SideZMetro:           r.SideZMetro,
		CommittedRttUs:       r.CommittedRttUs,
		CommittedJitterUs:    r.CommittedJitterUs,
		Buckets:              buckets,
	}
}

// rangePresetToDuration mirrors the enum on DZLinkLatencyInput.Range.
func rangePresetToDuration(preset string) time.Duration {
	switch preset {
	case "1h":
		return time.Hour
	case "3h":
		return 3 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "7d":
		return 7 * 24 * time.Hour
	case "14d":
		return 14 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// validateRawBucketWindow caps sub-5-minute buckets to the same window sizes
// that `bucket=auto` would pick. Without this, a caller could combine a
// fine-grained bucket with a multi-day window and force a heavy raw fact-table
// scan returning tens of thousands of buckets per link.
func validateRawBucketWindow(bucket string, window time.Duration) error {
	var maxWindow time.Duration
	switch bucket {
	case "10s":
		maxWindow = time.Hour
	case "30s":
		maxWindow = 3 * time.Hour
	case "1m":
		maxWindow = 6 * time.Hour
	default:
		return nil
	}
	if window > maxWindow {
		return fmt.Errorf("bucket=%s is only allowed for windows up to %s", bucket, formatHumanDuration(maxWindow))
	}
	return nil
}

func formatHumanDuration(d time.Duration) string {
	if d >= 24*time.Hour && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
	if d >= time.Hour && d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}
