package v1

import (
	"context"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/malbeclabs/lake/api/handlers"
)

// DZMetroPairLatencyBucket is a single time-bucketed comparison row for one
// normalized metro pair. Zero values on a side indicate that side had no
// samples in the bucket.
type DZMetroPairLatencyBucket struct {
	TS string `json:"ts" doc:"RFC3339 bucket start timestamp (UTC)" example:"2026-05-13T00:00:00Z"`

	DZSamples     uint64  `json:"dz_samples" doc:"DZ probe samples in the bucket (sum across both link directions)"`
	DZAvgRttUs    float64 `json:"dz_avg_rtt_us" doc:"DZ average RTT, microseconds"`
	DZMinRttUs    float64 `json:"dz_min_rtt_us" doc:"DZ min RTT, microseconds"`
	DZP50RttUs    float64 `json:"dz_p50_rtt_us" doc:"DZ p50 RTT, microseconds"`
	DZP90RttUs    float64 `json:"dz_p90_rtt_us" doc:"DZ p90 RTT, microseconds"`
	DZP95RttUs    float64 `json:"dz_p95_rtt_us" doc:"DZ p95 RTT, microseconds"`
	DZP99RttUs    float64 `json:"dz_p99_rtt_us" doc:"DZ p99 RTT, microseconds"`
	DZMaxRttUs    float64 `json:"dz_max_rtt_us" doc:"DZ max RTT, microseconds"`
	DZAvgJitterUs float64 `json:"dz_avg_jitter_us" doc:"DZ average jitter, microseconds"`
	DZMinJitterUs float64 `json:"dz_min_jitter_us" doc:"DZ min jitter, microseconds"`
	DZP50JitterUs float64 `json:"dz_p50_jitter_us" doc:"DZ p50 jitter, microseconds"`
	DZP90JitterUs float64 `json:"dz_p90_jitter_us" doc:"DZ p90 jitter, microseconds"`
	DZP95JitterUs float64 `json:"dz_p95_jitter_us" doc:"DZ p95 jitter, microseconds"`
	DZP99JitterUs float64 `json:"dz_p99_jitter_us" doc:"DZ p99 jitter, microseconds"`
	DZMaxJitterUs float64 `json:"dz_max_jitter_us" doc:"DZ max jitter, microseconds"`
	DZLossPct     float64 `json:"dz_loss_pct" doc:"DZ packet loss percent"`

	InternetSamples     uint64  `json:"internet_samples" doc:"Internet probe samples in the bucket"`
	InternetAvgRttUs    float64 `json:"internet_avg_rtt_us" doc:"Internet average RTT, microseconds"`
	InternetMinRttUs    float64 `json:"internet_min_rtt_us" doc:"Internet min RTT, microseconds"`
	InternetP50RttUs    float64 `json:"internet_p50_rtt_us" doc:"Internet p50 RTT, microseconds"`
	InternetP90RttUs    float64 `json:"internet_p90_rtt_us" doc:"Internet p90 RTT, microseconds"`
	InternetP95RttUs    float64 `json:"internet_p95_rtt_us" doc:"Internet p95 RTT, microseconds"`
	InternetP99RttUs    float64 `json:"internet_p99_rtt_us" doc:"Internet p99 RTT, microseconds"`
	InternetMaxRttUs    float64 `json:"internet_max_rtt_us" doc:"Internet max RTT, microseconds"`
	InternetAvgJitterUs float64 `json:"internet_avg_jitter_us" doc:"Internet average jitter, microseconds"`
	InternetMinJitterUs float64 `json:"internet_min_jitter_us" doc:"Internet min jitter, microseconds"`
	InternetP50JitterUs float64 `json:"internet_p50_jitter_us" doc:"Internet p50 jitter, microseconds"`
	InternetP90JitterUs float64 `json:"internet_p90_jitter_us" doc:"Internet p90 jitter, microseconds"`
	InternetP95JitterUs float64 `json:"internet_p95_jitter_us" doc:"Internet p95 jitter, microseconds"`
	InternetP99JitterUs float64 `json:"internet_p99_jitter_us" doc:"Internet p99 jitter, microseconds"`
	InternetMaxJitterUs float64 `json:"internet_max_jitter_us" doc:"Internet max jitter, microseconds"`
}

// DZMetroPairLatency is the per-pair entry in the listing response. Direction
// is normalized so each pair appears once: metro_a_code < metro_b_code
// lexicographically (least/greatest). A/B are labels, not directions — the
// underlying samples are bidirectional.
type DZMetroPairLatency struct {
	MetroACode string                     `json:"metro_a_code" doc:"Lower-codepoint metro code in the normalized pair"`
	MetroAName string                     `json:"metro_a_name" doc:"Display name for metro_a"`
	MetroBCode string                     `json:"metro_b_code" doc:"Higher-codepoint metro code in the normalized pair"`
	MetroBName string                     `json:"metro_b_name" doc:"Display name for metro_b"`
	Buckets    []DZMetroPairLatencyBucket `json:"buckets" doc:"Time-ordered buckets (oldest first)"`
}

// DZMetroPairLatencyResponse is the body returned by the metro-pair latency endpoint.
type DZMetroPairLatencyResponse struct {
	TimeRange     string               `json:"time_range" doc:"Resolved time range preset, when not using an explicit window"`
	BucketSeconds int                  `json:"bucket_seconds" doc:"Bucket width in seconds"`
	BucketCount   int                  `json:"bucket_count" doc:"Number of buckets per pair"`
	TotalPairs    int                  `json:"total_pairs" doc:"Total metro pairs matching the filter (before pagination)"`
	PairLimit     int                  `json:"pair_limit" doc:"Maximum pairs returned per page"`
	PairOffset    int                  `json:"pair_offset" doc:"Offset into the filtered pair set"`
	Pairs         []DZMetroPairLatency `json:"pairs" doc:"Metro pairs sorted by (metro1_code, metro2_code)"`
}

// DZMetroPairLatencyInput is the request for the metro-pair latency endpoint.
// Multi-value filters accept repeated query params, e.g.
// `?metro_code=NYC&metro_code=LAX`. Within a filter, values are OR'd; across
// filters, AND'd. The metro filter matches either side of a pair; the
// data_provider filter only narrows internet-side samples.
type DZMetroPairLatencyInput struct {
	MetroCode    []string `query:"metro_code,explode" doc:"Filter to metro pairs touching any of these metro codes (case-insensitive exact). Matches either side."`
	DataProvider []string `query:"data_provider,explode" doc:"Filter internet-side samples by data provider (e.g. ripe-atlas, wheresitup). DZ-side is unaffected."`
	Range        string   `query:"range" enum:"1h,3h,6h,12h,24h,3d,7d,14d,30d" default:"24h" doc:"Time range preset. Ignored if start_time and end_time are both set."`
	StartTime    int64    `query:"start_time" minimum:"0" default:"0" doc:"Custom window start (unix seconds). If set together with end_time, overrides range."`
	EndTime      int64    `query:"end_time" minimum:"0" default:"0" doc:"Custom window end (unix seconds)."`
	Bucket       string   `query:"bucket" enum:"auto,10s,30s,1m,5m,10m,15m,30m,1h,4h,12h,1d" default:"auto" doc:"Bucket size. 'auto' picks granularity based on the window. Sub-5-minute buckets are capped to short windows."`
	PairLimit    int      `query:"pair_limit" minimum:"1" maximum:"500" default:"100" doc:"Maximum pairs returned per page. Each pair includes the full bucket window."`
	PairOffset   int      `query:"pair_offset" minimum:"0" default:"0" doc:"Offset into the filtered, sorted pair set."`
}

// DZMetroPairLatencyOutput wraps the response body for huma.
type DZMetroPairLatencyOutput struct {
	Body DZMetroPairLatencyResponse
}

func registerDZMetroPairsLatency(humaAPI huma.API, api *handlers.API) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-dz-metro-pairs-latency",
		Method:      "GET",
		Path:        "/dz/metro-pairs/latency",
		Summary:     "List DZ vs public-internet latency time-series for metro pairs",
		Description: "Returns per-bucket DZ and internet RTT/jitter percentiles and sample counts for each metro pair with samples in the window. Direction is normalized (one row per pair, sorted lexicographically so metro_a_code < metro_b_code; A/B are labels, not directions). Use the filter query params to narrow to specific metros or internet data providers — within a filter values are OR'd; across filters AND'd. The metro filter matches either side of the pair; data_provider only affects the internet side.",
		Tags:        []string{"DZ/Metro Pairs"},
	}, func(ctx context.Context, input *DZMetroPairLatencyInput) (*DZMetroPairLatencyOutput, error) {
		opts := handlers.MetroPairLatencyOptions{
			TimeRange: input.Range,
			Bucket:    input.Bucket,
			Filter: handlers.MetroPairLatencyFilter{
				MetroCodes:    input.MetroCode,
				DataProviders: input.DataProvider,
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

		if err := validateRawBucketWindow(input.Bucket, windowDur); err != nil {
			return nil, huma.Error422UnprocessableEntity(err.Error())
		}

		result, err := api.FetchMetroPairLatency(ctx, opts)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to fetch metro pair latency", err)
		}

		total := len(result.Pairs)
		start := input.PairOffset
		if start > total {
			start = total
		}
		end := start + input.PairLimit
		if end > total {
			end = total
		}
		page := result.Pairs[start:end]

		pairs := make([]DZMetroPairLatency, len(page))
		for i, p := range page {
			pairs[i] = toDZMetroPairLatency(p)
		}

		return &DZMetroPairLatencyOutput{Body: DZMetroPairLatencyResponse{
			TimeRange:     result.TimeRange,
			BucketSeconds: result.BucketSeconds,
			BucketCount:   result.BucketCount,
			TotalPairs:    total,
			PairLimit:     input.PairLimit,
			PairOffset:    input.PairOffset,
			Pairs:         pairs,
		}}, nil
	})
}

func toDZMetroPairLatency(p *handlers.MetroPair) DZMetroPairLatency {
	buckets := make([]DZMetroPairLatencyBucket, len(p.Buckets))
	for i, b := range p.Buckets {
		buckets[i] = DZMetroPairLatencyBucket{
			TS: b.TS.Format(time.RFC3339),

			DZSamples:     b.DZSamples,
			DZAvgRttUs:    b.DZAvgRttUs,
			DZMinRttUs:    b.DZMinRttUs,
			DZP50RttUs:    b.DZP50RttUs,
			DZP90RttUs:    b.DZP90RttUs,
			DZP95RttUs:    b.DZP95RttUs,
			DZP99RttUs:    b.DZP99RttUs,
			DZMaxRttUs:    b.DZMaxRttUs,
			DZAvgJitterUs: b.DZAvgJitterUs,
			DZMinJitterUs: b.DZMinJitterUs,
			DZP50JitterUs: b.DZP50JitterUs,
			DZP90JitterUs: b.DZP90JitterUs,
			DZP95JitterUs: b.DZP95JitterUs,
			DZP99JitterUs: b.DZP99JitterUs,
			DZMaxJitterUs: b.DZMaxJitterUs,
			DZLossPct:     b.DZLossPct,

			InternetSamples:     b.InternetSamples,
			InternetAvgRttUs:    b.InternetAvgRttUs,
			InternetMinRttUs:    b.InternetMinRttUs,
			InternetP50RttUs:    b.InternetP50RttUs,
			InternetP90RttUs:    b.InternetP90RttUs,
			InternetP95RttUs:    b.InternetP95RttUs,
			InternetP99RttUs:    b.InternetP99RttUs,
			InternetMaxRttUs:    b.InternetMaxRttUs,
			InternetAvgJitterUs: b.InternetAvgJitterUs,
			InternetMinJitterUs: b.InternetMinJitterUs,
			InternetP50JitterUs: b.InternetP50JitterUs,
			InternetP90JitterUs: b.InternetP90JitterUs,
			InternetP95JitterUs: b.InternetP95JitterUs,
			InternetP99JitterUs: b.InternetP99JitterUs,
			InternetMaxJitterUs: b.InternetMaxJitterUs,
		}
	}

	return DZMetroPairLatency{
		MetroACode: p.MetroACode,
		MetroAName: p.MetroAName,
		MetroBCode: p.MetroBCode,
		MetroBName: p.MetroBName,
		Buckets:    buckets,
	}
}
