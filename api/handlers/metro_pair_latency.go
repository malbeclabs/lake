package handlers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/malbeclabs/lake/api/metrics"
)

// MetroPairLatencyFilter narrows the set of metro pairs returned by
// FetchMetroPairLatency. Values within a category are OR'd; non-empty
// categories are AND'd together. MetroCodes match either side of a pair.
// DataProviders only apply to the internet-side samples.
type MetroPairLatencyFilter struct {
	MetroCodes    []string
	DataProviders []string
}

// MetroPairLatencyOptions configures FetchMetroPairLatency: time window,
// bucket granularity, and which metro pairs to include.
type MetroPairLatencyOptions struct {
	TimeRange string
	StartTime *time.Time
	EndTime   *time.Time
	Bucket    string
	Filter    MetroPairLatencyFilter
}

// MetroPairLatencyBucket is a single time-bucketed comparison row for one
// metro pair. Zero values on a side indicate that side had no samples in the
// bucket.
type MetroPairLatencyBucket struct {
	TS time.Time

	DZSamples     uint64
	DZAvgRttUs    float64
	DZMinRttUs    float64
	DZP50RttUs    float64
	DZP90RttUs    float64
	DZP95RttUs    float64
	DZP99RttUs    float64
	DZMaxRttUs    float64
	DZAvgJitterUs float64
	DZMinJitterUs float64
	DZP50JitterUs float64
	DZP90JitterUs float64
	DZP95JitterUs float64
	DZP99JitterUs float64
	DZMaxJitterUs float64
	DZLossPct     float64

	InternetSamples     uint64
	InternetAvgRttUs    float64
	InternetMinRttUs    float64
	InternetP50RttUs    float64
	InternetP90RttUs    float64
	InternetP95RttUs    float64
	InternetP99RttUs    float64
	InternetMaxRttUs    float64
	InternetAvgJitterUs float64
	InternetMinJitterUs float64
	InternetP50JitterUs float64
	InternetP90JitterUs float64
	InternetP95JitterUs float64
	InternetP99JitterUs float64
	InternetMaxJitterUs float64
}

// MetroPair is the per-pair entry in the listing. Direction is normalized:
// Metro1Code < Metro2Code lexicographically (via least/greatest), matching the
// existing dz_vs_internet_latency_comparison view.
type MetroPair struct {
	Metro1Code string
	Metro1Name string
	Metro2Code string
	Metro2Name string
	Buckets    []MetroPairLatencyBucket
}

// MetroPairLatencyResult is the response from FetchMetroPairLatency.
type MetroPairLatencyResult struct {
	TimeRange     string
	BucketSeconds int
	BucketCount   int
	Pairs         []*MetroPair
}

// FetchMetroPairLatency returns time-bucketed DZ-vs-internet latency
// comparison data for every metro pair with samples in the window.
//
// DZ side: re-aggregated from link_rollup_5m (≥5m bucket) or raw
// fact_dz_device_link_latency (sub-5m). Percentiles are re-aggregated via
// sample-weighted averaging of the per-5m rollup percentiles in the rollup
// path — same approximation as the link-latency endpoint.
//
// Internet side: always queried from raw fact_dz_internet_metro_latency
// (no rollup table exists for it).
//
// Pairs are normalized direction (one row per pair, least/greatest) matching
// the existing dz_vs_internet_latency_comparison view.
func (a *API) FetchMetroPairLatency(ctx context.Context, opts MetroPairLatencyOptions) (*MetroPairLatencyResult, error) {
	params, err := resolveMetroPairLatencyParams(opts)
	if err != nil {
		return nil, err
	}

	bucketSecs := params.BucketSeconds
	if bucketSecs == 0 {
		bucketSecs = params.BucketMinutes * 60
	}

	start := time.Now()
	rows, query, args, err := queryMetroPairLatency(ctx, a.envDB(ctx), params, opts.Filter)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("metro_pair_latency", duration, err)
	if err != nil {
		return nil, fmt.Errorf("metro pair latency query: %w (query=%s args=%v)", err, query, args)
	}
	defer rows.Close()

	type pairKey struct{ m1, m2 string }
	pairs := make(map[pairKey]*MetroPair)
	bucketsByPair := make(map[pairKey]map[time.Time]*MetroPairLatencyBucket)

	for rows.Next() {
		var (
			metro1, metro2         string
			metro1Name, metro2Name string
			bucketTS               time.Time

			dzSamples uint64
			dzAvg     *float64
			dzMin     *float64
			dzP50     *float64
			dzP90     *float64
			dzP95     *float64
			dzP99     *float64
			dzMax     *float64
			dzAvgJ    *float64
			dzMinJ    *float64
			dzP50J    *float64
			dzP90J    *float64
			dzP95J    *float64
			dzP99J    *float64
			dzMaxJ    *float64
			dzLoss    *float64

			inetSamples uint64
			inetAvg     *float64
			inetMin     *float64
			inetP50     *float64
			inetP90     *float64
			inetP95     *float64
			inetP99     *float64
			inetMax     *float64
			inetAvgJ    *float64
			inetMinJ    *float64
			inetP50J    *float64
			inetP90J    *float64
			inetP95J    *float64
			inetP99J    *float64
			inetMaxJ    *float64
		)

		if err := rows.Scan(
			&metro1, &metro2, &metro1Name, &metro2Name, &bucketTS,
			&dzSamples, &dzAvg, &dzMin, &dzP50, &dzP90, &dzP95, &dzP99, &dzMax,
			&dzAvgJ, &dzMinJ, &dzP50J, &dzP90J, &dzP95J, &dzP99J, &dzMaxJ, &dzLoss,
			&inetSamples, &inetAvg, &inetMin, &inetP50, &inetP90, &inetP95, &inetP99, &inetMax,
			&inetAvgJ, &inetMinJ, &inetP50J, &inetP90J, &inetP95J, &inetP99J, &inetMaxJ,
		); err != nil {
			return nil, fmt.Errorf("metro pair latency scan: %w", err)
		}

		key := pairKey{metro1, metro2}
		p, ok := pairs[key]
		if !ok {
			p = &MetroPair{
				Metro1Code: metro1,
				Metro1Name: metro1Name,
				Metro2Code: metro2,
				Metro2Name: metro2Name,
			}
			pairs[key] = p
			bucketsByPair[key] = make(map[time.Time]*MetroPairLatencyBucket)
		}
		// Carry forward metro names if a later row has a populated name we missed.
		if p.Metro1Name == "" && metro1Name != "" {
			p.Metro1Name = metro1Name
		}
		if p.Metro2Name == "" && metro2Name != "" {
			p.Metro2Name = metro2Name
		}

		b := &MetroPairLatencyBucket{
			TS:        bucketTS.UTC(),
			DZSamples: dzSamples, InternetSamples: inetSamples,
		}
		applyFloat(&b.DZAvgRttUs, dzAvg)
		applyFloat(&b.DZMinRttUs, dzMin)
		applyFloat(&b.DZP50RttUs, dzP50)
		applyFloat(&b.DZP90RttUs, dzP90)
		applyFloat(&b.DZP95RttUs, dzP95)
		applyFloat(&b.DZP99RttUs, dzP99)
		applyFloat(&b.DZMaxRttUs, dzMax)
		applyFloat(&b.DZAvgJitterUs, dzAvgJ)
		applyFloat(&b.DZMinJitterUs, dzMinJ)
		applyFloat(&b.DZP50JitterUs, dzP50J)
		applyFloat(&b.DZP90JitterUs, dzP90J)
		applyFloat(&b.DZP95JitterUs, dzP95J)
		applyFloat(&b.DZP99JitterUs, dzP99J)
		applyFloat(&b.DZMaxJitterUs, dzMaxJ)
		applyFloat(&b.DZLossPct, dzLoss)
		applyFloat(&b.InternetAvgRttUs, inetAvg)
		applyFloat(&b.InternetMinRttUs, inetMin)
		applyFloat(&b.InternetP50RttUs, inetP50)
		applyFloat(&b.InternetP90RttUs, inetP90)
		applyFloat(&b.InternetP95RttUs, inetP95)
		applyFloat(&b.InternetP99RttUs, inetP99)
		applyFloat(&b.InternetMaxRttUs, inetMax)
		applyFloat(&b.InternetAvgJitterUs, inetAvgJ)
		applyFloat(&b.InternetMinJitterUs, inetMinJ)
		applyFloat(&b.InternetP50JitterUs, inetP50J)
		applyFloat(&b.InternetP90JitterUs, inetP90J)
		applyFloat(&b.InternetP95JitterUs, inetP95J)
		applyFloat(&b.InternetP99JitterUs, inetP99J)
		applyFloat(&b.InternetMaxJitterUs, inetMaxJ)
		bucketsByPair[key][bucketTS.UTC()] = b
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metro pair latency rows: %w", err)
	}

	bucketDuration := time.Duration(bucketSecs) * time.Second
	endRef := time.Now().UTC()
	if params.EndTime != nil {
		endRef = *params.EndTime
	}

	result := &MetroPairLatencyResult{
		TimeRange:     params.TimeRange,
		BucketSeconds: bucketSecs,
		BucketCount:   params.BucketCount,
		Pairs:         make([]*MetroPair, 0, len(pairs)),
	}

	for key, p := range pairs {
		buckets := make([]MetroPairLatencyBucket, 0, params.BucketCount)
		for i := params.BucketCount - 1; i >= 0; i-- {
			var bucketStart time.Time
			if params.StartTime != nil {
				bucketStart = params.StartTime.Truncate(bucketDuration).Add(time.Duration(params.BucketCount-1-i) * bucketDuration)
			} else {
				bucketStart = endRef.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			}
			if existing, ok := bucketsByPair[key][bucketStart]; ok {
				buckets = append(buckets, *existing)
				continue
			}
			buckets = append(buckets, MetroPairLatencyBucket{TS: bucketStart})
		}
		p.Buckets = buckets
		result.Pairs = append(result.Pairs, p)
	}

	sort.Slice(result.Pairs, func(i, j int) bool {
		if result.Pairs[i].Metro1Code != result.Pairs[j].Metro1Code {
			return result.Pairs[i].Metro1Code < result.Pairs[j].Metro1Code
		}
		return result.Pairs[i].Metro2Code < result.Pairs[j].Metro2Code
	})

	return result, nil
}

func applyFloat(dst *float64, src *float64) {
	if src == nil {
		return
	}
	v := *src
	if v != v { // NaN
		return
	}
	*dst = v
}

// resolveMetroPairLatencyParams mirrors resolveLinkLatencyParams.
func resolveMetroPairLatencyParams(opts MetroPairLatencyOptions) (bucketParams, error) {
	timeRange := opts.TimeRange
	if timeRange == "" {
		timeRange = "24h"
	}

	var params bucketParams
	if opts.StartTime != nil && opts.EndTime != nil {
		params = parseBucketParamsCustom(*opts.StartTime, *opts.EndTime, 24)
	} else {
		now := time.Now().UTC()
		duration := presetToDuration(timeRange)
		startTime := now.Add(-duration)
		params = parseBucketParamsCustom(startTime, now, 24)
		params.TimeRange = timeRange
	}

	if opts.Bucket != "" && opts.Bucket != "auto" {
		interval, ok := parseBucketString(opts.Bucket)
		if !ok {
			return params, fmt.Errorf("invalid bucket %q", opts.Bucket)
		}
		secs := intervalToSeconds(interval)
		var totalSecs int
		if params.StartTime != nil && params.EndTime != nil {
			totalSecs = int(params.EndTime.Sub(*params.StartTime).Seconds())
		} else {
			totalSecs = params.TotalMinutes * 60
		}
		count := totalSecs / secs
		if count < 1 {
			count = 1
		}
		params.BucketSeconds = secs
		params.BucketMinutes = secs / 60
		params.BucketInterval = interval
		params.BucketCount = count
		params.UseRaw = isRawBucket(interval)
	}

	return params, nil
}

// queryMetroPairLatency runs the combined DZ + Internet bucketed query and
// returns the open rows iterator (caller must Close).
func queryMetroPairLatency(ctx context.Context, db driver.Conn, params bucketParams, filter MetroPairLatencyFilter) (driver.Rows, string, []any, error) {
	dzBucketCol := "r.bucket_ts"
	dzSource := "link_rollup_5m"
	if params.UseRaw {
		dzBucketCol = "f.event_ts"
		dzSource = "fact_dz_device_link_latency"
	}
	dzBucketExpr := bucketIntervalExprFromParams(dzBucketCol, params)
	inetBucketExpr := bucketIntervalExprFromParams("f.event_ts", params)

	var (
		startTime time.Time
		endTime   time.Time
	)
	if params.StartTime != nil {
		startTime = *params.StartTime
	} else {
		startTime = time.Now().UTC().Add(-time.Duration(params.TotalMinutes) * time.Minute)
	}
	if params.EndTime != nil {
		endTime = *params.EndTime
	} else {
		endTime = time.Now().UTC()
	}

	args := []any{startTime, endTime}

	metroFilterSQL := ""
	if len(filter.MetroCodes) > 0 {
		quoted := make([]string, 0, len(filter.MetroCodes))
		for _, code := range filter.MetroCodes {
			c := strings.TrimSpace(code)
			if c == "" {
				continue
			}
			quoted = append(quoted, fmt.Sprintf("'%s'", strings.ToLower(escapeSingleQuote(c))))
		}
		if len(quoted) > 0 {
			inList := strings.Join(quoted, ",")
			metroFilterSQL = fmt.Sprintf(" AND (lower(ma.code) IN (%s) OR lower(mz.code) IN (%s))", inList, inList)
		}
	}

	inetProviderSQL := ""
	if len(filter.DataProviders) > 0 {
		quoted := make([]string, 0, len(filter.DataProviders))
		for _, p := range filter.DataProviders {
			c := strings.TrimSpace(p)
			if c == "" {
				continue
			}
			quoted = append(quoted, fmt.Sprintf("'%s'", strings.ToLower(escapeSingleQuote(c))))
		}
		if len(quoted) > 0 {
			inetProviderSQL = fmt.Sprintf(" AND lower(f.data_provider) IN (%s)", strings.Join(quoted, ","))
		}
	}

	var dzCTE string
	if params.UseRaw {
		// Raw fact-table path: aggregate samples directly per metro pair + bucket.
		dzCTE = fmt.Sprintf(`
		dz_data AS (
			SELECT
				least(ma.code, mz.code) AS metro1,
				greatest(ma.code, mz.code) AS metro2,
				if(ma.code < mz.code, ma.name, mz.name) AS metro1_name,
				if(ma.code < mz.code, mz.name, ma.name) AS metro2_name,
				%s AS bucket_ts,
				toUInt64(count()) AS samples,
				avg(f.rtt_us) AS avg_rtt_us,
				toFloat64(min(f.rtt_us)) AS min_rtt_us,
				quantile(0.50)(f.rtt_us) AS p50_rtt_us,
				quantile(0.90)(f.rtt_us) AS p90_rtt_us,
				quantile(0.95)(f.rtt_us) AS p95_rtt_us,
				quantile(0.99)(f.rtt_us) AS p99_rtt_us,
				toFloat64(max(f.rtt_us)) AS max_rtt_us,
				avg(abs(f.ipdv_us)) AS avg_jitter_us,
				toFloat64(min(abs(f.ipdv_us))) AS min_jitter_us,
				quantile(0.50)(abs(f.ipdv_us)) AS p50_jitter_us,
				quantile(0.90)(abs(f.ipdv_us)) AS p90_jitter_us,
				quantile(0.95)(abs(f.ipdv_us)) AS p95_jitter_us,
				quantile(0.99)(abs(f.ipdv_us)) AS p99_jitter_us,
				toFloat64(max(abs(f.ipdv_us))) AS max_jitter_us,
				countIf(f.loss = true OR f.rtt_us = 0) * 100.0 / greatest(count(), 1) AS loss_pct
			FROM %s f
			JOIN dz_links_current l ON f.link_pk = l.pk
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE f.event_ts >= $1 AND f.event_ts < $2
			  AND f.link_pk != ''
			  AND ma.code != mz.code%s
			GROUP BY metro1, metro2, metro1_name, metro2_name, bucket_ts
		)`, dzBucketExpr, dzSource, metroFilterSQL)
	} else {
		// Rollup path: sample-weighted re-aggregation of percentiles.
		dzCTE = fmt.Sprintf(`
		dz_data AS (
			SELECT
				metro1, metro2, metro1_name, metro2_name, bucket_ts,
				toUInt64(samples_total) AS samples,
				if(samples_total > 0, w_avg / samples_total, NULL) AS avg_rtt_us,
				if(samples_total > 0, min_rtt_us, NULL) AS min_rtt_us,
				if(samples_total > 0, w_p50 / samples_total, NULL) AS p50_rtt_us,
				if(samples_total > 0, w_p90 / samples_total, NULL) AS p90_rtt_us,
				if(samples_total > 0, w_p95 / samples_total, NULL) AS p95_rtt_us,
				if(samples_total > 0, w_p99 / samples_total, NULL) AS p99_rtt_us,
				if(samples_total > 0, max_rtt_us, NULL) AS max_rtt_us,
				if(samples_total > 0, jw_avg / samples_total, NULL) AS avg_jitter_us,
				if(samples_total > 0, min_jitter_us, NULL) AS min_jitter_us,
				if(samples_total > 0, jw_p50 / samples_total, NULL) AS p50_jitter_us,
				if(samples_total > 0, jw_p90 / samples_total, NULL) AS p90_jitter_us,
				if(samples_total > 0, jw_p95 / samples_total, NULL) AS p95_jitter_us,
				if(samples_total > 0, jw_p99 / samples_total, NULL) AS p99_jitter_us,
				if(samples_total > 0, max_jitter_us, NULL) AS max_jitter_us,
				loss_pct
			FROM (
				SELECT
					least(ma.code, mz.code) AS metro1,
					greatest(ma.code, mz.code) AS metro2,
					if(ma.code < mz.code, ma.name, mz.name) AS metro1_name,
					if(ma.code < mz.code, mz.name, ma.name) AS metro2_name,
					%s AS bucket_ts,
					sum(r.a_samples + r.z_samples) AS samples_total,
					sumIf(r.a_avg_rtt_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_avg_rtt_us * r.z_samples, r.z_samples > 0) AS w_avg,
					sumIf(r.a_p50_rtt_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p50_rtt_us * r.z_samples, r.z_samples > 0) AS w_p50,
					sumIf(r.a_p90_rtt_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p90_rtt_us * r.z_samples, r.z_samples > 0) AS w_p90,
					sumIf(r.a_p95_rtt_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p95_rtt_us * r.z_samples, r.z_samples > 0) AS w_p95,
					sumIf(r.a_p99_rtt_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p99_rtt_us * r.z_samples, r.z_samples > 0) AS w_p99,
					sumIf(r.a_avg_jitter_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_avg_jitter_us * r.z_samples, r.z_samples > 0) AS jw_avg,
					sumIf(r.a_p50_jitter_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p50_jitter_us * r.z_samples, r.z_samples > 0) AS jw_p50,
					sumIf(r.a_p90_jitter_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p90_jitter_us * r.z_samples, r.z_samples > 0) AS jw_p90,
					sumIf(r.a_p95_jitter_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p95_jitter_us * r.z_samples, r.z_samples > 0) AS jw_p95,
					sumIf(r.a_p99_jitter_us * r.a_samples, r.a_samples > 0)
						+ sumIf(r.z_p99_jitter_us * r.z_samples, r.z_samples > 0) AS jw_p99,
					toFloat64(least(minIf(r.a_min_rtt_us, r.a_samples > 0), minIf(r.z_min_rtt_us, r.z_samples > 0))) AS min_rtt_us,
					toFloat64(greatest(maxIf(r.a_max_rtt_us, r.a_samples > 0), maxIf(r.z_max_rtt_us, r.z_samples > 0))) AS max_rtt_us,
					toFloat64(least(minIf(r.a_min_jitter_us, r.a_samples > 0), minIf(r.z_min_jitter_us, r.z_samples > 0))) AS min_jitter_us,
					toFloat64(greatest(maxIf(r.a_max_jitter_us, r.a_samples > 0), maxIf(r.z_max_jitter_us, r.z_samples > 0))) AS max_jitter_us,
					max(greatest(r.a_loss_pct, r.z_loss_pct)) AS loss_pct
				FROM link_rollup_5m r
				JOIN dz_links_current l ON r.link_pk = l.pk
				JOIN dz_devices_current da ON l.side_a_pk = da.pk
				JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
				JOIN dz_metros_current ma ON da.metro_pk = ma.pk
				JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
				WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
				  AND r.link_pk != ''
				  AND ma.code != mz.code%s
				GROUP BY metro1, metro2, metro1_name, metro2_name, bucket_ts
			)
		)`, dzBucketExpr, metroFilterSQL)
	}

	inetCTE := fmt.Sprintf(`
		inet_data AS (
			SELECT
				least(ma.code, mz.code) AS metro1,
				greatest(ma.code, mz.code) AS metro2,
				if(ma.code < mz.code, ma.name, mz.name) AS metro1_name,
				if(ma.code < mz.code, mz.name, ma.name) AS metro2_name,
				%s AS bucket_ts,
				toUInt64(count()) AS samples,
				avg(f.rtt_us) AS avg_rtt_us,
				toFloat64(min(f.rtt_us)) AS min_rtt_us,
				quantile(0.50)(f.rtt_us) AS p50_rtt_us,
				quantile(0.90)(f.rtt_us) AS p90_rtt_us,
				quantile(0.95)(f.rtt_us) AS p95_rtt_us,
				quantile(0.99)(f.rtt_us) AS p99_rtt_us,
				toFloat64(max(f.rtt_us)) AS max_rtt_us,
				avg(abs(f.ipdv_us)) AS avg_jitter_us,
				toFloat64(min(abs(f.ipdv_us))) AS min_jitter_us,
				quantile(0.50)(abs(f.ipdv_us)) AS p50_jitter_us,
				quantile(0.90)(abs(f.ipdv_us)) AS p90_jitter_us,
				quantile(0.95)(abs(f.ipdv_us)) AS p95_jitter_us,
				quantile(0.99)(abs(f.ipdv_us)) AS p99_jitter_us,
				toFloat64(max(abs(f.ipdv_us))) AS max_jitter_us
			FROM fact_dz_internet_metro_latency f
			JOIN dz_metros_current ma ON f.origin_metro_pk = ma.pk
			JOIN dz_metros_current mz ON f.target_metro_pk = mz.pk
			WHERE f.event_ts >= $1 AND f.event_ts < $2
			  AND ma.code != mz.code%s%s
			GROUP BY metro1, metro2, metro1_name, metro2_name, bucket_ts
		)`, inetBucketExpr, metroFilterSQL, inetProviderSQL)

	// FULL OUTER JOIN ... USING is required here. ClickHouse's multi-column
	// ON-clause variant does not propagate the join-key column values from the
	// unmatched side, so rows that only exist in one CTE come back with empty
	// metro1/metro2/bucket_ts. USING merges the join keys into a single value
	// per row, which is the behavior we want.
	query := fmt.Sprintf(`
		WITH%s,%s
		SELECT
			metro1, metro2,
			COALESCE(d.metro1_name, i.metro1_name, '') AS metro1_name,
			COALESCE(d.metro2_name, i.metro2_name, '') AS metro2_name,
			bucket_ts,
			COALESCE(d.samples, toUInt64(0)) AS dz_samples,
			d.avg_rtt_us AS dz_avg_rtt_us,
			d.min_rtt_us AS dz_min_rtt_us,
			d.p50_rtt_us AS dz_p50_rtt_us,
			d.p90_rtt_us AS dz_p90_rtt_us,
			d.p95_rtt_us AS dz_p95_rtt_us,
			d.p99_rtt_us AS dz_p99_rtt_us,
			d.max_rtt_us AS dz_max_rtt_us,
			d.avg_jitter_us AS dz_avg_jitter_us,
			d.min_jitter_us AS dz_min_jitter_us,
			d.p50_jitter_us AS dz_p50_jitter_us,
			d.p90_jitter_us AS dz_p90_jitter_us,
			d.p95_jitter_us AS dz_p95_jitter_us,
			d.p99_jitter_us AS dz_p99_jitter_us,
			d.max_jitter_us AS dz_max_jitter_us,
			d.loss_pct AS dz_loss_pct,
			COALESCE(i.samples, toUInt64(0)) AS inet_samples,
			i.avg_rtt_us AS inet_avg_rtt_us,
			i.min_rtt_us AS inet_min_rtt_us,
			i.p50_rtt_us AS inet_p50_rtt_us,
			i.p90_rtt_us AS inet_p90_rtt_us,
			i.p95_rtt_us AS inet_p95_rtt_us,
			i.p99_rtt_us AS inet_p99_rtt_us,
			i.max_rtt_us AS inet_max_rtt_us,
			i.avg_jitter_us AS inet_avg_jitter_us,
			i.min_jitter_us AS inet_min_jitter_us,
			i.p50_jitter_us AS inet_p50_jitter_us,
			i.p90_jitter_us AS inet_p90_jitter_us,
			i.p95_jitter_us AS inet_p95_jitter_us,
			i.p99_jitter_us AS inet_p99_jitter_us,
			i.max_jitter_us AS inet_max_jitter_us
		FROM dz_data d
		FULL OUTER JOIN inet_data i USING (metro1, metro2, bucket_ts)
		ORDER BY metro1, metro2, bucket_ts
	`, dzCTE, inetCTE)

	rows, err := db.Query(ctx, query, args...)
	return rows, query, args, err
}
