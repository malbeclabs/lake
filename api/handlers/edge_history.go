package handlers

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// The Edge History page reports what the multicast pcap warehouse holds for each feed:
// since when each recorder has captured it, how much, and where the capture has holes.
// It reads dz_edge_pcap_file_current, one row per pcap file as the newest offload
// manifest to write it describes it (indexer/pkg/dz/pcapwarehouse). Every query filters
// on hour_ts, a grouping key of that view, so the filter reaches the table's partitions.

const (
	// edgeHistoryMinGap is the shortest hole reported as an interruption. Consecutive
	// files of a healthy capture abut to the microsecond on busy feeds, and on the
	// quietest ones (a few packets a second) the hole between the last packet of one
	// file and the first of the next stays under ten seconds; recorder restarts land
	// around a minute. Anything shorter than this is a quiet feed, not a missing one.
	edgeHistoryMinGap = 60 * time.Second

	// edgeHistoryLiveWithin is how recent a recorder's last packet must be for the
	// capture to read as ongoing. A quiet feed's file is offloaded when its hour
	// rotates, so an hour and change behind is normal.
	edgeHistoryLiveWithin = 2 * time.Hour

	// edgeHistoryMaxGaps caps the gap list per (feed, recorder) in the payload; the
	// counts and totals always cover every gap.
	edgeHistoryMaxGaps = 200
)

// EdgeHistoryPageCacheKey holds the page's default view, the whole history. It is the
// one range that resolves every file ever indexed, and it grows with the bucket, so the
// page-cache worker computes it rather than each page load. Captures are offloaded
// hourly, so ten minutes of staleness is not visible.
const EdgeHistoryPageCacheKey = "edge_history:all"

type edgeHistoryRange struct {
	span   time.Duration // zero means all history
	bucket time.Duration
	name   string
}

var edgeHistoryRanges = map[string]edgeHistoryRange{
	"7d":  {span: 7 * 24 * time.Hour, bucket: time.Hour, name: "hour"},
	"30d": {span: 30 * 24 * time.Hour, bucket: 24 * time.Hour, name: "day"},
	"90d": {span: 90 * 24 * time.Hour, bucket: 24 * time.Hour, name: "day"},
	"all": {bucket: 24 * time.Hour, name: "day"},
}

type EdgeHistoryGap struct {
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Seconds float64   `json:"seconds"`
}

type EdgeHistoryInstance struct {
	IP      string    `json:"ip"`
	FirstTS time.Time `json:"first_ts"`
	LastTS  time.Time `json:"last_ts"`
	Files   uint64    `json:"files"`
}

type EdgeHistoryRecorder struct {
	Recorder  string                `json:"recorder"`
	Site      string                `json:"site"`
	Instances []EdgeHistoryInstance `json:"instances"`
	FirstTS   time.Time             `json:"first_ts"`
	LastTS    time.Time             `json:"last_ts"`
	Live      bool                  `json:"live"`
	Files     uint64                `json:"files"`
	Bytes     uint64                `json:"bytes"`
	Packets   uint64                `json:"packets"`
	// Overwritten counts captures the bucket no longer holds because a later upload
	// reused their key — a recorder restart resetting its file counter mid-hour. The
	// time they covered reads as a gap unless the replacing file happens to cover it.
	Overwritten uint64 `json:"overwritten"`
	// ArchivedHours counts hours in the window with an offload manifest the bucket
	// has archived and nothing has restored, so the indexer could not read what the
	// recorder uploaded then. A manifest spans every feed the recorder captured that
	// hour, so the count is the recorder's and repeats on each of its feeds. Gaps in
	// those hours are unread, not necessarily lost.
	ArchivedHours uint64 `json:"archived_hours"`
	// CoveragePct is the share of the recorder's span inside the window — from its
	// first packet (or the window start) to its last — not lost to a gap.
	CoveragePct float64          `json:"coverage_pct"`
	GapCount    int              `json:"gap_count"`
	GapSeconds  float64          `json:"gap_seconds"`
	Gaps        []EdgeHistoryGap `json:"gaps"`
	// Per-bucket series aligned to the response's window: bucket i starts at
	// window_start + i*bucket_seconds. Coverage is the captured share of the part of
	// the bucket inside the recorder's first..last span — so the day a capture starts
	// or stops reads whole unless it has a gap — or -1 where the bucket lies wholly
	// outside that span.
	BucketBytes    []uint64  `json:"bucket_bytes"`
	BucketCoverage []float64 `json:"bucket_coverage"`
}

type EdgeHistoryFeed struct {
	MulticastGroup string                `json:"multicast_group"`
	Code           string                `json:"code"`
	FirstTS        time.Time             `json:"first_ts"`
	LastTS         time.Time             `json:"last_ts"`
	Files          uint64                `json:"files"`
	Bytes          uint64                `json:"bytes"`
	Packets        uint64                `json:"packets"`
	Overwritten    uint64                `json:"overwritten"`
	Recorders      []EdgeHistoryRecorder `json:"recorders"`
}

type EdgeHistoryResponse struct {
	Range         string            `json:"range"`
	WindowStart   time.Time         `json:"window_start"`
	WindowEnd     time.Time         `json:"window_end"`
	Bucket        string            `json:"bucket"`
	BucketSeconds int64             `json:"bucket_seconds"`
	MinGapSeconds float64           `json:"min_gap_seconds"`
	Files         uint64            `json:"files"`
	Bytes         uint64            `json:"bytes"`
	Packets       uint64            `json:"packets"`
	Overwritten   uint64            `json:"overwritten"`
	ArchivedHours uint64            `json:"archived_hours"`
	AsOf          *time.Time        `json:"as_of,omitempty"`
	Feeds         []EdgeHistoryFeed `json:"feeds"`
}

// edgeHistorySite reads the site code out of a recorder host name:
// aws-cmh-mn-recorder1 -> cmh, chi-mn-recorder1 -> chi.
func edgeHistorySite(host string) string {
	parts := strings.Split(strings.TrimPrefix(host, "aws-"), "-")
	return parts[0]
}

type edgeHistoryKey struct{ group, recorder string }

func (a *API) FetchEdgeHistoryData(ctx context.Context, rangeName string, now time.Time) (*EdgeHistoryResponse, error) {
	rng, ok := edgeHistoryRanges[rangeName]
	if !ok {
		return nil, fmt.Errorf("unknown range %q", rangeName)
	}
	now = now.UTC()
	resp := &EdgeHistoryResponse{
		Range:         rangeName,
		Bucket:        rng.name,
		BucketSeconds: int64(rng.bucket / time.Second),
		MinGapSeconds: edgeHistoryMinGap.Seconds(),
		Feeds:         []EdgeHistoryFeed{},
	}
	db := a.envDB(ctx)

	// The "all" range reads from the Unix epoch, not Go's zero time, which is outside
	// what a ClickHouse DateTime can hold.
	windowStart := time.Unix(0, 0).UTC()
	if rng.span > 0 {
		windowStart = now.Add(-rng.span).Truncate(rng.bucket)
	}

	// The "all" window opens on the earliest capture. The fact table answers that
	// without resolving versions: a rewritten key keeps its hour, and the newest first
	// packet of any key is never earlier than the table's minimum.
	if rng.span == 0 {
		var first time.Time
		var n uint64
		start := time.Now()
		rows, err := db.Query(ctx, `SELECT min(first_packet_ts), count() FROM fact_dz_edge_pcap_file`)
		metrics.RecordClickHouseQuery("edge_history_first", time.Since(start), err)
		if err != nil {
			return nil, err
		}
		if rows.Next() {
			err = rows.Scan(&first, &n)
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			resp.WindowStart, resp.WindowEnd = now.Truncate(rng.bucket), now
			return resp, nil
		}
		windowStart = first.UTC().Truncate(rng.bucket)
	}
	resp.WindowStart, resp.WindowEnd = windowStart, now
	nBuckets := int(now.Sub(windowStart)/rng.bucket) + 1

	type instanceRow struct {
		key                   edgeHistoryKey
		ip                    string
		first, last           time.Time
		files, bytes, packets uint64
		overwritten           uint64
		ingested              time.Time
	}
	type bucketRow struct {
		key    edgeHistoryKey
		bucket int64
		bytes  uint64
	}
	var (
		instances []instanceRow
		gaps      = map[edgeHistoryKey][]EdgeHistoryGap{}
		buckets   []bucketRow
		codes     map[string]string
		// The first file inside the window per (feed, recorder), and the last packet
		// before the window, however long ago: the hole between them is a gap that
		// opens the window, which a scan of the window alone cannot see.
		firstInWindow = map[edgeHistoryKey]time.Time{}
		lastBefore    = map[edgeHistoryKey]time.Time{}
		archivedHours = map[string]uint64{} // recorder host -> archived hours in the window
	)

	// The four reads are independent once the window is fixed, and each scans the
	// window's files, so they run at once rather than one after another.
	g, gctx := errgroup.WithContext(ctx)

	// Totals per (feed, recorder, instance).
	g.Go(func() error {
		start := time.Now()
		rows, err := db.Query(gctx, `
			SELECT multicast_group, recorder, recorder_ip,
			       -- The span and the live status come from files that captured
			       -- something: a zero-packet file carries no packet times (the indexer
			       -- stamps it with its hour directory), so it must not stretch the
			       -- span over hours nothing was captured in. It still counts as a file
			       -- and its bytes; an instance with only such files falls back to them.
			       if(countIf(packets > 0) > 0, minIf(first_packet_ts, packets > 0), min(first_packet_ts)),
			       if(countIf(packets > 0) > 0, maxIf(last_packet_ts, packets > 0), max(last_packet_ts)),
			       count(), sum(size_bytes), sum(packets), max(ingested_at), toUInt64(sum(versions - 1))
			FROM dz_edge_pcap_file_current
			WHERE hour_ts >= ?
			GROUP BY multicast_group, recorder, recorder_ip
		`, windowStart.Truncate(time.Hour))
		metrics.RecordClickHouseQuery("edge_history_totals", time.Since(start), err)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r instanceRow
			if err := rows.Scan(&r.key.group, &r.key.recorder, &r.ip, &r.first, &r.last, &r.files, &r.bytes, &r.packets, &r.ingested, &r.overwritten); err != nil {
				return err
			}
			instances = append(instances, r)
		}
		return rows.Err()
	})

	// Gaps: the hole between each file's first packet and the latest last packet of
	// every file before it, for one feed at one recorder host. The host rather than
	// the instance, so a recorder rebuilt on a new address reads as one history with
	// the rebuild as its gap. The first file of each series is returned too, to be
	// joined with lastBefore below.
	g.Go(func() error {
		start := time.Now()
		rows, err := db.Query(gctx, `
			SELECT multicast_group, recorder, prev_last, first_ts, rn
			FROM (
				SELECT multicast_group, recorder, first_packet_ts AS first_ts,
				       max(last_packet_ts) OVER (
				           PARTITION BY multicast_group, recorder
				           ORDER BY first_packet_ts, s3_key
				           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				       ) AS prev_last,
				       row_number() OVER (
				           PARTITION BY multicast_group, recorder
				           ORDER BY first_packet_ts, s3_key
				       ) AS rn
				FROM dz_edge_pcap_file_current
				-- A file with no packets carries no packet times (the indexer stamps it
				-- with its hour directory), so it bounds nothing and must not split a gap.
				WHERE hour_ts >= ? AND packets > 0
			)
			WHERE rn = 1
			   OR (first_ts >= ? AND dateDiff('millisecond', prev_last, first_ts) >= ?)
			ORDER BY multicast_group, recorder, first_ts
		`, windowStart.Truncate(time.Hour), windowStart, edgeHistoryMinGap.Milliseconds())
		metrics.RecordClickHouseQuery("edge_history_gaps", time.Since(start), err)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var k edgeHistoryKey
			var from, to time.Time
			var rn uint64
			if err := rows.Scan(&k.group, &k.recorder, &from, &to, &rn); err != nil {
				return err
			}
			if rn == 1 {
				firstInWindow[k] = to.UTC()
				continue
			}
			from = from.UTC()
			if from.Before(windowStart) {
				from = windowStart
			}
			gaps[k] = append(gaps[k], EdgeHistoryGap{Start: from, End: to.UTC(), Seconds: to.Sub(from).Seconds()})
		}
		return rows.Err()
	})

	// The last packet before the window, for ranges that have a before.
	if rng.span > 0 {
		g.Go(func() error {
			start := time.Now()
			rows, err := db.Query(gctx, `
				SELECT multicast_group, recorder, max(last_packet_ts)
				FROM dz_edge_pcap_file_current
				WHERE hour_ts < ? AND packets > 0
				GROUP BY multicast_group, recorder
			`, windowStart.Truncate(time.Hour))
			metrics.RecordClickHouseQuery("edge_history_before", time.Since(start), err)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var k edgeHistoryKey
				var last time.Time
				if err := rows.Scan(&k.group, &k.recorder, &last); err != nil {
					return err
				}
				lastBefore[k] = last.UTC()
			}
			return rows.Err()
		})
	}

	// Volume per bucket.
	g.Go(func() error {
		start := time.Now()
		rows, err := db.Query(gctx, `
			SELECT multicast_group, recorder,
			       -- floor, not intDiv, which truncates toward zero and would fold a file
			       -- starting just before the window into bucket 0.
			       toInt64(floor((toInt64(toUnixTimestamp(first_packet_ts)) - ?) / ?)) AS bucket,
			       sum(size_bytes)
			FROM dz_edge_pcap_file_current
			WHERE hour_ts >= ?
			GROUP BY multicast_group, recorder, bucket
		`, windowStart.Unix(), int64(rng.bucket/time.Second), windowStart.Truncate(time.Hour))
		metrics.RecordClickHouseQuery("edge_history_buckets", time.Since(start), err)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r bucketRow
			if err := rows.Scan(&r.key.group, &r.key.recorder, &r.bucket, &r.bytes); err != nil {
				return err
			}
			buckets = append(buckets, r)
		}
		return rows.Err()
	})

	// Archived hours, per recorder host. Like the codes, a qualifier on what the page
	// shows rather than part of it, so a failure is a warning.
	g.Go(func() error {
		start := time.Now()
		rows, err := db.Query(gctx, `
			SELECT recorder, uniqExact(hour_ts)
			FROM dz_edge_pcap_archived_manifest FINAL
			WHERE state = 'archived' AND hour_ts >= ?
			GROUP BY recorder
		`, windowStart.Truncate(time.Hour))
		metrics.RecordClickHouseQuery("edge_history_archived", time.Since(start), err)
		if err != nil {
			logWarn("edge history: archived manifests unavailable", "error", err)
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			var host string
			var n uint64
			if err := rows.Scan(&host, &n); err != nil {
				logWarn("edge history: archived manifests unreadable", "error", err)
				return nil
			}
			archivedHours[host] = n
		}
		return nil
	})

	g.Go(func() error {
		var err error
		if codes, err = a.edgeHistoryGroupCodes(gctx); err != nil {
			// The ledger names are a label; the history is readable without them.
			logWarn("edge history: multicast group codes unavailable", "error", err)
			codes = map[string]string{}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}
	for k, first := range firstInWindow {
		last, ok := lastBefore[k]
		if !ok || !first.After(windowStart) || first.Sub(last) < edgeHistoryMinGap {
			continue
		}
		from := last
		if from.Before(windowStart) {
			from = windowStart
		}
		// The earliest gap of the series, so it goes first and the list stays in order.
		gaps[k] = append([]EdgeHistoryGap{{Start: from, End: first, Seconds: first.Sub(from).Seconds()}}, gaps[k]...)
	}

	var asOf time.Time
	recorders := map[edgeHistoryKey]*EdgeHistoryRecorder{}
	for _, r := range instances {
		if r.ingested.After(asOf) {
			asOf = r.ingested
		}
		rec := recorders[r.key]
		if rec == nil {
			rec = &EdgeHistoryRecorder{
				Recorder:       r.key.recorder,
				Site:           edgeHistorySite(r.key.recorder),
				FirstTS:        r.first.UTC(),
				LastTS:         r.last.UTC(),
				Gaps:           []EdgeHistoryGap{},
				BucketBytes:    make([]uint64, nBuckets),
				BucketCoverage: make([]float64, nBuckets),
			}
			recorders[r.key] = rec
		}
		rec.Instances = append(rec.Instances, EdgeHistoryInstance{IP: r.ip, FirstTS: r.first.UTC(), LastTS: r.last.UTC(), Files: r.files})
		if r.first.Before(rec.FirstTS) {
			rec.FirstTS = r.first.UTC()
		}
		if r.last.After(rec.LastTS) {
			rec.LastTS = r.last.UTC()
		}
		rec.Files += r.files
		rec.Bytes += r.bytes
		rec.Packets += r.packets
		rec.Overwritten += r.overwritten
		rec.ArchivedHours = archivedHours[r.key.recorder]
	}
	if len(recorders) == 0 {
		return resp, nil
	}
	asOf = asOf.UTC()
	resp.AsOf = &asOf
	for _, b := range buckets {
		if rec := recorders[b.key]; rec != nil && b.bucket >= 0 && b.bucket < int64(nBuckets) {
			rec.BucketBytes[b.bucket] += b.bytes
		}
	}

	feeds := map[string]*EdgeHistoryFeed{}
	for k, rec := range recorders {
		g := gaps[k]
		edgeHistoryFinishRecorder(rec, g, windowStart, now, rng.bucket)
		f := feeds[k.group]
		if f == nil {
			f = &EdgeHistoryFeed{MulticastGroup: k.group, Code: codes[k.group], FirstTS: rec.FirstTS, LastTS: rec.LastTS}
			feeds[k.group] = f
		}
		if rec.FirstTS.Before(f.FirstTS) {
			f.FirstTS = rec.FirstTS
		}
		if rec.LastTS.After(f.LastTS) {
			f.LastTS = rec.LastTS
		}
		f.Files += rec.Files
		f.Bytes += rec.Bytes
		f.Packets += rec.Packets
		f.Overwritten += rec.Overwritten
		f.Recorders = append(f.Recorders, *rec)
	}
	for _, n := range archivedHours {
		resp.ArchivedHours += n
	}
	for _, f := range feeds {
		slices.SortFunc(f.Recorders, func(a, b EdgeHistoryRecorder) int { return strings.Compare(a.Recorder, b.Recorder) })
		resp.Files += f.Files
		resp.Bytes += f.Bytes
		resp.Packets += f.Packets
		resp.Overwritten += f.Overwritten
		resp.Feeds = append(resp.Feeds, *f)
	}
	// Named feeds first, alphabetically by ledger code; unnamed ones after, by address.
	slices.SortFunc(resp.Feeds, func(a, b EdgeHistoryFeed) int {
		if (a.Code == "") != (b.Code == "") {
			if a.Code == "" {
				return 1
			}
			return -1
		}
		if c := strings.Compare(a.Code, b.Code); c != 0 {
			return c
		}
		return edgeHistoryCompareAddr(a.MulticastGroup, b.MulticastGroup)
	})
	return resp, nil
}

// edgeHistoryFinishRecorder fills in the gap totals, coverage and per-bucket coverage
// from the recorder's span and its gaps (sorted by start, non-overlapping).
func edgeHistoryFinishRecorder(rec *EdgeHistoryRecorder, gaps []EdgeHistoryGap, windowStart, now time.Time, bucket time.Duration) {
	slices.SortFunc(rec.Instances, func(a, b EdgeHistoryInstance) int { return a.FirstTS.Compare(b.FirstTS) })
	rec.Live = now.Sub(rec.LastTS) <= edgeHistoryLiveWithin
	rec.GapCount = len(gaps)
	for _, g := range gaps {
		rec.GapSeconds += g.Seconds
	}
	// Longest gaps first; the payload keeps the worst of them.
	listed := slices.Clone(gaps)
	slices.SortStableFunc(listed, func(a, b EdgeHistoryGap) int { return -compareFloat(a.Seconds, b.Seconds) })
	if len(listed) > edgeHistoryMaxGaps {
		listed = listed[:edgeHistoryMaxGaps]
	}
	rec.Gaps = listed

	spanStart := rec.FirstTS
	if spanStart.Before(windowStart) {
		spanStart = windowStart
	}
	// A gap found from the last packet before the window ends at the first file
	// inside it, so it lies before FirstTS. The recorder was capturing this feed before the window, and the window
	// opened on an outage: the span starts where that gap does, or the gap would be
	// subtracted from a span it is not part of and the buckets it covers would read
	// "not recording".
	for _, g := range gaps {
		if g.Start.Before(spanStart) {
			spanStart = g.Start
		}
	}
	spanEnd := rec.LastTS
	if rec.Live {
		// An ongoing capture is covered up to now; the not-yet-offloaded tail is not a
		// hole in it.
		spanEnd = now
	}
	if span := spanEnd.Sub(spanStart).Seconds(); span > 0 {
		rec.CoveragePct = roundTo(100*math.Max(0, span-rec.GapSeconds)/span, 3)
	}

	for i := range rec.BucketCoverage {
		bs := windowStart.Add(time.Duration(i) * bucket)
		be := bs.Add(bucket)
		if be.After(now) {
			be = now
		}
		inSpan := overlap(bs, be, spanStart, spanEnd)
		if inSpan <= 0 {
			rec.BucketCoverage[i] = -1
			continue
		}
		covered := inSpan
		for _, g := range gaps {
			covered -= overlap(bs, be, g.Start, g.End)
		}
		// Four places, so the shortest reportable gap (a minute) still reads below 1 in
		// a day bucket.
		rec.BucketCoverage[i] = roundTo(math.Max(0, covered)/inSpan, 4)
	}
}

func overlap(aStart, aEnd, bStart, bEnd time.Time) float64 {
	s, e := aStart, aEnd
	if bStart.After(s) {
		s = bStart
	}
	if bEnd.Before(e) {
		e = bEnd
	}
	if !e.After(s) {
		return 0
	}
	return e.Sub(s).Seconds()
}

func roundTo(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

// edgeHistoryCompareAddr orders addresses numerically, so 233.84.178.3 sorts before
// 233.84.178.20; text that is not an address falls back to string order after them.
func edgeHistoryCompareAddr(a, b string) int {
	pa, errA := netip.ParseAddr(a)
	pb, errB := netip.ParseAddr(b)
	switch {
	case errA == nil && errB == nil:
		return pa.Compare(pb)
	case errA == nil:
		return -1
	case errB == nil:
		return 1
	}
	return strings.Compare(a, b)
}

func compareFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (a *API) edgeHistoryGroupCodes(ctx context.Context) (map[string]string, error) {
	// One code per address, deterministically: an activated group wins over any
	// other on the same address, then the lowest code.
	rows, err := a.envDB(ctx).Query(ctx, `
		SELECT multicast_ip, argMin(code, (status != 'activated', code))
		FROM dz_multicast_groups_current
		GROUP BY multicast_ip`)
	if err != nil {
		return map[string]string{}, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var ip, code string
		if err := rows.Scan(&ip, &code); err != nil {
			return out, err
		}
		out[ip] = code
	}
	return out, rows.Err()
}

func (a *API) GetEdgeHistory(w http.ResponseWriter, r *http.Request) {
	rangeName := r.URL.Query().Get("range")
	if rangeName == "" {
		rangeName = "all"
	}
	if _, ok := edgeHistoryRanges[rangeName]; !ok {
		http.Error(w, "range must be one of 7d, 30d, 90d, all", http.StatusBadRequest)
		return
	}
	// Only mainnet: the worker computes with no environment in context, so the entry
	// holds mainnet's history.
	if rangeName == "all" && isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), EdgeHistoryPageCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	data, err := a.FetchEdgeHistoryData(ctx, rangeName, time.Now())
	if err != nil {
		if isMissingTable(err) {
			logWarn("edge history table missing", "error", err)
			writeJSON(w, &EdgeHistoryResponse{Range: rangeName, Feeds: []EdgeHistoryFeed{}})
			return
		}
		logError("edge history query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, data)
}
