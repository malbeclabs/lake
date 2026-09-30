package pcapwarehouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

const (
	// DefaultBudget bounds one Sync, which a backfill fills and then resumes from its
	// cursor on the next pass; steady state finishes in seconds. dzingest does not
	// wait on the pass within its cycle, so this is bounded only by the activity's
	// StartToClose (dzingest.pcapWarehouseStartToClose), which it must stay well under:
	// the budget is checked between hours, and one busy hour takes a few seconds.
	DefaultBudget = 3 * time.Minute

	// DefaultRescan is how far behind each recorder's newest ingested hour a sync
	// re-lists. An offload lands in the hour it ran in, so a directory keeps gaining
	// manifests for as long as that hour is current plus the last offload after it;
	// three hours covers that and a recorder that paused uploading for a while.
	// Manifests already read are skipped by key, so the rescan costs listings only.
	DefaultRescan = 3 * time.Hour

	// Every recorder runs at once, so a backfill shares the budget across all of
	// them instead of draining the first few alphabetically before the rest start.
	// There are about ten; at eight manifest reads each that is ~80 GETs in flight.
	defaultRecorderConcurrency = 16
	defaultFetchConcurrency    = 8

	// flushRows batches small hours into one insert. A backfill walks tens of
	// thousands of hour directories, and an insert per hour would be a part per hour.
	flushRows = 20_000
)

var fetchRetry = dberror.RetryConfig{
	MaxAttempts: 3,
	BaseBackoff: 100 * time.Millisecond,
	MaxBackoff:  time.Second,
}

// store is what Sync needs from Store; tests substitute it.
type store interface {
	Cursors(ctx context.Context) (map[string]time.Time, error)
	IngestedManifests(ctx context.Context, rec Recorder, since time.Time) (map[string]struct{}, error)
	Insert(ctx context.Context, files []FileRow) error
}

type SyncerConfig struct {
	Logger *slog.Logger
	Bucket Warehouse
	Store  store
	Budget time.Duration // zero means DefaultBudget
	Rescan time.Duration // zero means DefaultRescan
	Now    func() time.Time
}

type Syncer struct {
	cfg SyncerConfig

	// barren holds manifests read that produced no row — no files listed, or
	// unparseable — keyed to their hour. IngestedManifests is answered from the
	// table, so without this they would be fetched again, and an unparseable one
	// warned about again, on every pass while their hour is inside the rescan window.
	// Kept per recorder and pruned against that recorder's own rescan start — during a
	// backfill the window trails the recorder's cursor, not the wall clock. A restart
	// forgets them, which costs one more read each.
	barrenMu sync.Mutex
	barren   map[string]map[string]time.Time // recorder prefix -> manifest key -> hour
}

func NewSyncer(cfg SyncerConfig) (*Syncer, error) {
	if cfg.Logger == nil || cfg.Bucket == nil || cfg.Store == nil {
		return nil, errors.New("pcapwarehouse: logger, bucket and store are required")
	}
	if cfg.Budget <= 0 {
		cfg.Budget = DefaultBudget
	}
	if cfg.Rescan <= 0 {
		cfg.Rescan = DefaultRescan
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Syncer{cfg: cfg, barren: map[string]map[string]time.Time{}}, nil
}

// SyncResult reports what one Sync did.
type SyncResult struct {
	Recorders        int
	Hours            int
	ManifestsRead    int
	ManifestsSkipped int // unparseable manifests, logged and left out
	FilesWritten     int
	// Behind is set when the budget ran out before some recorder reached its newest
	// hour: a backfill in progress, which the next pass continues.
	Behind   bool
	MinHour  time.Time
	MaxHour  time.Time
	resultMu sync.Mutex
}

func (r *SyncResult) add(o recorderResult) {
	r.resultMu.Lock()
	defer r.resultMu.Unlock()
	r.Hours += o.hours
	r.ManifestsRead += o.manifestsRead
	r.ManifestsSkipped += o.manifestsSkipped
	r.FilesWritten += o.filesWritten
	r.Behind = r.Behind || o.behind
	if !o.minHour.IsZero() && (r.MinHour.IsZero() || o.minHour.Before(r.MinHour)) {
		r.MinHour = o.minHour
	}
	if o.maxHour.After(r.MaxHour) {
		r.MaxHour = o.maxHour
	}
}

type recorderResult struct {
	hours, manifestsRead, manifestsSkipped, filesWritten int
	behind                                               bool
	minHour, maxHour                                     time.Time
}

// Sync reads every manifest not yet ingested and writes its files. Recorders are
// independent: one that fails does not stop the others, and its error is returned
// alongside theirs once all have finished.
func (s *Syncer) Sync(ctx context.Context) (*SyncResult, error) {
	deadline := s.cfg.Now().Add(s.cfg.Budget)
	prefixes, err := s.cfg.Bucket.ListRecorders(ctx)
	if err != nil {
		return nil, err
	}
	cursors, err := s.cfg.Store.Cursors(ctx)
	if err != nil {
		return nil, err
	}

	res := &SyncResult{}
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	sem := make(chan struct{}, defaultRecorderConcurrency)
	for _, prefix := range prefixes {
		rec, err := ParseRecorder(prefix)
		if err != nil {
			s.cfg.Logger.Warn("pcapwarehouse: skipping unrecognised recorder directory", "prefix", prefix, "error", err)
			continue
		}
		res.Recorders++
		cursor, seen := cursors[prefix]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rr, err := s.syncRecorder(ctx, rec, cursor, seen, deadline)
			res.add(rr)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("recorder %s: %w", prefix, err))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return res, errors.Join(errs...)
}

func (s *Syncer) syncRecorder(ctx context.Context, rec Recorder, cursor time.Time, hasCursor bool, deadline time.Time) (recorderResult, error) {
	var rr recorderResult
	var since time.Time
	ingested := map[string]struct{}{}
	if hasCursor {
		since = cursor.Add(-s.cfg.Rescan)
		var err error
		if ingested, err = s.cfg.Store.IngestedManifests(ctx, rec, since); err != nil {
			return rr, err
		}
	}
	s.pruneBarren(rec.Prefix, since.Truncate(time.Hour))
	hours, err := s.cfg.Bucket.ListHours(ctx, rec.Prefix, since)
	if err != nil {
		return rr, err
	}

	// Hours are read oldest first and flushed in that order, so the cursor — the
	// newest hour with a row — never runs ahead of an hour that was not written.
	var buf []FileRow
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := s.cfg.Store.Insert(ctx, buf); err != nil {
			return err
		}
		rr.filesWritten += len(buf)
		buf = nil
		return nil
	}
	// Every pass reads at least one hour past the cursor before it checks the budget.
	// Counting the rescan hours toward that would let a pass whose budget the rescan
	// uses up re-read the same window forever and never advance.
	advanced := false
	for _, hour := range hours {
		if advanced && s.cfg.Now().After(deadline) {
			rr.behind = true
			break
		}
		rows, read, skipped, err := s.readHour(ctx, rec, hour, ingested)
		if err != nil {
			// Whole hours already buffered are still good; keep them.
			return rr, errors.Join(err, flush())
		}
		rr.hours++
		rr.manifestsRead += read
		rr.manifestsSkipped += skipped
		if rr.minHour.IsZero() {
			rr.minHour = hour
		}
		rr.maxHour = hour
		advanced = advanced || !hasCursor || hour.After(cursor)
		buf = append(buf, rows...)
		if len(buf) >= flushRows {
			if err := flush(); err != nil {
				return rr, err
			}
		}
	}
	return rr, flush()
}

func (s *Syncer) markBarren(recorder, key string, hour time.Time) {
	s.barrenMu.Lock()
	defer s.barrenMu.Unlock()
	if s.barren[recorder] == nil {
		s.barren[recorder] = map[string]time.Time{}
	}
	s.barren[recorder][key] = hour
}

// pruneBarren drops a recorder's remembered manifests from hours before since, which
// no later pass of that recorder reads again.
func (s *Syncer) pruneBarren(recorder string, since time.Time) {
	s.barrenMu.Lock()
	defer s.barrenMu.Unlock()
	for k, h := range s.barren[recorder] {
		if h.Before(since) {
			delete(s.barren[recorder], k)
		}
	}
}

func (s *Syncer) readHour(ctx context.Context, rec Recorder, hour time.Time, ingested map[string]struct{}) ([]FileRow, int, int, error) {
	keys, err := dberror.Retry(ctx, fetchRetry, func() ([]string, error) {
		return s.cfg.Bucket.ListManifests(ctx, rec.Prefix, hour)
	})
	if err != nil {
		return nil, 0, 0, err
	}
	var todo []string
	s.barrenMu.Lock()
	barrenHere := s.barren[rec.Prefix]
	for _, k := range keys {
		_, done := ingested[k]
		_, barren := barrenHere[k]
		if !done && !barren {
			todo = append(todo, k)
		}
	}
	s.barrenMu.Unlock()
	perKey := make([][]FileRow, len(todo))
	var skipped int
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultFetchConcurrency)
	for i, key := range todo {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			data, err := dberror.Retry(gctx, fetchRetry, func() ([]byte, error) {
				return s.cfg.Bucket.GetObject(gctx, key)
			})
			if err != nil {
				return err
			}
			rows, err := ParseManifest(data, rec, hour, key)
			if err != nil {
				// A malformed manifest is the recorder's defect, not a reason to stop
				// ingesting everything behind it. It is logged once and remembered as
				// barren, so later passes skip it until its hour leaves the rescan
				// window; a restart forgets it, and it is read (and logged) once more.
				// A manifest corrected in place inside the window is therefore not
				// re-read by this process.
				s.cfg.Logger.Warn("pcapwarehouse: skipping unreadable manifest", "key", key, "error", err)
				mu.Lock()
				skipped++
				mu.Unlock()
				s.markBarren(rec.Prefix, key, hour)
				return nil
			}
			if len(rows) == 0 {
				s.markBarren(rec.Prefix, key, hour)
			}
			perKey[i] = rows
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, 0, err
	}
	var rows []FileRow
	for _, r := range perKey {
		rows = append(rows, r...)
	}
	return rows, len(todo) - skipped, skipped, nil
}
