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

	// archivedPerPass bounds how many archived manifests one pass re-reads. The
	// backfill meets a few hundred across the bucket; a read of one still archived is a
	// cheap refused GET, so this only matters if a whole backlog is pending at once.
	archivedPerPass = 500
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
	RecordArchived(ctx context.Context, manifests []ArchivedManifest, state string) error
	PendingArchived(ctx context.Context, limit int) ([]ArchivedManifest, error)
}

type SyncerConfig struct {
	Logger *slog.Logger
	Bucket Warehouse
	Store  store
	Budget time.Duration // zero means DefaultBudget
	Rescan time.Duration // zero means DefaultRescan
	Now    func() time.Time

	// RepairSince, when set, makes each recorder's first passes in this process re-list
	// every hour from this time — not just the rescan window behind its cursor — and
	// read any manifest no row came from. It recovers what an earlier build dropped
	// behind the cursor, where the normal rescan never looks again: the build that
	// refused a whole manifest for one bad file entry lost every good file beside it.
	// The re-listing is bounded by the pass budget like the backfill and resumes on the
	// next pass; once it reaches the rescan window the recorder carries on as normal.
	// Each process repairs once, so the setting is harmless to leave until convenient.
	RepairSince time.Time
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

	// repairFrom is where each recorder's repair resumes; repaired holds the recorders
	// whose repair has reached the rescan window. Both empty when RepairSince is unset.
	repairMu   sync.Mutex
	repairFrom map[string]time.Time
	repaired   map[string]bool
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
	return &Syncer{
		cfg:        cfg,
		barren:     map[string]map[string]time.Time{},
		repairFrom: map[string]time.Time{},
		repaired:   map[string]bool{},
	}, nil
}

// SyncResult reports what one Sync did.
type SyncResult struct {
	Recorders        int
	Hours            int
	ManifestsRead    int
	ManifestsSkipped int // manifests refused whole (unreadable, unknown version), logged and left out
	// FilesRejected counts file entries dropped from otherwise good manifests — an
	// invalid or out-of-hour packet range. A recorder defect, logged, never escalated.
	FilesRejected int
	FilesWritten  int
	// ManifestsArchived counts manifests newly found archived this pass; they are
	// recorded and retried on later passes, not skipped. ArchivedPending is how many
	// earlier ones were still archived when retried, and ArchivedIndexed how many had
	// been restored and were read.
	ManifestsArchived int
	ArchivedPending   int
	ArchivedIndexed   int
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
	r.FilesRejected += o.filesRejected
	r.FilesWritten += o.filesWritten
	r.ManifestsArchived += o.manifestsArchived
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
	manifestsArchived, filesRejected                     int
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
	if err := s.retryArchived(ctx, res, deadline); err != nil {
		return res, err
	}
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
	if hasCursor {
		since = cursor.Add(-s.cfg.Rescan)
	}
	normalSince := since
	repairing := false
	if from, ok := s.repairStart(rec.Prefix, hasCursor); ok {
		if from.Before(since) {
			since, repairing = from, true
		} else {
			// Nothing behind the rescan window left to repair.
			s.finishRepair(rec.Prefix)
		}
	}
	ingested := map[string]struct{}{}
	if hasCursor {
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
	var archived []ArchivedManifest
	flush := func() error {
		if len(buf) > 0 {
			if err := s.cfg.Store.Insert(ctx, buf); err != nil {
				return err
			}
			rr.filesWritten += len(buf)
			buf = nil
		}
		// Recorded with the rows around them: once the cursor passes an archived hour
		// the rescan never lists it again, so this table is the only way back to it.
		if len(archived) > 0 {
			if err := s.cfg.Store.RecordArchived(ctx, archived, archivedStateArchived); err != nil {
				return err
			}
			rr.manifestsArchived += len(archived)
			archived = nil
		}
		return nil
	}
	// Every pass reads at least one hour past the cursor before it checks the budget.
	// Counting the rescan hours toward that would let a pass whose budget the rescan
	// uses up re-read the same window forever and never advance.
	// A repair is bounded by the budget too, after at least one hour, so a pass that
	// re-lists weeks of history hands the loop back and resumes where it stopped.
	advanced := false
	processed := 0
	var lastHour time.Time
	completed := true
	for _, hour := range hours {
		if s.cfg.Now().After(deadline) && (advanced || (repairing && processed > 0)) {
			rr.behind = true
			completed = false
			break
		}
		hr, err := s.readHour(ctx, rec, hour, ingested)
		if err != nil {
			// Whole hours already buffered are still good; keep them.
			return rr, errors.Join(err, flush())
		}
		rr.hours++
		processed++
		lastHour = hour
		rr.manifestsRead += hr.read
		rr.manifestsSkipped += hr.skipped
		rr.filesRejected += hr.rejected
		archived = append(archived, hr.archived...)
		rows := hr.rows
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
	if err := flush(); err != nil {
		return rr, err
	}
	if repairing {
		// Resume after the last hour written, or stop repairing once the walk has
		// reached the hours the normal rescan covers anyway.
		if completed || !lastHour.Before(normalSince) {
			s.finishRepair(rec.Prefix)
		} else if processed > 0 {
			s.setRepair(rec.Prefix, lastHour.Add(time.Hour))
		}
	}
	return rr, nil
}

// repairStart reports where a recorder's repair should list from, if it has one
// outstanding. A recorder with no cursor is read from its first hour anyway, so it
// has nothing to repair.
func (s *Syncer) repairStart(recorder string, hasCursor bool) (time.Time, bool) {
	if s.cfg.RepairSince.IsZero() {
		return time.Time{}, false
	}
	s.repairMu.Lock()
	defer s.repairMu.Unlock()
	if s.repaired[recorder] {
		return time.Time{}, false
	}
	if !hasCursor {
		s.repaired[recorder] = true
		return time.Time{}, false
	}
	from, ok := s.repairFrom[recorder]
	if !ok {
		from = s.cfg.RepairSince.UTC().Truncate(time.Hour)
		s.repairFrom[recorder] = from
	}
	return from, true
}

func (s *Syncer) setRepair(recorder string, from time.Time) {
	s.repairMu.Lock()
	defer s.repairMu.Unlock()
	s.repairFrom[recorder] = from
}

func (s *Syncer) finishRepair(recorder string) {
	s.repairMu.Lock()
	defer s.repairMu.Unlock()
	delete(s.repairFrom, recorder)
	s.repaired[recorder] = true
	s.cfg.Logger.Info("pcapwarehouse: repair reached the rescan window", "recorder", recorder)
}

// warnRejected logs a manifest's rejected file entries as one line: a recorder with a
// bad clock writes many per manifest, and the first error names the shape.
func (s *Syncer) warnRejected(key string, rejected []error) {
	s.cfg.Logger.Warn("pcapwarehouse: rejected file entries in manifest",
		"key", key, "rejected", len(rejected), "first", rejected[0].Error())
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

type hourResult struct {
	rows                    []FileRow
	read, skipped, rejected int
	archived                []ArchivedManifest
}

func (s *Syncer) readHour(ctx context.Context, rec Recorder, hour time.Time, ingested map[string]struct{}) (hourResult, error) {
	var hr hourResult
	keys, err := dberror.Retry(ctx, fetchRetry, func() ([]string, error) {
		return s.cfg.Bucket.ListManifests(ctx, rec.Prefix, hour)
	})
	if err != nil {
		return hr, err
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
			if errors.Is(err, ErrArchived) {
				// Not a failure of the recorder: recorded for a later pass to read once
				// restored, and remembered here so the rescan window does not re-read it.
				mu.Lock()
				hr.archived = append(hr.archived, ArchivedManifest{Key: key, Recorder: rec, Hour: hour})
				mu.Unlock()
				s.markBarren(rec.Prefix, key, hour)
				return nil
			}
			if err != nil {
				return err
			}
			rows, rejected, err := ParseManifest(data, rec, hour, key)
			if len(rejected) > 0 {
				s.warnRejected(key, rejected)
				mu.Lock()
				hr.rejected += len(rejected)
				mu.Unlock()
			}
			if err != nil {
				// A malformed manifest is the recorder's defect, not a reason to stop
				// ingesting everything behind it. It is logged once and remembered as
				// barren, so later passes skip it until its hour leaves the rescan
				// window; a restart forgets it, and it is read (and logged) once more.
				// A manifest corrected in place inside the window is therefore not
				// re-read by this process. SyncResult.ManifestsSkipped carries the count
				// to the activity, which escalates when refusals continue across passes.
				s.cfg.Logger.Warn("pcapwarehouse: skipping unreadable manifest", "key", key, "error", err)
				mu.Lock()
				hr.skipped++
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
		return hourResult{}, err
	}
	for _, r := range perKey {
		hr.rows = append(hr.rows, r...)
	}
	hr.read = len(todo) - hr.skipped - len(hr.archived)
	return hr, nil
}

// retryArchived re-reads manifests an earlier pass found archived. One restored since
// is indexed like any other; one still archived stays pending; one that turns out to
// be malformed is refused, so it is not retried forever.
func (s *Syncer) retryArchived(ctx context.Context, res *SyncResult, deadline time.Time) error {
	pending, err := s.cfg.Store.PendingArchived(ctx, archivedPerPass)
	if err != nil {
		return err
	}
	var (
		mu               sync.Mutex
		rows             []FileRow
		indexed, refused []ArchivedManifest
		stillArchived    int
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(defaultFetchConcurrency)
	for _, m := range pending {
		g.Go(func() error {
			if s.cfg.Now().After(deadline) {
				mu.Lock()
				stillArchived++
				mu.Unlock()
				return nil
			}
			data, err := dberror.Retry(gctx, fetchRetry, func() ([]byte, error) {
				return s.cfg.Bucket.GetObject(gctx, m.Key)
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, ErrArchived):
				stillArchived++
			case err != nil:
				return err
			default:
				parsed, rejected, perr := ParseManifest(data, m.Recorder, m.Hour, m.Key)
				if len(rejected) > 0 {
					s.warnRejected(m.Key, rejected)
					res.FilesRejected += len(rejected)
				}
				if perr != nil {
					s.cfg.Logger.Warn("pcapwarehouse: skipping unreadable manifest", "key", m.Key, "error", perr)
					res.ManifestsSkipped++
					refused = append(refused, m)
					return nil
				}
				rows = append(rows, parsed...)
				indexed = append(indexed, m)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := s.cfg.Store.Insert(ctx, rows); err != nil {
		return err
	}
	res.FilesWritten += len(rows)
	if err := s.cfg.Store.RecordArchived(ctx, indexed, archivedStateIndexed); err != nil {
		return err
	}
	if err := s.cfg.Store.RecordArchived(ctx, refused, archivedStateRefused); err != nil {
		return err
	}
	res.ArchivedIndexed = len(indexed)
	res.ArchivedPending = stillArchived
	return nil
}
