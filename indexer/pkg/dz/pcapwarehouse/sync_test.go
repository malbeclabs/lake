package pcapwarehouse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

// fakeWarehouse serves manifests from memory: recorder prefix -> hour -> manifest keys.
type fakeWarehouse struct {
	mu        sync.Mutex
	hours     map[string]map[time.Time][]string
	objects   map[string][]byte
	gets      int
	listSince map[string]time.Time
	failGet   string
	archived  map[string]bool
}

func newFakeWarehouse() *fakeWarehouse {
	return &fakeWarehouse{hours: map[string]map[time.Time][]string{}, objects: map[string][]byte{}, listSince: map[string]time.Time{}, archived: map[string]bool{}}
}

func (f *fakeWarehouse) put(rec string, hour time.Time, name, group string, first time.Time) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("mainnet-beta/%s/%s/manifest_%s.yaml", rec, hourDir(hour), name)
	if f.hours[rec] == nil {
		f.hours[rec] = map[time.Time][]string{}
	}
	f.hours[rec][hour] = append(f.hours[rec][hour], key)
	f.objects[key] = fmt.Appendf(nil, `manifest_version: 1
offload:
    timestamp_utc: %q
files:
    - filename: capture_%s_%s.pcap
      size_bytes: 100
      packets_captured: 10
      first_packet_utc: %q
      last_packet_utc: %q
      multicast_group: %s
`, first.Add(time.Minute).Format(time.RFC3339), group, name, first.Format(time.RFC3339Nano), first.Add(30*time.Second).Format(time.RFC3339Nano), group)
	return key
}

func (f *fakeWarehouse) ListRecorders(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for r := range f.hours {
		out = append(out, r)
	}
	slices.Sort(out)
	return out, nil
}

func (f *fakeWarehouse) ListHours(_ context.Context, rec string, since time.Time) ([]time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listSince[rec] = since
	var out []time.Time
	for h := range f.hours[rec] {
		if since.IsZero() || h.Add(time.Hour).After(since) {
			out = append(out, h)
		}
	}
	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return out, nil
}

func (f *fakeWarehouse) ListManifests(_ context.Context, rec string, hour time.Time) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.hours[rec][hour]), nil
}

func (f *fakeWarehouse) GetObject(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if key == f.failGet {
		return nil, errors.New("access denied")
	}
	if f.archived[key] {
		return nil, fmt.Errorf("pcapwarehouse: get %s: %w", key, ErrArchived)
	}
	return f.objects[key], nil
}

// memStore is the store contract over a slice.
type memStore struct {
	mu       sync.Mutex
	rows     []FileRow
	archived map[string]ArchivedManifest // key -> manifest, while its state is archived
	states   map[string]string
}

func (m *memStore) RecordArchived(_ context.Context, manifests []ArchivedManifest, state string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.archived == nil {
		m.archived, m.states = map[string]ArchivedManifest{}, map[string]string{}
	}
	for _, a := range manifests {
		m.states[a.Key] = state
		if state == archivedStateArchived {
			m.archived[a.Key] = a
		} else {
			delete(m.archived, a.Key)
		}
	}
	return nil
}

func (m *memStore) PendingArchived(_ context.Context, limit int) ([]ArchivedManifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ArchivedManifest
	for _, a := range m.archived {
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b ArchivedManifest) int { return strings.Compare(a.Key, b.Key) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *memStore) Cursors(context.Context) (map[string]time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]time.Time{}
	for _, r := range m.rows {
		k := r.Recorder + "-" + r.RecorderIP
		if r.HourTS.After(out[k]) {
			out[k] = r.HourTS
		}
	}
	return out, nil
}

func (m *memStore) IngestedManifests(_ context.Context, rec Recorder, since time.Time) (map[string]struct{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]struct{}{}
	for _, r := range m.rows {
		if r.Recorder == rec.Host && r.RecorderIP == rec.IP && !r.HourTS.Before(since) {
			out[r.ManifestKey] = struct{}{}
		}
	}
	return out, nil
}

func (m *memStore) Insert(_ context.Context, rows []FileRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = append(m.rows, rows...)
	return nil
}

const (
	recA = "aws-cmh-mn-recorder1-3.151.138.124"
	recB = "chi-mn-recorder1-208.78.39.181"
)

func hourAt(d, h int) time.Time { return time.Date(2026, 9, d, h, 0, 0, 0, time.UTC) }

func newTestSyncer(t *testing.T, src Warehouse, st store, now func() time.Time, budget time.Duration) *Syncer {
	t.Helper()
	s, err := NewSyncer(SyncerConfig{Logger: laketesting.NewLogger(), Bucket: src, Store: st, Now: now, Budget: budget})
	require.NoError(t, err)
	return s
}

func TestSync_BackfillsThenReadsOnlyNewManifests(t *testing.T) {
	src := newFakeWarehouse()
	for h := range 5 {
		src.put(recA, hourAt(1, h), fmt.Sprintf("a%d", h), "233.84.178.15", hourAt(1, h))
	}
	src.put(recB, hourAt(1, 2), "b", "233.84.178.20", hourAt(1, 2))
	st := &memStore{}
	s := newTestSyncer(t, src, st, time.Now, time.Minute)

	res, err := s.Sync(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, res.Recorders)
	require.Equal(t, 6, res.FilesWritten)
	require.False(t, res.Behind)
	require.True(t, src.listSince[recA].IsZero(), "a recorder with no rows is read from its first hour")

	// A second pass rescans the recent hours but fetches only the new manifest.
	src.gets = 0
	src.put(recA, hourAt(1, 4), "a4-late", "233.84.178.15", hourAt(1, 4).Add(30*time.Minute))
	res, err = s.Sync(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, res.FilesWritten)
	require.Equal(t, 1, src.gets)
	require.Equal(t, hourAt(1, 4).Add(-DefaultRescan), src.listSince[recA])
	require.Len(t, st.rows, 7)
}

func TestSync_BudgetStopsBetweenHoursAndResumes(t *testing.T) {
	src := newFakeWarehouse()
	for h := range 6 {
		src.put(recA, hourAt(2, h), fmt.Sprintf("a%d", h), "233.84.178.15", hourAt(2, h))
	}
	st := &memStore{}
	// Each call to Now advances the clock a minute against a 90s budget, so the
	// first pass reads the first hour and stops before the next.
	var tick time.Time
	var tmu sync.Mutex
	now := func() time.Time {
		tmu.Lock()
		defer tmu.Unlock()
		tick = tick.Add(time.Minute)
		return tick
	}
	s := newTestSyncer(t, src, st, now, 90*time.Second)

	res, err := s.Sync(t.Context())
	require.NoError(t, err)
	require.True(t, res.Behind)
	first := len(st.rows)
	require.Positive(t, first)
	require.Less(t, first, 6)

	for range 10 {
		if _, err := s.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	require.Len(t, st.rows, 6, "passes resume from the cursor until every hour is read, each once")
}

func TestSync_OneRecorderFailingDoesNotStopTheOthers(t *testing.T) {
	src := newFakeWarehouse()
	src.failGet = src.put(recA, hourAt(3, 0), "a", "233.84.178.15", hourAt(3, 0))
	src.put(recB, hourAt(3, 0), "b", "233.84.178.20", hourAt(3, 0))
	st := &memStore{}
	s := newTestSyncer(t, src, st, time.Now, time.Minute)

	res, err := s.Sync(t.Context())
	require.EqualError(t, err, "recorder "+recA+": access denied")
	require.Equal(t, 1, res.FilesWritten)
	require.Equal(t, "chi-mn-recorder1", st.rows[0].Recorder)
}

func TestSync_SkipsUnparseableManifest(t *testing.T) {
	src := newFakeWarehouse()
	bad := src.put(recA, hourAt(4, 0), "bad", "233.84.178.15", hourAt(4, 0))
	src.objects[bad] = []byte("manifest_version: [")
	src.put(recA, hourAt(4, 0), "good", "233.84.178.15", hourAt(4, 0))
	st := &memStore{}
	s := newTestSyncer(t, src, st, time.Now, time.Minute)

	res, err := s.Sync(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, res.ManifestsSkipped)
	require.Equal(t, 1, res.FilesWritten)

	// It wrote no row, so the table cannot say it was read; the syncer remembers it
	// rather than fetching and warning about it on every pass of the rescan window.
	src.gets = 0
	res, err = s.Sync(t.Context())
	require.NoError(t, err)
	require.Zero(t, src.gets)
	require.Zero(t, res.ManifestsSkipped)
}

// An archived manifest is not a failure: the recorder carries on past it, the
// manifest is recorded, later passes retry it, and once restored it is indexed.
func TestSync_ArchivedManifestIsRecordedAndIndexedOnceRestored(t *testing.T) {
	src := newFakeWarehouse()
	src.put(recA, hourAt(6, 0), "a0", "233.84.178.15", hourAt(6, 0))
	archivedKey := src.put(recA, hourAt(6, 1), "a1", "233.84.178.15", hourAt(6, 1))
	src.put(recA, hourAt(6, 2), "a2", "233.84.178.15", hourAt(6, 2))
	src.archived[archivedKey] = true
	st := &memStore{}
	s := newTestSyncer(t, src, st, time.Now, time.Minute)

	res, err := s.Sync(t.Context())
	require.NoError(t, err, "an archived manifest does not fail the recorder")
	require.Equal(t, 1, res.ManifestsArchived)
	require.Zero(t, res.ManifestsSkipped, "archived is not a refusal")
	require.Equal(t, 2, res.FilesWritten, "the hours around it are read")
	require.Equal(t, archivedStateArchived, st.states[archivedKey])

	// Still archived: retried and left pending.
	res, err = s.Sync(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, res.ArchivedPending)
	require.Zero(t, res.ManifestsArchived, "not counted as newly archived again")

	// Restored: the next pass reads it, although its hour is behind the cursor.
	delete(src.archived, archivedKey)
	res, err = s.Sync(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, res.ArchivedIndexed)
	require.Zero(t, res.ArchivedPending)
	require.Equal(t, archivedStateIndexed, st.states[archivedKey])
	require.Len(t, st.rows, 3)
}

// A bad file entry costs that file, not the manifest: the others are written, the
// rejection is counted apart from refused manifests, and nothing escalates on it.
func TestSync_BadFileEntryRejectsOnlyThatFile(t *testing.T) {
	src := newFakeWarehouse()
	h := hourAt(7, 15)
	key := src.put(recA, h, "m", "233.84.178.23", h)
	src.objects[key] = []byte(`manifest_version: 1
files:
    - filename: bad.pcap
      packets_captured: 10
      first_packet_utc: "2026-09-07T15:10:00Z"
      last_packet_utc: "1983-03-16T07:25:38Z"
      multicast_group: 233.84.178.23
    - filename: good.pcap
      packets_captured: 10
      first_packet_utc: "2026-09-07T15:11:00Z"
      last_packet_utc: "2026-09-07T15:12:00Z"
      multicast_group: 233.84.178.23
`)
	st := &memStore{}
	s := newTestSyncer(t, src, st, time.Now, time.Minute)

	res, err := s.Sync(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, res.FilesRejected)
	require.Zero(t, res.ManifestsSkipped)
	require.Equal(t, 1, res.FilesWritten)
	require.True(t, strings.HasSuffix(st.rows[0].S3Key, "/good.pcap"))
}

// dropRows removes a manifest's rows, as if an earlier build had refused it.
func (m *memStore) dropRows(manifestKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = slices.DeleteFunc(m.rows, func(r FileRow) bool { return r.ManifestKey == manifestKey })
}

// A manifest dropped behind the cursor is out of the normal rescan's reach; a repair
// reaches it, within the pass budget, resuming across passes, and once per process.
func TestSync_RepairRecoversManifestsBehindTheCursor(t *testing.T) {
	src := newFakeWarehouse()
	var dropped string
	for h := range 12 {
		k := src.put(recA, hourAt(8, h), fmt.Sprintf("a%02d", h), "233.84.178.15", hourAt(8, h))
		if h == 2 {
			dropped = k
		}
	}
	st := &memStore{}
	_, err := newTestSyncer(t, src, st, time.Now, time.Minute).Sync(t.Context())
	require.NoError(t, err)
	require.Len(t, st.rows, 12)
	st.dropRows(dropped)

	// The normal rescan (3h behind the hour-11 cursor) does not look at hour 2.
	res, err := newTestSyncer(t, src, st, time.Now, time.Minute).Sync(t.Context())
	require.NoError(t, err)
	require.Zero(t, res.FilesWritten)

	// A repair from hour 0, on a clock that spends the 90s budget after one hour: it
	// resumes pass after pass until the dropped hour is read and the walk is done.
	var tick time.Time
	var tmu sync.Mutex
	now := func() time.Time {
		tmu.Lock()
		defer tmu.Unlock()
		tick = tick.Add(time.Minute)
		return tick
	}
	s, err := NewSyncer(SyncerConfig{Logger: laketesting.NewLogger(), Bucket: src, Store: st, Now: now,
		Budget: 90 * time.Second, RepairSince: hourAt(8, 0)})
	require.NoError(t, err)
	res, err = s.Sync(t.Context())
	require.NoError(t, err)
	require.True(t, res.Behind, "the first repair pass stops at the budget")
	require.True(t, src.listSince[recA].Equal(hourAt(8, 0)))
	for range 20 {
		if _, err := s.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	require.Len(t, st.rows, 12, "the dropped manifest is back, and nothing was written twice")
	require.True(t, s.repaired[recA])

	// Once repaired, passes list from the rescan window again.
	_, err = s.Sync(t.Context())
	require.NoError(t, err)
	require.True(t, src.listSince[recA].Equal(hourAt(8, 11).Add(-DefaultRescan)))
}
