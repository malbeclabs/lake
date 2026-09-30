package pcapwarehouse

import (
	"context"
	"os"
	"testing"
	"time"

	clickhousetesting "github.com/malbeclabs/lake/indexer/pkg/clickhouse/testing"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

var sharedDB *clickhousetesting.DB

func TestMain(m *testing.M) {
	log := laketesting.NewLogger()
	var err error
	sharedDB, err = clickhousetesting.NewDB(context.Background(), log, nil)
	if err != nil {
		log.Error("failed to create shared DB", "error", err)
		os.Exit(1)
	}
	code := m.Run()
	sharedDB.Close()
	os.Exit(code)
}

// TestStore_RoundTrip writes rows through the bare INSERT and reads them back through
// the queries the sync depends on, which is what catches a ToRow order that no longer
// matches the migration.
func TestStore_RoundTrip(t *testing.T) {
	client := laketesting.NewClient(t, sharedDB)
	st, err := NewStore(StoreConfig{Logger: laketesting.NewLogger(), ClickHouse: client})
	require.NoError(t, err)
	ctx := t.Context()

	rec, _ := ParseRecorder(recA)
	h1, h2 := hourAt(5, 1), hourAt(5, 2)
	first := h2.Add(90 * time.Second).Add(123456 * time.Microsecond)
	row := func(hour time.Time, key, manifest string) FileRow {
		return FileRow{
			FirstPacketTS: first, Recorder: rec.Host, RecorderIP: rec.IP, HourTS: hour,
			S3Key: key, MulticastGroup: "233.84.178.15", MulticastPort: 7, SizeBytes: 52_429_000,
			Packets: 83_107, LastPacketTS: first.Add(9 * time.Second), MD5: "abc",
			ManifestKey: manifest, OffloadTS: h2.Add(5 * time.Minute),
		}
	}
	require.NoError(t, st.Insert(ctx, []FileRow{row(h1, "k1", "m1"), row(h2, "k2", "m2")}))
	// Re-reading a manifest collapses on (s3_key, manifest_key).
	require.NoError(t, st.Insert(ctx, []FileRow{row(h2, "k2", "m2")}))
	// A later offload that reuses the key — a restarted recorder — is kept beside the
	// first, and the current view reads it as the file the bucket holds.
	over := row(h2, "k2", "m3")
	over.OffloadTS = over.OffloadTS.Add(time.Minute)
	over.SizeBytes = 1_000
	require.NoError(t, st.Insert(ctx, []FileRow{over}))

	cursors, err := st.Cursors(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]time.Time{recA: h2}, cursors)

	seen, err := st.IngestedManifests(ctx, rec, h2)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"m2": {}, "m3": {}}, seen)

	conn, err := client.Conn(ctx)
	require.NoError(t, err)
	var (
		n, versions uint64
		group       string
		port        uint16
		size        uint64
		gotFirst    time.Time
	)
	rows, err := conn.Query(ctx, `
		SELECT count(), sum(versions), any(multicast_group), any(multicast_port), sum(size_bytes), min(first_packet_ts)
		FROM dz_edge_pcap_file_current`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&n, &versions, &group, &port, &size, &gotFirst))
	require.Equal(t, uint64(2), n)
	require.Equal(t, uint64(3), versions, "k2 was written by two offloads")
	require.Equal(t, "233.84.178.15", group)
	require.Equal(t, uint16(7), port)
	require.Equal(t, uint64(52_429_000+1_000), size, "k2 reads as its newest offload")
	require.True(t, first.Equal(gotFirst), "microsecond packet time survives: %s vs %s", first, gotFirst)
}

// Archived manifests round-trip through their explicit-column insert, and the newest
// state per key decides whether one is still pending.
func TestStore_ArchivedManifests(t *testing.T) {
	client := laketesting.NewClient(t, sharedDB)
	st, err := NewStore(StoreConfig{Logger: laketesting.NewLogger(), ClickHouse: client})
	require.NoError(t, err)
	ctx := t.Context()

	rec, _ := ParseRecorder(recA)
	older := ArchivedManifest{Key: "m-older", Recorder: rec, Hour: hourAt(1, 2)}
	newer := ArchivedManifest{Key: "m-newer", Recorder: rec, Hour: hourAt(3, 4)}
	require.NoError(t, st.RecordArchived(ctx, []ArchivedManifest{newer, older}, archivedStateArchived))

	pending, err := st.PendingArchived(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ArchivedManifest{older, newer}, pending, "oldest hour first, recorder resolved")

	// updated_at has millisecond resolution; keep the second write strictly later.
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, st.RecordArchived(ctx, []ArchivedManifest{older}, archivedStateIndexed))
	pending, err = st.PendingArchived(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []ArchivedManifest{newer}, pending)

	pending, err = st.PendingArchived(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, pending)
}
