package mroute

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStore_SCD2_RoundTrip is layer B: prove ReplaceEntries -> the
// underlying DimensionType2Dataset.WriteBatch path round-trips correctly for
// the column types and 5-column PK shape declared in EntrySchema.
//
// Three batches exercise the SCD2 contract:
//
//   - batch1 inserts three (S,G) entries; _current view returns all three.
//   - batch2 omits one entry (tombstone), changes one (new attrs), keeps one
//     identical; _current returns the surviving two with the updated payload
//     visible on the changed row.
//   - batch3 re-runs batch2 unchanged; _current is identical and the active
//     history row count does not grow.
//
// This is the test that catches a column-type mismatch, a bad attrs_hash
// expression, or MissingMeansDeleted semantics that differ from what the
// store assumes.
func TestStore_SCD2_RoundTrip(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: info.Client,
	})
	require.NoError(t, err)

	const devicePK = "DzPkAbCdEfGhJkLmNoPqRsTuVwXyZ1234567890aaaaa"

	mk := func(group, source, flags string, rpfIface string, oifs []string, rpf *RPF) Row {
		e := Entry{
			VRF:           "default",
			Mode:          ModeSparse,
			GroupAddress:  group,
			SourceAddress: source,
			RouteFlags:    flags,
			RpfInterface:  rpfIface,
			OifList:       oifs,
			RPF:           rpf,
			CreationTime:  time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		}
		return EntryToRow(devicePK, e)
	}

	rpfOK := &RPF{Rib: "U", Prefix: "10.0.0.1/32", Preference: 200, Metric: 0, Neighbor: "172.16.0.1"}

	batch1 := []Row{
		mk("233.84.178.1", "10.0.0.1", "SMP", "Port-Channel1000", []string{"Port-Channel3000"}, rpfOK),
		mk("233.84.178.1", "10.0.0.2", "ME", "Null0", nil, nil),
		mk("233.84.178.10", "10.0.0.3", "SBNP", "Tunnel504", []string{"Port-Channel3000", "Port-Channel1000"}, nil),
	}

	t.Run("batch1: initial insert visible in _current", func(t *testing.T) {
		require.NoError(t, store.ReplaceEntries(t.Context(), batch1))

		got := queryCurrent(t, info.Client, devicePK)
		require.Len(t, got, 3)
		assertHasEntry(t, got, "233.84.178.1", "10.0.0.1", "SMP", `["Port-Channel3000"]`, 1)
		assertHasEntry(t, got, "233.84.178.1", "10.0.0.2", "ME", `[]`, 0)
		assertHasEntry(t, got, "233.84.178.10", "10.0.0.3", "SBNP", `["Port-Channel3000","Port-Channel1000"]`, 2)
	})

	// batch2: drop "10.0.0.2", change OIF for "10.0.0.1", keep "10.0.0.3".
	batch2 := []Row{
		mk("233.84.178.1", "10.0.0.1", "SMP", "Port-Channel1000", []string{"Port-Channel3000", "Port-Channel2000"}, rpfOK),
		mk("233.84.178.10", "10.0.0.3", "SBNP", "Tunnel504", []string{"Port-Channel3000", "Port-Channel1000"}, nil),
	}

	t.Run("batch2: missing row tombstoned, changed row updates", func(t *testing.T) {
		require.NoError(t, store.ReplaceEntries(t.Context(), batch2))

		got := queryCurrent(t, info.Client, devicePK)
		require.Len(t, got, 2, "10.0.0.2 should be tombstoned out of _current")
		for _, row := range got {
			assert.NotEqual(t, "10.0.0.2", row.SourceAddress, "tombstoned entry should not appear in _current")
		}
		assertHasEntry(t, got, "233.84.178.1", "10.0.0.1", "SMP", `["Port-Channel3000","Port-Channel2000"]`, 2)
		assertHasEntry(t, got, "233.84.178.10", "10.0.0.3", "SBNP", `["Port-Channel3000","Port-Channel1000"]`, 2)
	})

	t.Run("batch3: identical re-run does not grow active history", func(t *testing.T) {
		before := historyActiveCount(t, info.Client, devicePK)
		require.NoError(t, store.ReplaceEntries(t.Context(), batch2))
		after := historyActiveCount(t, info.Client, devicePK)
		assert.Equal(t, before, after,
			"identical batch should produce no new active history rows (attrs_hash unchanged)")

		got := queryCurrent(t, info.Client, devicePK)
		require.Len(t, got, 2)
		assertHasEntry(t, got, "233.84.178.1", "10.0.0.1", "SMP", `["Port-Channel3000","Port-Channel2000"]`, 2)
		assertHasEntry(t, got, "233.84.178.10", "10.0.0.3", "SBNP", `["Port-Channel3000","Port-Channel1000"]`, 2)
	})

	t.Run("empty batch tombstones all remaining entries", func(t *testing.T) {
		require.NoError(t, store.ReplaceEntries(t.Context(), nil))

		got := queryCurrent(t, info.Client, devicePK)
		assert.Empty(t, got, "empty batch with MissingMeansDeleted=true should tombstone all remaining entries")
	})
}

type currentRow struct {
	DevicePubkey  string
	GroupAddress  string
	SourceAddress string
	RouteFlags    string
	OifList       string
	OifCount      int64
}

func queryCurrent(t *testing.T, client clickhouse.Client, devicePK string) []currentRow {
	t.Helper()
	conn, err := client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT device_pubkey, group_address, source_address, route_flags, oif_list, oif_count
		FROM dz_ip_mroute_entries_current
		WHERE device_pubkey = ?
		ORDER BY group_address, source_address
	`, devicePK)
	require.NoError(t, err)
	defer rows.Close()

	var out []currentRow
	for rows.Next() {
		var r currentRow
		require.NoError(t, rows.Scan(&r.DevicePubkey, &r.GroupAddress, &r.SourceAddress, &r.RouteFlags, &r.OifList, &r.OifCount))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

func historyActiveCount(t *testing.T, client clickhouse.Client, devicePK string) uint64 {
	t.Helper()
	conn, err := client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT count() FROM dim_dz_ip_mroute_entries_history
		WHERE device_pubkey = ? AND is_deleted = 0
	`, devicePK)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var n uint64
	require.NoError(t, rows.Scan(&n))
	return n
}

func assertHasEntry(t *testing.T, got []currentRow, group, source, flags, oifListJSON string, oifCount int64) {
	t.Helper()
	for _, r := range got {
		if r.GroupAddress == group && r.SourceAddress == source {
			assert.Equal(t, flags, r.RouteFlags, "%s,%s: route_flags", group, source)
			assert.Equal(t, oifListJSON, r.OifList, "%s,%s: oif_list", group, source)
			assert.Equal(t, oifCount, r.OifCount, "%s,%s: oif_count", group, source)
			return
		}
	}
	t.Errorf("no entry for (%s, %s) in: %+v", group, source, got)
}
