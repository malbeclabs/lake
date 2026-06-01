package msdp

import (
	"testing"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStore_SACache_SCD2_RoundTrip exercises the most complex of the
// three MSDP dimensions: dim_dz_ip_msdp_sa_cache, which has a 5-column
// PK including the `status` discriminator that lets the same (S,G)
// appear in both accepted and rejected buckets simultaneously.
//
// Three batches:
//   - batch1 inserts 3 rows (2 accepted + 1 rejected); _current shows all.
//   - batch2 drops one accepted row, flips a rejected row to accepted (new
//     entity_id, since status is in the PK), keeps one row unchanged.
//   - batch3 re-runs batch2 identically and asserts no new active history.
func TestStore_SACache_SCD2_RoundTrip(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: info.Client,
	})
	require.NoError(t, err)

	const devicePK = "DzPkMSDP1111111111111111111111111111111111"

	mk := func(group, source, remote, status string) SACacheRow {
		return SACacheToRow(devicePK, SACacheEntry{
			GroupAddress:  group,
			SourceAddress: source,
			RemoteAddress: remote,
			RPAddress:     "10.0.0.0",
			Status:        SACacheStatus(status),
		})
	}

	batch1 := []SACacheRow{
		mk("233.84.178.1", "148.51.120.1", "172.16.0.1", "accepted"),
		mk("233.84.178.1", "148.51.120.2", "172.16.0.2", "accepted"),
		mk("233.84.178.99", "10.99.99.99", "172.16.0.9", "rejected"),
	}

	t.Run("batch1: initial 3 rows visible in _current", func(t *testing.T) {
		require.NoError(t, store.ReplaceSACache(t.Context(), batch1))
		got := queryCurrentSACache(t, info.Client, devicePK)
		require.Len(t, got, 3)
		assertHasSACache(t, got, "148.51.120.1", "172.16.0.1", "accepted")
		assertHasSACache(t, got, "148.51.120.2", "172.16.0.2", "accepted")
		assertHasSACache(t, got, "10.99.99.99", "172.16.0.9", "rejected")
	})

	// batch2: drop 148.51.120.2, change the rejected SA to accepted (new
	// PK because status is in the key — so 99.99.99/rejected becomes
	// tombstoned and 99.99.99/accepted is created), keep 120.1 as-is.
	batch2 := []SACacheRow{
		mk("233.84.178.1", "148.51.120.1", "172.16.0.1", "accepted"),
		mk("233.84.178.99", "10.99.99.99", "172.16.0.9", "accepted"),
	}

	t.Run("batch2: missing tombstoned, status flip behaves as PK change", func(t *testing.T) {
		require.NoError(t, store.ReplaceSACache(t.Context(), batch2))
		got := queryCurrentSACache(t, info.Client, devicePK)
		require.Len(t, got, 2, "120.2 tombstoned, 99.99.99/rejected tombstoned, 99.99.99/accepted new")

		for _, r := range got {
			assert.NotEqual(t, "rejected", r.Status, "no rejected rows should remain in _current")
			assert.NotEqual(t, "148.51.120.2", r.SourceAddress, "120.2 was omitted")
		}
		assertHasSACache(t, got, "148.51.120.1", "172.16.0.1", "accepted")
		assertHasSACache(t, got, "10.99.99.99", "172.16.0.9", "accepted")
	})

	t.Run("batch3: identical re-run does not grow active history", func(t *testing.T) {
		before := historyActiveSACacheCount(t, info.Client, devicePK)
		require.NoError(t, store.ReplaceSACache(t.Context(), batch2))
		after := historyActiveSACacheCount(t, info.Client, devicePK)
		assert.Equal(t, before, after,
			"identical batch should produce no new active history rows (attrs_hash unchanged)")

		got := queryCurrentSACache(t, info.Client, devicePK)
		require.Len(t, got, 2)
	})
}

type currentSACache struct {
	DevicePubkey  string
	GroupAddress  string
	SourceAddress string
	RemoteAddress string
	Status        string
	RPAddress     string
}

func queryCurrentSACache(t *testing.T, client clickhouse.Client, devicePK string) []currentSACache {
	t.Helper()
	conn, err := client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT device_pubkey, group_address, source_address, remote_address, status, rp_address
		FROM dz_ip_msdp_sa_cache_current
		WHERE device_pubkey = ?
		ORDER BY group_address, source_address, remote_address, status
	`, devicePK)
	require.NoError(t, err)
	defer rows.Close()

	var out []currentSACache
	for rows.Next() {
		var r currentSACache
		require.NoError(t, rows.Scan(&r.DevicePubkey, &r.GroupAddress, &r.SourceAddress, &r.RemoteAddress, &r.Status, &r.RPAddress))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

func historyActiveSACacheCount(t *testing.T, client clickhouse.Client, devicePK string) uint64 {
	t.Helper()
	conn, err := client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT count() FROM dim_dz_ip_msdp_sa_cache_history
		WHERE device_pubkey = ? AND is_deleted = 0
	`, devicePK)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var n uint64
	require.NoError(t, rows.Scan(&n))
	return n
}

func assertHasSACache(t *testing.T, got []currentSACache, source, remote, status string) {
	t.Helper()
	for _, r := range got {
		if r.SourceAddress == source && r.RemoteAddress == remote && r.Status == status {
			return
		}
	}
	t.Errorf("no row for (source=%s, remote=%s, status=%s) in: %+v", source, remote, status, got)
}
