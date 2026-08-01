package msdp

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigration_Applies proves the MSDP migration SQL is valid for the
// ClickHouse version lake uses and that every artifact the store/sync
// path touches has materialized.
func TestMigration_Applies(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	cases := []struct {
		name string
		kind string // "table" or "view"
	}{
		// Peers
		{"dim_dz_ip_msdp_peers_history", "table"},
		{"stg_dim_dz_ip_msdp_peers_snapshot", "table"},
		{"dz_ip_msdp_peers_current", "view"},
		// PIM SA cache
		{"dim_dz_ip_msdp_pim_sa_cache_history", "table"},
		{"stg_dim_dz_ip_msdp_pim_sa_cache_snapshot", "table"},
		{"dz_ip_msdp_pim_sa_cache_current", "view"},
		// SA cache (accepted + rejected combined)
		{"dim_dz_ip_msdp_sa_cache_history", "table"},
		{"stg_dim_dz_ip_msdp_sa_cache_snapshot", "table"},
		{"dz_ip_msdp_sa_cache_current", "view"},
		// Enriched MSDP views
		{"enriched_ip_msdp_peers", "view"},
		{"enriched_ip_msdp_pim_sa_cache", "view"},
		{"enriched_ip_msdp_sa_cache", "view"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, err := conn.Query(t.Context(), `
				SELECT engine FROM system.tables
				WHERE database = currentDatabase() AND name = ?
			`, c.name)
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next(), "%s did not materialize", c.name)
			var got string
			require.NoError(t, rows.Scan(&got))
			if c.kind == "view" {
				assert.Equal(t, "View", got, "%s should be a view, got engine %q", c.name, got)
			} else {
				assert.Equal(t, "MergeTree", got, "%s should be MergeTree, got engine %q", c.name, got)
			}
		})
	}
}
