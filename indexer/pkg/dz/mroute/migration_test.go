package mroute

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigration_Applies is layer A: prove the migration SQL is valid for the
// ClickHouse version lake uses and that every artifact the store/sync path
// touches has materialized. Reused across the rest of this package's
// integration tests via laketesting.NewClient (which runs RunMigrations).
func TestMigration_Applies(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	cases := []struct {
		name string
		kind string // "table", "view", or "mv" (refreshable materialized view)
	}{
		{"dim_dz_ip_mroute_entries_history", "table"},
		{"stg_dim_dz_ip_mroute_entries_snapshot", "table"},
		{"dz_ip_mroute_entries_current", "view"},
		{"dz_device_interface_ips", "mv"},
		{"enriched_ip_mroute", "view"},
		{"enriched_ip_mroute_oifs", "view"},
		{"health_mroute", "view"},
		{"health_missing_sg", "view"},
		{"health_multicast_user", "view"},
		{"health_publisher_subscriber_path", "view"},
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
			switch c.kind {
			case "view":
				assert.Equal(t, "View", got, "%s should be a view, got engine %q", c.name, got)
			case "mv":
				assert.Equal(t, "MaterializedView", got, "%s should be a MaterializedView, got engine %q", c.name, got)
			default:
				assert.Equal(t, "MergeTree", got, "%s should be MergeTree, got engine %q", c.name, got)
			}
		})
	}
}
