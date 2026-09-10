package remotetables

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/stretchr/testify/require"
)

// TestDiscoverProxyableTablesSkipsMaterializedViewInnerTables verifies that the
// table discovery used to build remote proxies excludes ClickHouse's internal
// materialized-view inner tables (".inner_id.<uuid>"). Those cannot be proxied
// via remoteSecure() and previously crashed setup with "Invalid qualified name".
func TestDiscoverProxyableTablesSkipsMaterializedViewInnerTables(t *testing.T) {
	ctx := context.Background()
	log := slog.Default()

	chdb, err := apitesting.NewClickHouseDB(ctx, log, nil)
	require.NoError(t, err)
	t.Cleanup(chdb.Close)

	conn, dbName := apitesting.SetupClickHouseForTest(t, chdb)

	// A materialized view (without an explicit TO table) creates a hidden inner
	// storage table named ".inner_id.<uuid>" in the same database.
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id"))
	require.NoError(t, conn.Exec(ctx, "CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM src"))

	// Sanity: the MV really did create an internal inner table, so the filter
	// is exercising the case it is meant to handle.
	var innerCount uint64
	require.NoError(t, conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name LIKE '.inner%'", dbName,
	).Scan(&innerCount))
	require.Positive(t, innerCount, "expected the materialized view to create an internal .inner table")

	// Exercise discovery through the same client type setup.go uses.
	client, err := clickhouse.NewClient(ctx, log, chdb.Addr(), dbName, chdb.Username(), chdb.Password(), false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	remoteConn, err := client.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = remoteConn.Close() })

	names, err := discoverProxyableTables(ctx, remoteConn, dbName)
	require.NoError(t, err)

	require.Contains(t, names, "src", "base table should be proxied")
	require.Contains(t, names, "mv", "materialized view itself should be proxied")
	for _, n := range names {
		require.False(t, strings.HasPrefix(n, ".inner"), "internal MV inner table must be excluded: %s", n)
	}
}

func TestExternalRemoteTablesIncludesFeeds(t *testing.T) {
	// Every feeds table an API handler reads must be proxied here, or the handler degrades to
	// its empty state in every environment that relies on this setup.
	want := []string{
		"hyperliquid_bbo_feed_race_summary",
		"hyperliquid_bbo_observations",
		"kalshi_bbo_feed_race_summary",
		"kalshi_bbo_observations",
		"kalshi_mbp_levels",
	}
	have := map[string]bool{}
	for _, e := range externalRemoteTables {
		if e.RemoteDB == "feeds" {
			have[e.RemoteTable] = true
		}
	}
	for _, table := range want {
		if !have[table] {
			t.Errorf("externalRemoteTables missing feeds.%s", table)
		}
	}
}

func TestExternalRemoteTablesIncludesCompetitorSource(t *testing.T) {
	// The competitor rollup's source table. Unlike the feeds tables above, a missing proxy here
	// does not degrade to an empty state: ComputeCompetitorDay returns the ClickHouse error
	// verbatim, so every 30s iteration fails and the escalator reaches ERROR. Any environment
	// that sets CLICKHOUSE_COMPETITOR_DB depends on this entry existing.
	for _, e := range externalRemoteTables {
		if e.RemoteDB == "dzf_data" && e.RemoteTable == "competitors_pairwise_feed_race" {
			return
		}
	}
	t.Error("externalRemoteTables missing dzf_data.competitors_pairwise_feed_race")
}
