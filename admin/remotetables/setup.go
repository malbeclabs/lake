package remotetables

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
)

// remoteSecurePort is the ClickHouse Cloud secure native port.
const remoteSecurePort = "9440"

// externalRemoteTables defines cross-service tables to proxy from remote ClickHouse.
// Add new entries here when additional external tables are needed.
var externalRemoteTables = []struct {
	RemoteDB    string
	RemoteTable string
}{
	{"shredder", "publisher_shred_stats"},
	{"shredder", "shred_timing_events"},
	{"shredder", "slot_feed_races"},
	{"shredder", "slot_feed_race_summary"},
	{"shredder", "slot_feed_race_summary_v2"},
	{"shredder_qa", "publisher_shred_stats"},
	{"shredder_qa", "shred_timing_events"},
	{"shredder_qa", "slot_feed_races"},
	{"shredder_qa", "slot_feed_race_summary"},
	{"shredder_qa", "slot_feed_race_summary_v2"},
	{"mainnet-beta", "location_offsets"},
	{"testnet", "location_offsets"},
	{"testnet", "controller_grpc_getconfig_success"},
	{"mainnet-beta", "controller_grpc_getconfig_success"},
	{"dzdp", "offsets"},
	{"dzdp", "location_decisions"},
	{"dzdp", "location_state"},
	{"feeds", "hyperliquid_bbo_feed_race_summary"},
	{"feeds", "hyperliquid_bbo_observations"},
	{"feeds", "kalshi_bbo_feed_race_summary"},
	{"feeds", "kalshi_bbo_observations"},
	{"feeds", "kalshi_mbp_levels"},
	{"dzf_data", "competitors_pairwise_feed_race"},
}

// externalRemoteDatabases lists remote databases to mirror in full, discovering
// tables dynamically. Migration-tracking tables (goose_db_version) are skipped.
var externalRemoteDatabases = []string{
	"telemetry_testnet",
	"telemetry_mainnet_beta",
}

// skippedExternalTables are table names excluded when mirroring an entire
// external database (e.g., goose migration tracking).
var skippedExternalTables = map[string]bool{
	"goose_db_version": true,
}

// Config holds configuration for creating remote proxy tables.
type Config struct {
	// LocalAddr is the local ClickHouse address (host:port).
	LocalAddr string
	// LocalDatabase is the local ClickHouse database to connect to.
	LocalDatabase string
	// LocalUsername is the local ClickHouse username.
	LocalUsername string
	// LocalPassword is the local ClickHouse password.
	LocalPassword string
	// LocalSecure enables TLS for the local connection.
	LocalSecure bool

	// RemoteHost is the remote ClickHouse Cloud host.
	RemoteHost string
	// RemoteUser is the remote ClickHouse Cloud user.
	RemoteUser string
	// RemotePassword is the remote ClickHouse Cloud password.
	RemotePassword string
	// RemoteDatabase is the remote database to discover tables from (default: "lake").
	// Proxy tables are created in a local database with the same name, since
	// ClickHouse pushes multi-table queries to the remote where the database
	// name must match.
	RemoteDatabase string
}

// Setup creates remoteSecure() proxy tables in local ClickHouse
// pointing to a remote ClickHouse Cloud instance.
//
// Discovers all tables in the remote database and creates proxies, plus
// any configured external service table proxies.
func Setup(log *slog.Logger, cfg Config) error {
	ctx := context.Background()
	remoteAddr := cfg.RemoteHost + ":" + remoteSecurePort

	if cfg.RemoteDatabase == "" {
		cfg.RemoteDatabase = "lake"
	}

	// Connect to local ClickHouse
	localDB, err := clickhouse.NewClient(ctx, log, cfg.LocalAddr, cfg.LocalDatabase, cfg.LocalUsername, cfg.LocalPassword, cfg.LocalSecure)
	if err != nil {
		return fmt.Errorf("failed to connect to local ClickHouse: %w", err)
	}
	defer localDB.Close()

	localConn, err := localDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get local connection: %w", err)
	}
	defer localConn.Close()

	// Create the proxy database if it doesn't exist
	if err := localConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", cfg.RemoteDatabase)); err != nil {
		return fmt.Errorf("failed to create proxy database %s: %w", cfg.RemoteDatabase, err)
	}

	// Connect to remote ClickHouse to discover tables
	remoteDB, err := clickhouse.NewClient(ctx, log, remoteAddr, cfg.RemoteDatabase, cfg.RemoteUser, cfg.RemotePassword, true)
	if err != nil {
		return fmt.Errorf("failed to connect to remote ClickHouse: %w", err)
	}
	defer remoteDB.Close()

	remoteConn, err := remoteDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get remote connection: %w", err)
	}
	defer remoteConn.Close()

	// Discover all tables in the remote database
	tableNames, err := discoverProxyableTables(ctx, remoteConn, cfg.RemoteDatabase)
	if err != nil {
		return err
	}

	created, skipped := 0, 0
	for _, tableName := range tableNames {
		result, err := checkTable(ctx, localConn, cfg.RemoteDatabase, tableName, log)
		if err != nil {
			return err
		}
		if result == tableSkipped {
			skipped++
			continue
		}

		query := fmt.Sprintf(
			"CREATE OR REPLACE TABLE `%s`.`%s` AS remoteSecure('%s', '%s.%s', '%s', '%s')",
			cfg.RemoteDatabase, tableName, remoteAddr, cfg.RemoteDatabase, tableName, cfg.RemoteUser, cfg.RemotePassword,
		)
		if err := localConn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create proxy for %s.%s: %w", cfg.RemoteDatabase, tableName, err)
		}
		log.Info("created proxy table", "table", fmt.Sprintf("%s.%s", cfg.RemoteDatabase, tableName))
		created++
	}
	fmt.Printf("Created %d proxy tables in %s (from remote %s database", created, cfg.RemoteDatabase, cfg.RemoteDatabase)
	if skipped > 0 {
		fmt.Printf(", skipped %d non-proxy tables", skipped)
	}
	fmt.Println(")")

	// Create external table proxies in their original databases
	// (e.g., shredder.publisher_shred_stats) since the API references them
	// with fully qualified names.
	extCreated, extSkipped := 0, 0
	for _, t := range externalRemoteTables {
		if err := localConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", t.RemoteDB)); err != nil {
			return fmt.Errorf("failed to create database %s: %w", t.RemoteDB, err)
		}

		result, err := checkTable(ctx, localConn, t.RemoteDB, t.RemoteTable, log)
		if err != nil {
			return err
		}
		if result == tableSkipped {
			extSkipped++
			continue
		}

		query := fmt.Sprintf(
			"CREATE OR REPLACE TABLE `%s`.`%s` AS remoteSecure('%s', '%s.%s', '%s', '%s')",
			t.RemoteDB, t.RemoteTable, remoteAddr, t.RemoteDB, t.RemoteTable, cfg.RemoteUser, cfg.RemotePassword,
		)
		if err := localConn.Exec(ctx, query); err != nil {
			log.Warn("skipping external proxy table (remote table may not exist)", "table", fmt.Sprintf("%s.%s", t.RemoteDB, t.RemoteTable), "error", err)
			extSkipped++
			continue
		}
		log.Info("created external proxy table", "table", fmt.Sprintf("%s.%s", t.RemoteDB, t.RemoteTable))
		extCreated++
	}
	fmt.Printf("Created %d external proxy tables", extCreated)
	if extSkipped > 0 {
		fmt.Printf(" (skipped %d non-proxy tables)", extSkipped)
	}
	fmt.Println()

	// Mirror entire external databases by discovering tables on the remote.
	for _, db := range externalRemoteDatabases {
		dbCreated, dbSkipped, err := mirrorRemoteDatabase(ctx, log, localConn, remoteConn, remoteAddr, db, cfg.RemoteUser, cfg.RemotePassword)
		if err != nil {
			return err
		}
		fmt.Printf("Created %d proxy tables in %s", dbCreated, db)
		if dbSkipped > 0 {
			fmt.Printf(" (skipped %d tables)", dbSkipped)
		}
		fmt.Println()
	}

	return nil
}

// mirrorRemoteDatabase discovers all tables in the given remote database and
// creates remoteSecure proxies for each in a local database of the same name,
// skipping migration-tracking tables.
func mirrorRemoteDatabase(ctx context.Context, log *slog.Logger, localConn, remoteConn clickhouse.Connection, remoteAddr, database, remoteUser, remotePassword string) (created, skipped int, err error) {
	if err := localConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)); err != nil {
		return 0, 0, fmt.Errorf("failed to create database %s: %w", database, err)
	}

	tableNames, err := discoverProxyableTables(ctx, remoteConn, database)
	if err != nil {
		return 0, 0, err
	}

	for _, tableName := range tableNames {
		if skippedExternalTables[tableName] {
			log.Info("skipping excluded table", "table", fmt.Sprintf("%s.%s", database, tableName))
			skipped++
			continue
		}

		result, err := checkTable(ctx, localConn, database, tableName, log)
		if err != nil {
			return 0, 0, err
		}
		if result == tableSkipped {
			skipped++
			continue
		}

		query := fmt.Sprintf(
			"CREATE OR REPLACE TABLE `%s`.`%s` AS remoteSecure('%s', '%s.%s', '%s', '%s')",
			database, tableName, remoteAddr, database, tableName, remoteUser, remotePassword,
		)
		if err := localConn.Exec(ctx, query); err != nil {
			log.Warn("skipping proxy table (remote table may not exist)", "table", fmt.Sprintf("%s.%s", database, tableName), "error", err)
			skipped++
			continue
		}
		log.Info("created proxy table", "table", fmt.Sprintf("%s.%s", database, tableName))
		created++
	}
	return created, skipped, nil
}

// discoverProxyableTables returns the names of tables in the given remote
// database to proxy, excluding ClickHouse internal materialized-view inner
// tables (".inner.<name>" and ".inner_id.<uuid>"). Those inner tables cannot be
// proxied: their leading-dot names are not valid qualified names, so
// remoteSecure('db.<name>') is rejected with "Invalid qualified name" (code 62).
func discoverProxyableTables(ctx context.Context, remoteConn clickhouse.Connection, database string) ([]string, error) {
	rows, err := remoteConn.Query(ctx,
		"SELECT name FROM system.tables WHERE database = ? AND name NOT LIKE '.inner%' ORDER BY name",
		database,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query remote tables for %s: %w", database, err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name for %s: %w", database, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating remote tables for %s: %w", database, err)
	}
	return names, nil
}

type tableCheckResult int

const (
	tableNew     tableCheckResult = iota // table doesn't exist or is a proxy, (re)create it
	tableSkipped                         // non-proxy table exists, leave it alone
)

// checkTable checks whether a table already exists and what action to take.
func checkTable(ctx context.Context, conn clickhouse.Connection, database, table string, log *slog.Logger) (tableCheckResult, error) {
	rows, err := conn.Query(ctx,
		"SELECT engine FROM system.tables WHERE database = ? AND name = ?",
		database, table,
	)
	if err != nil {
		return tableNew, fmt.Errorf("failed to check table %s.%s: %w", database, table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return tableNew, nil
	}

	var engine string
	if err := rows.Scan(&engine); err != nil {
		return tableNew, fmt.Errorf("failed to scan engine for %s.%s: %w", database, table, err)
	}

	if engine == "StorageProxy" {
		return tableNew, nil
	}

	log.Warn("skipping existing non-proxy table", "table", fmt.Sprintf("%s.%s", database, table), "engine", engine)
	return tableSkipped, nil
}
