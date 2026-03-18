package admin

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
	{"shredder", "slot_feed_races"},
	{"shredder_qa", "publisher_shred_stats"},
	{"shredder_qa", "slot_feed_races"},
}

// SetupRemoteTablesConfig holds configuration for creating remote proxy tables.
type SetupRemoteTablesConfig struct {
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

	// Force overwrites existing non-proxy tables.
	Force bool
}

// SetupRemoteTables creates remoteSecure() proxy tables in local ClickHouse
// pointing to a remote ClickHouse Cloud instance.
//
// Proxy tables are created in a separate database (ProxyDatabase, default:
// "<remote>_remote") to avoid accidentally overwriting local data tables.
// Discovers all tables in the remote database and creates proxies, plus
// any configured external service table proxies.
func SetupRemoteTables(log *slog.Logger, cfg SetupRemoteTablesConfig) error {
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
	rows, err := remoteConn.Query(ctx, "SELECT name FROM system.tables WHERE database = ? ORDER BY name", cfg.RemoteDatabase)
	if err != nil {
		return fmt.Errorf("failed to query remote tables: %w", err)
	}
	defer rows.Close()

	created, existing, skipped := 0, 0, 0
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}

		result, err := checkTable(ctx, localConn, cfg.RemoteDatabase, tableName, cfg.Force, log)
		if err != nil {
			return err
		}
		if result == tableSkipped {
			skipped++
			continue
		}
		if result == tableExisting {
			existing++
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
	if created == 0 && existing > 0 && skipped == 0 {
		fmt.Printf("All %d proxy tables in %s already exist (from remote %s database)\n", existing, cfg.RemoteDatabase, cfg.RemoteDatabase)
	} else {
		fmt.Printf("Created %d proxy tables in %s (from remote %s database", created, cfg.RemoteDatabase, cfg.RemoteDatabase)
		if existing > 0 {
			fmt.Printf(", %d already existed", existing)
		}
		if skipped > 0 {
			fmt.Printf(", skipped %d non-proxy tables", skipped)
		}
		fmt.Println(")")
	}

	// Create external table proxies in their original databases
	// (e.g., shredder.publisher_shred_stats) since the API references them
	// with fully qualified names.
	extCreated, extExisting, extSkipped := 0, 0, 0
	for _, t := range externalRemoteTables {
		if err := localConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", t.RemoteDB)); err != nil {
			return fmt.Errorf("failed to create database %s: %w", t.RemoteDB, err)
		}

		result, err := checkTable(ctx, localConn, t.RemoteDB, t.RemoteTable, cfg.Force, log)
		if err != nil {
			return err
		}
		if result == tableSkipped {
			extSkipped++
			continue
		}
		if result == tableExisting {
			extExisting++
			continue
		}

		query := fmt.Sprintf(
			"CREATE OR REPLACE TABLE `%s`.`%s` AS remoteSecure('%s', '%s.%s', '%s', '%s')",
			t.RemoteDB, t.RemoteTable, remoteAddr, t.RemoteDB, t.RemoteTable, cfg.RemoteUser, cfg.RemotePassword,
		)
		if err := localConn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create proxy for %s.%s: %w", t.RemoteDB, t.RemoteTable, err)
		}
		log.Info("created external proxy table", "table", fmt.Sprintf("%s.%s", t.RemoteDB, t.RemoteTable))
		extCreated++
	}
	if extCreated == 0 && extExisting > 0 && extSkipped == 0 {
		fmt.Printf("All %d external proxy tables already exist\n", extExisting)
	} else {
		fmt.Printf("Created %d external proxy tables", extCreated)
		if extExisting > 0 {
			fmt.Printf(" (%d already existed)", extExisting)
		}
		if extSkipped > 0 {
			fmt.Printf(" (skipped %d non-proxy tables)", extSkipped)
		}
		fmt.Println()
	}

	return nil
}

type tableCheckResult int

const (
	tableNew      tableCheckResult = iota // table doesn't exist, create it
	tableExisting                         // proxy already exists, skip it
	tableSkipped                          // non-proxy table exists, skip unless --force
	tableForced                           // non-proxy table exists but --force was set
)

// checkTable checks whether a table already exists and what action to take.
func checkTable(ctx context.Context, conn clickhouse.Connection, database, table string, force bool, log *slog.Logger) (tableCheckResult, error) {
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
		if force {
			return tableNew, nil
		}
		return tableExisting, nil
	}

	fqn := fmt.Sprintf("%s.%s", database, table)
	if force {
		log.Warn("overwriting existing non-proxy table (--force)", "table", fqn, "engine", engine)
		return tableForced, nil
	}

	log.Warn("skipping existing non-proxy table (use --force to overwrite)", "table", fqn, "engine", engine)
	return tableSkipped, nil
}
