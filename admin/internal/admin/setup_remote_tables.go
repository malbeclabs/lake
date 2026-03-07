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
	{"shredder_qa", "publisher_shred_stats"},
}

// SetupRemoteTables creates remoteSecure() proxy tables in local ClickHouse
// pointing to a remote ClickHouse Cloud instance.
//
// mode "all": discover all tables in the remote lake database and create proxies,
// then create all external table proxies.
// mode "external": only create external table proxies (e.g., shredder.publisher_shred_stats).
func SetupRemoteTables(
	log *slog.Logger,
	localAddr, localDatabase, localUsername, localPassword string, localSecure bool,
	remoteHost, remoteUser, remotePassword string,
	mode string,
) error {
	ctx := context.Background()
	remoteAddr := remoteHost + ":" + remoteSecurePort

	// Connect to local ClickHouse
	localDB, err := clickhouse.NewClient(ctx, log, localAddr, localDatabase, localUsername, localPassword, localSecure)
	if err != nil {
		return fmt.Errorf("failed to connect to local ClickHouse: %w", err)
	}
	defer localDB.Close()

	localConn, err := localDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get local connection: %w", err)
	}
	defer localConn.Close()

	if mode == "all" {
		// Connect to remote ClickHouse to discover tables
		remoteDB, err := clickhouse.NewClient(ctx, log, remoteAddr, localDatabase, remoteUser, remotePassword, true)
		if err != nil {
			return fmt.Errorf("failed to connect to remote ClickHouse: %w", err)
		}
		defer remoteDB.Close()

		remoteConn, err := remoteDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("failed to get remote connection: %w", err)
		}
		defer remoteConn.Close()

		// Discover all tables in the remote lake database
		rows, err := remoteConn.Query(ctx, "SELECT name FROM system.tables WHERE database = ? ORDER BY name", localDatabase)
		if err != nil {
			return fmt.Errorf("failed to query remote tables: %w", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("failed to scan table name: %w", err)
			}

			query := fmt.Sprintf(
				"CREATE OR REPLACE TABLE `%s`.`%s` AS remoteSecure('%s', '%s.%s', '%s', '%s')",
				localDatabase, tableName, remoteAddr, localDatabase, tableName, remoteUser, remotePassword,
			)
			if err := localConn.Exec(ctx, query); err != nil {
				return fmt.Errorf("failed to create proxy for %s.%s: %w", localDatabase, tableName, err)
			}
			log.Info("created proxy table", "table", fmt.Sprintf("%s.%s", localDatabase, tableName))
			count++
		}
		fmt.Printf("Created %d proxy tables from %s database\n", count, localDatabase)
	}

	// Create external table proxies
	for _, t := range externalRemoteTables {
		if err := localConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", t.RemoteDB)); err != nil {
			return fmt.Errorf("failed to create database %s: %w", t.RemoteDB, err)
		}

		query := fmt.Sprintf(
			"CREATE OR REPLACE TABLE `%s`.`%s` AS remoteSecure('%s', '%s.%s', '%s', '%s')",
			t.RemoteDB, t.RemoteTable, remoteAddr, t.RemoteDB, t.RemoteTable, remoteUser, remotePassword,
		)
		if err := localConn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create proxy for %s.%s: %w", t.RemoteDB, t.RemoteTable, err)
		}
		log.Info("created external proxy table", "table", fmt.Sprintf("%s.%s", t.RemoteDB, t.RemoteTable))
	}
	fmt.Printf("Created %d external proxy tables\n", len(externalRemoteTables))

	return nil
}
