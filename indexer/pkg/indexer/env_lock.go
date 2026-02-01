package indexer

import (
	"context"
	"fmt"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
)

func checkClickHouseEnvLock(ctx context.Context, ch clickhouse.Client, dzEnv string) error {
	conn, err := ch.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	rows, err := conn.Query(ctx, "SELECT dz_env FROM _env_lock LIMIT 1")
	if err != nil {
		return fmt.Errorf("failed to query env lock: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var storedEnv string
		if err := rows.Scan(&storedEnv); err != nil {
			return fmt.Errorf("failed to scan env lock: %w", err)
		}
		if storedEnv != dzEnv {
			return fmt.Errorf("database is locked to env %q but indexer is configured for %q", storedEnv, dzEnv)
		}
		return nil
	}

	// No rows — insert the lock
	if err := conn.Exec(ctx, "INSERT INTO _env_lock (dz_env) VALUES ($1)", dzEnv); err != nil {
		return fmt.Errorf("failed to insert env lock: %w", err)
	}
	return nil
}

func checkNeo4jEnvLock(ctx context.Context, client neo4j.Client, dzEnv string) error {
	sess, err := client.Session(ctx)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer sess.Close(ctx)

	res, err := sess.Run(ctx, "MATCH (l:_EnvLock) RETURN l.dz_env AS dz_env LIMIT 1", nil)
	if err != nil {
		return fmt.Errorf("failed to query env lock: %w", err)
	}

	if res.Next(ctx) {
		record := res.Record()
		storedEnv, _ := record.Get("dz_env")
		if storedEnvStr, ok := storedEnv.(string); ok && storedEnvStr != dzEnv {
			return fmt.Errorf("database is locked to env %q but indexer is configured for %q", storedEnvStr, dzEnv)
		}
		return nil
	}

	// No lock exists — create one
	_, err = sess.Run(ctx, "CREATE (l:_EnvLock {dz_env: $env})", map[string]any{"env": dzEnv})
	if err != nil {
		return fmt.Errorf("failed to create env lock: %w", err)
	}
	return nil
}
