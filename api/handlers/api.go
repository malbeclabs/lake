package handlers

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
)

var errNoPgPool = errors.New("postgres not configured")

// API holds all dependencies for HTTP handlers. It is constructed once at
// startup and passed through to every handler and middleware, replacing the
// former package-level globals in api/config.
type API struct {
	// ClickHouse
	DB           driver.Conn
	HealthDB     driver.Conn
	EnvDBs       map[string]driver.Conn
	EnvDatabases map[string]string
	Database     string
	ShredderDB   string

	// PostgreSQL
	PgPool *pgxpool.Pool

	// Neo4j
	Neo4jClient   neo4j.Client
	Neo4jDatabase string

	// Build info
	BuildVersion string
	BuildCommit  string
	BuildDate    string

	// Workflow manager (manages background workflow execution)
	Manager *WorkflowManager

	// OnSlackInstallationChange is called when a Slack installation changes.
	OnSlackInstallationChange func(teamID string)
}

// envDB returns the ClickHouse connection for the environment in the context.
func (a *API) envDB(ctx context.Context) driver.Conn {
	env := string(EnvFromContext(ctx))
	if conn, ok := a.EnvDBs[env]; ok {
		return conn
	}
	return a.DB
}

// databaseForEnvFromContext returns the database name for the environment in the context.
func (a *API) DatabaseForEnvFromContext(ctx context.Context) string {
	env := EnvFromContext(ctx)
	if db, ok := a.EnvDatabases[string(env)]; ok {
		return db
	}
	return a.Database
}

// buildEnvContext returns the agent system prompt context for the given environment.
func (a *API) buildEnvContext(env DZEnv) string {
	return BuildEnvContext(env, a.Database)
}

// neo4jSession creates a new Neo4j session.
func (a *API) neo4jSession(ctx context.Context) neo4j.Session {
	session, _ := a.Neo4jClient.Session(ctx)
	return session
}

// availableEnvs returns the list of environments that have databases configured.
func (a *API) availableEnvs() []string {
	envs := make([]string, 0, len(a.EnvDatabases))
	for env := range a.EnvDatabases {
		envs = append(envs, env)
	}
	return envs
}

// readPageCache reads a cached JSON value from Postgres.
func (a *API) readPageCache(ctx context.Context, key string) (json.RawMessage, error) {
	if a.PgPool == nil {
		return nil, errNoPgPool
	}
	var data json.RawMessage
	err := a.PgPool.QueryRow(ctx,
		`SELECT data FROM page_cache WHERE key = $1`, key,
	).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WritePageCache upserts a cache entry in Postgres.
func (a *API) WritePageCache(ctx context.Context, key string, value any) error {
	if a.PgPool == nil {
		return errNoPgPool
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = a.PgPool.Exec(ctx,
		`INSERT INTO page_cache (key, data, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (key) DO UPDATE SET data = $2, updated_at = NOW()`,
		key, data,
	)
	return err
}
