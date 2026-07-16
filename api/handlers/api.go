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

// ShredGroupPK is the multicast group PK for edge-solana-shreds (formerly "bebop").
const ShredGroupPK = "31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj"

// ShredsInternalUserPayer is the primary UserPayer used by the Shreds product when
// creating access passes on behalf of clients. Passes with this payer are
// product-managed and should not be grouped as "same payer" matches with each other.
const ShredsInternalUserPayer = "331ov6bjNUTLTATEUC4m7wxdHfAE5KxWwA6ng1Y1VZh8"

// ShredsInternalUserPayer2 is a second UserPayer used by the Shreds product.
const ShredsInternalUserPayer2 = "3b2Ze7VYUvhwQBfx5oCMCmsc2xvyZ74s2Lata5vmQeeN"

// isShredsInternalPayer reports whether payer is one of the internal Shreds
// product payers that should be matched only by client_ip, not by owner/payer.
func isShredsInternalPayer(payer string) bool {
	return payer == ShredsInternalUserPayer || payer == ShredsInternalUserPayer2
}

// API holds all dependencies for HTTP handlers. It is constructed once at
// startup and passed through to every handler and middleware, replacing the
// former package-level globals in api/config.
type API struct {
	// ClickHouse
	DB            driver.Conn
	HealthDB      driver.Conn
	PublicQueryDB driver.Conn
	EnvDBs        map[string]driver.Conn
	EnvDatabases  map[string]string
	Database      string
	ShredderDB    string
	PublisherDB   string
	DZDPDB        string
	FeedsDB       string

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

	// TelegramSender sends outbound Telegram messages. Set to the real
	// telegram bot client in main; nil when Telegram is not configured.
	TelegramSender TelegramSender
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
