package apitesting

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
)

// NewTestAPIBare creates an isolated *handlers.API for a single test with a
// per-test ClickHouse database (empty, no migrations). Use this for tests
// that create their own table schemas.
func NewTestAPIBare(t *testing.T, chDB *ClickHouseDB) *handlers.API {
	t.Helper()
	conn, dbName := SetupClickHouseForTest(t, chDB)
	api := &handlers.API{
		DB:            conn,
		PublicQueryDB: conn,
		EnvDBs:        map[string]driver.Conn{},
		EnvDatabases:  map[string]string{},
		Database:      dbName,
		ShredderDB:    dbName,
	}
	api.Manager = handlers.NewWorkflowManager(api)
	return api
}

// NewTestAPI creates an isolated *handlers.API for a single test with a
// per-test ClickHouse database (with full schema migrations).
// This is the most common test setup pattern.
func NewTestAPI(t *testing.T, chDB *ClickHouseDB) *handlers.API {
	t.Helper()
	conn, dbName := SetupClickHouseWithMigrationsForTest(t, chDB)
	api := &handlers.API{
		DB:            conn,
		PublicQueryDB: conn,
		EnvDBs:        map[string]driver.Conn{},
		EnvDatabases:  map[string]string{},
		Database:      dbName,
		ShredderDB:    dbName,
	}
	api.Manager = handlers.NewWorkflowManager(api)
	return api
}

// NewTestAPIPg creates an isolated *handlers.API for a single test with a
// PostgreSQL pool (with migrations applied).
func NewTestAPIPg(t *testing.T, pgDB *DB) *handlers.API {
	t.Helper()
	pool := SetupPostgresForTest(t, pgDB)
	api := &handlers.API{
		EnvDBs:       map[string]driver.Conn{},
		EnvDatabases: map[string]string{},
		PgPool:       pool,
	}
	api.Manager = handlers.NewWorkflowManager(api)
	return api
}

// NewTestAPIAll creates an isolated *handlers.API with ClickHouse, PostgreSQL,
// and Neo4j connections. Pass nil for any component to skip it.
func NewTestAPIAll(t *testing.T, chDB *ClickHouseDB, pgDB *DB, neo4jDB *Neo4jDB, seedFunc func(ctx context.Context, session neo4j.Session) error) *handlers.API {
	t.Helper()

	api := &handlers.API{
		EnvDBs:       map[string]driver.Conn{},
		EnvDatabases: map[string]string{},
	}

	if chDB != nil {
		conn, dbName := SetupClickHouseWithMigrationsForTest(t, chDB)
		api.DB = conn
		api.PublicQueryDB = conn
		api.Database = dbName
		api.ShredderDB = dbName
	}

	if pgDB != nil {
		api.PgPool = SetupPostgresForTest(t, pgDB)
	}

	if neo4jDB != nil {
		if seedFunc != nil {
			api.Neo4jClient = SetupNeo4jWithDataForTest(t, neo4jDB, seedFunc)
		} else {
			api.Neo4jClient = SetupNeo4jForTest(t, neo4jDB)
		}
	}

	api.Manager = handlers.NewWorkflowManager(api)
	return api
}
