package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
)

var (
	testPgDB    *apitesting.DB
	testChDB    *apitesting.ClickHouseDB
	testNeo4jDB *apitesting.Neo4jDB

	// testAPI is a shared API instance wired to the test containers.
	// ClickHouse goes through ConnProxy for per-test database isolation.
	testAPI *handlers.API
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	log := slog.Default()

	var wg sync.WaitGroup
	var pgErr, chErr, neo4jErr error

	// Start all containers in parallel
	wg.Add(3)

	go func() {
		defer wg.Done()
		testPgDB, pgErr = apitesting.NewDB(ctx, log, nil)
	}()

	go func() {
		defer wg.Done()
		testChDB, chErr = apitesting.NewClickHouseDB(ctx, log, nil)
	}()

	go func() {
		defer wg.Done()
		testNeo4jDB, neo4jErr = apitesting.NewNeo4jDB(ctx, log, nil)
	}()

	wg.Wait()

	// Check for errors
	if pgErr != nil {
		slog.Error("failed to start PostgreSQL container", "error", pgErr)
		os.Exit(1)
	}
	if chErr != nil {
		slog.Error("failed to start ClickHouse container", "error", chErr)
		os.Exit(1)
	}
	if neo4jErr != nil {
		slog.Error("failed to start Neo4j container", "error", neo4jErr)
		os.Exit(1)
	}

	// Install ConnProxy so parallel tests get per-goroutine ClickHouse connections.
	// The proxy wraps config.DB and dispatches to per-test overrides registered by
	// SetupTestClickHouse*. Tests without an override fall through to the default.
	dbProxy := &apitesting.DatabaseProxy{}
	shredderDBProxy := &apitesting.DatabaseProxy{}
	config.DB = &apitesting.ConnProxy{}
	config.TestDatabaseProxy = dbProxy
	config.TestShredderDBProxy = shredderDBProxy

	// Create a shared API instance that uses the proxied config.DB.
	// This bridges the old test infrastructure (ConnProxy-based isolation)
	// with the new API struct pattern.
	//
	// Note: testAPI.Database is "default" statically. For tests that need the
	// per-test database name (e.g., schema queries), the test setup dynamically
	// resolves via config.Database() which reads from the TestDatabaseProxy.
	// We hook into the proxy by having testAPI.Database be a placeholder that
	// gets overridden per-test via config.Database() in the API's Database field.
	testAPI = &handlers.API{
		DB:           config.DB,
		EnvDBs:       map[string]driver.Conn{},
		EnvDatabases: map[string]string{},
		Database:     "default",
		ShredderDB:   "shredder",
	}
	testAPI.Manager = handlers.NewWorkflowManager(testAPI)

	// Override DatabaseFunc to dynamically resolve per-test database names
	// through the ConnProxy/DatabaseProxy infrastructure.
	testAPI.DatabaseFunc = config.Database
	testAPI.ShredderDBFunc = config.GetShredderDB

	code := m.Run()

	// Cleanup all containers
	if testPgDB != nil {
		testPgDB.Close()
	}
	if testChDB != nil {
		testChDB.Close()
	}
	if testNeo4jDB != nil {
		testNeo4jDB.Close()
	}

	os.Exit(code)
}

// setupTestPg runs migrations, creates a PgPool for the test, and wires it
// into both config.PgPool (for legacy code paths) and testAPI.PgPool.
func setupTestPg(t *testing.T) {
	apitesting.SetupTestDB(t, testPgDB)
	testAPI.PgPool = config.PgPool
}

// setupTestNeo4j wires the Neo4j test client into testAPI.
func setupTestNeo4j(t *testing.T) {
	apitesting.SetupTestNeo4j(t, testNeo4jDB)
	testAPI.Neo4jClient = config.Neo4jClient
}

// syncDatabaseName keeps testAPI.Database in sync with the per-test database
// name set by the ClickHouse test setup. This is called automatically by the
// proxy infrastructure but testAPI.Database is a static field, so we use
// config.Database() which reads from the TestDatabaseProxy.
func syncDatabaseName() string {
	db := config.Database()
	testAPI.Database = db
	testAPI.ShredderDB = config.GetShredderDB()
	return db
}
