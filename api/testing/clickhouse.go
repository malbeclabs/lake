package apitesting

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	chmigrations "github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/stretchr/testify/require"
	tcch "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// ClickHouseDBConfig holds the ClickHouse test container configuration.
type ClickHouseDBConfig struct {
	Database       string
	Username       string
	Password       string
	Port           string
	ContainerImage string
}

// ClickHouseDB represents a ClickHouse test container.
type ClickHouseDB struct {
	log       *slog.Logger
	cfg       *ClickHouseDBConfig
	addr      string
	httpAddr  string
	container *tcch.ClickHouseContainer

	// templateDB caches a migrated database name so migrations run once per container.
	templateOnce sync.Once
	templateDB   string
	templateErr  error
}

// Addr returns the ClickHouse native protocol address (host:port).
func (db *ClickHouseDB) Addr() string {
	return db.addr
}

// HTTPAddr returns the HTTP endpoint URL (http://host:port) for the ClickHouse container.
func (db *ClickHouseDB) HTTPAddr() string {
	return db.httpAddr
}

// Username returns the ClickHouse username.
func (db *ClickHouseDB) Username() string {
	return db.cfg.Username
}

// Password returns the ClickHouse password.
func (db *ClickHouseDB) Password() string {
	return db.cfg.Password
}

// Database returns the ClickHouse database name.
func (db *ClickHouseDB) Database() string {
	return db.cfg.Database
}

// Close terminates the ClickHouse container.
func (db *ClickHouseDB) Close() {
	terminateCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.container.Terminate(terminateCtx); err != nil {
		db.log.Error("failed to terminate ClickHouse container", "error", err)
	}
}

func (cfg *ClickHouseDBConfig) Validate() error {
	if cfg.Database == "" {
		cfg.Database = "test"
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}
	if cfg.Password == "" {
		cfg.Password = "password"
	}
	if cfg.Port == "" {
		cfg.Port = "9000"
	}
	if cfg.ContainerImage == "" {
		cfg.ContainerImage = "clickhouse/clickhouse-server:latest"
	}
	return nil
}

// NewClickHouseDB creates a new ClickHouse testcontainer.
func NewClickHouseDB(ctx context.Context, log *slog.Logger, cfg *ClickHouseDBConfig) (*ClickHouseDB, error) {
	if cfg == nil {
		cfg = &ClickHouseDBConfig{}
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate ClickHouse DB config: %w", err)
	}

	// Retry container start up to 3 times for retryable errors
	var container *tcch.ClickHouseContainer
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		var err error
		container, err = tcch.Run(ctx,
			cfg.ContainerImage,
			tcch.WithDatabase(cfg.Database),
			tcch.WithUsername(cfg.Username),
			tcch.WithPassword(cfg.Password),
		)
		if err != nil {
			lastErr = err
			if isRetryableContainerStartErr(err) && attempt < 3 {
				time.Sleep(time.Duration(attempt) * 750 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("failed to start ClickHouse container after retries: %w", lastErr)
		}
		break
	}

	if container == nil {
		return nil, fmt.Errorf("failed to start ClickHouse container after retries: %w", lastErr)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse container host: %w", err)
	}

	port := nat.Port(fmt.Sprintf("%s/tcp", cfg.Port))
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse container mapped port: %w", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())

	// Get HTTP port for schema fetching
	httpPort := nat.Port("8123/tcp")
	mappedHTTPPort, err := container.MappedPort(ctx, httpPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse container HTTP port: %w", err)
	}
	httpAddr := fmt.Sprintf("http://%s:%s", host, mappedHTTPPort.Port())

	db := &ClickHouseDB{
		log:       log,
		cfg:       cfg,
		addr:      addr,
		httpAddr:  httpAddr,
		container: container,
	}

	return db, nil
}

// createClickHouseConn creates a ClickHouse connection.
func createClickHouseConn(ctx context.Context, addr, database, username, password string) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	// Test the connection with retries
	for attempt := 1; attempt <= 3; attempt++ {
		if err := conn.Ping(ctx); err != nil {
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("failed to ping ClickHouse after retries: %w", err)
		}
		break
	}

	return conn, nil
}

// ensureTemplateDB runs migrations once into a cached template database.
func (db *ClickHouseDB) ensureTemplateDB(ctx context.Context) (string, error) {
	db.templateOnce.Do(func() {
		templateName := "test_template_migrated"
		adminConn, err := createClickHouseConn(ctx, db.addr, db.cfg.Database, db.cfg.Username, db.cfg.Password)
		if err != nil {
			db.templateErr = fmt.Errorf("template admin conn: %w", err)
			return
		}
		defer adminConn.Close()

		if err := adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", templateName)); err != nil {
			db.templateErr = fmt.Errorf("create template db: %w", err)
			return
		}
		if err := chmigrations.RunMigrations(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), chmigrations.MigrationConfig{
			Addr:     db.addr,
			Database: templateName,
			Username: db.cfg.Username,
			Password: db.cfg.Password,
		}); err != nil {
			db.templateErr = fmt.Errorf("template migrations: %w", err)
			return
		}
		db.templateDB = templateName
	})
	return db.templateDB, db.templateErr
}

// SetupClickHouseForTest creates a per-test ClickHouse database and returns
// the direct connection and database name. No proxy registration.
func SetupClickHouseForTest(t *testing.T, db *ClickHouseDB) (driver.Conn, string) {
	t.Helper()
	ctx := t.Context()

	randomSuffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	databaseName := fmt.Sprintf("test_%s", randomSuffix)

	adminConn, err := createClickHouseConn(ctx, db.addr, db.cfg.Database, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create ClickHouse admin connection")

	err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", databaseName))
	require.NoError(t, err, "failed to create test database")

	testConn, err := createClickHouseConn(ctx, db.addr, databaseName, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create ClickHouse test connection")

	t.Cleanup(func() {
		_ = adminConn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", databaseName))
		_ = testConn.Close()
		_ = adminConn.Close()
	})

	return testConn, databaseName
}

// SetupClickHouseWithMigrationsForTest creates a per-test ClickHouse database
// with full schema migrations and returns the direct connection and database name.
func SetupClickHouseWithMigrationsForTest(t *testing.T, db *ClickHouseDB) (driver.Conn, string) {
	t.Helper()
	ctx := t.Context()

	templateDB, err := db.ensureTemplateDB(ctx)
	require.NoError(t, err, "failed to ensure template database")

	randomSuffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	databaseName := fmt.Sprintf("test_%s", randomSuffix)

	adminConn, err := createClickHouseConn(ctx, db.addr, db.cfg.Database, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create ClickHouse admin connection")

	err = adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", databaseName))
	require.NoError(t, err, "failed to create test database")

	rows, err := adminConn.Query(ctx, "SELECT name, engine_full, create_table_query FROM system.tables WHERE database = $1", templateDB)
	require.NoError(t, err, "failed to list template tables")
	defer rows.Close()

	for rows.Next() {
		var name, engineFull, createQuery string
		require.NoError(t, rows.Scan(&name, &engineFull, &createQuery))
		if name == "goose_db_version" {
			continue
		}
		cloneQuery := strings.Replace(createQuery, fmt.Sprintf("%s.", templateDB), fmt.Sprintf("%s.", databaseName), -1)
		cloneQuery = strings.Replace(cloneQuery, fmt.Sprintf("CREATE TABLE %s", templateDB), fmt.Sprintf("CREATE TABLE %s", databaseName), 1)
		cloneQuery = strings.Replace(cloneQuery, fmt.Sprintf("CREATE VIEW %s", templateDB), fmt.Sprintf("CREATE VIEW %s", databaseName), 1)
		cloneQuery = strings.Replace(cloneQuery, fmt.Sprintf("CREATE MATERIALIZED VIEW %s", templateDB), fmt.Sprintf("CREATE MATERIALIZED VIEW %s", databaseName), 1)
		err := adminConn.Exec(ctx, cloneQuery)
		require.NoError(t, err, "failed to clone table %s: query=%s", name, cloneQuery)
	}

	testConn, err := createClickHouseConn(ctx, db.addr, databaseName, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create ClickHouse test connection")

	t.Cleanup(func() {
		_ = adminConn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", databaseName))
		_ = testConn.Close()
		_ = adminConn.Close()
	})

	return testConn, databaseName
}
