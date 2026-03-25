package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DB is the global ClickHouse connection pool (mainnet-beta)
var DB driver.Conn

// HealthDB is a separate single-connection pool used exclusively for health
// checks (/readyz). Keeping it isolated from the main pool prevents cache
// refresh storms from starving the readiness probe.
var HealthDB driver.Conn

// shredderDB is the ClickHouse database name for shredder tables (default: "shredder").
var shredderDB = "shredder"

// EnvDBs maps environment names to their ClickHouse connection pools.
// The mainnet-beta entry always points to DB.
var EnvDBs map[string]driver.Conn

// Config holds the ClickHouse configuration
type CHConfig struct {
	Addr     string
	Database string
	Username string
	Password string
}

// EnvDatabases maps environment names to their ClickHouse database names.
var EnvDatabases map[string]string

// cfg holds the parsed configuration
var cfg CHConfig

// TestDatabaseProxy holds a per-goroutine database name proxy for parallel tests.
// When non-nil, Database() checks it before returning the global default.
// This is set by the test infrastructure; production code leaves it nil.
var TestDatabaseProxy interface {
	Get() string
}

// TestShredderDBProxy holds a per-goroutine shredder database name proxy for
// parallel tests. When non-nil, GetShredderDB() checks it before returning the
// global default.
var TestShredderDBProxy interface {
	Get() string
}

// GetShredderDB returns the shredder database name.
// In parallel tests, returns the per-goroutine override if registered.
func GetShredderDB() string {
	if TestShredderDBProxy != nil {
		if name := TestShredderDBProxy.Get(); name != "" {
			return name
		}
	}
	return shredderDB
}

// SetShredderDB sets the shredder database name.
func SetShredderDB(db string) {
	shredderDB = db
}

// Database returns the configured database name.
// In parallel tests, returns the per-goroutine override if registered.
func Database() string {
	if TestDatabaseProxy != nil {
		if name := TestDatabaseProxy.Get(); name != "" {
			return name
		}
	}
	return cfg.Database
}

// SetDatabase sets the configured database name (for testing)
func SetDatabase(db string) {
	cfg.Database = db
}

// DatabaseForEnv returns the database name for the given environment.
// Returns the database name and true if found, or empty string and false if not.
func DatabaseForEnv(env string) (string, bool) {
	db, ok := EnvDatabases[env]
	return db, ok
}

// AvailableEnvs returns the list of environments that have databases configured.
func AvailableEnvs() []string {
	envs := make([]string, 0, len(EnvDatabases))
	for env := range EnvDatabases {
		envs = append(envs, env)
	}
	return envs
}

// DBForEnv returns the ClickHouse connection pool for the given environment.
// Falls back to the default DB if the environment is not configured.
func DBForEnv(env string) driver.Conn {
	if conn, ok := EnvDBs[env]; ok {
		return conn
	}
	return DB
}

// Load initializes configuration from environment variables and creates the connection pool
func Load() error {
	cfg.Addr = os.Getenv("CLICKHOUSE_ADDR_TCP")
	if cfg.Addr == "" {
		cfg.Addr = "localhost:9000"
	}

	cfg.Database = os.Getenv("CLICKHOUSE_DATABASE")
	if cfg.Database == "" {
		cfg.Database = "default"
	}

	cfg.Username = os.Getenv("CLICKHOUSE_USERNAME")
	if cfg.Username == "" {
		cfg.Username = "default"
	}

	cfg.Password = os.Getenv("CLICKHOUSE_PASSWORD")

	// CLICKHOUSE_USE_REMOTE=true switches to the remote proxy database (lake).
	if os.Getenv("CLICKHOUSE_USE_REMOTE") == "true" {
		cfg.Database = "lake"
	}

	if db := os.Getenv("CLICKHOUSE_SHREDDER_DB"); db != "" {
		shredderDB = db
	}

	// Build env -> database mapping
	EnvDatabases = map[string]string{
		"mainnet-beta": cfg.Database,
	}
	if db := os.Getenv("CLICKHOUSE_DATABASE_DEVNET"); db != "" {
		EnvDatabases["devnet"] = db
	}
	if db := os.Getenv("CLICKHOUSE_DATABASE_TESTNET"); db != "" {
		EnvDatabases["testnet"] = db
	}

	secure := os.Getenv("CLICKHOUSE_SECURE") == "true"

	slog.Info("connecting to ClickHouse", "addr", cfg.Addr, "database", cfg.Database, "username", cfg.Username, "secure", secure, "shredder_db", shredderDB)

	// Create connection pool
	opts := &clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    100,
		MaxIdleConns:    100,
		ConnMaxLifetime: 10 * time.Minute,
	}

	// Enable TLS for ClickHouse Cloud (port 9440)
	if secure {
		opts.TLS = &tls.Config{}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to create clickhouse connection: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	DB = conn
	slog.Info("connected to ClickHouse")

	// Create a dedicated health-check connection with a tiny pool so that
	// readiness probes are never blocked by cache refresh connection storms.
	healthOpts := &clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    2,
		MaxIdleConns:    1,
		ConnMaxLifetime: 10 * time.Minute,
	}
	if secure {
		healthOpts.TLS = &tls.Config{}
	}
	healthConn, err := clickhouse.Open(healthOpts)
	if err != nil {
		return fmt.Errorf("failed to create clickhouse health connection: %w", err)
	}
	if err := healthConn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping clickhouse health connection: %w", err)
	}
	HealthDB = healthConn
	slog.Info("health-check ClickHouse connection ready")

	// Create connections for each env database
	EnvDBs = map[string]driver.Conn{
		"mainnet-beta": DB,
	}
	for env, dbName := range EnvDatabases {
		if env == "mainnet-beta" {
			continue
		}
		envOpts := &clickhouse.Options{
			Addr: []string{cfg.Addr},
			Auth: clickhouse.Auth{
				Database: dbName,
				Username: cfg.Username,
				Password: cfg.Password,
			},
			DialTimeout:     5 * time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: 10 * time.Minute,
		}
		if secure {
			envOpts.TLS = &tls.Config{}
		}
		envConn, err := clickhouse.Open(envOpts)
		if err != nil {
			return fmt.Errorf("failed to create ClickHouse connection for %s (database=%s): %w", env, dbName, err)
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := envConn.Ping(pingCtx); err != nil {
			pingCancel()
			return fmt.Errorf("failed to connect to ClickHouse for %s (database=%s): %w", env, dbName, err)
		}
		pingCancel()
		EnvDBs[env] = envConn
		slog.Info("connected to ClickHouse", "env", env, "database", dbName)
	}

	return nil
}

// Close closes all ClickHouse connection pools
func Close() error {
	if HealthDB != nil {
		_ = HealthDB.Close()
	}
	for env, conn := range EnvDBs {
		if env == "mainnet-beta" {
			continue // closed below as DB
		}
		if conn != nil {
			_ = conn.Close()
		}
	}
	if DB != nil {
		return DB.Close()
	}
	return nil
}
