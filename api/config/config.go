package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DB is the global ClickHouse connection pool (mainnet-beta)
var DB driver.Conn

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

// Database returns the configured database name
func Database() string {
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

	log.Printf("Connecting to ClickHouse: addr=%s, database=%s, username=%s, secure=%v", cfg.Addr, cfg.Database, cfg.Username, secure)

	// Create connection pool
	opts := &clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    30,
		MaxIdleConns:    10,
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
	log.Printf("Connected to ClickHouse successfully")

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
		log.Printf("Connected to ClickHouse for %s (database=%s)", env, dbName)
	}

	return nil
}

// Close closes all ClickHouse connection pools
func Close() error {
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
