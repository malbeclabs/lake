package config

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver with database/sql
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var EmbedMigrations embed.FS

// PgPool is the global PostgreSQL connection pool
var PgPool *pgxpool.Pool

// PgConfig holds the PostgreSQL configuration
type PgConfig struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

// pgCfg holds the parsed configuration
var pgCfg PgConfig

// LoadPostgres initializes the PostgreSQL connection pool
func LoadPostgres() error {
	pgCfg.Host = os.Getenv("POSTGRES_HOST")
	if pgCfg.Host == "" {
		pgCfg.Host = "localhost"
	}

	pgCfg.Port = os.Getenv("POSTGRES_PORT")
	if pgCfg.Port == "" {
		pgCfg.Port = "5432"
	}

	pgCfg.Database = os.Getenv("POSTGRES_DB")
	if pgCfg.Database == "" {
		return fmt.Errorf("POSTGRES_DB is required")
	}

	pgCfg.Username = os.Getenv("POSTGRES_USER")
	if pgCfg.Username == "" {
		return fmt.Errorf("POSTGRES_USER is required")
	}

	pgCfg.Password = os.Getenv("POSTGRES_PASSWORD")
	if pgCfg.Password == "" {
		return fmt.Errorf("POSTGRES_PASSWORD is required")
	}

	sslMode := os.Getenv("POSTGRES_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		pgCfg.Username, pgCfg.Password, pgCfg.Host, pgCfg.Port, pgCfg.Database, sslMode,
	)

	slog.Info("connecting to PostgreSQL", "host", pgCfg.Host, "port", pgCfg.Port, "database", pgCfg.Database, "username", pgCfg.Username)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse postgres config: %w", err)
	}

	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	PgPool = pool
	slog.Info("connected to PostgreSQL")

	// Run migrations only if explicitly enabled
	if os.Getenv("POSTGRES_RUN_MIGRATIONS") == "true" {
		if err := runMigrations(connStr); err != nil {
			return fmt.Errorf("failed to run migrations: %w", err)
		}
	}

	return nil
}

// slogGooseLogger adapts slog to the goose.Logger interface.
type slogGooseLogger struct{}

func (*slogGooseLogger) Fatalf(format string, v ...any) {
	slog.Error(fmt.Sprintf(format, v...))
	os.Exit(1)
}

func (*slogGooseLogger) Printf(format string, v ...any) {
	slog.Info(fmt.Sprintf(format, v...))
}

// runMigrations runs database migrations using goose
func runMigrations(connStr string) error {
	slog.Info("running PostgreSQL migrations")

	goose.SetLogger(&slogGooseLogger{})
	goose.SetBaseFS(EmbedMigrations)

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database for migrations: %w", err)
	}
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	slog.Info("PostgreSQL migrations completed")
	return nil
}

// ClosePostgres closes the PostgreSQL connection pool
func ClosePostgres() {
	if PgPool != nil {
		PgPool.Close()
	}
}
