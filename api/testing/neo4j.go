package apitesting

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/stretchr/testify/require"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
)

// Neo4jDBConfig holds the Neo4j test container configuration.
type Neo4jDBConfig struct {
	Username       string
	Password       string
	ContainerImage string
}

// Neo4jDB represents a Neo4j test container.
type Neo4jDB struct {
	log       *slog.Logger
	cfg       *Neo4jDBConfig
	boltURL   string
	container *tcneo4j.Neo4jContainer
}

// BoltURL returns the Bolt protocol URL for the Neo4j container.
func (db *Neo4jDB) BoltURL() string {
	return db.boltURL
}

// Username returns the Neo4j username.
func (db *Neo4jDB) Username() string {
	return db.cfg.Username
}

// Password returns the Neo4j password.
func (db *Neo4jDB) Password() string {
	return db.cfg.Password
}

// Close terminates the Neo4j container.
func (db *Neo4jDB) Close() {
	terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.container.Terminate(terminateCtx); err != nil {
		db.log.Error("failed to terminate Neo4j container", "error", err)
	}
}

func (cfg *Neo4jDBConfig) Validate() error {
	if cfg.Username == "" {
		cfg.Username = "neo4j"
	}
	if cfg.Password == "" {
		cfg.Password = "password"
	}
	if cfg.ContainerImage == "" {
		cfg.ContainerImage = "neo4j:5-community"
	}
	return nil
}

// NewNeo4jDB creates a new Neo4j testcontainer.
func NewNeo4jDB(ctx context.Context, log *slog.Logger, cfg *Neo4jDBConfig) (*Neo4jDB, error) {
	if cfg == nil {
		cfg = &Neo4jDBConfig{}
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to validate Neo4j DB config: %w", err)
	}

	// Retry container start up to 3 times for retryable errors
	var container *tcneo4j.Neo4jContainer
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		var err error
		container, err = tcneo4j.Run(ctx,
			cfg.ContainerImage,
			tcneo4j.WithAdminPassword(cfg.Password),
			tcneo4j.WithoutAuthentication(),
		)
		if err != nil {
			lastErr = err
			if isRetryableNeo4jContainerStartErr(err) && attempt < 3 {
				time.Sleep(time.Duration(attempt) * 750 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("failed to start Neo4j container after retries: %w", lastErr)
		}
		break
	}

	if container == nil {
		return nil, fmt.Errorf("failed to start Neo4j container after retries: %w", lastErr)
	}

	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get Neo4j bolt URL: %w", err)
	}

	db := &Neo4jDB{
		log:       log,
		cfg:       cfg,
		boltURL:   boltURL,
		container: container,
	}

	return db, nil
}

// SetupNeo4jForTest creates a read-only Neo4j client for the test.
// Does NOT touch config.Neo4jClient.
func SetupNeo4jForTest(t *testing.T, db *Neo4jDB) neo4j.Client {
	t.Helper()
	ctx := t.Context()

	client, err := neo4j.NewReadOnlyClient(ctx, slog.Default(), db.boltURL, neo4j.DefaultDatabase, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create Neo4j client")

	t.Cleanup(func() {
		client.Close(context.Background())
	})

	return client
}

// SetupNeo4jWithDataForTest seeds data and returns a read-only Neo4j client.
// Does NOT touch config.Neo4jClient.
func SetupNeo4jWithDataForTest(t *testing.T, db *Neo4jDB, seedFunc func(ctx context.Context, session neo4j.Session) error) neo4j.Client {
	t.Helper()
	ctx := t.Context()

	// Create a read-write client for seeding
	rwClient, err := neo4j.NewClient(ctx, slog.Default(), db.boltURL, neo4j.DefaultDatabase, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create Neo4j read-write client")

	session, err := rwClient.Session(ctx)
	require.NoError(t, err, "failed to create Neo4j session")

	result, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err, "failed to clear Neo4j database")
	_, err = result.Consume(ctx)
	require.NoError(t, err, "failed to consume clear result")

	err = neo4j.RunMigrations(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), neo4j.MigrationConfig{
		URI:      db.boltURL,
		Database: neo4j.DefaultDatabase,
		Username: db.cfg.Username,
		Password: db.cfg.Password,
	})
	require.NoError(t, err, "failed to run Neo4j migrations")

	if seedFunc != nil {
		err = seedFunc(ctx, session)
		require.NoError(t, err, "failed to seed Neo4j data")
	}

	session.Close(ctx)
	rwClient.Close(ctx)

	roClient, err := neo4j.NewReadOnlyClient(ctx, slog.Default(), db.boltURL, neo4j.DefaultDatabase, db.cfg.Username, db.cfg.Password)
	require.NoError(t, err, "failed to create Neo4j read-only client")

	t.Cleanup(func() {
		roClient.Close(context.Background())
	})

	return roClient
}

func isRetryableNeo4jContainerStartErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "wait until ready") ||
		strings.Contains(s, "mapped port") ||
		strings.Contains(s, "timeout") ||
		strings.Contains(s, "context deadline exceeded") ||
		strings.Contains(s, "/containers/") && strings.Contains(s, "json") ||
		strings.Contains(s, "Get \"http://%2Fvar%2Frun%2Fdocker.sock")
}
