package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/config"
)

// DZEnv represents a DoubleZero network environment.
type DZEnv string

const (
	EnvMainnet DZEnv = "mainnet-beta"
	EnvDevnet  DZEnv = "devnet"
	EnvTestnet DZEnv = "testnet"
)

// ValidEnvs contains all recognized environment values.
var ValidEnvs = map[DZEnv]bool{
	EnvMainnet: true,
	EnvDevnet:  true,
	EnvTestnet: true,
}

type envContextKey struct{}

// ContextWithEnv returns a new context with the given environment.
func ContextWithEnv(ctx context.Context, env DZEnv) context.Context {
	return context.WithValue(ctx, envContextKey{}, env)
}

// EnvFromContext returns the environment from the context, defaulting to mainnet.
func EnvFromContext(ctx context.Context) DZEnv {
	if env, ok := ctx.Value(envContextKey{}).(DZEnv); ok {
		return env
	}
	return EnvMainnet
}

// envDB returns the ClickHouse connection pool for the environment in the context.
func envDB(ctx context.Context) driver.Conn {
	return config.DBForEnv(string(EnvFromContext(ctx)))
}

// DatabaseForEnvFromContext returns the database name for the environment in the context.
func DatabaseForEnvFromContext(ctx context.Context) string {
	env := EnvFromContext(ctx)
	db, ok := config.DatabaseForEnv(string(env))
	if !ok {
		return config.Database()
	}
	return db
}

// buildEnvContext returns the agent system prompt context for the given environment.
// For mainnet, it mentions other available envs and their database names.
// For non-mainnet, it says what env we're in and how to cross-query.
func BuildEnvContext(env DZEnv) string {
	currentDB, _ := config.DatabaseForEnv(string(env))

	// Build list of other environments and their databases
	var others []string
	for _, e := range config.AvailableEnvs() {
		if DZEnv(e) == env {
			continue
		}
		db, _ := config.DatabaseForEnv(e)
		others = append(others, fmt.Sprintf("%s (database: `%s`)", e, db))
	}

	crossQuery := ""
	if len(others) > 0 {
		example := "other_db"
		for _, e := range config.AvailableEnvs() {
			if DZEnv(e) != env {
				example, _ = config.DatabaseForEnv(e)
				break
			}
		}
		crossQuery = fmt.Sprintf(" Other environments are available: %s. If the user asks about another environment, use fully-qualified `database.table` syntax (e.g. `%s.dz_devices_current`).", strings.Join(others, ", "), example)
	}

	if env == EnvMainnet {
		if crossQuery == "" {
			return ""
		}
		return fmt.Sprintf("You are querying the mainnet-beta deployment (database: `%s`).%s", currentDB, crossQuery)
	}

	return fmt.Sprintf("You are querying the DZ %s deployment (database: `%s`). Neo4j graph queries, Solana validator data, and GeoIP location data are only available on mainnet-beta, not on this deployment.%s", string(env), currentDB, crossQuery)
}

// isMainnet returns true if the request context is for the mainnet-beta environment.
func isMainnet(ctx context.Context) bool {
	return EnvFromContext(ctx) == EnvMainnet
}

// RequireNeo4jMiddleware returns 503 for non-mainnet requests on Neo4j-dependent
// endpoints, since Neo4j only contains mainnet data.
func RequireNeo4jMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMainnet(r.Context()) || config.Neo4jClient == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"This feature is only available on mainnet-beta"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// EnvMiddleware extracts the X-DZ-Env header and stores the environment in the
// request context. Defaults to mainnet-beta if not provided or invalid.
func EnvMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env := DZEnv(r.Header.Get("X-DZ-Env"))
		if !ValidEnvs[env] {
			env = EnvMainnet
		}
		ctx := ContextWithEnv(r.Context(), env)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
