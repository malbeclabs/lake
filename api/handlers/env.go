package handlers

import (
	"context"
	"fmt"
	"net/http"

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

// BuildEnvContext returns the agent system prompt context for the given environment.
// All agent queries run against the mainnet database by default. For other environments,
// the agent uses fully-qualified table names (e.g., lake_devnet.dim_devices_current).
func BuildEnvContext(env DZEnv) string {
	mainnetDB := config.Database()

	if env == EnvMainnet {
		return fmt.Sprintf("You are querying the mainnet-beta environment (database: `%s`). Other DZ environments are available: devnet (`lake_devnet`), testnet (`lake_testnet`). To query these, use fully-qualified `database.table` syntax (e.g., `lake_devnet.dim_devices_current`).", mainnetDB)
	}

	// For non-mainnet envs, tell the agent to USE the environment's database
	envDB := "lake_" + string(env)
	return fmt.Sprintf(`The user is viewing the %s environment. You MUST prefix all table names with the database name "%s." to query %s data.

Example: Instead of "SELECT * FROM dim_devices_current", write "SELECT * FROM %s.dim_devices_current"

Queries without the "%s." prefix will return mainnet-beta data. This is incorrect UNLESS the user explicitly asks for mainnet or mainnet-beta data.

Note: Neo4j graph queries, Solana validator data, and GeoIP location data are only available on mainnet-beta.`, string(env), envDB, string(env), envDB, envDB)
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
