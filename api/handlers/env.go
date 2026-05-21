package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// DZEnv represents a DoubleZero network environment.
type DZEnv string

const (
	EnvMainnet DZEnv = "mainnet-beta"
	EnvDevnet  DZEnv = "devnet"
	EnvTestnet DZEnv = "testnet"
)

// TelemetryDatabaseForEnv returns the ClickHouse database that mirrors the
// dz/telemetry tables for the given environment (e.g. "mainnet-beta" →
// "telemetry_mainnet_beta"). These databases are created by the admin
// `setup-remote-tables` command and proxy the remote telemetry cluster.
func TelemetryDatabaseForEnv(env DZEnv) string {
	return "telemetry_" + strings.ReplaceAll(string(env), "-", "_")
}

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

// BuildEnvContext returns the agent system prompt context for the given environment.
// All agent queries run against the mainnet database by default. For other environments,
// the agent uses fully-qualified table names (e.g., lake_devnet.dim_devices_current).
func BuildEnvContext(env DZEnv, mainnetDB string) string {
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
func (a *API) RequireNeo4jMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMainnet(r.Context()) || a.Neo4jClient == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"This feature is only available on mainnet-beta"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// EnvMiddleware extracts the request's target environment and stores it in
// the context. The canonical channel is the `X-DZ-Env` header; when no valid
// header is present we fall back to the `env` query parameter so URLs pasted
// into a browser (or the Scalar API docs page) can target a non-default env
// without needing custom header support. Defaults to mainnet-beta when
// neither carries a recognized value.
func EnvMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env := DZEnv(r.Header.Get("X-DZ-Env"))
		if !ValidEnvs[env] {
			env = DZEnv(r.URL.Query().Get("env"))
		}
		if !ValidEnvs[env] {
			env = EnvMainnet
		}
		ctx := ContextWithEnv(r.Context(), env)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
