package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvFromContext(t *testing.T) {
	t.Parallel()

	t.Run("defaults to mainnet", func(t *testing.T) {
		t.Parallel()
		env := handlers.EnvFromContext(context.Background())
		assert.Equal(t, handlers.EnvMainnet, env)
	})

	t.Run("round-trips with ContextWithEnv", func(t *testing.T) {
		t.Parallel()
		ctx := handlers.ContextWithEnv(context.Background(), handlers.EnvTestnet)
		assert.Equal(t, handlers.EnvTestnet, handlers.EnvFromContext(ctx))

		ctx = handlers.ContextWithEnv(context.Background(), handlers.EnvTestnet)
		assert.Equal(t, handlers.EnvTestnet, handlers.EnvFromContext(ctx))
	})
}

func TestDatabaseForEnvFromContext(t *testing.T) {
	t.Parallel()
	api := &handlers.API{
		EnvDBs:       map[string]driver.Conn{},
		EnvDatabases: map[string]string{},
	}

	// Set up env databases for the test
	api.EnvDatabases = map[string]string{
		"mainnet-beta": "lake_mainnet",
		"testnet":      "lake_testnet",
	}
	api.Database = "lake_mainnet"

	tests := []struct {
		name     string
		env      handlers.DZEnv
		expected string
	}{
		{"mainnet", handlers.EnvMainnet, "lake_mainnet"},
		{"testnet", handlers.EnvTestnet, "lake_testnet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := handlers.ContextWithEnv(context.Background(), tt.env)
			assert.Equal(t, tt.expected, api.DatabaseForEnvFromContext(ctx))
		})
	}
}

func TestBuildEnvContext(t *testing.T) {
	t.Parallel()

	t.Run("mainnet mentions other envs and cross-query syntax", func(t *testing.T) {
		t.Parallel()
		result := handlers.BuildEnvContext(handlers.EnvMainnet, "default")
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "mainnet-beta")
		assert.Contains(t, result, "lake_testnet")
		assert.Contains(t, result, "lake_testnet")
		assert.Contains(t, result, "database.table")
	})

	t.Run("testnet requires database prefix", func(t *testing.T) {
		t.Parallel()
		result := handlers.BuildEnvContext(handlers.EnvTestnet, "default")
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "testnet")
		assert.Contains(t, result, "lake_testnet.")
		assert.Contains(t, result, "MUST prefix")
		assert.Contains(t, result, "Neo4j graph queries")
	})

	t.Run("different envs produce different context", func(t *testing.T) {
		t.Parallel()
		mainnet := handlers.BuildEnvContext(handlers.EnvMainnet, "default")
		testnetCtx := handlers.BuildEnvContext(handlers.EnvTestnet, "default")
		assert.NotEqual(t, mainnet, testnetCtx)
	})
}

func TestEnvMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headerValue string
		queryEnv    string
		expectedEnv handlers.DZEnv
	}{
		{"header present testnet", "testnet", "", handlers.EnvTestnet},
		{"header present testnet", "testnet", "", handlers.EnvTestnet},
		{"header present mainnet", "mainnet-beta", "", handlers.EnvMainnet},
		{"header missing defaults to mainnet", "", "", handlers.EnvMainnet},
		{"invalid header defaults to mainnet", "invalid-env", "", handlers.EnvMainnet},

		// Query-string fallback fires when the header is missing or invalid.
		{"query fallback testnet, no header", "", "testnet", handlers.EnvTestnet},
		{"query fallback testnet, no header", "", "testnet", handlers.EnvTestnet},
		{"query fallback mainnet, no header", "", "mainnet-beta", handlers.EnvMainnet},
		{"invalid header falls through to query", "invalid-env", "testnet", handlers.EnvTestnet},
		{"invalid query defaults to mainnet", "", "invalid-env", handlers.EnvMainnet},

		// Header wins when both are present.
		{"header wins over conflicting query", "testnet", "mainnet-beta", handlers.EnvTestnet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var capturedEnv handlers.DZEnv
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedEnv = handlers.EnvFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			})

			handler := handlers.EnvMiddleware(inner)
			url := "/test"
			if tt.queryEnv != "" {
				url += "?env=" + tt.queryEnv
			}
			req := httptest.NewRequest("GET", url, nil)
			if tt.headerValue != "" {
				req.Header.Set("X-DZ-Env", tt.headerValue)
			}
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Equal(t, tt.expectedEnv, capturedEnv)
		})
	}
}

func TestRequireNeo4jMiddleware(t *testing.T) {
	t.Parallel()
	api := &handlers.API{Neo4jClient: apitesting.SetupNeo4jForTest(t, testNeo4jDB)}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := api.RequireNeo4jMiddleware(inner)

	t.Run("returns 503 for non-mainnet", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := handlers.ContextWithEnv(req.Context(), handlers.EnvTestnet)
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		assert.Contains(t, rr.Body.String(), "only available on mainnet-beta")
	})

	t.Run("returns 503 when Neo4jClient is nil", func(t *testing.T) {
		t.Parallel()
		nilAPI := &handlers.API{}
		nilHandler := nilAPI.RequireNeo4jMiddleware(inner)

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := handlers.ContextWithEnv(req.Context(), handlers.EnvMainnet)
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		nilHandler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	})
}

func TestValidEnvs(t *testing.T) {
	t.Parallel()

	t.Run("known envs are valid", func(t *testing.T) {
		t.Parallel()
		require.True(t, handlers.ValidEnvs[handlers.EnvMainnet])
		require.True(t, handlers.ValidEnvs[handlers.EnvTestnet])
		require.True(t, handlers.ValidEnvs[handlers.EnvTestnet])
	})

	t.Run("unknown envs are not valid", func(t *testing.T) {
		t.Parallel()
		assert.False(t, handlers.ValidEnvs["unknown"])
		assert.False(t, handlers.ValidEnvs[""])
		assert.False(t, handlers.ValidEnvs["production"])
	})
}
