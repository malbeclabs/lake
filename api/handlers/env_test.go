package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
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
		ctx := handlers.ContextWithEnv(context.Background(), handlers.EnvDevnet)
		assert.Equal(t, handlers.EnvDevnet, handlers.EnvFromContext(ctx))

		ctx = handlers.ContextWithEnv(context.Background(), handlers.EnvTestnet)
		assert.Equal(t, handlers.EnvTestnet, handlers.EnvFromContext(ctx))
	})
}

func TestDatabaseForEnvFromContext(t *testing.T) {
	t.Parallel()

	// Set up env databases for the test
	origEnvDatabases := config.EnvDatabases
	config.EnvDatabases = map[string]string{
		"mainnet-beta": "dz_mainnet",
		"devnet":       "dz_devnet",
		"testnet":      "dz_testnet",
	}
	config.SetDatabase("dz_mainnet")
	t.Cleanup(func() {
		config.EnvDatabases = origEnvDatabases
	})

	tests := []struct {
		name     string
		env      handlers.DZEnv
		expected string
	}{
		{"mainnet", handlers.EnvMainnet, "dz_mainnet"},
		{"devnet", handlers.EnvDevnet, "dz_devnet"},
		{"testnet", handlers.EnvTestnet, "dz_testnet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := handlers.ContextWithEnv(context.Background(), tt.env)
			assert.Equal(t, tt.expected, handlers.DatabaseForEnvFromContext(ctx))
		})
	}
}

func TestBuildEnvContext(t *testing.T) {
	t.Parallel()

	// Set up env databases for the test
	origEnvDatabases := config.EnvDatabases
	config.EnvDatabases = map[string]string{
		"mainnet-beta": "dz_mainnet",
		"devnet":       "dz_devnet",
	}
	t.Cleanup(func() {
		config.EnvDatabases = origEnvDatabases
	})

	t.Run("mainnet mentions other envs", func(t *testing.T) {
		result := handlers.BuildEnvContext(handlers.EnvMainnet)
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "mainnet-beta")
		assert.Contains(t, result, "devnet")
	})

	t.Run("devnet mentions limitations", func(t *testing.T) {
		result := handlers.BuildEnvContext(handlers.EnvDevnet)
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "devnet")
		assert.Contains(t, result, "Neo4j graph queries")
	})

	t.Run("different envs produce different context", func(t *testing.T) {
		mainnet := handlers.BuildEnvContext(handlers.EnvMainnet)
		devnet := handlers.BuildEnvContext(handlers.EnvDevnet)
		assert.NotEqual(t, mainnet, devnet)
	})
}

func TestEnvMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headerValue string
		expectedEnv handlers.DZEnv
	}{
		{"header present devnet", "devnet", handlers.EnvDevnet},
		{"header present testnet", "testnet", handlers.EnvTestnet},
		{"header present mainnet", "mainnet-beta", handlers.EnvMainnet},
		{"header missing defaults to mainnet", "", handlers.EnvMainnet},
		{"invalid header defaults to mainnet", "invalid-env", handlers.EnvMainnet},
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
			req := httptest.NewRequest("GET", "/test", nil)
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

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := handlers.RequireNeo4jMiddleware(inner)

	t.Run("returns 503 for non-mainnet", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := handlers.ContextWithEnv(req.Context(), handlers.EnvDevnet)
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		assert.Contains(t, rr.Body.String(), "only available on mainnet-beta")
	})

	t.Run("returns 503 when Neo4jClient is nil", func(t *testing.T) {
		t.Parallel()
		origClient := config.Neo4jClient
		config.Neo4jClient = nil
		t.Cleanup(func() { config.Neo4jClient = origClient })

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := handlers.ContextWithEnv(req.Context(), handlers.EnvMainnet)
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	})
}

func TestValidEnvs(t *testing.T) {
	t.Parallel()

	t.Run("known envs are valid", func(t *testing.T) {
		t.Parallel()
		require.True(t, handlers.ValidEnvs[handlers.EnvMainnet])
		require.True(t, handlers.ValidEnvs[handlers.EnvDevnet])
		require.True(t, handlers.ValidEnvs[handlers.EnvTestnet])
	})

	t.Run("unknown envs are not valid", func(t *testing.T) {
		t.Parallel()
		assert.False(t, handlers.ValidEnvs["unknown"])
		assert.False(t, handlers.ValidEnvs[""])
		assert.False(t, handlers.ValidEnvs["production"])
	})
}
