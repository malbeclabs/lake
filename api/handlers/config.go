package handlers

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"

	appconfig "github.com/malbeclabs/lake/api/config"
)

// PublicConfig holds configuration that is safe to expose to the frontend
type PublicConfig struct {
	GoogleClientID    string          `json:"googleClientId,omitempty"`
	SentryDSN         string          `json:"sentryDsn,omitempty"`
	SentryEnvironment string          `json:"sentryEnvironment,omitempty"`
	SlackEnabled      bool            `json:"slackEnabled,omitempty"`
	Env               string          `json:"env"`
	AvailableEnvs     []string        `json:"availableEnvs"`
	Features          map[string]bool `json:"features"`
}

// GetConfig returns public configuration for the frontend
func GetConfig(w http.ResponseWriter, r *http.Request) {
	sentryEnv := os.Getenv("SENTRY_ENVIRONMENT")
	if sentryEnv == "" {
		sentryEnv = "development"
	}

	env := EnvFromContext(r.Context())

	// Determine available envs
	availableEnvs := appconfig.AvailableEnvs()
	sort.Strings(availableEnvs)

	// Feature flags based on environment
	// Mainnet gets all features; non-mainnet environments have restricted features
	features := map[string]bool{
		"neo4j":  appconfig.Neo4jClient != nil && env == EnvMainnet,
		"solana": env == EnvMainnet,
		"geoip":  env == EnvMainnet,
	}

	config := PublicConfig{
		GoogleClientID:    os.Getenv("GOOGLE_CLIENT_ID"),
		SentryDSN:         os.Getenv("SENTRY_DSN_WEB"),
		SentryEnvironment: sentryEnv,
		SlackEnabled:      os.Getenv("SLACK_CLIENT_ID") != "",
		Env:               string(env),
		AvailableEnvs:     availableEnvs,
		Features:          features,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(config)
}
