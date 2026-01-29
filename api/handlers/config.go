package handlers

import (
	"encoding/json"
	"net/http"
	"os"
)

// PublicConfig holds configuration that is safe to expose to the frontend
type PublicConfig struct {
	GoogleClientID    string `json:"googleClientId,omitempty"`
	SentryDSN         string `json:"sentryDsn,omitempty"`
	SentryEnvironment string `json:"sentryEnvironment,omitempty"`
	SlackEnabled      bool   `json:"slackEnabled,omitempty"`
}

// GetConfig returns public configuration for the frontend
func GetConfig(w http.ResponseWriter, r *http.Request) {
	sentryEnv := os.Getenv("SENTRY_ENVIRONMENT")
	if sentryEnv == "" {
		sentryEnv = "development"
	}

	config := PublicConfig{
		GoogleClientID:    os.Getenv("GOOGLE_CLIENT_ID"),
		SentryDSN:         os.Getenv("SENTRY_DSN_WEB"),
		SentryEnvironment: sentryEnv,
		SlackEnabled:      os.Getenv("SLACK_CLIENT_ID") != "",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(config)
}
