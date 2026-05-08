package handlers_test

import (
	"errors"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeError_NilError(t *testing.T) {
	t.Parallel()
	result := handlers.SanitizeError(nil)
	assert.Equal(t, "", result)
}

func TestSanitizeError_PlainError(t *testing.T) {
	t.Parallel()
	err := errors.New("something went wrong")
	result := handlers.SanitizeError(err)
	assert.Equal(t, "something went wrong", result)
}

func TestSanitizeError_RedactsCredentialsInURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with user:pass",
			input:    "failed to connect: postgres://user:secretpass@localhost:5432/db",
			expected: "failed to connect: postgres://redacted@localhost:5432/db",
		},
		{
			name:     "URL with just user",
			input:    "error at: postgres://admin@localhost:5432/db",
			expected: "error at: postgres://redacted@localhost:5432/db",
		},
		{
			name:     "HTTPS URL with credentials",
			input:    "cannot reach: https://api_key:secret123@api.example.com/v1",
			expected: "cannot reach: https://redacted@api.example.com/v1",
		},
		{
			name:     "URL without credentials",
			input:    "connecting to: postgres://localhost:5432/db",
			expected: "connecting to: postgres://localhost:5432/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.input)
			result := handlers.SanitizeError(err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeError_RedactsSensitiveQueryParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "token in query is redacted, other params preserved",
			input:    "error fetching: https://api.example.com/data?token=secret123&foo=bar",
			expected: "error fetching: https://api.example.com/data?foo=bar&token=redacted",
		},
		{
			name:     "non-sensitive query is preserved",
			input:    "GET https://api.example.com?retries=3 failed",
			expected: "GET https://api.example.com?retries=3 failed",
		},
		{
			name:     "api-key in query is redacted",
			input:    "requesting 'https://api.example.com?api-key=xxx' returned error",
			expected: "requesting 'https://api.example.com?api-key=redacted' returned error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.input)
			result := handlers.SanitizeError(err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeError_CombinedCredentialsAndQuery(t *testing.T) {
	t.Parallel()
	err := errors.New("connect to: postgres://user:pass@localhost:5432/db?token=secret&sslmode=disable")
	result := handlers.SanitizeError(err)

	assert.Contains(t, result, "redacted@localhost")
	assert.Contains(t, result, "token=redacted")
	assert.Contains(t, result, "sslmode=disable")
	assert.NotContains(t, result, "user:pass")
	assert.NotContains(t, result, "secret")
}

func TestSanitizeError_NoProtocol(t *testing.T) {
	t.Parallel()
	err := errors.New("failed: user@host denied")
	result := handlers.SanitizeError(err)
	assert.Equal(t, "failed: user@host denied", result)
}

func TestSanitizeError_MultipleURLs(t *testing.T) {
	t.Parallel()
	err := errors.New("from https://user:pass@a.com to https://user2:pass2@b.com")
	result := handlers.SanitizeError(err)

	assert.Contains(t, result, "https://redacted@a.com")
	assert.Contains(t, result, "https://redacted@b.com")
	assert.NotContains(t, result, "user:pass")
	assert.NotContains(t, result, "user2:pass2")
}

func TestSanitizeError_PathEmbeddedToken(t *testing.T) {
	t.Parallel()
	err := errors.New("GET https://doublezero-mainnet-beta.rpcpool.com/db336024-e7a8-46b1-80e5-352dd77060ab failed")
	result := handlers.SanitizeError(err)
	assert.Equal(t, "GET https://doublezero-mainnet-beta.rpcpool.com/redacted failed", result)
}
