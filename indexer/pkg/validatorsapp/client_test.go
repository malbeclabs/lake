package validatorsapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func loadTestFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/validators_mainnet.json")
	require.NoError(t, err)
	return data
}

func TestHTTPClient(t *testing.T) {
	t.Parallel()

	t.Run("parses real API response", func(t *testing.T) {
		t.Parallel()

		fixture := loadTestFixture(t)

		var receivedAuthHeader string
		var receivedURL string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuthHeader = r.Header.Get("Token")
			receivedURL = r.URL.String()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fixture)
		}))
		defer server.Close()

		client := NewHTTPClient(server.URL, "test-api-key")
		validators, err := client.GetValidators(context.Background())
		require.NoError(t, err)

		// Verify we parsed a meaningful number of validators
		require.Greater(t, len(validators), 100, "should parse >100 validators from real fixture")

		// Verify the auth header was sent
		require.Equal(t, "test-api-key", receivedAuthHeader, "should send Token header")

		// Verify the URL path and query params
		require.Equal(t, "/api/v1/validators/mainnet.json?limit=9999&active_only=true", receivedURL)

		// Verify multiple software client types exist in the data
		clientTypes := make(map[string]bool)
		for _, v := range validators {
			clientTypes[v.SoftwareClient] = true
		}
		require.Greater(t, len(clientTypes), 1, "should have multiple software client types")
		require.True(t, clientTypes["Agave"] || clientTypes["AgaveBam"], "should have Agave or AgaveBam client")
		require.True(t, clientTypes["Firedancer"] || clientTypes["Frankendancer"], "should have Firedancer or Frankendancer client")

		// Verify first validator has expected fields populated
		// The fixture's first entry is Solflare
		first := validators[0]
		require.Equal(t, "722RdWmHC5TGXBjTejzNjbc8xEiduVDLqZvoUGz6Xzbp", first.Account)
		require.Equal(t, "Solflare", first.Name)
		require.Equal(t, "EXhYxF25PJEHb3v5G1HY8Jn8Jm7bRjJtaxEghGrUuhQw", first.VoteAccount)
		require.True(t, first.IsActive)
		require.False(t, first.IsDZ)
		require.Greater(t, first.ActiveStake, uint64(0), "active stake should be non-zero")

		// Verify jito fields
		require.True(t, first.Jito)
		require.Equal(t, uint32(10000), first.JitoCommission)

		// Find a DZ validator and verify IsDZ
		var foundDZ bool
		for _, v := range validators {
			if v.IsDZ {
				foundDZ = true
				break
			}
		}
		require.True(t, foundDZ, "should have at least one DZ validator")
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limited"))
		}))
		defer server.Close()

		client := NewHTTPClient(server.URL, "test-api-key")
		validators, err := client.GetValidators(context.Background())
		require.Error(t, err)
		require.Nil(t, validators)
		require.Contains(t, err.Error(), "429")
	})

	t.Run("returns error on invalid JSON", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not valid json"))
		}))
		defer server.Close()

		client := NewHTTPClient(server.URL, "test-api-key")
		validators, err := client.GetValidators(context.Background())
		require.Error(t, err)
		require.Nil(t, validators)
		require.Contains(t, err.Error(), "failed to decode response")
	})
}
