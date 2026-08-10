package ingestionlog

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBuildRecord_RedactsCredentialsInErrorMessage is a credential-leak regression.
//
// solana-go puts the full endpoint URL into its error text, and this record's
// ErrorMessage is stored in log_ingestion_runs, a column readable through
// /api/sql/query and the hosted MCP. Redacting only in the slog handler left the
// database sink exposed: production held seven rows with unredacted endpoint
// credentials, including six with an api-key query parameter and one with a keyed
// rpcpool path.
func TestBuildRecord_RedactsCredentialsInErrorMessage(t *testing.T) {
	t.Parallel()

	// Shaped like a real Triton/RPCPool key: 8-4-4-4-12 hex, which is what
	// redact's tokenSegment matches. An off-shape fake silently fails to match
	// and makes this test pass for the wrong reason.
	secret := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

	tests := []struct {
		name string
		err  error
	}{
		{
			// The shape solana-go produces on a >=400 whose body did not decode.
			name: "keyed path in a url.Error",
			err: &url.Error{
				Op:  "Post",
				URL: "https://doublezero-mainnet-beta.rpcpool.com/" + secret,
				Err: errors.New("context deadline exceeded"),
			},
		},
		{
			name: "api-key query parameter",
			err: fmt.Errorf(`failed to send request: Post "https://mainnet.legacy.helius-rpc.com/?api-key=%s": `+
				`context deadline exceeded`, secret),
		},
		{
			// QuickNode-style: a long opaque segment rather than a UUID.
			name: "long opaque path token",
			err: &url.Error{
				Op:  "Post",
				URL: "https://example-endpoint.quiknode.pro/" + strings.ReplaceAll(secret, "-", ""),
				Err: errors.New("EOF"),
			},
		},
		{
			// Wrapped the way an activity wraps a view error before it reaches here.
			name: "wrapped several layers deep",
			err: fmt.Errorf("permission events refresh: drain account: get transaction: %w",
				&url.Error{Op: "Post", URL: "https://x.rpcpool.com/" + secret, Err: errors.New("EOF")}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := buildRecord("dzingest", "RefreshPermissionEvents", "mainnet-beta",
				time.Now().Add(-time.Second), RefreshResult{}, tt.err)

			require.Equal(t, "error", rec.Status)
			require.NotNil(t, rec.ErrorMessage)
			require.NotContains(t, *rec.ErrorMessage, secret,
				"the endpoint credential must not reach log_ingestion_runs.error_message; "+
					"that column is readable through the public SQL and MCP endpoints")
			// The cause must survive redaction, or the row stops being diagnostic.
			require.NotEmpty(t, *rec.ErrorMessage)
		})
	}
}
