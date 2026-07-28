package dberror_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/stretchr/testify/require"
)

func TestClassifyAndIsTransient(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantType  dberror.ErrorType
		transient bool
	}{
		{"nil", nil, dberror.ErrorTypeUnknown, false},

		// Rate limits (new): upstream 429 — transient, self-healing.
		{"rpc rate limit message", errors.New("rate limited (status 429)"), dberror.ErrorTypeRateLimit, true},
		{"too many requests", errors.New("upstream returned Too Many Requests"), dberror.ErrorTypeRateLimit, true},
		{"grpc resource exhausted", errors.New("rpc error: code = ResourceExhausted desc = token bucket exhausted"), dberror.ErrorTypeRateLimit, true},
		// InfluxDB Cloud throttle signatures. It emits the plural "Resources exhausted",
		// which the singular "resource exhausted" pattern does not substring-match, plus a
		// "request too large" prefix — both must classify as transient rate limits.
		{"influxdb resources exhausted plural", errors.New("Resources exhausted: Heap exhausted"), dberror.ErrorTypeRateLimit, true},
		{"influxdb request too large", errors.New("request too large: Error running series plans for namespace 'x': Resources exhausted: Heap exhausted"), dberror.ErrorTypeRateLimit, true},

		// The actual prod CH-connection-drop errors we saw — must be transient.
		{"ch read packet reset", errors.New("query processing: failed to read packet from 52.4.220.199:9440 (conn_id=492): read: connection reset by peer"), dberror.ErrorTypeConnectivity, true},
		{"ch read packet io timeout", errors.New("failed to read packet from 54.166.56.105:9440: read: i/o timeout"), dberror.ErrorTypeConnectivity, true},
		{"unexpected eof", errors.New("read tcp 10.0.0.1:9000: unexpected EOF"), dberror.ErrorTypeConnectivity, true},
		{"bare eof", errors.New("EOF"), dberror.ErrorTypeConnectivity, true},

		// "eof" must match as a word, not an embedded trigram.
		{"geofence not transient", errors.New("failed to update geofence for device"), dberror.ErrorTypeUnknown, false},

		// AWS SDK v2 transient S3 responses (SyncMSDP staging noise, #731).
		// The observed shape: a 200 with an error embedded mid-body.
		{"s3 200 embedded error", errors.New("msdp fetch: msdp: list snapshots/ip-msdp-sa-cache-rejected/device=x/date=2026-07-26/: operation error S3: ListObjectsV2, https response error StatusCode: 200, RequestID: abc123, HostID: def, api error InternalError"), dberror.ErrorTypeConnectivity, true},
		{"s3 500 internal error", errors.New("operation error S3: ListObjectsV2, https response error StatusCode: 500, RequestID: x, InternalError"), dberror.ErrorTypeConnectivity, true},
		{"s3 503 slow down", errors.New("operation error S3: GetObject, https response error StatusCode: 503, RequestID: x, SlowDown"), dberror.ErrorTypeConnectivity, true},
		// The production 5xx shape: the SDK retries 500/502/503/504 internally,
		// so a sustained 5xx reaches us wrapped in the max-attempts message.
		// Pins the regex staying unanchored to the message start.
		{"s3 500 after sdk retries", errors.New("operation error S3: ListObjectsV2, exceeded maximum number of attempts, 3, https response error StatusCode: 500, RequestID: x, api error InternalError"), dberror.ErrorTypeConnectivity, true},
		// 501 NotImplemented is a permanent endpoint/config failure, outside the
		// SDK's DefaultRetryableHTTPStatusCodes {500, 502, 503, 504} — must page.
		{"s3 501 not implemented", errors.New("operation error S3: ListObjectsV2, https response error StatusCode: 501, RequestID: x, NotImplemented"), dberror.ErrorTypeUnknown, false},

		// Actionable S3 4xx must keep paging (not transient). The SDK spells it
		// "AccessDenied" (no space), so it stays Unknown rather than matching the
		// auth pattern — either way it is non-transient, which is the point.
		{"s3 403 access denied", errors.New("operation error S3: ListObjectsV2, https response error StatusCode: 403, RequestID: x, AccessDenied"), dberror.ErrorTypeUnknown, false},
		{"s3 404 no such key", errors.New("operation error S3: GetObject, https response error StatusCode: 404, RequestID: x, NoSuchKey"), dberror.ErrorTypeUnknown, false},
		// A non-AWS message mentioning statuscode: 200 without the SDK prefix must not match.
		{"non-aws statuscode 200", errors.New("handler returned statuscode: 200 but body was empty"), dberror.ErrorTypeUnknown, false},

		// Non-transient: real, actionable failures should still escalate to ERROR.
		{"syntax error", errors.New("Code: 62. DB::Exception: Syntax error"), dberror.ErrorTypeQuery, false},
		{"access denied", errors.New("access denied for user"), dberror.ErrorTypeAuth, false},
		{"context canceled", context.Canceled, dberror.ErrorTypeUnknown, false},
		{"deadline exceeded", context.DeadlineExceeded, dberror.ErrorTypeTimeout, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.transient, dberror.IsTransient(tt.err), "IsTransient")
			if tt.err != nil {
				require.Equal(t, tt.wantType, dberror.Classify(tt.err), "Classify")
			}
		})
	}
}

func TestIsTransientSentinel(t *testing.T) {
	// The explicit ErrTransient marker makes IsTransient true even when the
	// message alone would classify as non-transient (e.g. a not-found miss).
	notFound := errors.New("get transaction: not found")
	require.False(t, dberror.IsTransient(notFound), "plain not-found is not transient")

	wrapped := fmt.Errorf("%w (%w)", notFound, dberror.ErrTransient)
	require.True(t, dberror.IsTransient(wrapped), "not-found wrapped with ErrTransient is transient")
	require.True(t, errors.Is(wrapped, dberror.ErrTransient), "wrapped unwraps to ErrTransient")
	require.True(t, errors.Is(wrapped, notFound), "wrapped still unwraps to the original cause")

	// The marker does not override a genuine context cancellation.
	require.False(t, dberror.IsTransient(context.Canceled))
}
