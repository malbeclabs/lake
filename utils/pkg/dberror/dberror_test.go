package dberror_test

import (
	"context"
	"errors"
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

		// The actual prod CH-connection-drop errors we saw — must be transient.
		{"ch read packet reset", errors.New("query processing: failed to read packet from 52.4.220.199:9440 (conn_id=492): read: connection reset by peer"), dberror.ErrorTypeConnectivity, true},
		{"ch read packet io timeout", errors.New("failed to read packet from 54.166.56.105:9440: read: i/o timeout"), dberror.ErrorTypeConnectivity, true},

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
