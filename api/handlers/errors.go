package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"syscall"

	"github.com/getsentry/sentry-go"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/utils/pkg/redact"
)

// isClientDisconnect returns true if the error is caused by the client
// disconnecting: context cancellation, deadline exceeded, broken pipe,
// connection reset, or unexpected EOF.
func isClientDisconnect(err error) bool {
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Some drivers (e.g. neo4j) wrap context errors without using Go's
	// standard error wrapping, so errors.Is fails. Fall back to checking
	// the error message.
	msg := err.Error()
	return strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "context deadline exceeded")
}

// logError logs a handler error, silently skipping client disconnects.
//
// Transient (self-healing) causes — upstream connection blips, timeouts, and
// rate limits (see dberror.IsTransient) — are logged at WARN rather than ERROR
// so a momentary ClickHouse/RPC hiccup on the request path doesn't page on-call.
// Genuine failures (query/syntax/auth errors, nil-derived, anything else) still
// log at ERROR. Sustained outages are caught elsewhere (the page-cache
// consecutive-failure threshold and the lake-api-down/crash-loop alerts).
func logError(msg string, args ...any) {
	if hasClientDisconnect(args) {
		return
	}
	if err := errorFromArgs(args); err != nil && dberror.IsTransient(err) {
		slog.Warn(msg, args...)
		return
	}
	slog.Error(msg, args...)
}

// logWarn logs at WARN level, silently skipping client disconnects.
func logWarn(msg string, args ...any) {
	if hasClientDisconnect(args) {
		return
	}
	slog.Warn(msg, args...)
}

// errorFromArgs returns the value of the first "error" key in a slog-style
// args slice, or nil if none is present.
func errorFromArgs(args []any) error {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "error" {
			if err, ok := args[i+1].(error); ok {
				return err
			}
		}
	}
	return nil
}

// hasClientDisconnect reports whether args contains an "error" key whose
// value is a client-disconnect error (context cancellation, broken pipe, etc.).
func hasClientDisconnect(args []any) bool {
	err := errorFromArgs(args)
	return err != nil && isClientDisconnect(err)
}

// internalError logs the full error internally and returns a user-safe message.
// The returned message does not contain sensitive information like credentials,
// hostnames, or query details.
func internalError(operation string, err error) string {
	if isClientDisconnect(err) {
		return operation
	}

	// Transient (self-healing) causes aren't actionable — log at WARN and skip
	// the Sentry capture so a momentary hiccup neither pages nor opens an issue.
	if dberror.IsTransient(err) {
		slog.Warn(operation, "error", err)
		return operation
	}

	// Log full error for debugging
	slog.Error(operation, "error", err)

	// Capture to Sentry if configured
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("operation", operation)
		sentry.CaptureException(err)
	})

	// Return sanitized message
	return operation
}

// SanitizeError removes sensitive information from error messages by
// redacting basic-auth credentials, sensitive query-param values, and
// token-shaped path segments in any URLs the message contains.
func SanitizeError(err error) string {
	return redact.Error(err)
}
