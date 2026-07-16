package handlers

import (
	"log/slog"

	"github.com/getsentry/sentry-go"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/malbeclabs/lake/utils/pkg/logger"
	"github.com/malbeclabs/lake/utils/pkg/redact"
)

// isClientDisconnect returns true if the error is caused by the client
// disconnecting: context cancellation, deadline exceeded, broken pipe,
// connection reset, or unexpected EOF.
func isClientDisconnect(err error) bool {
	return logger.IsClientDisconnect(err)
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
	logger.Error(slog.Default(), msg, args...)
}

// logWarn logs at WARN level, silently skipping client disconnects.
func logWarn(msg string, args ...any) {
	logger.Warn(slog.Default(), msg, args...)
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
