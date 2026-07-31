package handlers

import (
	"log/slog"

	"github.com/getsentry/sentry-go"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/malbeclabs/lake/utils/pkg/logger"
	"github.com/malbeclabs/lake/utils/pkg/redact"
)

// isCallerGone reports whether err means nobody is left to serve, so dropping
// the log line loses nothing. A deadline is deliberately excluded: net/http
// never sets one on r.Context(), so on a request path it is a handler's own
// budget (e.g. multicastDeliveryRequestTimeout) expiring, and dropping it turns
// a 500 into a silent one.
func isCallerGone(err error) bool {
	return logger.IsClientDisconnect(err) && !logger.IsDeadlineExceeded(err)
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
	// Request path: a client disconnect means the caller is gone — skip the
	// log line entirely rather than warn. A handler's own expired deadline is
	// not that (see isCallerGone) and lands at WARN via logger.Error.
	if err := logger.ErrorFromArgs(args); err != nil && isCallerGone(err) {
		return
	}
	logger.Error(slog.Default(), msg, args...)
}

// logWarn logs at WARN level, silently skipping client disconnects. Like
// logError it keeps a handler's own expired deadline (see isCallerGone):
// several handlers bound themselves with context.WithTimeout and no SQL-side
// cap, so that line is the only signal the request ran out of budget.
func logWarn(msg string, args ...any) {
	if err := logger.ErrorFromArgs(args); err != nil && isCallerGone(err) {
		return
	}
	slog.Default().Warn(msg, args...)
}

// internalError logs the full error internally and returns a user-safe message.
// The returned message does not contain sensitive information like credentials,
// hostnames, or query details.
func internalError(operation string, err error) string {
	if isCallerGone(err) {
		return operation
	}

	// Transient (self-healing) causes and a handler's own expired deadline
	// aren't actionable — log at WARN and skip the Sentry capture so a momentary
	// hiccup neither pages nor opens an issue.
	if dberror.IsTransient(err) || logger.IsDeadlineExceeded(err) {
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
