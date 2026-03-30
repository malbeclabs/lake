package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"syscall"

	"github.com/getsentry/sentry-go"
)

// isClientDisconnect returns true if the error is caused by the client
// disconnecting: context cancellation, deadline exceeded, broken pipe,
// connection reset, or unexpected EOF.
func isClientDisconnect(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

// logError logs at ERROR level, silently skipping client disconnects.
func logError(msg string, args ...any) {
	// Extract the error from args to check for client disconnect.
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "error" {
			if err, ok := args[i+1].(error); ok && isClientDisconnect(err) {
				return
			}
		}
	}
	slog.Error(msg, args...)
}

// internalError logs the full error internally and returns a user-safe message.
// The returned message does not contain sensitive information like credentials,
// hostnames, or query details.
func internalError(operation string, err error) string {
	if isClientDisconnect(err) {
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

// SanitizeError removes sensitive information from error messages.
// Use this when you need to include some error context but want to
// strip credentials and internal details.
func SanitizeError(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()

	// Remove anything that looks like credentials in URLs
	// Pattern: protocol://user:pass@host or protocol://user@host
	if idx := strings.Index(msg, "://"); idx != -1 {
		// Find the @ symbol that separates credentials from host
		atIdx := strings.Index(msg[idx:], "@")
		if atIdx != -1 {
			// Replace credentials with ***
			endOfProto := idx + 3 // len("://")
			msg = msg[:endOfProto] + "***@" + msg[idx+atIdx+1:]
		}
	}

	// Remove query parameters which may contain SQL
	if idx := strings.Index(msg, "?"); idx != -1 {
		// Find the end of the URL (next space or quote)
		endIdx := len(msg)
		for _, delim := range []string{" ", "'", "\""} {
			if i := strings.Index(msg[idx:], delim); i != -1 && idx+i < endIdx {
				endIdx = idx + i
			}
		}
		msg = msg[:idx] + "?..." + msg[endIdx:]
	}

	return msg
}
