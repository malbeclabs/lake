package logger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"syscall"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// Logger is the minimal leveled interface needed by the error-classification
// helpers. It is satisfied by both *slog.Logger and Temporal's log.Logger.
type Logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Error logs msg at a level chosen by the error carried in args (the value of
// the first "error" key): client disconnects are skipped entirely, transient
// self-healing causes (see dberror.IsTransient) log at WARN, and everything
// else logs at ERROR.
//
// Alerts fire on ERROR lines only, so this is the default way to log a
// failure that can carry a transient, not-found, or client-caused error.
// Reserve a raw log.Error call for genuinely-actionable terminal failures.
func Error(log Logger, msg string, args ...any) {
	if err := ErrorFromArgs(args); err != nil {
		if IsClientDisconnect(err) {
			return
		}
		if dberror.IsTransient(err) {
			log.Warn(msg, args...)
			return
		}
	}
	log.Error(msg, args...)
}

// Warn logs at WARN level, silently skipping client disconnects.
func Warn(log Logger, msg string, args ...any) {
	if err := ErrorFromArgs(args); err != nil && IsClientDisconnect(err) {
		return
	}
	log.Warn(msg, args...)
}

// IsClientDisconnect returns true if the error is caused by the client
// disconnecting: context cancellation, deadline exceeded, broken pipe,
// connection reset, or unexpected EOF.
func IsClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
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

// ErrorFromArgs returns the value of the first "error" key in a slog-style
// args slice, or nil if none is present.
func ErrorFromArgs(args []any) error {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "error" {
			if err, ok := args[i+1].(error); ok {
				return err
			}
		}
	}
	return nil
}

// slog.Logger's Warn/Error methods satisfy Logger.
var _ Logger = (*slog.Logger)(nil)
