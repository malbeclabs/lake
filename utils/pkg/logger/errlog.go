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

// Error logs msg at ERROR or WARN, chosen by the error carried in args (the
// value of the first "error" key wins): transient self-healing causes (see
// dberror.IsTransient) and disconnect-class context errors (see
// IsClientDisconnect) log at WARN, and everything else — including args with
// no "error" key at all — logs at ERROR. log must be non-nil.
//
// Alerts fire on ERROR lines only, so this is the default way to log a
// failure that can carry a transient, not-found, or client-caused error.
// Reserve a raw log.Error call for genuinely-actionable terminal failures.
//
// The helper never drops a line: outside a request handler a context
// deadline is usually a server-side timeout, which deserves a WARN. Request
// handlers that want to skip logging entirely when the client has gone away
// should check IsClientDisconnect first (see api/handlers.logError).
func Error(log Logger, msg string, args ...any) {
	if err := ErrorFromArgs(args); err != nil && (IsClientDisconnect(err) || dberror.IsTransient(err)) {
		log.Warn(msg, args...)
		return
	}
	log.Error(msg, args...)
}

// Warn logs at WARN level, silently skipping client disconnects. Meant for
// request paths, where a disconnect-class error means the client is gone and
// there is nothing to log; elsewhere prefer a plain log.Warn.
func Warn(log Logger, msg string, args ...any) {
	if err := ErrorFromArgs(args); err != nil && IsClientDisconnect(err) {
		return
	}
	log.Warn(msg, args...)
}

// IsCanceled reports whether err is a context cancellation, including
// non-standard wrappings that only carry the message. Unlike
// IsClientDisconnect it does not match deadline errors, so callers can treat
// "the caller went away" differently from "the operation timed out".
func IsCanceled(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) ||
		strings.Contains(err.Error(), "context canceled")
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
// args slice, or nil if none is present. It walks args the way slog does:
// a string key consumes two slots, a slog.Attr consumes one, and anything
// else where a key is expected consumes one (slog's badkey case) — so a
// slog.Attr mixed into args doesn't shift parity and defeat classification.
func ErrorFromArgs(args []any) error {
	for i := 0; i < len(args); {
		switch k := args[i].(type) {
		case string:
			if i+1 >= len(args) {
				return nil
			}
			if k == "error" {
				if err, ok := args[i+1].(error); ok {
					return err
				}
			}
			i += 2
		case slog.Attr:
			if k.Key == "error" {
				if err, ok := k.Value.Any().(error); ok {
					return err
				}
			}
			i++
		default:
			i++
		}
	}
	return nil
}

// slog.Logger's Warn/Error methods satisfy Logger.
var _ Logger = (*slog.Logger)(nil)
