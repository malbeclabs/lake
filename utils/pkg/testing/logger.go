package laketesting

import (
	"context"
	"log/slog"
	"os"
)

func NewLogger() *slog.Logger {
	debugLevel := os.Getenv("DEBUG")
	var level slog.Level
	switch debugLevel {
	case "2":
		level = slog.LevelDebug
	case "1":
		level = slog.LevelInfo
	default:
		// Suppress logs by default (only show errors and above)
		level = slog.LevelError
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// RecordingHandler keeps every record logged through it so a test can assert on
// what a code path logged: its level, its message, or one of its attributes.
type RecordingHandler struct{ records []slog.Record }

// NewRecordingLogger returns a logger that writes into a fresh RecordingHandler.
func NewRecordingLogger() (*slog.Logger, *RecordingHandler) {
	h := &RecordingHandler{}
	return slog.New(h), h
}

func (h *RecordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *RecordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}
func (h *RecordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *RecordingHandler) WithGroup(string) slog.Handler      { return h }

// Records returns every record handled so far, in order.
func (h *RecordingHandler) Records() []slog.Record { return h.records }

// Last returns the most recent record, and false if nothing was logged.
func (h *RecordingHandler) Last() (slog.Record, bool) {
	if len(h.records) == 0 {
		return slog.Record{}, false
	}
	return h.records[len(h.records)-1], true
}

// Find returns the first record carrying the given message.
func (h *RecordingHandler) Find(msg string) (slog.Record, bool) {
	for _, r := range h.records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

// RecordAttr returns a record's named attribute value.
func RecordAttr(r slog.Record, key string) (any, bool) {
	var (
		val   any
		found bool
	)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			val, found = a.Value.Any(), true
			return false
		}
		return true
	})
	return val, found
}
