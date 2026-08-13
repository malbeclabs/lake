package logger

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// levelRecorder is a slog.Handler that records each emitted record.
type levelRecorder struct{ records *[]slog.Record }

func (h levelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h levelRecorder) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}
func (h levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h levelRecorder) WithGroup(string) slog.Handler      { return h }

func capLogger() (*slog.Logger, *[]slog.Record) {
	var recs []slog.Record
	return slog.New(levelRecorder{&recs}), &recs
}

func TestError_ClassifiesByErrorArg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		args      []any
		wantLevel slog.Level
	}{
		{"transient connection reset", []any{"error", errors.New("read: connection reset by peer")}, slog.LevelWarn},
		{"transient io timeout", []any{"error", errors.New("read: i/o timeout")}, slog.LevelWarn},
		{"transient rate limit", []any{"error", errors.New("rate limited (status 429)")}, slog.LevelWarn},
		{"actionable syntax error", []any{"error", errors.New("DB::Exception: Syntax error")}, slog.LevelError},
		{"actionable generic", []any{"error", errors.New("boom")}, slog.LevelError},
		// Disconnect-class errors demote to WARN, never vanish: outside a
		// request handler a context deadline is usually a server-side timeout.
		{"client cancel warns", []any{"error", context.Canceled}, slog.LevelWarn},
		{"deadline exceeded warns", []any{"error", context.DeadlineExceeded}, slog.LevelWarn},
		{"wrapped cancel warns", []any{"error", errors.New("neo4j: context canceled")}, slog.LevelWarn},
		{"no error arg", []any{"key", "value"}, slog.LevelError},
		{"no args", nil, slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log, recs := capLogger()
			Error(log, "op failed", tt.args...)
			require.Len(t, *recs, 1, "Error never drops a line")
			require.Equal(t, tt.wantLevel, (*recs)[0].Level)
		})
	}
}

func TestErrorFromArgs(t *testing.T) {
	t.Parallel()

	err := errors.New("boom")
	require.Equal(t, err, ErrorFromArgs([]any{"a", 1, "error", err}))
	require.NoError(t, ErrorFromArgs([]any{"error", "not an error"}))
	require.NoError(t, ErrorFromArgs(nil))
	// Odd-length slice: trailing "error" key with no value is ignored.
	require.NoError(t, ErrorFromArgs([]any{"error"}))
}

func TestErrorFromArgs_SlogAttrDoesNotShiftParity(t *testing.T) {
	t.Parallel()

	err := errors.New("boom")
	// A slog.Attr consumes one slot; the "error" key after it must still be
	// found (previously the parity shift hid it).
	require.Equal(t, err, ErrorFromArgs([]any{slog.String("cache", "topology"), "error", err}))
	// An Attr can carry the error itself.
	require.Equal(t, err, ErrorFromArgs([]any{slog.Any("error", err)}))
	// slog badkey case: a non-string non-Attr where a key is expected
	// consumes one slot.
	require.Equal(t, err, ErrorFromArgs([]any{42, "error", err}))
}

func TestIsDeadlineExceeded(t *testing.T) {
	require.True(t, IsDeadlineExceeded(context.DeadlineExceeded))
	require.True(t, IsDeadlineExceeded(errors.New("query failed: context deadline exceeded")))
	require.False(t, IsDeadlineExceeded(nil))
	require.False(t, IsDeadlineExceeded(context.Canceled), "cancellation is not a deadline")
	require.False(t, IsDeadlineExceeded(errors.New("boom")))
}
