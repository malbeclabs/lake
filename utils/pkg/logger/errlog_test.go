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
		name       string
		args       []any
		wantLogged bool
		wantLevel  slog.Level
	}{
		{"transient connection reset", []any{"error", errors.New("read: connection reset by peer")}, true, slog.LevelWarn},
		{"transient io timeout", []any{"error", errors.New("read: i/o timeout")}, true, slog.LevelWarn},
		{"transient rate limit", []any{"error", errors.New("rate limited (status 429)")}, true, slog.LevelWarn},
		{"actionable syntax error", []any{"error", errors.New("DB::Exception: Syntax error")}, true, slog.LevelError},
		{"actionable generic", []any{"error", errors.New("boom")}, true, slog.LevelError},
		{"client cancel skipped", []any{"error", context.Canceled}, false, 0},
		{"wrapped cancel skipped", []any{"error", errors.New("neo4j: context canceled")}, false, 0},
		{"no error arg", []any{"key", "value"}, true, slog.LevelError},
		{"no args", nil, true, slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			log, recs := capLogger()
			Error(log, "op failed", tt.args...)
			if !tt.wantLogged {
				require.Empty(t, *recs)
				return
			}
			require.Len(t, *recs, 1)
			require.Equal(t, tt.wantLevel, (*recs)[0].Level)
		})
	}
}

func TestWarn_SkipsClientDisconnect(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	Warn(log, "op failed", "error", context.Canceled)
	require.Empty(t, *recs)

	Warn(log, "op failed", "error", errors.New("boom"))
	require.Len(t, *recs, 1)
	require.Equal(t, slog.LevelWarn, (*recs)[0].Level)
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
