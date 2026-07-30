package handlers

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// levelRecorder is a slog.Handler that records the level of each emitted record.
type levelRecorder struct{ records *[]slog.Record }

func (h levelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h levelRecorder) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}
func (h levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h levelRecorder) WithGroup(string) slog.Handler      { return h }

// Not parallel: swaps the global slog default. Go runs non-parallel tests to
// completion before parallel tests resume, so the swap window is isolated.
func TestLogError_DowngradesTransientToWarn(t *testing.T) {
	var recs []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(levelRecorder{&recs}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	tests := []struct {
		name       string
		err        error
		wantLogged bool
		wantLevel  slog.Level
	}{
		// The actual transient prod errors → WARN (self-healing, not actionable).
		{"ch connection reset", errors.New("query processing: failed to read packet from 54.166.56.105:9440 (conn_id=2819): read: connection reset by peer"), true, slog.LevelWarn},
		{"ch io timeout", errors.New("failed to read packet from x:9440: read: i/o timeout"), true, slog.LevelWarn},
		{"upstream rate limit", errors.New("rate limited (status 429)"), true, slog.LevelWarn},
		// Genuine failures still escalate to ERROR (still page).
		{"syntax error", errors.New("Code: 62. DB::Exception: Syntax error"), true, slog.LevelError},
		{"generic error", errors.New("boom"), true, slog.LevelError},
		// Client disconnects are dropped entirely (not logged).
		{"client cancel", context.Canceled, false, 0},
		// A deadline is the handler's own budget expiring (net/http never sets
		// one on r.Context()), so it stays visible at WARN. Dropping it made an
		// overrun return a 500 with no log line at all.
		{"handler deadline", context.DeadlineExceeded, true, slog.LevelWarn},
		{"wrapped handler deadline", errors.New("query failed: context deadline exceeded"), true, slog.LevelWarn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recs = nil
			logError("op failed", "error", tt.err)
			if !tt.wantLogged {
				require.Empty(t, recs)
				return
			}
			require.Len(t, recs, 1)
			require.Equal(t, tt.wantLevel, recs[0].Level)
		})
	}
}

func TestInternalError_DowngradesTransientToWarn(t *testing.T) {
	var recs []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(levelRecorder{&recs}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	recs = nil
	internalError("fetch failed", errors.New("failed to read packet from x:9440: read: connection reset by peer"))
	require.Len(t, recs, 1)
	require.Equal(t, slog.LevelWarn, recs[0].Level, "transient → WARN")

	recs = nil
	internalError("fetch failed", errors.New("unexpected boom"))
	require.Len(t, recs, 1)
	require.Equal(t, slog.LevelError, recs[0].Level, "genuine → ERROR")
}

func TestInternalError_HandlerDeadlineWarns(t *testing.T) {
	var recs []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(levelRecorder{&recs}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	internalError("fetch failed", context.DeadlineExceeded)
	require.Len(t, recs, 1, "a handler's own deadline must not be dropped as a disconnect")
	require.Equal(t, slog.LevelWarn, recs[0].Level)
}
