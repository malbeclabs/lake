package rollup

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// capturingHandler records the level of the last log record it handled.
type capturingHandler struct{ last slog.Level }

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.last = r.Level
	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func TestTemporalLoggerErrorLevel(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		err  error
		want slog.Level
	}{
		{
			name: "transient activity error demoted to warn",
			msg:  "Activity error.",
			err:  errors.New("link latency query: query processing: failed to read first block packet from 52.4.220.199:9440 (conn_id=15): read: EOF"),
			want: slog.LevelWarn,
		},
		{
			name: "non-transient activity error stays error",
			msg:  "Activity error.",
			err:  errors.New("Code: 62. DB::Exception: Syntax error"),
			want: slog.LevelError,
		},
		{
			name: "context cancellation demoted to warn",
			msg:  "Activity error.",
			err:  context.Canceled,
			want: slog.LevelWarn,
		},
		{
			name: "transient error under a different message stays error",
			msg:  "some other failure",
			err:  errors.New("read: EOF"),
			want: slog.LevelError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &capturingHandler{}
			l := &temporalLogger{log: slog.New(h)}
			l.Error(tt.msg, "Error", tt.err)
			require.Equal(t, tt.want, h.last)
		})
	}
}
