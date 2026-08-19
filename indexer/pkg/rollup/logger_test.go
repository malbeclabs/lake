package rollup

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
)

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
			log, recs := laketesting.NewRecordingLogger()
			l := &temporalLogger{log: log}
			l.Error(tt.msg, "Error", tt.err)
			rec, ok := recs.Last()
			require.True(t, ok, "the log call must emit a record")
			require.Equal(t, tt.want, rec.Level)
		})
	}
}
