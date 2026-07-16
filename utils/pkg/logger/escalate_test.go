package logger

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func countLevel(recs []slog.Record, level slog.Level) int {
	n := 0
	for _, r := range recs {
		if r.Level == level {
			n++
		}
	}
	return n
}

func TestEscalator_EscalatesAtThreshold(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	var esc Escalator
	err := errors.New("boom") // non-transient → strict threshold (default 3)

	for range DefaultErrorAfter - 1 {
		esc.Fail(log, "k", "refresh failed", "error", err)
	}
	require.Equal(t, DefaultErrorAfter-1, countLevel(*recs, slog.LevelWarn))
	require.Zero(t, countLevel(*recs, slog.LevelError), "below threshold must stay WARN")

	esc.Fail(log, "k", "refresh failed", "error", err)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError), "at threshold must escalate to ERROR")
}

func TestEscalator_TransientUsesHigherThreshold(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	var esc Escalator
	err := errors.New("read: connection reset by peer") // transient

	for range DefaultTransientErrorAfter - 1 {
		esc.Fail(log, "k", "refresh failed", "error", err)
	}
	require.Zero(t, countLevel(*recs, slog.LevelError), "transient blips must not page below the transient threshold")

	esc.Fail(log, "k", "refresh failed", "error", err)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

func TestEscalator_ResetStartsFreshRun(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	var esc Escalator
	err := errors.New("boom")

	for range DefaultErrorAfter - 1 {
		esc.Fail(log, "k", "refresh failed", "error", err)
	}
	esc.Reset("k")
	esc.Fail(log, "k", "refresh failed", "error", err)
	require.Zero(t, countLevel(*recs, slog.LevelError), "reset must clear the consecutive count")
}

func TestEscalator_KeysAreIndependent(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	var esc Escalator
	err := errors.New("boom")

	for range DefaultErrorAfter {
		esc.Fail(log, "a", "refresh failed", "error", err)
	}
	esc.Fail(log, "b", "refresh failed", "error", err)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError), "key b must not inherit key a's count")
}

func TestEscalator_CustomThresholds(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	// TransientErrorAfter equal to ErrorAfter: transient causes escalate at
	// the strict threshold (the workflow-side configuration).
	esc := Escalator{ErrorAfter: 2, TransientErrorAfter: 2}
	err := errors.New("read: i/o timeout") // transient

	esc.Fail(log, "k", "refresh failed", "error", err)
	require.Zero(t, countLevel(*recs, slog.LevelError))
	esc.Fail(log, "k", "refresh failed", "error", err)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

func TestEscalator_AppendsConsecutiveFailures(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	var esc Escalator
	esc.Fail(log, "k", "refresh failed", "error", errors.New("boom"))

	require.Len(t, *recs, 1)
	found := false
	(*recs)[0].Attrs(func(a slog.Attr) bool {
		if a.Key == "consecutive_failures" {
			found = true
			require.EqualValues(t, 1, a.Value.Any())
		}
		return true
	})
	require.True(t, found, "log line must carry consecutive_failures")
}
