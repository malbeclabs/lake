package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// capHandler records emitted records so tests can assert on log level.
type capHandler struct{ records *[]slog.Record }

func (h capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h capHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}
func (h capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h capHandler) WithGroup(string) slog.Handler      { return h }

func capLogger() (*slog.Logger, *[]slog.Record) {
	var recs []slog.Record
	return slog.New(capHandler{&recs}), &recs
}

func warnCount(recs []slog.Record) int {
	n := 0
	for _, r := range recs {
		if r.Level == slog.LevelWarn {
			n++
		}
	}
	return n
}

func TestDurationEnv(t *testing.T) {
	const def, lo, hi = 30 * time.Second, 5 * time.Second, 10 * time.Minute

	t.Run("unset uses default, no warn", func(t *testing.T) {
		log, recs := capLogger()
		require.Equal(t, def, durationEnv(log, "PC_DUR_UNSET", def, lo, hi))
		require.Zero(t, warnCount(*recs))
	})
	t.Run("valid override", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR", "90s")
		require.Equal(t, 90*time.Second, durationEnv(log, "PC_DUR", def, lo, hi))
		require.Zero(t, warnCount(*recs))
	})
	t.Run("invalid (missing unit) keeps default + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR_BAD", "90") // no unit → ParseDuration fails
		require.Equal(t, def, durationEnv(log, "PC_DUR_BAD", def, lo, hi))
		require.Equal(t, 1, warnCount(*recs))
	})
	t.Run("below min clamps + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR_LOW", "1ms")
		require.Equal(t, lo, durationEnv(log, "PC_DUR_LOW", def, lo, hi))
		require.Equal(t, 1, warnCount(*recs))
	})
	t.Run("above max clamps + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR_HIGH", "1h")
		require.Equal(t, hi, durationEnv(log, "PC_DUR_HIGH", def, lo, hi))
		require.Equal(t, 1, warnCount(*recs))
	})
}

func TestIntEnv(t *testing.T) {
	const def, lo, hi = 8, 1, 32

	t.Run("unset uses default", func(t *testing.T) {
		log, recs := capLogger()
		require.Equal(t, def, intEnv(log, "PC_INT_UNSET", def, lo, hi))
		require.Zero(t, warnCount(*recs))
	})
	t.Run("valid override", func(t *testing.T) {
		log, _ := capLogger()
		t.Setenv("PC_INT", "3")
		require.Equal(t, 3, intEnv(log, "PC_INT", def, lo, hi))
	})
	t.Run("invalid keeps default + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_INT_BAD", "three")
		require.Equal(t, def, intEnv(log, "PC_INT_BAD", def, lo, hi))
		require.Equal(t, 1, warnCount(*recs))
	})
	t.Run("non-positive clamps to min + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_INT_ZERO", "0")
		require.Equal(t, lo, intEnv(log, "PC_INT_ZERO", def, lo, hi))
		require.Equal(t, 1, warnCount(*recs))
	})
	t.Run("above max clamps + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_INT_HIGH", "1000")
		require.Equal(t, hi, intEnv(log, "PC_INT_HIGH", def, lo, hi))
		require.Equal(t, 1, warnCount(*recs))
	})
}

func TestLoadRefreshConfig(t *testing.T) {
	log, _ := capLogger()

	t.Run("unset → prod defaults, threshold derived for 30s", func(t *testing.T) {
		p, conc := loadRefreshConfig(log)
		require.Equal(t, defaultRefreshInterval, p.RefreshInterval)
		require.Equal(t, defaultActivityTimeout, p.ActivityTimeout)
		require.Equal(t, defaultRefreshConcurrency, conc)
		// 30min / 30s = 60 iterations.
		require.Equal(t, 60, p.ContinueAsNewThreshold)
	})

	t.Run("staging overrides → gentler values, threshold scales down", func(t *testing.T) {
		t.Setenv("PAGE_CACHE_REFRESH_INTERVAL", "90s")
		t.Setenv("PAGE_CACHE_REFRESH_CONCURRENCY", "3")
		p, conc := loadRefreshConfig(log)
		require.Equal(t, 90*time.Second, p.RefreshInterval)
		require.Equal(t, 3, conc)
		// 30min / 90s = 20 iterations → history stays bounded despite the longer interval.
		require.Equal(t, 20, p.ContinueAsNewThreshold)
	})
}
