package worker

import (
	"context"
	"errors"
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

func countLevel(recs []slog.Record, lvl slog.Level) int {
	n := 0
	for _, r := range recs {
		if r.Level == lvl {
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
		require.Zero(t, countLevel(*recs, slog.LevelWarn))
	})
	t.Run("valid override", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR", "90s")
		require.Equal(t, 90*time.Second, durationEnv(log, "PC_DUR", def, lo, hi))
		require.Zero(t, countLevel(*recs, slog.LevelWarn))
	})
	t.Run("invalid (missing unit) keeps default + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR_BAD", "90") // no unit → ParseDuration fails
		require.Equal(t, def, durationEnv(log, "PC_DUR_BAD", def, lo, hi))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})
	t.Run("below min clamps + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR_LOW", "1ms")
		require.Equal(t, lo, durationEnv(log, "PC_DUR_LOW", def, lo, hi))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})
	t.Run("above max clamps + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_DUR_HIGH", "1h")
		require.Equal(t, hi, durationEnv(log, "PC_DUR_HIGH", def, lo, hi))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})
}

func TestIntEnv(t *testing.T) {
	const def, lo, hi = 8, 1, 32

	t.Run("unset uses default", func(t *testing.T) {
		log, recs := capLogger()
		require.Equal(t, def, intEnv(log, "PC_INT_UNSET", def, lo, hi))
		require.Zero(t, countLevel(*recs, slog.LevelWarn))
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
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})
	t.Run("non-positive clamps to min + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_INT_ZERO", "0")
		require.Equal(t, lo, intEnv(log, "PC_INT_ZERO", def, lo, hi))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})
	t.Run("above max clamps + warns", func(t *testing.T) {
		log, recs := capLogger()
		t.Setenv("PC_INT_HIGH", "1000")
		require.Equal(t, hi, intEnv(log, "PC_INT_HIGH", def, lo, hi))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})
}

// clearRefreshEnv makes the test hermetic against an ambient PAGE_CACHE_* env
// (durationEnv/intEnv treat empty as unset).
func clearRefreshEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PAGE_CACHE_REFRESH_INTERVAL", "")
	t.Setenv("PAGE_CACHE_REFRESH_CONCURRENCY", "")
	t.Setenv("PAGE_CACHE_REFRESH_TIMEOUT", "")
}

func TestLoadRefreshConfig(t *testing.T) {
	log, _ := capLogger()

	t.Run("unset → prod defaults; threshold left to withDefaults", func(t *testing.T) {
		clearRefreshEnv(t)
		p, conc := loadRefreshConfig(log)
		require.Equal(t, defaultRefreshInterval, p.RefreshInterval)
		require.Equal(t, defaultActivityTimeout, p.ActivityTimeout)
		require.Equal(t, defaultRefreshConcurrency, conc)
		require.Zero(t, p.ContinueAsNewThreshold, "derivation is withDefaults' job")
	})

	t.Run("staging overrides → gentler values", func(t *testing.T) {
		clearRefreshEnv(t)
		t.Setenv("PAGE_CACHE_REFRESH_INTERVAL", "90s")
		t.Setenv("PAGE_CACHE_REFRESH_CONCURRENCY", "3")
		p, conc := loadRefreshConfig(log)
		require.Equal(t, 90*time.Second, p.RefreshInterval)
		require.Equal(t, 3, conc)
	})
}

func TestPageCacheParamsWithDefaults(t *testing.T) {
	t.Run("zero params → all defaults (pre-change history compat path)", func(t *testing.T) {
		p := PageCacheParams{}.withDefaults()
		require.Equal(t, defaultRefreshInterval, p.RefreshInterval)
		require.Equal(t, defaultActivityTimeout, p.ActivityTimeout)
		// 30min / 30s = 60.
		require.Equal(t, 60, p.ContinueAsNewThreshold)
	})

	t.Run("longer interval scales threshold down to bound history", func(t *testing.T) {
		p := PageCacheParams{RefreshInterval: 90 * time.Second}.withDefaults()
		require.Equal(t, 90*time.Second, p.RefreshInterval)
		require.Equal(t, defaultActivityTimeout, p.ActivityTimeout)
		// 30min / 90s = 20.
		require.Equal(t, 20, p.ContinueAsNewThreshold)
	})

	t.Run("idempotent", func(t *testing.T) {
		once := PageCacheParams{RefreshInterval: 90 * time.Second}.withDefaults()
		require.Equal(t, once, once.withDefaults())
	})
}

// TestInterruptedEscalation pins the fix for the fast-loop regression: a
// deadline-cut fast-cadence entry must escalate at the transient threshold (WARN
// well past 3), while the slow batch escalates strictly at errorAfterFailures.
func TestInterruptedEscalation(t *testing.T) {
	notStopping := func() bool { return false }

	// n calls, chosen to sit above the strict threshold but below the transient one.
	const n = errorAfterFailures + 1 // 4: > errorAfterFailures(3), < transientErrorAfterFailures(10)

	t.Run("fast loop deadline stays WARN below transient threshold", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		for range n {
			a.interrupted("edge scoreboard (latest)", "fastkey", context.DeadlineExceeded, notStopping, errFastRefreshDeadline)
		}
		require.Zero(t, countLevel(*recs, slog.LevelError), "fast-loop blips must not page below the transient threshold")
		require.Equal(t, n, countLevel(*recs, slog.LevelWarn))
	})

	t.Run("slow batch deadline escalates to ERROR at the strict threshold", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		for range n {
			a.interrupted("geo concentration", "slowkey", nil, notStopping, errBatchDeadline)
		}
		require.NotZero(t, countLevel(*recs, slog.LevelError), "starvation should escalate at the strict threshold")
	})

	t.Run("worker shutdown is benign (not counted)", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		stopping := func() bool { return true }
		a.interrupted("geo concentration", "k", context.Canceled, stopping, errBatchDeadline)
		require.Zero(t, countLevel(*recs, slog.LevelError))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn)) // "interrupted (shutdown)"
	})
}

// TestDueThisCycle pins the per-entry slow-refresh cadence: an everyN=4 entry
// (publisher_check) refreshes on cycles 0, 4, 8 and is skipped otherwise, while
// default (everyN<=1) entries refresh every cycle.
func TestDueThisCycle(t *testing.T) {
	t.Run("everyN=4 refreshes on multiples of 4 only", func(t *testing.T) {
		for cycle := 0; cycle <= 12; cycle++ {
			want := cycle%4 == 0 // cycles 0, 4, 8, 12
			require.Equal(t, want, dueThisCycle(4, cycle), "cycle %d", cycle)
		}
	})

	t.Run("default cadence refreshes every cycle", func(t *testing.T) {
		for cycle := 0; cycle <= 12; cycle++ {
			require.True(t, dueThisCycle(0, cycle), "everyN=0, cycle %d", cycle)
			require.True(t, dueThisCycle(1, cycle), "everyN=1, cycle %d", cycle)
		}
	})

	t.Run("publisher_check carries everyN=4 on its entry; others default", func(t *testing.T) {
		// entries() does not dereference a.API at construction, so a bare Activities
		// is safe. Cadence lives on the entry, so a rename can't orphan it.
		byKey := map[string]cacheEntry{}
		for _, e := range (&Activities{}).entries() {
			byKey[e.key] = e
		}

		pub, ok := byKey["publisher_check"]
		require.True(t, ok, "publisher_check entry must exist")
		require.Equal(t, publisherCheckEveryN, pub.everyN)
		require.Equal(t, 4, pub.everyN)

		topo, ok := byKey["topology"]
		require.True(t, ok, "topology entry must exist")
		require.LessOrEqual(t, topo.everyN, 1, "topology must refresh every cycle")

		// Model the RefreshCaches gate: publisher_check is due only on cycles 0,4,8;
		// topology is due every cycle.
		for _, cycle := range []int{0, 1, 2, 3, 4, 5, 8} {
			require.Equal(t, cycle%4 == 0, dueThisCycle(pub.everyN, cycle), "publisher_check cycle %d", cycle)
			require.True(t, dueThisCycle(topo.everyN, cycle), "topology cycle %d", cycle)
		}
	})
}

// TestWriteFailureEscalation pins that the cache-write leg escalates under its
// own counter: a sustained Postgres outage whose errors classify transient
// indefinitely (connection refused) must still reach ERROR — nothing else
// alerts on write failures.
func TestWriteFailureEscalation(t *testing.T) {
	log, recs := capLogger()
	a := &Activities{Log: log}
	err := errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")

	for range transientErrorAfterFailures {
		a.recordWriteFailure("topology", "topology", err)
	}
	require.Equal(t, transientErrorAfterFailures-1, countLevel(*recs, slog.LevelWarn))
	require.Equal(t, 1, countLevel(*recs, slog.LevelError), "sustained transient write failures must page")

	// A successful write resets the counter via the query-leg-independent key.
	a.esc.Reset("topology:write")
	*recs = nil
	a.recordWriteFailure("topology", "topology", err)
	require.Zero(t, countLevel(*recs, slog.LevelError))
}
