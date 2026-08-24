package logger

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

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

func TestEscalator_ThresholdFollowsLatestFailureClass(t *testing.T) {
	t.Parallel()

	// Pins the documented behavior: the threshold is chosen by the latest
	// failure's class. Two genuine failures followed by transient blips stay
	// WARN until the transient threshold, even though the strict threshold
	// was crossed mid-streak by count.
	log, recs := capLogger()
	var esc Escalator
	genuine := errors.New("boom")
	transient := errors.New("read: connection reset by peer")

	esc.Fail(log, "k", "refresh failed", "error", genuine)
	esc.Fail(log, "k", "refresh failed", "error", genuine)
	for n := 3; n < DefaultTransientErrorAfter; n++ {
		esc.Fail(log, "k", "refresh failed", "error", transient)
	}
	require.Zero(t, countLevel(*recs, slog.LevelError))

	esc.Fail(log, "k", "refresh failed", "error", transient)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

func TestEscalator_Observe(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	var esc Escalator
	err := errors.New("boom")

	for range DefaultErrorAfter - 1 {
		esc.Observe(log, "k", "refresh failed", err)
	}
	require.Equal(t, DefaultErrorAfter-1, countLevel(*recs, slog.LevelWarn))

	esc.Observe(log, "k", "refresh failed", nil) // success resets
	require.Len(t, *recs, DefaultErrorAfter-1, "nil error must not log")

	for range DefaultErrorAfter - 1 {
		esc.Observe(log, "k", "refresh failed", err)
	}
	require.Zero(t, countLevel(*recs, slog.LevelError), "reset must have cleared the count")

	esc.Observe(log, "k", "refresh failed", err, "extra", "attr")
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

func TestEscalator_ConcurrentFailReset(t *testing.T) {
	t.Parallel()

	// Hammer Fail/Reset from many goroutines over overlapping keys so `make
	// test`'s race detector exercises the mutex, then verify counts are exact
	// once the concurrency stops. The hammer phase discards output — the
	// capturing test handler is itself not concurrency-safe.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var esc Escalator
	keys := []string{"a", "b", "c"}
	err := errors.New("boom")

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 200 {
				key := keys[(i+j)%len(keys)]
				esc.Fail(log, key, "refresh failed", "error", err)
				if j%3 == 0 {
					esc.Reset(key)
				}
			}
		}(i)
	}
	wg.Wait()

	for _, key := range keys {
		esc.Reset(key)
	}
	// After the hammer, counting is exact again: below threshold stays WARN,
	// at threshold escalates.
	log2, recs := capLogger()
	for range DefaultErrorAfter {
		esc.Fail(log2, "a", "refresh failed", "error", err)
	}
	require.Equal(t, DefaultErrorAfter-1, countLevel(*recs, slog.LevelWarn))
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

// fakeClock is a manually advanced clock for the ErrorAfterDuration tests, so
// they pin the window without sleeping.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func TestEscalator_ErrorAfterDurationEscalatesOnTime(t *testing.T) {
	t.Parallel()

	// The count is far out of reach, so only the window can escalate. This is
	// the page-cache degraded-panel case: a refresh that writes its blob sleeps
	// for a full cadence afterwards, so the count alone no longer describes how
	// long the panel has actually been dark.
	log, recs := capLogger()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	esc := Escalator{ErrorAfter: 1000, TransientErrorAfter: 1000, ErrorAfterDuration: 10 * time.Minute, Now: clock.Now}
	err := errors.New("boom")

	esc.Fail(log, "k", "degraded", "error", err)
	require.Zero(t, countLevel(*recs, slog.LevelError), "a run's first failure has lasted zero time")

	clock.advance(9 * time.Minute)
	esc.Fail(log, "k", "degraded", "error", err)
	require.Zero(t, countLevel(*recs, slog.LevelError), "inside the window must stay WARN")

	clock.advance(time.Minute)
	esc.Fail(log, "k", "degraded", "error", err)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError), "a run that has lasted the window must page")

	// The line reports how long the run has been failing.
	rec := (*recs)[len(*recs)-1]
	var got any
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "failing_for" {
			got = a.Value.Any()
		}
		return true
	})
	require.Equal(t, 10*time.Minute, got)
}

func TestEscalator_CountStillWinsWhenItFiresFirst(t *testing.T) {
	t.Parallel()

	// ErrorAfterDuration can only make escalation earlier, never later: a key
	// failing every cycle still pages on the count long before the window.
	log, recs := capLogger()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	esc := Escalator{ErrorAfterDuration: time.Hour, Now: clock.Now}
	err := errors.New("boom")

	for range DefaultErrorAfter {
		clock.advance(time.Second)
		esc.Fail(log, "k", "refresh failed", "error", err)
	}
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

func TestEscalator_ResetClearsRunStart(t *testing.T) {
	t.Parallel()

	// A success in the middle of a long outage must restart the clock, or the
	// next single blip would page immediately.
	log, recs := capLogger()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	esc := Escalator{ErrorAfter: 1000, TransientErrorAfter: 1000, ErrorAfterDuration: 10 * time.Minute, Now: clock.Now}
	err := errors.New("boom")

	esc.Fail(log, "k", "degraded", "error", err)
	clock.advance(time.Hour)
	esc.Reset("k")
	esc.Fail(log, "k", "degraded", "error", err)
	require.Zero(t, countLevel(*recs, slog.LevelError), "reset must restart the run clock")

	clock.advance(10 * time.Minute)
	esc.Fail(log, "k", "degraded", "error", err)
	require.Equal(t, 1, countLevel(*recs, slog.LevelError))
}

func TestEscalator_ZeroValueReadsNoClock(t *testing.T) {
	t.Parallel()

	// The zero value must stay usable from Temporal workflow code, where a
	// clock read breaks replay determinism. Now would panic if it were read.
	log, recs := capLogger()
	esc := Escalator{Now: func() time.Time { panic("clock read without ErrorAfterDuration") }}

	esc.Fail(log, "k", "refresh failed", "error", errors.New("boom"))
	require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	(*recs)[0].Attrs(func(a slog.Attr) bool {
		require.NotEqual(t, "failing_for", a.Key, "no window configured, no duration attribute")
		return true
	})
}
