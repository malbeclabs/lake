package worker

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/utils/pkg/logger"
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

	t.Run("threshold floors at 2 so a heavy refresh still runs", func(t *testing.T) {
		// The loop skips the heavy refresh on its final iteration, so a
		// single-iteration window would never start one.
		p := PageCacheParams{RefreshInterval: continueAsNewTargetWindow}.withDefaults()
		require.Equal(t, 2, p.ContinueAsNewThreshold)
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

	t.Run("heavy refresh deadline stays WARN below transient threshold", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		for range n {
			a.interrupted("network health deferred", "heavykey", context.DeadlineExceeded, notStopping, errHeavyRefreshDeadline)
		}
		require.Zero(t, countLevel(*recs, slog.LevelError),
			"a slow heavy scan is served-degraded (last good blob), not starvation, so it must not page on the first few cycles")
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

// TestDueForRefresh pins the age gate: cadence is a wall-clock floor on the
// spacing between two refreshes of a key, evaluated against the last time its
// blob was written.
func TestDueForRefresh(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0).UTC()
	windowEnd := now.Truncate(24 * time.Hour)
	cadenced := cacheEntry{every: 5 * time.Minute}

	t.Run("due once the blob is at least the cadence old", func(t *testing.T) {
		require.False(t, dueForRefresh(cadenced, now.Add(-4*time.Minute), now, windowEnd))
		require.True(t, dueForRefresh(cadenced, now.Add(-5*time.Minute), now, windowEnd))
		require.True(t, dueForRefresh(cadenced, now.Add(-time.Hour), now, windowEnd))
	})

	t.Run("no cadence means every cycle", func(t *testing.T) {
		require.True(t, dueForRefresh(cacheEntry{}, now, now, windowEnd))
	})

	t.Run("a key that was never written is always due", func(t *testing.T) {
		require.True(t, dueForRefresh(cacheEntry{every: 30 * time.Minute}, time.Time{}, now, windowEnd))
	})
}

// TestDayAlignedEntriesRollWithTheWindow pins the fix for the cross-group window
// skew. handlers.DefaultNetworkHealthWindow moves at midnight UTC, and the
// frontend refuses to combine two Network Health payloads whose windows disagree
// (deriveAvailability). Without this rule, groups on 5- and 30-minute cadences
// would describe different windows for up to half an hour every night and the
// page's traffic-weighted availability stat would read as a dash.
func TestDayAlignedEntriesRollWithTheWindow(t *testing.T) {
	t.Parallel()

	// One minute past midnight UTC: the window just rolled, and every Network
	// Health blob was written under the previous one.
	windowEnd := time.Unix(1_700_000_000, 0).UTC().Truncate(24 * time.Hour)
	now := windowEnd.Add(time.Minute)
	writtenYesterday := windowEnd.Add(-time.Minute)

	aligned := cacheEntry{every: networkHealthHistoryInterval, dayAligned: true}
	require.True(t, dueForRefresh(aligned, writtenYesterday, now, windowEnd),
		"a blob describing yesterday's window is due whatever the cadence says")

	// Not a licence to ignore the cadence: a blob written under the current window
	// still waits for it.
	require.False(t, dueForRefresh(aligned, windowEnd.Add(time.Second), now, windowEnd))

	// Nothing changes for an entry that does not read the day-aligned window.
	unaligned := cacheEntry{every: networkHealthHistoryInterval}
	require.False(t, dueForRefresh(unaligned, writtenYesterday, now, windowEnd))

	// Every Network Health entry must carry the marker, or its group's window
	// lags the others'.
	byKey := map[string]cacheEntry{}
	a := &Activities{}
	for _, e := range append(a.entries(), a.heavyEntries()...) {
		byKey[e.key] = e
	}
	for _, key := range networkHealthKeys {
		require.True(t, byKey[key].dayAligned,
			"network health entry %q reads DefaultNetworkHealthWindow, so it must roll with it", key)
	}
	// And nothing else should: an entry that does not read that window would just
	// refresh once more per day for no reason.
	for _, e := range append(a.entries(), a.heavyEntries()...) {
		if !slices.Contains(networkHealthKeys, e.key) {
			require.False(t, e.dayAligned, "entry %q does not read the day-aligned window", e.key)
		}
	}
}

// TestCadenceIsIndependentOfCyclePeriod is the reason the cadence is a duration
// and not a cycle count. The cycle period is configured per environment (prod
// ~68s, staging ~4 min) and shortens as entries are taken off the every-cycle
// path; a cycle count would mean a different staleness in each environment and
// would drift as entries are added. Under the age gate, a 5-minute cadence is
// five minutes at every period — and never *below* five, whatever the period.
func TestCadenceIsIndependentOfCyclePeriod(t *testing.T) {
	t.Parallel()

	const every = 5 * time.Minute
	start := time.Unix(1_700_000_000, 0)

	for _, period := range []time.Duration{minRefreshInterval, 30 * time.Second, 68 * time.Second, 4 * time.Minute} {
		// Walk the cycles of a one-hour window, refreshing whenever the gate says
		// due, and record the spacing between consecutive refreshes.
		lastWrite := start
		var spacings []time.Duration
		entry := cacheEntry{every: every}
		for now := start; now.Sub(start) <= time.Hour; now = now.Add(period) {
			if dueForRefresh(entry, lastWrite, now, time.Time{}) {
				spacings = append(spacings, now.Sub(lastWrite))
				lastWrite = now
			}
		}
		require.NotEmpty(t, spacings, "period %s: the entry must refresh at all", period)
		for _, gap := range spacings {
			require.GreaterOrEqual(t, gap, every,
				"period %s: cadence is a floor, so no gap may be shorter than it", period)
			require.Less(t, gap, every+period,
				"period %s: the gate is checked once per cycle, so a gap may only overshoot by one period", period)
		}
	}
}

// TestEntryCadences pins the per-entry cadences. Cadence lives on the entry, so a
// rename cannot orphan it — but a value can still be edited by accident, and
// these are the values the CPU win is made of.
func TestEntryCadences(t *testing.T) {
	t.Parallel()

	// entries() does not dereference a.API at construction, so a bare Activities
	// is safe.
	a := &Activities{}
	byKey := map[string]cacheEntry{}
	for _, e := range append(a.entries(), a.heavyEntries()...) {
		byKey[e.key] = e
	}

	for key, want := range map[string]time.Duration{
		"publisher_check":                      publisherCheckInterval,
		handlers.ValidatorsPageCacheKey:        validatorsListingInterval,
		"algo_divergence":                      algoDivergenceInterval,
		"latency_comparison":                   latencyComparisonInterval,
		"edge_scoreboard":                      edgeScoreboardInterval,
		"edge_scoreboard:leaders":              edgeScoreboardInterval,
		handlers.NetworkHealthOverviewCacheKey: networkHealthOverviewInterval,
	} {
		e, ok := byKey[key]
		require.True(t, ok, "entry %q must exist", key)
		require.Equal(t, want, e.every, "entry %q cadence", key)
	}

	// The purely-historical Network Health groups share one cadence.
	for _, key := range []string{
		handlers.NetworkHealthAvailabilityCacheKey,
		handlers.NetworkHealthLatencyCacheKey,
		handlers.NetworkHealthCapacityCacheKey,
		handlers.NetworkHealthOutagesCacheKey,
		handlers.NetworkHealthDrainCacheKey,
		handlers.NetworkHealthTicketsCacheKey,
		handlers.NetworkHealthImpactfulCacheKey,
		handlers.NetworkHealthDeferredCacheKey,
	} {
		e, ok := byKey[key]
		require.True(t, ok, "entry %q must exist", key)
		require.Equal(t, networkHealthHistoryInterval, e.every, "entry %q cadence", key)
	}

	// Views that must track the live network keep every-cycle refresh.
	for _, key := range []string{"topology", "status", "incidents"} {
		e, ok := byKey[key]
		require.True(t, ok, "entry %q must exist", key)
		require.Zero(t, e.every, "entry %q must refresh every cycle", key)
	}

	// The fast-cadence entries are refreshed by their own activity, which does
	// not consult the gate: a cadence set there would be silently ignored.
	for _, e := range a.latestEntries() {
		require.Zero(t, e.every, "fast-cadence entry %q must not carry a cadence", e.key)
	}
}

// TestDueEntriesFailsOpen pins that a Postgres problem cannot freeze the cache.
// The gate's only input is page_cache.updated_at; if that read fails, skipping
// every cadenced entry would hold the whole cache stale for as long as the
// failure lasts, so everything is treated as due instead.
func TestDueEntriesFailsOpen(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	// No PgPool configured → PageCacheAges returns an error.
	a := &Activities{Log: log, API: &handlers.API{}}
	entries := a.entries()

	due, skipped := a.dueEntries(context.Background(), "batch", entries)
	require.Len(t, due, len(entries), "a failed age read must refresh everything")
	require.Zero(t, skipped)
	require.Equal(t, 1, countLevel(*recs, slog.LevelWarn), "and must say so")
}

// TestDueEntriesSkipsNothingWithoutCadences pins that a batch whose entries carry
// no cadence reads no ages at all — the fast and heavy paths must not pay for a
// Postgres round trip they cannot use. A nil API would panic on a read.
func TestDueEntriesSkipsNothingWithoutCadences(t *testing.T) {
	t.Parallel()

	a := &Activities{}
	entries := a.latestEntries()
	due, skipped := a.dueEntries(context.Background(), "batch", entries)
	require.Len(t, due, len(entries))
	require.Zero(t, skipped)
}

// TestUnadvancedUpdatedAtIsDueNextCycle pins the property that keeps the
// escalation counters honest under a long cadence. refresh only reaches
// WritePageCache on success, so a failed refresh leaves updated_at where it was —
// and the gate then makes the entry due again on the very next cycle, so a dark
// entry still pages at the cycle rate rather than the cadence rate.
func TestUnadvancedUpdatedAtIsDueNextCycle(t *testing.T) {
	t.Parallel()

	const every = 30 * time.Minute
	lastGoodWrite := time.Unix(1_700_000_000, 0)
	dueAt := lastGoodWrite.Add(every)

	// The refresh at dueAt fails, so updated_at is still lastGoodWrite one cycle
	// later — and the entry is due again.
	entry := cacheEntry{every: every}
	require.True(t, dueForRefresh(entry, lastGoodWrite, dueAt, time.Time{}))
	require.True(t, dueForRefresh(entry, lastGoodWrite, dueAt.Add(defaultRefreshInterval), time.Time{}))
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

// TestValidatorsCacheBound pins that the handler's staleness gate can never reject
// a healthy validators cache entry, at any refresh interval an operator can set.
//
// The gate is a dead-worker backstop, so rejecting healthy entries has no upside and
// a large downside: every rejection sends a request back to the ~13 CPU-sec live
// query, which is the cost the cache exists to avoid. A bound derived from the
// *default* interval looks fine in CI and silently degrades once the interval is
// raised, so the invariant is asserted against the configurable maximum instead.
//
// This lives in api/worker because it is the only package that sees both sides:
// api/worker imports api/handlers, not the reverse.
func TestValidatorsCacheBound(t *testing.T) {
	t.Parallel()

	// Worst-case age of a healthy entry: the entry's cadence, plus the one cycle
	// the age gate may overshoot it by at the slowest permitted interval, plus the
	// longest a refresh activity may itself take.
	worstCadence := validatorsListingInterval + maxRefreshInterval
	worstHealthyAge := worstCadence + maxActivityTimeout

	require.Greater(t, handlers.ValidatorsCacheStaleAfter, worstHealthyAge,
		"handlers.ValidatorsCacheStaleAfter (%s) must exceed the worst-case healthy entry age "+
			"(%s cadence + %s refresh); otherwise raising PAGE_CACHE_REFRESH_INTERVAL silently "+
			"sends validators listing traffic back to the live query",
		handlers.ValidatorsCacheStaleAfter, worstCadence, maxActivityTimeout)
}

// networkHealthKeys are the nine Network Health cache entries the page loads as
// independent groups.
var networkHealthKeys = []string{
	handlers.NetworkHealthOverviewCacheKey,
	handlers.NetworkHealthAvailabilityCacheKey,
	handlers.NetworkHealthLatencyCacheKey,
	handlers.NetworkHealthCapacityCacheKey,
	handlers.NetworkHealthOutagesCacheKey,
	handlers.NetworkHealthDrainCacheKey,
	handlers.NetworkHealthTicketsCacheKey,
	handlers.NetworkHealthImpactfulCacheKey,
	handlers.NetworkHealthDeferredCacheKey,
}

// TestNetworkHealthEntriesRegistered pins that all nine Network Health groups are
// registered across the two activities, so a rename cannot silently drop the
// entry that turns a group's Error into a refreshError (which is what keeps a
// zeroed blob out of the cache).
func TestNetworkHealthEntriesRegistered(t *testing.T) {
	a := &Activities{}
	byKey := map[string]cacheEntry{}
	for _, e := range append(a.entries(), a.heavyEntries()...) {
		byKey[e.key] = e
	}
	for _, k := range networkHealthKeys {
		_, ok := byKey[k]
		require.True(t, ok, "network health entry %q must be registered", k)
	}
}

// TestHeavyEntriesRegistered pins which entries live in the heavy activity. The
// two heavy Network Health groups must be there and nowhere else: back in the
// slow batch their 180s budget equalled the batch's own StartToCloseTimeout, so
// they could never record their own failure and they stretched every other
// page's refresh cadence.
func TestHeavyEntriesRegistered(t *testing.T) {
	a := &Activities{}

	heavyKeys := map[string]bool{}
	for _, e := range a.heavyEntries() {
		heavyKeys[e.key] = true
	}
	require.Len(t, heavyKeys, 2)
	require.True(t, heavyKeys[handlers.NetworkHealthImpactfulCacheKey])
	require.True(t, heavyKeys[handlers.NetworkHealthDeferredCacheKey])

	for _, e := range a.entries() {
		require.False(t, heavyKeys[e.key], "heavy entry %q must not also run in the slow batch", e.key)
	}
}

// TestEntryTimeoutsFitTheirActivityBudget is the guard that makes the original
// defect impossible to reintroduce: an entry may never claim its activity's
// whole budget, or the activity deadline cancels it at the same instant its own
// deadline fires. The entry then records batch starvation instead of its own
// failure, its second attempt is unreachable, and the entries queued behind it
// are starved.
func TestEntryTimeoutsFitTheirActivityBudget(t *testing.T) {
	a := &Activities{}

	t.Run("slow batch entries fit the per-entry ceiling", func(t *testing.T) {
		require.Less(t, defaultRefreshTimeout, defaultActivityTimeout,
			"the per-entry ceiling must leave the batch activity headroom")
		for _, e := range a.entries() {
			require.LessOrEqual(t, e.timeout, defaultRefreshTimeout,
				"entry %q wants %s, above the batch's per-entry ceiling; it belongs in heavyEntries()", e.key, e.timeout)
		}
	})

	t.Run("heavy entries leave their activity real headroom", func(t *testing.T) {
		require.Positive(t, heavyActivityHeadroom)
		for _, e := range a.heavyEntries() {
			require.Positive(t, e.timeout, "heavy entry %q must set its own timeout", e.key)
			require.LessOrEqual(t, e.timeout+heavyActivityHeadroom, heavyActivityTimeout,
				"heavy entry %q wants %s, which leaves no headroom under the %s activity budget", e.key, e.timeout, heavyActivityTimeout)
		}
	})
}

// TestBatchConcurrencyReservesHeavyShare pins that moving the heavy scans into
// their own activity did not raise the aggregate concurrent-query count against
// ClickHouse: they run alongside the batch, so the batch gives up their slots.
// The reservation is unconditional because a heavy scan is no longer awaited
// inside the workflow loop and so can span several batches (see
// TestPageCacheWorkflowHeavyDoesNotBlockCycles).
func TestBatchConcurrencyReservesHeavyShare(t *testing.T) {
	heavy := len((&Activities{}).heavyEntries())

	t.Run("default concurrency reserves the heavy share", func(t *testing.T) {
		require.Equal(t, defaultRefreshConcurrency-heavy, (&Activities{}).batchConcurrency())
	})

	t.Run("configured concurrency reserves the heavy share", func(t *testing.T) {
		require.Equal(t, 8-heavy, (&Activities{RefreshConcurrency: 8}).batchConcurrency())
	})

	t.Run("minimum concurrency still refreshes", func(t *testing.T) {
		require.Equal(t, 1, (&Activities{RefreshConcurrency: minRefreshConcurrency}).batchConcurrency())
	})
}

// topologyEntry is a stand-in slow-batch entry for the refresh() tests.
func topologyEntry(fn func(context.Context) (any, error), timeout time.Duration) cacheEntry {
	return cacheEntry{name: "topology", key: "topology", fn: fn, timeout: timeout}
}

// TestRefreshRetriesOnlyWithBudget pins that maxAttempts is honest: the second
// attempt runs only when the activity has room for it (see retryBudget).
// Retrying inside the last seconds of the budget just converts this entry's own
// error into deadline starvation; refusing a retry the budget can afford
// converts a recoverable blip into a recorded failure.
func TestRefreshRetriesOnlyWithBudget(t *testing.T) {
	notStopping := func() bool { return false }
	failing := func(calls *int) func(context.Context) (any, error) {
		return func(context.Context) (any, error) {
			*calls++
			return nil, errors.New("boom")
		}
	}

	t.Run("retries when the parent has room for a full attempt", func(t *testing.T) {
		log, _ := capLogger()
		a := &Activities{Log: log}
		calls := 0
		parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.refresh(parent, topologyEntry(failing(&calls), 10*time.Millisecond), notStopping, errBatchDeadline)
		require.Equal(t, 2, calls)
	})

	t.Run("no deadline is unlimited budget", func(t *testing.T) {
		log, _ := capLogger()
		a := &Activities{Log: log}
		calls := 0
		a.refresh(context.Background(), topologyEntry(failing(&calls), 10*time.Millisecond), notStopping, errBatchDeadline)
		require.Equal(t, 2, calls)
	})

	t.Run("skips the retry that would not fit, and records the entry's own failure", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		calls := 0
		parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		a.refresh(parent, topologyEntry(failing(&calls), time.Second), notStopping, errBatchDeadline)
		require.Equal(t, 1, calls)
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn), "the failure is still counted, just not retried")
	})

	// The gate is on what a second attempt costs, not on the entry's whole
	// budget. A cheap failure late in a long activity has ample room, and
	// a.esc.Reset only runs on success, so denying it converts a recoverable
	// blip into a recorded failure that counts toward escalation.
	t.Run("retries a cheap failure that has room, even below a full entry budget", func(t *testing.T) {
		log, _ := capLogger()
		a := &Activities{Log: log}
		calls := 0
		remaining := retryAttemptMargin + 2*time.Second
		parent, cancel := context.WithTimeout(context.Background(), remaining)
		defer cancel()
		require.Less(t, remaining, defaultRefreshTimeout, "the point of the case: less than a full entry budget left")
		a.refresh(parent, topologyEntry(failing(&calls), defaultRefreshTimeout), notStopping, errBatchDeadline)
		require.Equal(t, 2, calls)
	})
}

// TestRetryBudget pins what a second attempt is required to fit in: the failed
// attempt's own cost plus a margin, never more than the entry's timeout.
func TestRetryBudget(t *testing.T) {
	t.Parallel()

	t.Run("cheap failure asks for little", func(t *testing.T) {
		require.Equal(t, retryAttemptMargin+50*time.Millisecond,
			retryBudget(50*time.Millisecond, defaultRefreshTimeout))
	})

	t.Run("an attempt that burned its own budget asks for the whole timeout again", func(t *testing.T) {
		require.Equal(t, defaultRefreshTimeout, retryBudget(defaultRefreshTimeout, defaultRefreshTimeout))
		require.Equal(t, nhHeavyRefreshTimeout, retryBudget(nhHeavyRefreshTimeout, nhHeavyRefreshTimeout))
	})

	t.Run("never asks for more than the entry could use", func(t *testing.T) {
		for _, cost := range []time.Duration{0, time.Second, time.Hour} {
			require.LessOrEqual(t, retryBudget(cost, defaultRefreshTimeout), defaultRefreshTimeout, "cost %s", cost)
		}
	})

	t.Run("the margin fits inside every activity's headroom", func(t *testing.T) {
		// A retry admitted by the margin alone must still be able to finish
		// inside the activity that admitted it, or it is recorded as starvation.
		require.Less(t, retryAttemptMargin, defaultActivityTimeout-defaultRefreshTimeout)
		require.LessOrEqual(t, retryAttemptMargin, heavyActivityHeadroom)
	})
}

// findRecord returns the first captured record carrying msg.
func findRecord(recs []slog.Record, msg string) (slog.Record, bool) {
	for _, r := range recs {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

// recordAttr returns a record's named attribute value.
func recordAttr(r slog.Record, key string) (any, bool) {
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

// TestRefreshRecordsOwnFailureWhenParentHasHeadroom pins what
// heavyActivityHeadroom buys. An entry whose timeout equals its activity's
// StartToCloseTimeout is cancelled by the activity at the same instant its own
// deadline fires, so refresh takes the parentCtx branch and files the failure as
// the deadline sentinel (starvation) instead of the entry's own error. With
// headroom the entry reports what actually happened to it.
func TestRefreshRecordsOwnFailureWhenParentHasHeadroom(t *testing.T) {
	notStopping := func() bool { return false }
	stalls := func(ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	log, recs := capLogger()
	a := &Activities{Log: log}
	// Entry budget well inside the parent's, mirroring nhHeavyRefreshTimeout
	// under heavyActivityTimeout.
	parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	a.refresh(parent, cacheEntry{name: "network health deferred", key: "nh_deferred", fn: stalls, timeout: 20 * time.Millisecond}, notStopping, errHeavyRefreshDeadline)

	rec, ok := findRecord(*recs, "cache refresh failed")
	require.True(t, ok, "an over-budget entry must record a failure")
	got, ok := recordAttr(rec, "error")
	require.True(t, ok)
	err, ok := got.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the entry's own deadline is the cause")
	require.NotErrorIs(t, err, errHeavyRefreshDeadline,
		"with headroom the activity deadline has not fired, so this is not starvation")
	require.Zero(t, countLevel(*recs, slog.LevelError), "a single slow cycle must not page")
}

// TestRefreshDiscardsStraddlingDayAlignedWrite pins the fix for the window-skew
// hole the dayAligned marker alone left open. updated_at is stamped at write time,
// but the payload's window is fixed when the fetch starts — so a fetch crossing
// midnight UTC produces a previous-window payload with a post-boundary updated_at,
// which the gate reads as current and holds for a full cadence. That is exactly the
// skew the marker exists to prevent, so the write is dropped instead.
func TestRefreshDiscardsStraddlingDayAlignedWrite(t *testing.T) {
	t.Run("the straddle predicate", func(t *testing.T) {
		t.Parallel()

		beforeMidnight := time.Date(2026, 8, 24, 23, 59, 50, 0, time.UTC)
		afterMidnight := time.Date(2026, 8, 25, 0, 0, 10, 0, time.UTC)

		require.True(t, windowMovedDuring(beforeMidnight, afterMidnight))
		require.False(t, windowMovedDuring(beforeMidnight, beforeMidnight.Add(time.Second)),
			"two instants inside one day must compare equal, or every refresh is discarded")
		require.False(t, windowMovedDuring(afterMidnight, afterMidnight.Add(time.Hour)))

		// Why the discard is necessary: the blob a straddling write would leave behind
		// reads as not-due, so it would hold the previous window for a full cadence.
		require.False(t, dueForRefresh(
			cacheEntry{every: networkHealthHistoryInterval, dayAligned: true},
			afterMidnight, afterMidnight.Add(time.Minute), windowEndAt(afterMidnight)))
	})

	t.Run("an unstraddled refresh still writes", func(t *testing.T) {
		t.Parallel()

		// A nil-PgPool API fails the write, which is how the attempt is observable.
		log, recs := capLogger()
		a := &Activities{Log: log, API: &handlers.API{}}
		a.refresh(context.Background(), cacheEntry{
			name: "network health outages", key: "nh_outages", dayAligned: true,
			fn: func(context.Context) (any, error) { return "payload", nil },
		}, func() bool { return false }, errBatchDeadline)

		_, ok := findRecord(*recs, "cache write failed")
		require.True(t, ok, "the discard must not fire on a refresh inside one window")
	})
}

// TestPageCacheWorkflowSchedulesHeavyRefresh pins that RefreshHeavyCaches is
// executed alongside the slow batch, and that no run is started within
// heavyStartLeadCycles of the continue-as-new boundary. The two heavy Network
// Health blobs are refreshed nowhere else, so an activity that exists but is never
// scheduled would freeze both at whatever the last in-batch refresh wrote, with no
// failing test and no log line to say so. Starting one too close to the boundary
// is the other failure: the drain below the loop would then block on an unfinished
// scan, freezing every page cache (the 3s latest-slots one included) for up to the
// heavy budget.
func TestPageCacheWorkflowSchedulesHeavyRefresh(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a)
	env.OnActivity(a.RefreshCaches, mock.Anything).Return(nil)
	env.OnActivity(a.RefreshLatestCaches, mock.Anything).Return(nil)
	env.OnActivity(a.RefreshHeavyCaches, mock.Anything).Return(nil)

	// A window longer than the trailing lead so both the "starts" and the
	// "stops before the boundary" halves are exercised.
	const cycles = 12
	env.ExecuteWorkflow(PageCacheWorkflow, 0, PageCacheParams{
		RefreshInterval:        defaultRefreshInterval,
		ActivityTimeout:        defaultActivityTimeout,
		ContinueAsNewThreshold: cycles,
	})

	require.True(t, env.IsWorkflowCompleted())
	var continueAsNew *temporalworkflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &continueAsNew,
		"the loop must run to its continue-as-new boundary, got: %v", env.GetWorkflowError())

	env.AssertActivityNumberOfCalls(t, "RefreshCaches", cycles)
	// A heavy run that finishes inside its cycle is restarted on the next one, so
	// with an instant-returning mock one starts every cycle until the trailing
	// lead. The last heavyStartLeadCycles cycles start none, so the run the drain
	// waits on has already finished.
	lead := heavyStartLeadCycles(defaultRefreshInterval)
	env.AssertActivityNumberOfCalls(t, "RefreshHeavyCaches", cycles-lead)
}

// fakeFuture stands in for a workflow.Future so the heavy-run gate is testable
// without a workflow environment.
type fakeFuture struct{ ready bool }

func (f fakeFuture) Get(temporalworkflow.Context, any) error { return nil }
func (f fakeFuture) IsReady() bool                           { return f.ready }

// TestHeavyRefreshDue pins the "never two heavy runs at once" invariant that
// replaces the old end-of-iteration await. The heavy scans hold their reserved
// ClickHouse slots (see batchConcurrency) for as long as they run, so a second
// concurrent copy would double that reservation and oversubscribe the cluster.
func TestHeavyRefreshDue(t *testing.T) {
	t.Parallel()

	require.True(t, heavyRefreshDue(nil), "the first cycle has nothing outstanding")
	require.True(t, heavyRefreshDue(fakeFuture{ready: true}), "a finished run is restarted next cycle")
	require.False(t, heavyRefreshDue(fakeFuture{ready: false}), "an outstanding run must not be started twice")
}

// TestHeavyStartDue pins the boundary rule that keeps the end-of-loop drain from
// blocking: a heavy run may only start when at least heavyStartLeadCycles cycles
// remain, so it has finished within its budget by the time the drain waits on it.
func TestHeavyStartDue(t *testing.T) {
	t.Parallel()

	// Default cadence: 240s heavy budget over a 30s cycle rounds up to 8 cycles.
	require.Equal(t, 8, heavyStartLeadCycles(defaultRefreshInterval))
	// A longer interval needs fewer cycles; the ceil keeps it from rounding to 0.
	require.Equal(t, 1, heavyStartLeadCycles(heavyActivityTimeout))
	require.Equal(t, 2, heavyStartLeadCycles(heavyActivityTimeout/2+time.Second))
	require.Equal(t, 1, heavyStartLeadCycles(0), "a nonsensical interval floors at one skipped cycle")

	const threshold = 60
	lead := heavyStartLeadCycles(defaultRefreshInterval)

	// Starts early, and the LAST cycle that starts one still leaves >= the heavy
	// budget of fast-refresh windows before the drain at the boundary.
	lastStart := threshold - lead - 1
	require.True(t, heavyStartDue(0, threshold, defaultRefreshInterval))
	require.True(t, heavyStartDue(lastStart, threshold, defaultRefreshInterval))
	require.GreaterOrEqual(t,
		time.Duration(threshold-1-lastStart)*defaultRefreshInterval, heavyActivityTimeout,
		"the last heavy run started must have its full budget before the drain")

	// Stops within the lead of the boundary.
	require.False(t, heavyStartDue(lastStart+1, threshold, defaultRefreshInterval))
	require.False(t, heavyStartDue(threshold-1, threshold, defaultRefreshInterval))

	// A window shorter than the lead still starts one run (iteration 0) and skips
	// the final iteration, degrading to the old single-cycle-skip behavior.
	require.True(t, heavyStartDue(0, 2, defaultRefreshInterval))
	require.False(t, heavyStartDue(1, 2, defaultRefreshInterval))
}

// TestPageCacheWorkflowHeavyDoesNotBlockCycles pins that a slow heavy scan no
// longer stretches every other page cache's refresh interval: the batch keeps
// cycling while one heavy run is outstanding, no second copy is started, and the
// outstanding run is drained before continue-as-new rather than being cancelled
// mid-scan at the boundary.
func TestPageCacheWorkflowHeavyDoesNotBlockCycles(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(20 * time.Second)

	a := &Activities{}
	env.RegisterActivity(a)

	// The heavy run stays outstanding until the last batch cycle releases it.
	release := make(chan struct{})
	var heavyFinished atomic.Bool
	env.OnActivity(a.RefreshHeavyCaches, mock.Anything).Return(func(ctx context.Context) error {
		select {
		case <-release:
			heavyFinished.Store(true)
		case <-ctx.Done():
		}
		return nil
	})
	env.OnActivity(a.RefreshLatestCaches, mock.Anything).Return(nil)

	const cycles = 3
	var batches atomic.Int32
	env.OnActivity(a.RefreshCaches, mock.Anything).Return(func(ctx context.Context) error {
		if batches.Add(1) == cycles {
			// Release after the loop has reached its drain, so a workflow that
			// skipped the drain would finish with the scan still in flight.
			go func() {
				time.Sleep(50 * time.Millisecond)
				close(release)
			}()
		}
		return nil
	})

	env.ExecuteWorkflow(PageCacheWorkflow, 0, PageCacheParams{
		// Short enough that the cycles complete quickly; the test env waits on the
		// wall clock for timers while an activity is outstanding.
		RefreshInterval:        10 * time.Millisecond,
		ActivityTimeout:        defaultActivityTimeout,
		ContinueAsNewThreshold: cycles,
	})

	require.True(t, env.IsWorkflowCompleted())
	var continueAsNew *temporalworkflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &continueAsNew,
		"the loop must run to its continue-as-new boundary, got: %v", env.GetWorkflowError())

	// The batch must keep cycling while a heavy scan is outstanding, and no second
	// copy of that scan may start.
	env.AssertActivityNumberOfCalls(t, "RefreshCaches", cycles)
	env.AssertActivityNumberOfCalls(t, "RefreshHeavyCaches", 1)
	require.True(t, heavyFinished.Load(),
		"the outstanding heavy run must be drained before continue-as-new, not cancelled at the boundary")
}

// TestNetworkHealthDegradedEscalation pins the degraded-panel counter: a group
// that refreshes but cannot compute one panel still writes its blob, so it warns
// rather than paging, and only a sustained streak escalates. A clean cycle resets
// the streak so the next hole starts fresh.
func TestNetworkHealthDegradedEscalation(t *testing.T) {
	log, recs := capLogger()
	a := &Activities{Log: log, degradedEsc: logger.Escalator{
		ErrorAfter:          nhDegradedErrorAfter,
		TransientErrorAfter: nhDegradedErrorAfter,
	}}

	for range nhDegradedErrorAfter {
		a.nhDegraded("network health outages", "outages", []string{"outage_summary"})
	}
	require.Equal(t, nhDegradedErrorAfter-1, countLevel(*recs, slog.LevelWarn),
		"a brief hole must not page")
	require.Equal(t, 1, countLevel(*recs, slog.LevelError),
		"a panel that stays dark must eventually page")

	// An empty panel list is a clean cycle: it resets the streak.
	a.nhDegraded("network health outages", "outages", nil)
	*recs = nil
	a.nhDegraded("network health outages", "outages", []string{"outage_summary"})
	require.Zero(t, countLevel(*recs, slog.LevelError))
	require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
}

// TestNetworkHealthOutcome pins that one failure produces at most one
// alert-bearing line: a critical panel becomes a refreshError (the entry's own
// escalator owns that alert, and the blob is withheld), and the degraded counter
// only advances on a refresh that actually wrote.
func TestNetworkHealthOutcome(t *testing.T) {
	t.Run("critical panel returns a refreshError and does not count as degraded", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		err := a.nhOutcome("network health outages", "outages", "unavailable", []string{"reliability"})
		require.Error(t, err)
		var re *refreshError
		require.ErrorAs(t, err, &re)
		require.Empty(t, *recs, "the entry's escalator owns this alert, not the degraded counter")
	})

	t.Run("degraded panel writes and warns", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		require.NoError(t, a.nhOutcome("network health outages", "outages", "", []string{"error_hotspots"}))
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})

	t.Run("a hard failure clears the degraded run clock", func(t *testing.T) {
		// The degraded clock would otherwise keep running through an outage the
		// entry's own escalator is already paging for, so the first partially
		// recovered refresh would escalate on a duration belonging to the hard
		// failure — a fresh page as the group improves.
		log, recs := capLogger()
		now := time.Unix(1_700_000_000, 0)
		a := newActivities(log, nil, defaultRefreshConcurrency)
		a.degradedEsc.Now = func() time.Time { return now }

		require.NoError(t, a.nhOutcome("network health outages", "outages", "", []string{"error_hotspots"}),
			"a degraded refresh starts the run clock")

		now = now.Add(3 * time.Hour)
		require.Error(t, a.nhOutcome("network health outages", "outages", "unavailable", nil))

		*recs = nil
		require.NoError(t, a.nhOutcome("network health outages", "outages", "", []string{"error_hotspots"}))
		require.Zero(t, countLevel(*recs, slog.LevelError),
			"recovery to degraded-only must not page on the outage's duration")
		require.Equal(t, 1, countLevel(*recs, slog.LevelWarn))
	})

	t.Run("healthy refresh is silent", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		require.NoError(t, a.nhOutcome("network health outages", "outages", "", nil))
		require.Empty(t, *recs)
	})
}

// TestNetworkHealthDegradedThresholdAboveDefault pins that a degraded panel
// escalates far later than a failed refresh. On the zero-value Escalator it would
// page after 3 cycles (~90s), which is not the intent: the blob is still served,
// so only a sustained dark panel is worth an alert.
func TestNetworkHealthDegradedThresholdAboveDefault(t *testing.T) {
	require.Greater(t, nhDegradedErrorAfter, errorAfterFailures)
	require.Greater(t, nhDegradedErrorAfter, transientErrorAfterFailures)
}

// TestPageCacheStartOptionsRestartAtomically pins both policy fields: dropping
// either one lets a start adopt the previous deploy's run.
func TestPageCacheStartOptionsRestartAtomically(t *testing.T) {
	opts := pageCacheStartOptions()

	require.Equal(t, WorkflowID, opts.ID)
	require.Equal(t, TaskQueue, opts.TaskQueue)
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, opts.WorkflowIDConflictPolicy,
		"a running execution must be terminated by the start call, not adopted")
	require.True(t, opts.WorkflowExecutionErrorWhenAlreadyStarted,
		"without this the SDK turns an already-started error into a handle on the running execution")
	require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, opts.WorkflowIDReusePolicy,
		"the default ALLOW_DUPLICATE is what we want once the prior run has completed")
}

// TestStartPageCacheWorkflowStartsAndLogsItsRun simulates a redeploy: each start
// issues its own request and logs that start's run ID. Run IDs are allocated by the
// server, which a mock cannot model, so the fresh-run guarantee itself is pinned by
// the options test above.
func TestStartPageCacheWorkflowStartsAndLogsItsRun(t *testing.T) {
	tc := &mocks.Client{}
	var gotOpts []temporalclient.StartWorkflowOptions
	for _, runID := range []string{"run-1", "run-2"} {
		run := &mocks.WorkflowRun{}
		run.On("GetRunID").Return(runID)
		tc.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				gotOpts = append(gotOpts, args.Get(1).(temporalclient.StartWorkflowOptions))
			}).
			Return(run, nil).Once()
	}

	params := PageCacheParams{}.withDefaults()
	for range 2 {
		log, recs := capLogger()
		run, err := startPageCacheWorkflow(context.Background(), tc, log, params)
		require.NoError(t, err)

		rec, ok := findRecord(*recs, "page-cache: workflow started")
		require.True(t, ok, "a start must be attributable to a run")
		loggedRunID, ok := recordAttr(rec, "run_id")
		require.True(t, ok, "run_id is what makes the start line verifiable")
		require.Equal(t, run.GetRunID(), loggedRunID)
	}

	require.Len(t, gotOpts, 2, "a redeploy must issue its own start request")
	for _, o := range gotOpts {
		require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, o.WorkflowIDConflictPolicy)
	}
}

// TestStartPageCacheWorkflowWrapsStartFailure keeps the "page-cache:" prefix on the
// start error: Start returns it verbatim, and its only reader logs it as-is.
func TestStartPageCacheWorkflowWrapsStartFailure(t *testing.T) {
	tc := &mocks.Client{}
	tc.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("already started"))

	log, recs := capLogger()
	run, err := startPageCacheWorkflow(context.Background(), tc, log, PageCacheParams{}.withDefaults())
	require.Nil(t, run)
	require.ErrorContains(t, err, "page-cache: failed to start workflow")
	require.ErrorContains(t, err, "already started")
	require.Empty(t, *recs, "a failed start must not log that the workflow started")
}

// TestNewActivitiesPinsDegradedEscalation pins the escalation policy the worker
// actually constructs. The count alone stopped describing a duration once the
// Network Health groups took a cadence: a degraded refresh writes its blob, so the
// entry then sleeps for a full cadence and 20 due refreshes would be 10 hours at
// networkHealthHistoryInterval. The window is what bounds it instead.
func TestNewActivitiesPinsDegradedEscalation(t *testing.T) {
	t.Parallel()

	log, _ := capLogger()
	a := newActivities(log, nil, defaultRefreshConcurrency)

	require.Equal(t, nhDegradedErrorAfter, a.degradedEsc.ErrorAfter)
	require.Equal(t, nhDegradedErrorAfter, a.degradedEsc.TransientErrorAfter)
	require.Equal(t, nhDegradedErrorWindow, a.degradedEsc.ErrorAfterDuration)
	require.Less(t, nhDegradedErrorWindow, networkHealthHistoryInterval,
		"the window must fire before a single cadence elapses, or it buys nothing")
	require.Equal(t, defaultRefreshConcurrency, a.RefreshConcurrency)
}

// TestNetworkHealthDegradedWindowPages is the end-to-end version: a group on the
// slowest cadence that keeps reporting a dark panel must page on its second
// degraded refresh — the first one at or past nhDegradedErrorWindow into the run —
// not after nhDegradedErrorAfter of them.
func TestNetworkHealthDegradedWindowPages(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	now := time.Unix(1_700_000_000, 0)
	a := newActivities(log, nil, defaultRefreshConcurrency)
	a.degradedEsc.Now = func() time.Time { return now }

	// One degraded refresh per cadence, as a slowed entry actually behaves.
	for range 3 {
		a.nhDegraded("network health outages", "outages", []string{"error_hotspots"})
		now = now.Add(networkHealthHistoryInterval)
	}
	require.Equal(t, 1, countLevel(*recs, slog.LevelWarn), "the first refresh of a run must not page")
	require.Equal(t, 2, countLevel(*recs, slog.LevelError),
		"a panel dark for longer than the window must page well before %d refreshes (%s)",
		nhDegradedErrorAfter, time.Duration(nhDegradedErrorAfter)*networkHealthHistoryInterval)
}

// TestDueEntriesEscalatesSustainedAgeReadFailure pins that the gate's fail-open
// is not silent. A persistent age-read failure — a revoked grant on page_cache, a
// pool that never yields a connection — reverts every cadenced entry to
// every-cycle refresh, which is exactly the ClickHouse cost the cadence removes.
// Nothing else reports it, so a sustained failure has to page.
func TestDueEntriesEscalatesSustainedAgeReadFailure(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	a := &Activities{Log: log, API: &handlers.API{}} // no PgPool → every read errors
	entries := a.entries()

	for range errorAfterFailures {
		due, _ := a.dueEntries(context.Background(), "batch", entries)
		require.Len(t, due, len(entries))
	}
	require.Equal(t, errorAfterFailures-1, countLevel(*recs, slog.LevelWarn), "a blip stays WARN")
	require.Equal(t, 1, countLevel(*recs, slog.LevelError), "a sustained gate outage must page")

	// The heavy activity runs its own gate read — and now returns in milliseconds
	// whenever neither entry is due, so it reads nearly every cycle. Its streak is
	// counted separately, so it has to reach ERROR on its own.
	*recs = nil
	heavy := a.heavyEntries()
	for range errorAfterFailures {
		due, _ := a.dueEntries(context.Background(), "heavy", heavy)
		require.Len(t, due, len(heavy))
	}
	require.Equal(t, 1, countLevel(*recs, slog.LevelError), "the heavy caller must page on its own streak")
}

// TestDueEntriesKeysEscalationPerCaller pins that one caller's success cannot reset
// the other's failure streak. Both gate reads share an Activities and its escalator,
// so a shared key would let a healthy batch read mask a persistently failing heavy
// one — every cadence silently reverting to every-cycle ClickHouse load at WARN.
func TestDueEntriesKeysEscalationPerCaller(t *testing.T) {
	t.Parallel()

	log, recs := capLogger()
	a := &Activities{Log: log}

	// Reset stands in for a successful read by the other caller.
	for range errorAfterFailures - 1 {
		a.esc.Fail(log, cacheAgesEscalationKey+":heavy", "cache cadence age read failed")
		a.esc.Reset(cacheAgesEscalationKey + ":batch")
	}
	require.Zero(t, countLevel(*recs, slog.LevelError))
	a.esc.Fail(log, cacheAgesEscalationKey+":heavy", "cache cadence age read failed")
	require.Equal(t, 1, countLevel(*recs, slog.LevelError),
		"a batch success must not reset the heavy streak")
}
