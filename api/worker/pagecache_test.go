package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

		vals, ok := byKey[handlers.ValidatorsPageCacheKey]
		require.True(t, ok, "validators entry must exist")
		require.Equal(t, validatorsListingEveryN, vals.everyN)
		require.Equal(t, 2, vals.everyN)
		// Due on cycles 0, 2, 4, ... (~60s at the default 30s interval).
		for _, cycle := range []int{0, 1, 2, 3, 4, 5} {
			require.Equal(t, cycle%2 == 0, dueThisCycle(vals.everyN, cycle), "validators cycle %d", cycle)
		}

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

	// Worst-case age of a healthy entry: a full refresh cadence at the slowest
	// permitted interval, plus the longest a refresh activity may itself take.
	worstCadence := time.Duration(validatorsListingEveryN) * maxRefreshInterval
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
		a.refresh(parent, "topology", "topology", failing(&calls), 10*time.Millisecond, notStopping, errBatchDeadline)
		require.Equal(t, 2, calls)
	})

	t.Run("no deadline is unlimited budget", func(t *testing.T) {
		log, _ := capLogger()
		a := &Activities{Log: log}
		calls := 0
		a.refresh(context.Background(), "topology", "topology", failing(&calls), 10*time.Millisecond, notStopping, errBatchDeadline)
		require.Equal(t, 2, calls)
	})

	t.Run("skips the retry that would not fit, and records the entry's own failure", func(t *testing.T) {
		log, recs := capLogger()
		a := &Activities{Log: log}
		calls := 0
		parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		a.refresh(parent, "topology", "topology", failing(&calls), time.Second, notStopping, errBatchDeadline)
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
		a.refresh(parent, "topology", "topology", failing(&calls), defaultRefreshTimeout, notStopping, errBatchDeadline)
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

	a.refresh(parent, "network health deferred", "nh_deferred", stalls, 20*time.Millisecond, notStopping, errHeavyRefreshDeadline)

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

// TestPageCacheWorkflowSchedulesHeavyRefresh pins that RefreshHeavyCaches is
// executed, once per cycle, alongside the slow batch, and that the last cycle
// before continue-as-new starts none. The two heavy Network Health blobs are
// refreshed nowhere else, so an activity that exists but is never scheduled would
// freeze both at whatever the last in-batch refresh wrote, with no failing test
// and no log line to say so. Starting one on the final cycle is the other
// failure: the drain below the loop would then block on a scan started moments
// earlier, freezing every page cache for up to the heavy budget.
func TestPageCacheWorkflowSchedulesHeavyRefresh(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a)
	env.OnActivity(a.RefreshCaches, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.RefreshLatestCaches, mock.Anything).Return(nil)
	env.OnActivity(a.RefreshHeavyCaches, mock.Anything).Return(nil)

	const cycles = 3
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
	// the two heavy blobs keep the batch's cadence whenever the scans are quick.
	// Every cycle but the last starts one; the last leaves the drain nothing fresh
	// to wait on.
	env.AssertActivityNumberOfCalls(t, "RefreshHeavyCaches", cycles-1)
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
	env.OnActivity(a.RefreshCaches, mock.Anything, mock.Anything).Return(func(ctx context.Context, cycle int) error {
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
