package logger

import (
	"slices"
	"sync"
	"time"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

const (
	// DefaultErrorAfter is the default number of consecutive failures of the
	// same key after which Escalator.Fail logs at ERROR instead of WARN.
	DefaultErrorAfter = 3

	// DefaultTransientErrorAfter is the default ERROR-escalation threshold for
	// transient causes (upstream connection blips, timeouts, rate limits; see
	// dberror.IsTransient). It's higher than DefaultErrorAfter because a brief
	// transient failure is self-healing and not actionable — only a sustained
	// run is worth paging on.
	DefaultTransientErrorAfter = 10
)

// Escalator tracks consecutive failures per key and logs WARN below an
// escalation threshold, ERROR at/above it, so a periodic or background task
// pages on-call only when its failure is sustained rather than on the first
// blip. The zero value is ready to use with the default thresholds.
//
// Safe for concurrent use. The zero value reads no clock, so it stays safe
// inside Temporal workflow code: it then holds only a mutex-guarded plain map
// (no goroutines, atomics, or time), and counts are recomputed deterministically
// on replay. Setting ErrorAfterDuration opts into a wall-clock read and gives
// that up — see its own doc.
//
// Set ErrorAfter/TransientErrorAfter/ErrorAfterDuration/Now before the first Fail
// call and don't change them afterwards — they are read without the mutex. Keys
// must be low-cardinality (activity names, cache keys), never derived from
// request/user data: the maps only shrink via Reset.
type Escalator struct {
	// ErrorAfter is the consecutive-failure count at which Fail logs ERROR.
	// Zero means DefaultErrorAfter.
	ErrorAfter int
	// TransientErrorAfter is the threshold used instead of ErrorAfter when
	// the failure's error is transient (dberror.IsTransient). Zero means
	// DefaultTransientErrorAfter.
	TransientErrorAfter int
	// ErrorAfterDuration escalates to ERROR once the current unbroken run of
	// failures for a key has lasted at least this long, whichever threshold is
	// crossed first. Zero disables it. Use it when the interval between Fail
	// calls is not fixed, so a count alone does not describe how long a failure
	// has actually been going (e.g. a page-cache entry whose refresh cadence is a
	// wall-clock duration): it can only make escalation earlier, never later.
	//
	// It reads the wall clock, so set it only from activity or loop code — never
	// from Temporal workflow code, where a clock read breaks replay determinism.
	ErrorAfterDuration time.Duration
	// Now overrides the clock, for tests. Nil means time.Now. Read only when
	// ErrorAfterDuration > 0.
	Now func() time.Time

	mu sync.Mutex
	// failures is the length of the current unbroken failure run per key;
	// runStart is when that run began (tracked only under ErrorAfterDuration).
	failures map[string]int
	runStart map[string]time.Time
}

func (e *Escalator) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// Fail records a consecutive failure for key and logs msg — with args plus a
// trailing "consecutive_failures" attribute — at WARN below the escalation
// threshold, ERROR at/above it. The error in args (the value of the first
// "error" key) selects the threshold: transient causes use the higher
// TransientErrorAfter since they self-heal.
//
// The threshold is chosen by the latest failure's class, not the streak's:
// under mixed-cause flapping (genuine failures interleaved with transient
// blips) escalation can defer up to TransientErrorAfter. That's intentional —
// the streak length itself proves the failure is sustained, and re-deriving
// a per-streak class would make the level depend on failure order.
//
// Under ErrorAfterDuration the line also carries "failing_for", how long the
// current run has lasted. It is zero on a run's first failure, so the window can
// never escalate a single blip.
func (e *Escalator) Fail(log Logger, key, msg string, args ...any) {
	var failingFor time.Duration

	e.mu.Lock()
	if e.failures == nil {
		e.failures = make(map[string]int)
	}
	e.failures[key]++
	n := e.failures[key]
	if e.ErrorAfterDuration > 0 {
		now := e.now()
		if e.runStart == nil {
			e.runStart = make(map[string]time.Time)
		}
		start, ok := e.runStart[key]
		if !ok {
			start = now
			e.runStart[key] = now
		}
		failingFor = now.Sub(start)
	}
	e.mu.Unlock()

	threshold := e.ErrorAfter
	if threshold <= 0 {
		threshold = DefaultErrorAfter
	}
	if err := ErrorFromArgs(args); err != nil && dberror.IsTransient(err) {
		threshold = e.TransientErrorAfter
		if threshold <= 0 {
			threshold = DefaultTransientErrorAfter
		}
	}

	// Clip so the append can't clobber a caller-owned backing array.
	args = append(slices.Clip(args), "consecutive_failures", n)
	escalate := n >= threshold
	if e.ErrorAfterDuration > 0 {
		args = append(args, "failing_for", failingFor.Round(time.Second))
		escalate = escalate || failingFor >= e.ErrorAfterDuration
	}
	if escalate {
		log.Error(msg, args...)
	} else {
		log.Warn(msg, args...)
	}
}

// Reset clears the consecutive-failure count for key. Call it after a success
// so the next failure starts a fresh run.
func (e *Escalator) Reset(key string) {
	e.mu.Lock()
	delete(e.failures, key)
	delete(e.runStart, key)
	e.mu.Unlock()
}

// Observe folds the Fail/Reset pair into one call for the common
// run-loop shape: a nil err resets key's count, a non-nil err records a
// failure and logs msg with an "error" attribute (plus any extra args).
func (e *Escalator) Observe(log Logger, key, msg string, err error, args ...any) {
	if err == nil {
		e.Reset(key)
		return
	}
	e.Fail(log, key, msg, append([]any{"error", err}, args...)...)
}
