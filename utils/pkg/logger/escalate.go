package logger

import (
	"slices"
	"sync"

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
// Safe for concurrent use. Also safe inside Temporal workflow code: it holds
// only a mutex-guarded plain map (no goroutines, atomics, or time), so counts
// are recomputed deterministically on replay.
type Escalator struct {
	// ErrorAfter is the consecutive-failure count at which Fail logs ERROR.
	// Zero means DefaultErrorAfter.
	ErrorAfter int
	// TransientErrorAfter is the threshold used instead of ErrorAfter when
	// the failure's error is transient (dberror.IsTransient). Zero means
	// DefaultTransientErrorAfter.
	TransientErrorAfter int

	mu       sync.Mutex
	failures map[string]int
}

// Fail records a consecutive failure for key and logs msg — with args plus a
// trailing "consecutive_failures" attribute — at WARN below the escalation
// threshold, ERROR at/above it. The error in args (the value of the first
// "error" key) selects the threshold: transient causes use the higher
// TransientErrorAfter since they self-heal.
func (e *Escalator) Fail(log Logger, key, msg string, args ...any) {
	e.mu.Lock()
	if e.failures == nil {
		e.failures = make(map[string]int)
	}
	e.failures[key]++
	n := e.failures[key]
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
	if n >= threshold {
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
	e.mu.Unlock()
}
