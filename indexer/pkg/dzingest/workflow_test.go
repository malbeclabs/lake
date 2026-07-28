package dzingest

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// levelCapture records the messages logged at each level by the workflow's
// Temporal logger. Guarded by a mutex: the test env drives the workflow on its
// own goroutine.
type levelCapture struct {
	mu     sync.Mutex
	warns  []string
	errors []string
}

func (l *levelCapture) Debug(string, ...any) {}
func (l *levelCapture) Info(string, ...any)  {}
func (l *levelCapture) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *levelCapture) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, msg)
}

// has reports whether msg was logged at the given level ("warn" or "error").
// It selects the slice under the lock so no guarded field is read unlocked.
func (l *levelCapture) has(level, msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	list := l.warns
	if level == "error" {
		list = l.errors
	}
	for _, m := range list {
		if m == msg {
			return true
		}
	}
	return false
}

// TestDZIngestWorkflow_ActivityFailuresNeverLogError guards the #730 regression:
// under sustained activity failure the workflow-level failure lines must log at
// WARN and never at ERROR. The activity layer (activities.refresh) owns the
// escalation-gated paging decision; a second alert-bearing line here doubled
// every page (a #697 regression via #711's MaximumAttempts: 1).
//
// It mocks every activity to return an error to Temporal (simulating the
// StartToClose timeout / future-failure path) and runs several consecutive
// iterations — well past the old escalation threshold of 3 — then asserts every
// workflow failure message appears at WARN and none at ERROR.
func TestDZIngestWorkflow_ActivityFailuresNeverLogError(t *testing.T) {
	// Workflow failure messages emitted by DZIngestWorkflow. Every one must be
	// WARN-only after #730.
	failureMessages := []string{
		"serviceability refresh failed",
		"telemetry latency refresh failed",
		"geolocation refresh failed",
		"shreds refresh failed",
		"escrow events refresh failed",
		"isis sync failed",
		"mroute sync failed",
		"msdp sync failed",
		"graph sync failed",
		"telemetry usage refresh failed",
		"permission events refresh failed",
	}

	logCap := &levelCapture{}
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(logCap)
	env := suite.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a)

	boom := errors.New("activity StartToClose timeout")
	for _, fn := range []any{
		a.RefreshServiceability,
		a.RefreshTelemetryLatency,
		a.RefreshGeolocation,
		a.RefreshShreds,
		a.RefreshShredEscrowEvents,
		a.SyncISIS,
		a.SyncIPMroute,
		a.SyncMSDP,
		a.SyncGraph,
		a.RefreshTelemetryUsage,
		a.RefreshPermissionEvents,
	} {
		env.OnActivity(fn, mock.Anything).Return(boom)
	}

	// Start a few iterations short of continue-as-new so the run exercises >3
	// consecutive failures (past the old escalation threshold of 3) for every
	// per-iteration activity. Permission events fires once in this range
	// (iteration%permissionEventsEveryN == 0 at iteration 55); the rest fire
	// every iteration.
	env.ExecuteWorkflow(DZIngestWorkflow, continueAsNewThreshold-5)

	require.True(t, env.IsWorkflowCompleted())

	// Assert only on the workflow's own failure messages. The test env also logs
	// Temporal's per-attempt "Activity error." lines because the mocks return
	// errors to Temporal directly — a test artifact (in production activities.refresh
	// returns nil, so those lines never occur).
	for _, msg := range failureMessages {
		require.False(t, logCap.has("error", msg),
			"workflow failure %q must never log at ERROR (owns paging: activities.refresh)", msg)
		require.True(t, logCap.has("warn", msg),
			"workflow failure %q should log at WARN", msg)
	}
}
