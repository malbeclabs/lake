package rollup

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// backfillChunkStarts returns the WindowStart of every chunk a backfill over
// [start, start+span) with the given chunk size will produce.
func backfillChunkStarts(start time.Time, span, chunkSize time.Duration) []time.Time {
	var out []time.Time
	for t := start; t.Before(start.Add(span)); t = t.Add(chunkSize) {
		out = append(out, t)
	}
	return out
}

// TestBackfillRollupWorkflow_FailedChunkMustNotBeSilentlySkipped is the
// permanent-gap regression. The chunk loop advanced chunkStart unconditionally
// after runIteration swallowed the activity error, so a transient failure on one
// chunk left a hole in the backfilled rollups that nothing ever recomputes — and
// the workflow returned nil, reporting a complete backfill.
//
// A backfill is a one-shot operation with no later pass to catch a missed window,
// so an incomplete run must fail loudly enough that an operator knows to re-run
// the affected ranges. (Re-running is safe: both rollup tables are
// ReplacingMergeTree keyed on the bucket, so recomputing a window is idempotent.)
//
// The live ComputeRollupWorkflow is deliberately unaffected — it must keep
// looping on failure, and its overlap window gives each bucket several chances.
func TestBackfillRollupWorkflow_FailedChunkMustNotBeSilentlySkipped(t *testing.T) {
	const chunkSize = time.Hour
	// Well in the past so the live-overlap cap never trims the range.
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	span := 3 * chunkSize

	chunkStarts := backfillChunkStarts(start, span, chunkSize)
	require.Len(t, chunkStarts, 3, "test needs three chunks to prove the middle one is skipped")
	failWindow := chunkStarts[1]

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a)

	// The middle chunk fails; the ones on either side succeed.
	isFailWindow := mock.MatchedBy(func(in BackfillChunkInput) bool {
		return in.WindowStart.Equal(failWindow)
	})
	env.OnActivity(a.RollupLinks, mock.Anything, isFailWindow).
		Return(0, errors.New("clickhouse connection reset by peer"))
	env.OnActivity(a.RollupLinks, mock.Anything, mock.Anything).Return(1, nil)
	env.OnActivity(a.RollupDeviceInterfaces, mock.Anything, mock.Anything).Return(1, nil)

	env.ExecuteWorkflow(BackfillRollupWorkflow, BackfillInput{
		StartTime: start,
		EndTime:   start.Add(span),
		ChunkSize: chunkSize,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err,
		"a backfill that skipped a chunk must not report success — the window is a permanent gap")
	require.Contains(t, err.Error(), "incomplete",
		"the error must say the backfill was incomplete so an operator re-runs it")
	require.Contains(t, err.Error(), failWindow.Format(time.RFC3339),
		"the error must name the window to re-run, got: %v", err)
	// Exactly one chunk failed: the surrounding chunks must still have been
	// processed, so one bad window doesn't abandon the rest of the range. Without
	// this the test would also pass if every chunk failed.
	require.Contains(t, err.Error(), "1 of 3 chunks failed",
		"only the middle chunk should fail; the others must still run, got: %v", err)
}

// TestBackfillRollupWorkflow_AllChunksSucceedReportsSuccess pins the other side:
// the failure path above must not make a clean backfill look broken.
func TestBackfillRollupWorkflow_AllChunksSucceedReportsSuccess(t *testing.T) {
	const chunkSize = time.Hour
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a)
	env.OnActivity(a.RollupLinks, mock.Anything, mock.Anything).Return(1, nil)
	env.OnActivity(a.RollupDeviceInterfaces, mock.Anything, mock.Anything).Return(1, nil)

	env.ExecuteWorkflow(BackfillRollupWorkflow, BackfillInput{
		StartTime: start,
		EndTime:   start.Add(3 * chunkSize),
		ChunkSize: chunkSize,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
