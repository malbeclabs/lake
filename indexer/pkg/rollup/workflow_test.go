package rollup

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
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
// An incomplete run must fail loudly enough that an operator knows to re-run the
// affected ranges. See runIteration for why the backfill must not skip a window
// while the live ComputeRollupWorkflow must keep looping.
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

// TestBackfillRollupWorkflow_CancellationStillReportsGaps covers the shutdown
// path. A chunk that failed while the context was live is a gap whether or not
// the run is later cancelled, so the cancellation must not throw that away — an
// operator who cancels a backfill at chunk 400 still needs the failed windows.
// The workflow must also end Canceled, not Failed, so the cancellation stays in
// the error chain.
func TestBackfillRollupWorkflow_CancellationStillReportsGaps(t *testing.T) {
	const chunkSize = time.Hour
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	span := 3 * chunkSize

	chunkStarts := backfillChunkStarts(start, span, chunkSize)
	require.Len(t, chunkStarts, 3)
	failWindow, cancelWindow := chunkStarts[0], chunkStarts[1]

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a)

	matchWindow := func(w time.Time) any {
		return mock.MatchedBy(func(in BackfillChunkInput) bool { return in.WindowStart.Equal(w) })
	}

	// First chunk fails on its own. The second chunk runs, and the cancel arrives
	// while it is in flight.
	env.OnActivity(a.RollupLinks, mock.Anything, matchWindow(failWindow)).
		Return(0, errors.New("clickhouse connection reset by peer"))
	env.OnActivity(a.RollupLinks, mock.Anything, matchWindow(cancelWindow)).
		Run(func(mock.Arguments) { env.CancelWorkflow() }).Return(1, nil)
	env.OnActivity(a.RollupLinks, mock.Anything, mock.Anything).Return(1, nil)
	env.OnActivity(a.RollupDeviceInterfaces, mock.Anything, mock.Anything).Return(1, nil)

	env.ExecuteWorkflow(BackfillRollupWorkflow, BackfillInput{
		StartTime: start,
		EndTime:   start.Add(span),
		ChunkSize: chunkSize,
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)

	var canceled *temporal.CanceledError
	require.ErrorAs(t, err, &canceled,
		"a cancelled backfill must end Canceled, not Failed, got: %v", err)
	require.Contains(t, err.Error(), failWindow.Format(time.RFC3339),
		"the cancellation must still name the window that failed, got: %v", err)
	require.Contains(t, err.Error(), "1 of 2 chunks failed",
		"only the first chunk failed, and only two were attempted, got: %v", err)
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
