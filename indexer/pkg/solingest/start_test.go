package solingest

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
)

// TestDeployStartOptionsRestartAtomically pins both policy fields: dropping
// either one lets a start adopt the previous deploy's run.
func TestDeployStartOptionsRestartAtomically(t *testing.T) {
	opts := deployStartOptions("mainnet-beta")

	require.Equal(t, "indexer-sol-ingest-mainnet-beta", opts.ID)
	require.Equal(t, "indexer-sol-ingest-mainnet-beta", opts.TaskQueue)
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, opts.WorkflowIDConflictPolicy,
		"a running execution must be terminated by the start call, not adopted")
	require.True(t, opts.WorkflowExecutionErrorWhenAlreadyStarted,
		"a start that is not fresh must fail loudly, not return a handle on the running execution")
	require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, opts.WorkflowIDReusePolicy,
		"the default ALLOW_DUPLICATE is what we want once the prior run has completed")
}

// TestStartSolIngestWorkflowStartsAndLogsItsRun simulates a redeploy: each start
// issues its own request and logs that start's run ID. Run IDs are allocated by
// the server, which a mock cannot model, so the fresh-run guarantee itself is
// pinned by the options test above.
func TestStartSolIngestWorkflowStartsAndLogsItsRun(t *testing.T) {
	tc := &mocks.Client{}
	var gotOpts []temporalclient.StartWorkflowOptions
	for _, runID := range []string{"run-1", "run-2"} {
		run := &mocks.WorkflowRun{}
		run.On("GetRunID").Return(runID)
		tc.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				gotOpts = append(gotOpts, args.Get(1).(temporalclient.StartWorkflowOptions))
			}).
			Return(run, nil).Once()
	}

	for range 2 {
		log, recs := laketesting.NewRecordingLogger()
		run, err := startSolIngestWorkflow(context.Background(), tc, log, "mainnet-beta")
		require.NoError(t, err)

		rec, ok := recs.Find("solingest: workflow started")
		require.True(t, ok, "a start must be attributable to a run")
		loggedRunID, ok := laketesting.RecordAttr(rec, "run_id")
		require.True(t, ok, "run_id is what makes the start line verifiable")
		require.Equal(t, run.GetRunID(), loggedRunID)
	}

	require.Len(t, gotOpts, 2, "a redeploy must issue its own start request")
	for _, o := range gotOpts {
		require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, o.WorkflowIDConflictPolicy)
	}
}

// TestWatchWorkflowStopsOnTerminated covers a hand-terminated run: nothing starts
// a replacement, so the workflow ID's current run is the same closed one and its
// Get errors immediately. Reattaching to it would spin at RPC speed, logging a
// WARN per iteration, for as long as the process lives.
func TestWatchWorkflowStopsOnTerminated(t *testing.T) {
	run := &mocks.WorkflowRun{}
	run.On("Get", mock.Anything, mock.Anything).
		Return(errors.New("workflow execution error: workflow terminated"))

	tc := &mocks.Client{}
	tc.On("GetWorkflow", mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { t.Error("a terminated run must not be reattached to") }).
		Return(run)

	log, recs := laketesting.NewRecordingLogger()
	watchWorkflow(context.Background(), tc, log, workflowID("mainnet-beta"), run)

	tc.AssertNotCalled(t, "GetWorkflow", mock.Anything, mock.Anything, mock.Anything)
	require.Empty(t, recs.Records(), "a terminated run is expected, not worth a log line")
}

// TestWatchWorkflowReattachesOnFailure keeps the reattach that the terminated
// case skips: a run that failed for any other reason has a successor to watch.
func TestWatchWorkflowReattachesOnFailure(t *testing.T) {
	failed := &mocks.WorkflowRun{}
	failed.On("Get", mock.Anything, mock.Anything).Return(errors.New("activity failed"))

	next := &mocks.WorkflowRun{}
	next.On("Get", mock.Anything, mock.Anything).Return(nil)

	tc := &mocks.Client{}
	tc.On("GetWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(next).Once()

	log, recs := laketesting.NewRecordingLogger()
	watchWorkflow(context.Background(), tc, log, workflowID("mainnet-beta"), failed)

	tc.AssertNumberOfCalls(t, "GetWorkflow", 1)
	_, ok := recs.Find("solingest: workflow interrupted, reattaching")
	require.True(t, ok, "an unexpected failure must stay visible")
}
