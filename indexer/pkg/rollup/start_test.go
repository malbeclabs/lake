package rollup

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

// findRecord returns the first record with the given message.
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

// TestComputeRollupStartOptionsRestartAtomically pins both policy fields:
// dropping either one lets a start adopt the previous deploy's run.
func TestComputeRollupStartOptionsRestartAtomically(t *testing.T) {
	opts := computeRollupStartOptions("mainnet-beta")

	require.Equal(t, "indexer-rollup-mainnet-beta", opts.ID)
	require.Equal(t, "indexer-rollup-mainnet-beta", opts.TaskQueue)
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, opts.WorkflowIDConflictPolicy,
		"a running execution must be terminated by the start call, not adopted")
	require.True(t, opts.WorkflowExecutionErrorWhenAlreadyStarted,
		"a start that is not fresh must fail loudly, not return a handle on the running execution")
	require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED, opts.WorkflowIDReusePolicy,
		"the default ALLOW_DUPLICATE is what we want once the prior run has completed")
}

// TestStartComputeRollupWorkflowStartsAndLogsItsRun simulates a redeploy: each
// start issues its own request and logs that start's run ID. Run IDs are
// allocated by the server, which a mock cannot model, so the fresh-run guarantee
// itself is pinned by the options test above.
func TestStartComputeRollupWorkflowStartsAndLogsItsRun(t *testing.T) {
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
		h := &capturingHandler{}
		run, err := startComputeRollupWorkflow(context.Background(), tc, slog.New(h), "mainnet-beta")
		require.NoError(t, err)

		rec, ok := findRecord(h.records, "rollup: workflow started")
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
