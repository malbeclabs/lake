package bot

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestAI_Slack_Processor_HasResponded(t *testing.T) {
	t.Parallel()

	p := NewProcessor(nil, nil, nil, slog.Default(), "")
	messageKey := "C123:1234567890.123456"

	require.False(t, p.HasResponded(messageKey))

	p.MarkResponded(messageKey)
	require.True(t, p.HasResponded(messageKey))
}

func TestAI_Slack_Processor_MarkResponded(t *testing.T) {
	t.Parallel()

	p := NewProcessor(nil, nil, nil, slog.Default(), "")
	messageKey := "C123:1234567890.123456"

	p.MarkResponded(messageKey)
	require.True(t, p.HasResponded(messageKey))
}

func TestAI_Slack_Processor_StartCleanup(t *testing.T) {
	t.Parallel()

	p := NewProcessor(nil, nil, nil, slog.Default(), "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start cleanup - should not panic
	p.StartCleanup(ctx)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Cancel context to stop cleanup
	cancel()

	// Wait a bit for cleanup to stop
	time.Sleep(10 * time.Millisecond)
}

func TestAI_Slack_Processor_NewProcessor(t *testing.T) {
	t.Parallel()

	p := NewProcessor(nil, nil, nil, slog.Default(), "")
	require.NotNil(t, p)
	require.IsType(t, &Processor{}, p)
}

func TestAI_Slack_FormatThinkingMessage(t *testing.T) {
	t.Parallel()

	t.Run("classifying", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{Stage: workflow.StageClassifying})
		require.Equal(t, "_:hourglass_flowing_sand: Understanding your question..._", result)
	})

	t.Run("thinking without steps", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{Stage: workflow.StageThinking})
		require.Equal(t, "_:brain: Thinking..._", result)
	})

	t.Run("thinking with steps", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage: workflow.StageThinking,
			DataQuestions: []workflow.DataQuestion{
				{Question: "How many validators?"},
			},
			QueriesTotal: 1,
			QueriesDone:  0,
		})
		require.Contains(t, result, "_:brain: Thinking..._")
		require.Contains(t, result, "Q1. How many validators?")
	})

	t.Run("executing shows steps list", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage: workflow.StageExecuting,
			DataQuestions: []workflow.DataQuestion{
				{Question: "Validator count"},
				{Question: "Average stake"},
			},
			QueriesTotal: 2,
			QueriesDone:  1,
		})
		require.Contains(t, result, "_:hourglass_flowing_sand: Working..._")
		require.Contains(t, result, "Validator count ✓")
		require.Contains(t, result, "Average stake :hourglass_flowing_sand:")
	})

	t.Run("executing with doc reads", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage: workflow.StageExecuting,
			DataQuestions: []workflow.DataQuestion{
				{Question: "Validator count"},
				{Question: "Reading network-overview", Rationale: "doc_read"},
				{Question: "Average stake"},
			},
			QueriesTotal: 3,
			QueriesDone:  2,
		})
		// Queries numbered, doc reads use bullets
		require.Contains(t, result, "Q1. Validator count ✓")
		require.Contains(t, result, "• Reading network-overview ✓")
		require.Contains(t, result, "Q2. Average stake :hourglass_flowing_sand:")
	})

	t.Run("synthesizing", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage:        workflow.StageSynthesizing,
			QueriesTotal: 3,
		})
		require.Equal(t, "_:hourglass_flowing_sand: Preparing answer (3 steps complete)..._", result)
	})

	t.Run("complete filters doc reads", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage:          workflow.StageComplete,
			Classification: workflow.ClassificationDataAnalysis,
			DataQuestions: []workflow.DataQuestion{
				{Question: "Validator count"},
				{Question: "Reading docs", Rationale: "doc_read"},
				{Question: "Average stake"},
			},
		})
		require.Contains(t, result, "Q1. Validator count")
		require.Contains(t, result, "Q2. Average stake")
		require.NotContains(t, result, "Reading docs")
	})

	t.Run("complete conversational shows nothing", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage:          workflow.StageComplete,
			Classification: workflow.ClassificationConversational,
		})
		require.Empty(t, result)
	})

	t.Run("error with message", func(t *testing.T) {
		t.Parallel()
		result := formatThinkingMessage(workflow.Progress{
			Stage: workflow.StageError,
			Error: fmt.Errorf("something broke"),
		})
		require.Contains(t, result, ":x: *Error*")
		require.Contains(t, result, "something broke")
	})
}

func TestAI_Slack_WriteStepsList(t *testing.T) {
	t.Parallel()

	t.Run("queries numbered and doc reads bulleted", func(t *testing.T) {
		t.Parallel()
		var sb strings.Builder
		writeStepsList(&sb, workflow.Progress{
			DataQuestions: []workflow.DataQuestion{
				{Question: "First query"},
				{Question: "Reading docs", Rationale: "doc_read"},
				{Question: "Second query"},
			},
			QueriesDone: 1,
		})
		result := sb.String()
		require.Contains(t, result, "_Q1. First query ✓_")
		require.Contains(t, result, "_• Reading docs :hourglass_flowing_sand:_")
		require.Contains(t, result, "_Q2. Second query_")
	})

	t.Run("all done", func(t *testing.T) {
		t.Parallel()
		var sb strings.Builder
		writeStepsList(&sb, workflow.Progress{
			DataQuestions: []workflow.DataQuestion{
				{Question: "Query A"},
				{Question: "Query B"},
			},
			QueriesDone: 2,
		})
		result := sb.String()
		require.Contains(t, result, "Query A ✓")
		require.Contains(t, result, "Query B ✓")
		require.NotContains(t, result, "hourglass")
	})
}

func TestAI_Slack_FormatCompletionSummary(t *testing.T) {
	t.Parallel()

	t.Run("no web base url shows plain Q labels", func(t *testing.T) {
		t.Parallel()
		result := formatCompletionSummary(
			[]workflow.DataQuestion{
				{Question: "Validator count"},
				{Question: "Average stake"},
			},
			[]workflow.ExecutedQuery{
				{GeneratedQuery: workflow.GeneratedQuery{SQL: "SELECT COUNT(*) FROM validators"}},
				{GeneratedQuery: workflow.GeneratedQuery{SQL: "SELECT AVG(stake) FROM validators"}},
			},
			"",
		)
		require.Contains(t, result, "Q1. Validator count")
		require.Contains(t, result, "Q2. Average stake")
		require.NotContains(t, result, "<http")
	})

	t.Run("with web base url shows linked Q labels", func(t *testing.T) {
		t.Parallel()
		result := formatCompletionSummary(
			[]workflow.DataQuestion{
				{Question: "Validator count"},
			},
			[]workflow.ExecutedQuery{
				{GeneratedQuery: workflow.GeneratedQuery{SQL: "SELECT COUNT(*) FROM validators"}},
			},
			"https://app.example.com",
		)
		require.Contains(t, result, "<https://app.example.com/query/")
		require.Contains(t, result, "?sql=")
		require.Contains(t, result, "|Q1>")
		require.Contains(t, result, "Validator count")
	})

	t.Run("cypher queries use cypher param", func(t *testing.T) {
		t.Parallel()
		result := formatCompletionSummary(
			[]workflow.DataQuestion{
				{Question: "Graph query"},
			},
			[]workflow.ExecutedQuery{
				{GeneratedQuery: workflow.GeneratedQuery{Cypher: "MATCH (n) RETURN n"}},
			},
			"https://app.example.com",
		)
		require.Contains(t, result, "?cypher=")
		require.NotContains(t, result, "?sql=")
	})

	t.Run("filters doc reads", func(t *testing.T) {
		t.Parallel()
		result := formatCompletionSummary(
			[]workflow.DataQuestion{
				{Question: "Validator count"},
				{Question: "Reading network-overview", Rationale: "doc_read"},
				{Question: "Average stake"},
			},
			[]workflow.ExecutedQuery{
				{GeneratedQuery: workflow.GeneratedQuery{SQL: "SELECT 1"}},
				{GeneratedQuery: workflow.GeneratedQuery{SQL: "SELECT 2"}},
			},
			"",
		)
		require.Contains(t, result, "Q1. Validator count")
		require.Contains(t, result, "Q2. Average stake")
		require.NotContains(t, result, "Reading network-overview")
	})

	t.Run("no executed queries shows plain labels", func(t *testing.T) {
		t.Parallel()
		result := formatCompletionSummary(
			[]workflow.DataQuestion{
				{Question: "Some question"},
			},
			nil,
			"https://app.example.com",
		)
		require.Contains(t, result, "Q1. Some question")
		require.NotContains(t, result, "<http")
	})
}
