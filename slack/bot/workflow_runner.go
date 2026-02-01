package bot

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/google/uuid"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	v3 "github.com/malbeclabs/lake/agent/pkg/workflow/v3"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
)

// ChatStreamResult holds the result from a chat workflow.
type ChatStreamResult struct {
	Answer          string
	Classification  workflow.Classification
	DataQuestions   []workflow.DataQuestion
	ExecutedQueries []workflow.ExecutedQuery
	SessionID       string
}

// ChatRunner runs chat workflows and returns results.
type ChatRunner interface {
	ChatStream(
		ctx context.Context,
		message string,
		history []workflow.ConversationMessage,
		sessionID string,
		onProgress func(workflow.Progress),
	) (ChatStreamResult, error)
}

// WorkflowRunner runs chat workflows directly by invoking the agent workflow
// in-process, without going through HTTP.
type WorkflowRunner struct {
	log *slog.Logger
}

// NewWorkflowRunner creates a new workflow runner.
func NewWorkflowRunner(log *slog.Logger) *WorkflowRunner {
	return &WorkflowRunner{log: log}
}

// ChatStream runs the agent workflow directly and streams progress via the callback.
func (r *WorkflowRunner) ChatStream(
	ctx context.Context,
	message string,
	history []workflow.ConversationMessage,
	sessionID string,
	onProgress func(workflow.Progress),
) (ChatStreamResult, error) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		return ChatStreamResult{}, fmt.Errorf("ANTHROPIC_API_KEY is required")
	}

	// Generate session ID if not provided
	if sessionID == "" {
		sessionID = uuid.New().String()
	}

	// Load prompts
	prompts, err := v3.LoadPrompts()
	if err != nil {
		return ChatStreamResult{}, fmt.Errorf("failed to load prompts: %w", err)
	}

	// Create workflow components
	llm := workflow.NewAnthropicLLMClient(anthropic.ModelClaude3_5Haiku20241022, 4096)
	querier := handlers.NewDBQuerier()
	schemaFetcher := handlers.NewDBSchemaFetcher()

	// Create workflow config
	cfg := &workflow.Config{
		Logger:        r.log,
		LLM:           llm,
		Querier:       querier,
		SchemaFetcher: schemaFetcher,
		Prompts:       prompts,
		MaxTokens:     4096,
		FormatContext: prompts.Slack,
	}

	// Add env context so agent knows about other databases for cross-querying
	cfg.EnvContext = handlers.BuildEnvContext(handlers.EnvMainnet)

	// Add Neo4j support if available
	if config.Neo4jClient != nil {
		cfg.GraphQuerier = handlers.NewNeo4jQuerier()
		cfg.GraphSchemaFetcher = handlers.NewNeo4jSchemaFetcher()
	}

	// Create workflow
	wf, err := v3.New(cfg)
	if err != nil {
		return ChatStreamResult{}, fmt.Errorf("failed to create workflow: %w", err)
	}

	// Track queries and doc reads for progress reporting
	var queriesTotal, queriesDone int
	var dataQuestions []workflow.DataQuestion

	// Map workflow progress stages to the progress format expected by the processor
	wrappedProgress := func(progress workflow.Progress) {
		switch progress.Stage {
		case workflow.StageThinking:
			onProgress(workflow.Progress{
				Stage:           workflow.StageThinking,
				ThinkingContent: progress.ThinkingContent,
				DataQuestions:   dataQuestions,
				QueriesTotal:    queriesTotal,
				QueriesDone:     queriesDone,
			})

		case workflow.StageSQLStarted:
			queriesTotal++
			dataQuestions = append(dataQuestions, workflow.DataQuestion{
				Question: progress.SQLQuestion,
			})
			onProgress(workflow.Progress{
				Stage:         workflow.StageExecuting,
				DataQuestions: dataQuestions,
				QueriesTotal:  queriesTotal,
				QueriesDone:   queriesDone,
			})

		case workflow.StageSQLComplete:
			queriesDone++
			onProgress(workflow.Progress{
				Stage:         workflow.StageExecuting,
				DataQuestions: dataQuestions,
				QueriesTotal:  queriesTotal,
				QueriesDone:   queriesDone,
			})

		case workflow.StageCypherStarted:
			queriesTotal++
			dataQuestions = append(dataQuestions, workflow.DataQuestion{
				Question: progress.CypherQuestion,
			})
			onProgress(workflow.Progress{
				Stage:         workflow.StageExecuting,
				DataQuestions: dataQuestions,
				QueriesTotal:  queriesTotal,
				QueriesDone:   queriesDone,
			})

		case workflow.StageCypherComplete:
			queriesDone++
			onProgress(workflow.Progress{
				Stage:         workflow.StageExecuting,
				DataQuestions: dataQuestions,
				QueriesTotal:  queriesTotal,
				QueriesDone:   queriesDone,
			})

		case workflow.StageReadDocsStarted:
			queriesTotal++
			dataQuestions = append(dataQuestions, workflow.DataQuestion{
				Question:  "Reading " + progress.DocsPage,
				Rationale: "doc_read",
			})
			onProgress(workflow.Progress{
				Stage:         workflow.StageExecuting,
				DataQuestions: dataQuestions,
				QueriesTotal:  queriesTotal,
				QueriesDone:   queriesDone,
			})

		case workflow.StageReadDocsComplete:
			queriesDone++
			onProgress(workflow.Progress{
				Stage:         workflow.StageExecuting,
				DataQuestions: dataQuestions,
				QueriesTotal:  queriesTotal,
				QueriesDone:   queriesDone,
			})
		}
	}

	// Run the workflow with progress
	result, err := wf.RunWithProgress(ctx, message, history, wrappedProgress)
	if err != nil {
		return ChatStreamResult{}, err
	}

	// Build the result
	var classification workflow.Classification
	if queriesTotal > 0 {
		classification = workflow.ClassificationDataAnalysis
	} else {
		classification = workflow.ClassificationConversational
	}

	// Send completion progress
	onProgress(workflow.Progress{
		Stage:          workflow.StageComplete,
		Classification: classification,
		DataQuestions:  dataQuestions,
		QueriesTotal:   queriesTotal,
		QueriesDone:    queriesDone,
	})

	streamResult := ChatStreamResult{
		Answer:         result.Answer,
		Classification: classification,
		SessionID:      sessionID,
	}

	// Convert data questions
	for _, dq := range result.DataQuestions {
		streamResult.DataQuestions = append(streamResult.DataQuestions, workflow.DataQuestion{
			Question:  dq.Question,
			Rationale: dq.Rationale,
		})
	}

	// Convert executed queries
	for _, eq := range result.ExecutedQueries {
		streamResult.ExecutedQueries = append(streamResult.ExecutedQueries, workflow.ExecutedQuery{
			GeneratedQuery: eq.GeneratedQuery,
			Result:         eq.Result,
		})
	}

	return streamResult, nil
}
