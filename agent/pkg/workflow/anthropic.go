package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/getsentry/sentry-go"
	"github.com/malbeclabs/lake/api/metrics"
)

// AnthropicLLMClient implements LLMClient using the Anthropic API.
type AnthropicLLMClient struct {
	client    anthropic.Client
	model     anthropic.Model
	maxTokens int64
	name      string // optional label for logging (e.g., "agent", "eval")
}

// NewAnthropicLLMClient creates a new Anthropic-based LLM client.
func NewAnthropicLLMClient(model anthropic.Model, maxTokens int64) *AnthropicLLMClient {
	return &AnthropicLLMClient{
		client:    anthropic.NewClient(),
		model:     model,
		maxTokens: maxTokens,
		name:      "agent",
	}
}

// NewAnthropicLLMClientWithName creates a new Anthropic-based LLM client with a custom name for logging.
func NewAnthropicLLMClientWithName(model anthropic.Model, maxTokens int64, name string) *AnthropicLLMClient {
	return &AnthropicLLMClient{
		client:    anthropic.NewClient(),
		model:     model,
		maxTokens: maxTokens,
		name:      name,
	}
}

// Complete sends a prompt to Claude and returns the response text.
func (c *AnthropicLLMClient) Complete(ctx context.Context, systemPrompt, userPrompt string, opts ...CompleteOption) (string, error) {
	// Apply options
	options := &CompleteOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Start Sentry span for AI monitoring
	span := sentry.StartSpan(ctx, "gen_ai.chat", sentry.WithDescription(fmt.Sprintf("chat %s", c.model)))
	span.SetData("gen_ai.operation.name", "chat")
	span.SetData("gen_ai.request.model", string(c.model))
	span.SetData("gen_ai.request.max_tokens", c.maxTokens)
	span.SetData("gen_ai.system", "anthropic")
	// Add session/workflow IDs for grouping if available
	if sessionID, ok := SessionIDFromContext(ctx); ok {
		span.SetTag("session_id", sessionID)
	}
	if workflowID, ok := WorkflowIDFromContext(ctx); ok {
		span.SetTag("workflow_id", workflowID)
	}
	ctx = span.Context()
	defer span.Finish()

	start := time.Now()
	slog.Info("Anthropic API call starting", "phase", c.name, "model", c.model, "maxTokens", c.maxTokens, "userPromptLen", len(userPrompt), "cacheEnabled", options.CacheSystemPrompt)

	// Build system prompt block with optional cache control
	systemBlock := anthropic.TextBlockParam{Type: "text", Text: systemPrompt}
	if options.CacheSystemPrompt {
		systemBlock.CacheControl = anthropic.NewCacheControlEphemeralParam()
	}

	msg, err := c.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     c.model,
		MaxTokens: c.maxTokens,
		System: []anthropic.TextBlockParam{
			systemBlock,
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(userPrompt)),
		},
	})

	duration := time.Since(start)
	if err != nil {
		slog.Error("Anthropic API call failed", "phase", c.name, "duration", duration, "error", err)
		metrics.RecordAnthropicRequest(c.name, duration, err)
		span.Status = sentry.SpanStatusInternalError
		return "", fmt.Errorf("anthropic API error: %w", err)
	}

	// Log with cache metrics if available
	slog.Info("Anthropic API call completed",
		"phase", c.name,
		"duration", duration,
		"stopReason", msg.StopReason,
		"inputTokens", msg.Usage.InputTokens,
		"outputTokens", msg.Usage.OutputTokens,
		"cacheCreationInputTokens", msg.Usage.CacheCreationInputTokens,
		"cacheReadInputTokens", msg.Usage.CacheReadInputTokens,
	)

	// Record Prometheus metrics
	metrics.RecordAnthropicRequest(c.name, duration, nil)
	metrics.RecordAnthropicTokensWithCache(
		msg.Usage.InputTokens,
		msg.Usage.OutputTokens,
		msg.Usage.CacheCreationInputTokens,
		msg.Usage.CacheReadInputTokens,
	)

	// Record Sentry AI metrics
	span.SetData("gen_ai.usage.input_tokens", msg.Usage.InputTokens)
	span.SetData("gen_ai.usage.output_tokens", msg.Usage.OutputTokens)
	span.SetData("gen_ai.usage.total_tokens", msg.Usage.InputTokens+msg.Usage.OutputTokens)
	if msg.Usage.CacheReadInputTokens > 0 {
		span.SetData("gen_ai.usage.input_tokens.cached", msg.Usage.CacheReadInputTokens)
	}
	span.Status = sentry.SpanStatusOK

	// Extract text from response
	for _, block := range msg.Content {
		if block.Type == "text" {
			return block.Text, nil
		}
	}

	return "", fmt.Errorf("no text content in response")
}

// CompleteWithTools sends a request with tools and returns a response that may contain tool calls.
// Implements ToolLLMClient interface.
func (c *AnthropicLLMClient) CompleteWithTools(
	ctx context.Context,
	systemPrompt string,
	messages []ToolMessage,
	tools []ToolDefinition,
	opts ...CompleteOption,
) (*ToolLLMResponse, error) {
	// Apply options
	options := &CompleteOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Start Sentry span for AI monitoring
	span := sentry.StartSpan(ctx, "gen_ai.chat", sentry.WithDescription(fmt.Sprintf("chat %s", c.model)))
	span.SetData("gen_ai.operation.name", "chat")
	span.SetData("gen_ai.request.model", string(c.model))
	span.SetData("gen_ai.request.max_tokens", c.maxTokens)
	span.SetData("gen_ai.system", "anthropic")
	span.SetData("gen_ai.request.tool_count", len(tools))
	// Add session/workflow IDs for grouping if available
	if sessionID, ok := SessionIDFromContext(ctx); ok {
		span.SetTag("session_id", sessionID)
	}
	if workflowID, ok := WorkflowIDFromContext(ctx); ok {
		span.SetTag("workflow_id", workflowID)
	}
	ctx = span.Context()
	defer span.Finish()

	start := time.Now()
	slog.Info("Anthropic API call starting",
		"phase", c.name,
		"model", c.model,
		"maxTokens", c.maxTokens,
		"messageCount", len(messages),
		"toolCount", len(tools),
		"cacheEnabled", options.CacheSystemPrompt,
	)

	// Build system prompt block with optional cache control
	systemBlock := anthropic.TextBlockParam{Type: "text", Text: systemPrompt}
	if options.CacheSystemPrompt {
		systemBlock.CacheControl = anthropic.NewCacheControlEphemeralParam()
	}

	// Convert tool definitions to Anthropic format
	anthropicTools := make([]anthropic.ToolUnionParam, 0, len(tools))
	for _, tool := range tools {
		// Parse the input schema
		var schemaProps map[string]any
		var schemaRequired []string

		if schemaBytes, err := json.Marshal(tool.InputSchema); err == nil {
			var schema map[string]any
			if json.Unmarshal(schemaBytes, &schema) == nil {
				if props, ok := schema["properties"].(map[string]any); ok {
					schemaProps = props
				}
				if req, ok := schema["required"].([]any); ok {
					for _, r := range req {
						if s, ok := r.(string); ok {
							schemaRequired = append(schemaRequired, s)
						}
					}
				}
			}
		}

		anthropicTools = append(anthropicTools, anthropic.ToolUnionParam{
			OfTool: &anthropic.ToolParam{
				Name:        tool.Name,
				Description: anthropic.String(tool.Description),
				InputSchema: anthropic.ToolInputSchemaParam{
					Properties: schemaProps,
					Required:   schemaRequired,
				},
			},
		})
	}

	// Convert messages to Anthropic format
	anthropicMessages := make([]anthropic.MessageParam, 0, len(messages))
	for _, msg := range messages {
		var contentBlocks []anthropic.ContentBlockParamUnion

		for _, block := range msg.Content {
			switch block.Type {
			case "text":
				contentBlocks = append(contentBlocks, anthropic.NewTextBlock(block.Text))
			case "tool_use":
				contentBlocks = append(contentBlocks, anthropic.NewToolUseBlock(block.ID, block.Input, block.Name))
			case "tool_result":
				contentBlocks = append(contentBlocks, anthropic.NewToolResultBlock(
					block.ToolUseID,
					block.Content,
					block.IsError,
				))
			}
		}

		anthropicMessages = append(anthropicMessages, anthropic.MessageParam{
			Role:    anthropic.MessageParamRole(msg.Role),
			Content: contentBlocks,
		})
	}

	// Build params for the API call
	params := anthropic.MessageNewParams{
		Model:     c.model,
		MaxTokens: c.maxTokens,
		System: []anthropic.TextBlockParam{
			systemBlock,
		},
		Messages: anthropicMessages,
		Tools:    anthropicTools,
	}

	// Use streaming if a callback is provided
	if options.OnTextDelta != nil {
		return c.completeWithToolsStreaming(ctx, params, options, span, start)
	}

	// Non-streaming path (original implementation)
	msg, err := c.client.Messages.New(ctx, params)

	duration := time.Since(start)
	if err != nil {
		slog.Error("Anthropic API call failed", "phase", c.name, "duration", duration, "error", err)
		metrics.RecordAnthropicRequest(c.name, duration, err)
		span.Status = sentry.SpanStatusInternalError
		return nil, fmt.Errorf("anthropic API error: %w", err)
	}

	// Log with cache metrics if available (use same format as Complete for eval script parsing)
	slog.Info("Anthropic API call completed",
		"phase", c.name,
		"duration", duration,
		"stopReason", msg.StopReason,
		"inputTokens", msg.Usage.InputTokens,
		"outputTokens", msg.Usage.OutputTokens,
		"cacheCreationInputTokens", msg.Usage.CacheCreationInputTokens,
		"cacheReadInputTokens", msg.Usage.CacheReadInputTokens,
	)

	// Record Prometheus metrics
	metrics.RecordAnthropicRequest(c.name, duration, nil)
	metrics.RecordAnthropicTokensWithCache(
		msg.Usage.InputTokens,
		msg.Usage.OutputTokens,
		msg.Usage.CacheCreationInputTokens,
		msg.Usage.CacheReadInputTokens,
	)

	// Record Sentry AI metrics
	span.SetData("gen_ai.usage.input_tokens", msg.Usage.InputTokens)
	span.SetData("gen_ai.usage.output_tokens", msg.Usage.OutputTokens)
	span.SetData("gen_ai.usage.total_tokens", msg.Usage.InputTokens+msg.Usage.OutputTokens)
	if msg.Usage.CacheReadInputTokens > 0 {
		span.SetData("gen_ai.usage.input_tokens.cached", msg.Usage.CacheReadInputTokens)
	}
	span.Status = sentry.SpanStatusOK

	return c.convertMessageToResponse(msg), nil
}

// completeWithToolsStreaming handles streaming LLM requests.
// It calls the OnTextDelta callback as text tokens arrive, reducing perceived latency.
func (c *AnthropicLLMClient) completeWithToolsStreaming(
	ctx context.Context,
	params anthropic.MessageNewParams,
	options *CompleteOptions,
	span *sentry.Span,
	start time.Time,
) (*ToolLLMResponse, error) {
	slog.Info("Anthropic API call starting (streaming)", "phase", c.name)

	stream := c.client.Messages.NewStreaming(ctx, params)

	// Accumulate the full message from stream events
	msg := anthropic.Message{}
	for stream.Next() {
		event := stream.Current()
		if err := msg.Accumulate(event); err != nil {
			slog.Warn("Failed to accumulate stream event", "error", err)
			continue
		}

		// Call the text delta callback when we receive text
		switch eventVariant := event.AsAny().(type) {
		case anthropic.ContentBlockDeltaEvent:
			switch deltaVariant := eventVariant.Delta.AsAny().(type) {
			case anthropic.TextDelta:
				if options.OnTextDelta != nil && deltaVariant.Text != "" {
					options.OnTextDelta(deltaVariant.Text)
				}
			}
		}
	}

	duration := time.Since(start)
	if err := stream.Err(); err != nil {
		slog.Error("Anthropic API streaming failed", "phase", c.name, "duration", duration, "error", err)
		metrics.RecordAnthropicRequest(c.name, duration, err)
		span.Status = sentry.SpanStatusInternalError
		return nil, fmt.Errorf("anthropic streaming error: %w", err)
	}

	// Log completion
	slog.Info("Anthropic API call completed (streaming)",
		"phase", c.name,
		"duration", duration,
		"stopReason", msg.StopReason,
		"inputTokens", msg.Usage.InputTokens,
		"outputTokens", msg.Usage.OutputTokens,
		"cacheCreationInputTokens", msg.Usage.CacheCreationInputTokens,
		"cacheReadInputTokens", msg.Usage.CacheReadInputTokens,
	)

	// Record metrics
	metrics.RecordAnthropicRequest(c.name, duration, nil)
	metrics.RecordAnthropicTokensWithCache(
		msg.Usage.InputTokens,
		msg.Usage.OutputTokens,
		msg.Usage.CacheCreationInputTokens,
		msg.Usage.CacheReadInputTokens,
	)

	// Record Sentry metrics
	span.SetData("gen_ai.usage.input_tokens", msg.Usage.InputTokens)
	span.SetData("gen_ai.usage.output_tokens", msg.Usage.OutputTokens)
	span.SetData("gen_ai.usage.total_tokens", msg.Usage.InputTokens+msg.Usage.OutputTokens)
	if msg.Usage.CacheReadInputTokens > 0 {
		span.SetData("gen_ai.usage.input_tokens.cached", msg.Usage.CacheReadInputTokens)
	}
	span.Status = sentry.SpanStatusOK

	return c.convertMessageToResponse(&msg), nil
}

// convertMessageToResponse converts an Anthropic Message to our ToolLLMResponse format.
func (c *AnthropicLLMClient) convertMessageToResponse(msg *anthropic.Message) *ToolLLMResponse {
	response := &ToolLLMResponse{
		StopReason:   string(msg.StopReason),
		InputTokens:  int(msg.Usage.InputTokens),
		OutputTokens: int(msg.Usage.OutputTokens),
	}

	for _, block := range msg.Content {
		switch block.Type {
		case "text":
			response.Content = append(response.Content, ToolContentBlock{
				Type: "text",
				Text: block.Text,
			})
		case "tool_use":
			// Parse the input JSON
			rawInput := string(block.Input)
			if len(rawInput) > 500 {
				slog.Info("DEBUG: Raw tool input from API (truncated)", "name", block.Name, "raw", rawInput[:500]+"...")
			} else {
				slog.Info("DEBUG: Raw tool input from API", "name", block.Name, "raw", rawInput)
			}

			var input map[string]any
			if err := json.Unmarshal(block.Input, &input); err != nil {
				slog.Warn("Failed to parse tool input", "error", err, "raw", rawInput)
				input = make(map[string]any)
			}

			response.Content = append(response.Content, ToolContentBlock{
				Type:  "tool_use",
				ID:    block.ID,
				Name:  block.Name,
				Input: input,
			})
		}
	}

	return response
}
