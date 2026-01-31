package bot

import (
	"context"
	"fmt"
	"log/slog"
	neturl "net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/slack-go/slack/slackevents"
	slackmdgo "github.com/snormore/slackmd/slackgo"
)

const (
	respondedMessagesMaxAge = 1 * time.Hour
)

// Processor processes Slack messages and generates responses
type Processor struct {
	slackClient *Client
	chatRunner  ChatRunner
	convManager *Manager
	log         *slog.Logger
	webBaseURL  string // Base URL for web UI (for query editor links)

	// Track messages we've already responded to (by message timestamp) to prevent duplicate error messages
	respondedMessages   map[string]time.Time
	respondedMessagesMu sync.RWMutex

	// Per-thread locks to ensure messages in the same thread are processed sequentially
	threadLocks   map[string]*threadLockEntry
	threadLocksMu sync.Mutex
}

// threadLockEntry holds a mutex and tracks when it was last used
type threadLockEntry struct {
	mu       sync.Mutex
	lastUsed time.Time
}

// NewProcessor creates a new message processor
func NewProcessor(
	slackClient *Client,
	chatRunner ChatRunner,
	convManager *Manager,
	log *slog.Logger,
	webBaseURL string,
) *Processor {
	return &Processor{
		slackClient:       slackClient,
		chatRunner:        chatRunner,
		convManager:       convManager,
		log:               log,
		webBaseURL:        webBaseURL,
		respondedMessages: make(map[string]time.Time),
		threadLocks:       make(map[string]*threadLockEntry),
	}
}

// getThreadLock returns the mutex for a given thread, creating one if it doesn't exist
func (p *Processor) getThreadLock(threadKey string) *sync.Mutex {
	p.threadLocksMu.Lock()
	defer p.threadLocksMu.Unlock()

	if entry, exists := p.threadLocks[threadKey]; exists {
		entry.lastUsed = time.Now()
		return &entry.mu
	}

	entry := &threadLockEntry{
		lastUsed: time.Now(),
	}
	p.threadLocks[threadKey] = entry
	return &entry.mu
}

// StartCleanup starts a background goroutine to clean up old responded messages
func (p *Processor) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.cleanup()
			}
		}
	}()
}

func (p *Processor) cleanup() {
	now := time.Now()

	// Clean up old responded messages
	p.respondedMessagesMu.Lock()
	for msgKey, timestamp := range p.respondedMessages {
		if now.Sub(timestamp) > respondedMessagesMaxAge {
			delete(p.respondedMessages, msgKey)
		}
	}
	p.respondedMessagesMu.Unlock()

	// Clean up old thread locks (those not used in the last hour)
	// Only delete if the lock is not currently held (TryLock succeeds)
	p.threadLocksMu.Lock()
	for threadKey, entry := range p.threadLocks {
		if now.Sub(entry.lastUsed) > respondedMessagesMaxAge {
			// Try to acquire the lock - if we can't, it's in use so don't delete
			if entry.mu.TryLock() {
				entry.mu.Unlock()
				delete(p.threadLocks, threadKey)
			}
		}
	}
	p.threadLocksMu.Unlock()
}

// HasResponded checks if we've already responded to a message
func (p *Processor) HasResponded(messageKey string) bool {
	p.respondedMessagesMu.RLock()
	_, responded := p.respondedMessages[messageKey]
	p.respondedMessagesMu.RUnlock()
	return responded
}

// MarkResponded marks a message as responded to
func (p *Processor) MarkResponded(messageKey string) {
	p.respondedMessagesMu.Lock()
	p.respondedMessages[messageKey] = time.Now()
	p.respondedMessagesMu.Unlock()
}

// containsNonBotMention checks if the message text contains a user mention that is not the bot
func containsNonBotMention(text, botUserID string) bool {
	if botUserID == "" {
		return false
	}
	// Match mention patterns: <@USERID> or <@USERID|username>
	mentionRegex := regexp.MustCompile(`<@([A-Z0-9]+)(?:\|[^>]+)?>`)
	matches := mentionRegex.FindAllStringSubmatch(text, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		mentionedUserID := match[1]
		// Check if this mention is NOT the bot
		if mentionedUserID != botUserID {
			return true
		}
	}
	return false
}

// formatThinkingMessage formats the thinking message based on progress.
func formatThinkingMessage(progress workflow.Progress) string {
	var sb strings.Builder

	switch progress.Stage {
	case workflow.StageClassifying:
		sb.WriteString("_:hourglass_flowing_sand: Understanding your question..._")

	case workflow.StageThinking:
		sb.WriteString("_:brain: Thinking..._")
		if len(progress.DataQuestions) > 0 {
			sb.WriteString("\n")
			writeStepsList(&sb, progress)
		}

	case workflow.StageDecomposing:
		sb.WriteString("_:hourglass_flowing_sand: Breaking down into queries..._")

	case workflow.StageDecomposed:
		sb.WriteString("_Identified data questions:_\n")
		for i, q := range progress.DataQuestions {
			sb.WriteString(fmt.Sprintf("_%d. %s_\n", i+1, q.Question))
		}

	case workflow.StageExecuting:
		sb.WriteString("_:hourglass_flowing_sand: Working..._\n")
		writeStepsList(&sb, progress)

	case workflow.StageSynthesizing:
		sb.WriteString(fmt.Sprintf("_:hourglass_flowing_sand: Preparing answer (%d steps complete)..._", progress.QueriesTotal))

	case workflow.StageComplete:
		// For data_analysis, show compact summary
		if progress.Classification == workflow.ClassificationDataAnalysis && len(progress.DataQuestions) > 0 {
			sb.WriteString("_:mag: Answered by querying:_\n")
			n := 0
			for _, q := range progress.DataQuestions {
				if q.Rationale == "doc_read" {
					continue
				}
				n++
				sb.WriteString(fmt.Sprintf("_Q%d. %s_\n", n, q.Question))
			}
		}
		// For conversational/out_of_scope, we don't show anything (just answer)

	case workflow.StageError:
		sb.WriteString(":x: *Error*\n")
		if progress.Error != nil {
			sb.WriteString(fmt.Sprintf("_%s_", progress.Error.Error()))
		}
	}

	return sb.String()
}

// writeStepsList writes the ordered list of steps (queries and doc reads) with status indicators.
// Queries are numbered Q1, Q2, etc. Doc reads use bullet points.
func writeStepsList(sb *strings.Builder, progress workflow.Progress) {
	qNum := 0
	for i, q := range progress.DataQuestions {
		isDocRead := q.Rationale == "doc_read"
		prefix := "• "
		if !isDocRead {
			qNum++
			prefix = fmt.Sprintf("Q%d. ", qNum)
		}

		if i < progress.QueriesDone {
			sb.WriteString(fmt.Sprintf("_%s%s ✓_\n", prefix, q.Question))
		} else if i == progress.QueriesDone {
			sb.WriteString(fmt.Sprintf("_%s%s :hourglass_flowing_sand:_\n", prefix, q.Question))
		} else {
			sb.WriteString(fmt.Sprintf("_%s%s_\n", prefix, q.Question))
		}
	}
}

// formatCompletionSummary builds the "Answered by querying" completion message
// with Q labels linked to the web query editor when webBaseURL is set.
func formatCompletionSummary(dataQuestions []workflow.DataQuestion, executedQueries []workflow.ExecutedQuery, webBaseURL string) string {
	var sb strings.Builder
	sb.WriteString("_:mag: Answered by querying:_\n")

	n := 0
	for _, q := range dataQuestions {
		if q.Rationale == "doc_read" {
			continue
		}
		n++
		if webBaseURL != "" && n-1 < len(executedQueries) {
			eq := executedQueries[n-1]
			queryType := "sql"
			queryText := eq.GeneratedQuery.SQL
			if eq.GeneratedQuery.IsCypher() {
				queryType = "cypher"
				queryText = eq.GeneratedQuery.Cypher
			}
			if queryText != "" {
				sessionID := uuid.New().String()
				url := fmt.Sprintf("%s/query/%s?%s=%s", webBaseURL, sessionID, queryType, neturl.QueryEscape(queryText))
				sb.WriteString(fmt.Sprintf("<%s|Q%d>. %s\n", url, n, q.Question))
				continue
			}
		}
		sb.WriteString(fmt.Sprintf("Q%d. %s\n", n, q.Question))
	}

	return sb.String()
}

// ProcessMessage processes a single Slack message
func (p *Processor) ProcessMessage(
	ctx context.Context,
	client *Client,
	ev *slackevents.MessageEvent,
	messageKey string,
	eventID string,
	isChannel bool,
) {
	startTime := time.Now()

	p.log.Info("replying to message",
		"channel", ev.Channel,
		"user", ev.User,
		"message_ts", ev.TimeStamp,
		"thread_ts", ev.ThreadTimeStamp,
		"text", ev.Text,
		"message_key", messageKey,
		"envelope_id", eventID,
		"is_channel", isChannel,
	)

	// Skip processing if in a thread and message contains another user being mentioned
	if ev.ThreadTimeStamp != "" && containsNonBotMention(ev.Text, client.BotUserID()) {
		p.log.Info("skipping message in thread that contains non-bot mention",
			"channel", ev.Channel,
			"user", ev.User,
			"message_ts", ev.TimeStamp,
			"thread_ts", ev.ThreadTimeStamp,
			"text_preview", TruncateString(ev.Text, 100),
		)
		MessagesIgnoredTotal.WithLabelValues("thread_non_bot_mention").Inc()
		return
	}

	// Skip processing if message contains :mute: emoji
	if strings.Contains(ev.Text, ":mute:") {
		p.log.Info("skipping message with :mute: emoji",
			"channel", ev.Channel,
			"user", ev.User,
			"message_ts", ev.TimeStamp,
			"thread_ts", ev.ThreadTimeStamp,
			"text_preview", TruncateString(ev.Text, 100),
		)
		MessagesIgnoredTotal.WithLabelValues("mute_emoji").Inc()
		return
	}

	txt := strings.TrimSpace(ev.Text)

	// Remove bot mention from text for cleaner processing
	if isChannel {
		txt = client.RemoveBotMention(txt)
	}

	defer func() {
		MessageProcessingDuration.WithLabelValues("api").Observe(time.Since(startTime).Seconds())
	}()

	// Always thread responses (both channels and DMs)
	// Determine thread key: use thread timestamp if in thread, otherwise use message timestamp
	threadKey := ev.TimeStamp
	threadTS := ev.ThreadTimeStamp
	if threadTS == "" {
		threadTS = ev.TimeStamp
	}
	if ev.ThreadTimeStamp != "" {
		threadKey = ev.ThreadTimeStamp
	}

	// Acquire per-thread lock to ensure sequential processing within the same thread
	// This prevents race conditions when multiple messages arrive in quick succession
	threadLockKey := fmt.Sprintf("%s:%s", ev.Channel, threadKey)
	threadLock := p.getThreadLock(threadLockKey)
	threadLock.Lock()
	defer threadLock.Unlock()

	p.log.Debug("acquired thread lock", "thread_lock_key", threadLockKey)

	// Fetch conversation history from Slack if not cached
	fetcher := NewDefaultFetcher(p.log)
	history, err := p.convManager.GetConversationHistory(
		ctx,
		client.API(),
		ev.Channel,
		ev.TimeStamp,
		ev.ThreadTimeStamp,
		client.BotUserID(),
		fetcher,
	)
	if err != nil {
		p.log.Warn("failed to get conversation history", "error", err)
		ConversationHistoryErrorsTotal.Inc()
		history = []workflow.ConversationMessage{}
	}

	// Generate session ID for workflow tracking
	sessionID := uuid.New().String()

	// Post initial thinking message
	initialThinking := formatThinkingMessage(workflow.Progress{Stage: workflow.StageClassifying})
	thinkingTS, err := slackmdgo.Post(ctx, client.API(), ev.Channel, initialThinking,
		slackmdgo.WithThreadTS(threadTS), slackmdgo.WithRetry(nil))
	if err != nil {
		p.log.Warn("failed to post thinking message", "error", err)
		SlackAPIErrorsTotal.WithLabelValues("post_message").Inc()
		// Continue anyway - we can still process without the thinking message
	}

	// Track last progress stage to avoid redundant updates
	var lastStage workflow.ProgressStage
	var lastQueriesDone int
	var thinkingMu sync.Mutex

	// Progress callback to update thinking message
	onProgress := func(progress workflow.Progress) {
		thinkingMu.Lock()
		defer thinkingMu.Unlock()

		// Skip if same stage and same query count (avoid redundant updates)
		if progress.Stage == lastStage && progress.QueriesDone == lastQueriesDone {
			return
		}
		lastStage = progress.Stage
		lastQueriesDone = progress.QueriesDone

		// Don't update on complete - we'll handle that separately
		if progress.Stage == workflow.StageComplete {
			return
		}

		// Update thinking message
		if thinkingTS != "" {
			thinkingText := formatThinkingMessage(progress)
			if err := slackmdgo.Update(ctx, client.API(), ev.Channel, thinkingTS, thinkingText, slackmdgo.WithRetry(nil)); err != nil {
				p.log.Debug("failed to update thinking message", "error", err)
			}
		}
	}

	// Run the API chat stream
	result, err := p.chatRunner.ChatStream(ctx, txt, history, sessionID, onProgress)
	if err != nil {
		AgentErrorsTotal.WithLabelValues("workflow", "api").Inc()
		p.log.Error("API error", "error", err, "message_ts", ev.TimeStamp, "envelope_id", eventID)

		p.MarkResponded(messageKey)

		// Update thinking message to show error
		if thinkingTS != "" {
			errorText := fmt.Sprintf(":x: *Error*\n_%s_", SanitizeErrorMessage(err.Error()))
			if err := slackmdgo.Update(ctx, client.API(), ev.Channel, thinkingTS, errorText, slackmdgo.WithRetry(nil)); err != nil {
				p.log.Debug("failed to update thinking message with error", "error", err)
			}
		}

		MessagesPostedTotal.WithLabelValues("error", "api").Inc()
		return
	}

	reply := strings.TrimSpace(result.Answer)
	if reply == "" {
		reply = "I didn't get a response. Please try again."
	}
	reply = normalizeTwoWayArrow(reply)

	p.log.Debug("API response",
		"reply", reply,
		"classification", result.Classification,
		"data_questions", len(result.DataQuestions))

	// For data analysis, update thinking message with summary and session link
	if result.Classification == workflow.ClassificationDataAnalysis && len(result.DataQuestions) > 0 && thinkingTS != "" {
		summaryText := formatCompletionSummary(result.DataQuestions, result.ExecutedQueries, p.webBaseURL)
		if err := slackmdgo.Update(ctx, client.API(), ev.Channel, thinkingTS, summaryText, slackmdgo.WithRetry(nil)); err != nil {
			p.log.Debug("failed to update thinking message with summary", "error", err)
		}
	} else if thinkingTS != "" {
		// For conversational/out_of_scope, delete the thinking message so only the answer shows
		if err := client.DeleteMessage(ctx, ev.Channel, thinkingTS); err != nil {
			p.log.Debug("failed to delete thinking message", "error", err)
		}
	}

	// Post the final answer
	p.MarkResponded(messageKey)

	respTS, err := slackmdgo.Post(ctx, client.API(), ev.Channel, reply,
		slackmdgo.WithThreadTS(threadTS), slackmdgo.WithFallbackText(reply), slackmdgo.WithRetry(nil))

	if err != nil {
		SlackAPIErrorsTotal.WithLabelValues("post_message").Inc()
		MessagesPostedTotal.WithLabelValues("error", "api").Inc()
		errorReply := "Sorry, I encountered an error. Please try again."
		errorReply = normalizeTwoWayArrow(errorReply)
		_, _ = slackmdgo.Post(ctx, client.API(), ev.Channel, errorReply,
			slackmdgo.WithThreadTS(threadTS), slackmdgo.WithRetry(nil))
	} else {
		MessagesPostedTotal.WithLabelValues("success", "api").Inc()
		p.log.Info("reply posted successfully", "channel", ev.Channel, "thread_ts", threadKey, "reply_ts", respTS)

		// Extract queries from executed queries
		var executedSQL []string
		for _, eq := range result.ExecutedQueries {
			executedSQL = append(executedSQL, eq.GeneratedQuery.QueryText())
		}

		// Update conversation history with the new exchange
		newHistory := append(history,
			workflow.ConversationMessage{Role: "user", Content: txt},
			workflow.ConversationMessage{Role: "assistant", Content: result.Answer, ExecutedQueries: executedSQL},
		)
		p.convManager.UpdateConversationHistory(threadKey, newHistory)
	}
}
