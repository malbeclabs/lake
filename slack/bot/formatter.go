package bot

import (
	"strings"
	"unicode"
)

// SanitizeErrorMessage converts raw error messages to user-friendly messages
func SanitizeErrorMessage(errMsg string) string {
	// Rate limit errors
	if strings.Contains(errMsg, "429") || strings.Contains(errMsg, "rate_limit_error") || strings.Contains(errMsg, "rate limit") {
		return "I'm currently experiencing high demand. Please try again in a moment."
	}

	// Connection errors (should be retried, but if they still fail, show generic message)
	if strings.Contains(errMsg, "connection closed") ||
		strings.Contains(errMsg, "EOF") ||
		strings.Contains(errMsg, "client is closing") ||
		strings.Contains(errMsg, "failed to get tools") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "connection reset") {
		return "I'm having trouble connecting to the data service. Please try again in a moment."
	}

	// SQL-related errors (should be handled by agent, but fallback here)
	if strings.Contains(errMsg, "SQL validation failed") ||
		strings.Contains(errMsg, "query execution failed") ||
		strings.Contains(errMsg, "SQLSTATE") ||
		strings.Contains(errMsg, "Binder Error") ||
		strings.Contains(errMsg, "unknown statement") {
		return "I encountered an issue processing your query. Please try rephrasing your question or providing more specific details."
	}

	// Generic API errors
	if strings.Contains(errMsg, "failed to get response") || strings.Contains(errMsg, "POST") {
		return "I encountered an error processing your request. Please try again."
	}

	// Remove internal details like Request-IDs, URLs, etc.
	// Keep only the core error message
	lines := strings.Split(errMsg, "\n")
	var cleanLines []string
	for _, line := range lines {
		// Skip lines with internal details
		if strings.Contains(line, "Request-ID:") ||
			strings.Contains(line, "https://") ||
			strings.Contains(line, `"type":"error"`) ||
			strings.Contains(line, "POST \"") ||
			strings.Contains(line, "tools/list") ||
			strings.Contains(line, "calling \"") {
			continue
		}
		// Skip empty lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		cleanLines = append(cleanLines, line)
	}

	return "Sorry, I encountered an error processing your request. Please try again."
}

// normalizeTwoWayArrow replaces the two-way arrow (↔) and :left_right_arrow: emoji with the double arrow (⇔) and removes variation selectors
func normalizeTwoWayArrow(s string) string {
	// First replace the Slack emoji :left_right_arrow: with ⇔
	s = strings.ReplaceAll(s, ":left_right_arrow:", "⇔")

	var b strings.Builder
	for _, r := range s {
		if unicode.Is(unicode.Variation_Selector, r) {
			continue
		}
		if r == '↔' {
			b.WriteRune('⇔')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
