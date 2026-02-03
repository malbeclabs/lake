package bot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAI_Slack_SanitizeErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errMsg   string
		want     string
		contains string
	}{
		{
			name:   "rate limit error",
			errMsg: "rate_limit_error: too many requests",
			want:   "I'm currently experiencing high demand. Please try again in a moment.",
		},
		{
			name:   "rate limit 429",
			errMsg: "HTTP 429: rate limit exceeded",
			want:   "I'm currently experiencing high demand. Please try again in a moment.",
		},
		{
			name:   "connection closed",
			errMsg: "connection closed by peer",
			want:   "I'm having trouble connecting to the data service. Please try again in a moment.",
		},
		{
			name:   "EOF error",
			errMsg: "EOF error occurred",
			want:   "I'm having trouble connecting to the data service. Please try again in a moment.",
		},
		{
			name:   "failed to get tools",
			errMsg: "failed to get tools from server",
			want:   "I'm having trouble connecting to the data service. Please try again in a moment.",
		},
		{
			name:   "generic API error",
			errMsg: "failed to get response from API",
			want:   "I encountered an error processing your request. Please try again.",
		},
		{
			name:   "error with internal details",
			errMsg: "Error occurred\nRequest-ID: abc123\nhttps://api.example.com/error\nActual error message",
			want:   "Sorry, I encountered an error processing your request. Please try again.",
		},
		{
			name:   "error with only internal details",
			errMsg: "Request-ID: abc123\nhttps://api.example.com/error\nPOST \"https://api.example.com\"",
			want:   "I encountered an error processing your request. Please try again.",
		},
		{
			name:   "generic error",
			errMsg: "something went wrong",
			want:   "Sorry, I encountered an error processing your request. Please try again.",
		},
		{
			name:   "empty error",
			errMsg: "",
			want:   "Sorry, I encountered an error processing your request. Please try again.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SanitizeErrorMessage(tt.errMsg)
			if tt.contains != "" {
				require.Contains(t, got, tt.contains)
			} else {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestAI_Slack_NormalizeTwoWayArrow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "two-way arrow",
			input:    "nyc ↔ lon",
			expected: "nyc ⇔ lon",
		},
		{
			name:     "slack emoji",
			input:    "nyc :left_right_arrow: lon",
			expected: "nyc ⇔ lon",
		},
		{
			name:     "already correct",
			input:    "nyc ⇔ lon",
			expected: "nyc ⇔ lon",
		},
		{
			name:     "no arrow",
			input:    "just text",
			expected: "just text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := normalizeTwoWayArrow(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
