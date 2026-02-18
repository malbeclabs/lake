package logs

import "testing"

func TestParseLevel(t *testing.T) {
	tests := []struct {
		line string
		want LogLevel
	}{
		{"2024-01-15T10:30:45Z level=ERROR failed to connect", LogLevelError},
		{"level=WARN connection timeout", LogLevelWarn},
		{"level=INFO service started successfully", LogLevelInfo},
		{"level=DEBUG loaded config from file", LogLevelDebug},
		{"WARN: connection timeout", LogLevelWarn},
		{"info: service started successfully", LogLevelInfo},
		{"[DEBUG] loaded config from file", LogLevelDebug},
		{"ERROR: fatal error occurred", LogLevelError},
		{"panic: runtime error", LogLevelError},
		// No level marker — defaults to INFO, not UNKNOWN
		{"regular log line without level", LogLevelInfo},
		{"This is a normal message", LogLevelInfo},
		// Go standard logger format (2006/01/02 15:04:05) → INFO
		{"2024/01/15 10:30:45 Starting lake-api version=dev", LogLevelInfo},
		{"2024/01/15 10:30:45 Connected to ClickHouse successfully", LogLevelInfo},
		// Short 3-char codes (zerolog style)
		{"2024-01-15T10:30:45Z INF service started", LogLevelInfo},
		{"2024-01-15T10:30:45Z DBG loading config", LogLevelDebug},
		{"2024-01-15T10:30:45Z WRN slow query detected", LogLevelWarn},
		{"2024-01-15T10:30:45Z ERR connection refused", LogLevelError},
		{"2024-01-15T10:30:45Z TRC entering function", LogLevelTrace},
		// WARN keyword patterns
		{"connection timeout after 30s", LogLevelWarn},
		{"feature is deprecated, use newFeature instead", LogLevelWarn},
		{"retrying request (attempt 3)", LogLevelWarn},
		{"skipping migration: already applied", LogLevelWarn},
		// Error override: stated level is debug/info but line contains error-indicating word
		{"2024-01-15T10:30:45Z DBG connection failed after retry", LogLevelError},
		{"2024-01-15T10:30:45Z INF task failure recorded", LogLevelError},
		{"2024-01-15T10:30:45Z INF service started successfully", LogLevelInfo},
		// Go panic/goroutine header
		{"goroutine 1 [running]:", LogLevelError},
	}

	parser := NewLogParser()
	for _, tt := range tests {
		got := parser.ParseLevel(tt.line)
		if got != tt.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}
