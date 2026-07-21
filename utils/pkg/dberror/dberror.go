// Package dberror provides utilities for handling database connectivity errors.
package dberror

import (
	"context"
	"errors"
	"net"
	"regexp"
	"strings"
	"time"
)

// eofRe matches "eof" as a standalone word in a lowercased error message.
var eofRe = regexp.MustCompile(`\beof\b`)

// ErrorType classifies database errors for appropriate handling.
type ErrorType int

const (
	// ErrorTypeUnknown is an unclassified error.
	ErrorTypeUnknown ErrorType = iota
	// ErrorTypeConnectivity indicates the database is unreachable.
	ErrorTypeConnectivity
	// ErrorTypeTimeout indicates the operation timed out.
	ErrorTypeTimeout
	// ErrorTypeAuth indicates authentication/authorization failure.
	ErrorTypeAuth
	// ErrorTypeQuery indicates a query/syntax error.
	ErrorTypeQuery
	// ErrorTypeRateLimit indicates the upstream throttled the request (e.g. HTTP 429).
	ErrorTypeRateLimit
)

// IsTransient returns true if the error is likely transient and worth retrying.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not transient (user cancelled or deadline exceeded)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	errType := Classify(err)
	switch errType {
	case ErrorTypeConnectivity, ErrorTypeTimeout, ErrorTypeRateLimit:
		return true
	default:
		return false
	}
}

// Classify determines the type of database error.
func Classify(err error) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}

	errStr := strings.ToLower(err.Error())

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return ErrorTypeTimeout
		}
		return ErrorTypeConnectivity
	}

	// "eof" needs word-boundary matching: a bare substring check would match
	// embedded trigrams (e.g. "geofence") and misclassify actionable errors
	// as transient connectivity blips. Matches "EOF", "unexpected eof", etc.
	if eofRe.MatchString(errStr) {
		return ErrorTypeConnectivity
	}

	// Connection/connectivity patterns
	connectivityPatterns := []string{
		"connectivityerror",
		"connection refused",
		"connection reset",
		"connection closed",
		"no such host",
		"dial tcp",
		"dial unix",
		"broken pipe",
		"network is unreachable",
		"no route to host",
		"i/o timeout",
		"read/write on closed",
		"client is closing",
		"server shutdown",
		"neo4j is unavailable",
		"pool is closed",
		"driver is closed",
	}

	for _, pattern := range connectivityPatterns {
		if strings.Contains(errStr, pattern) {
			return ErrorTypeConnectivity
		}
	}

	// Timeout patterns
	timeoutPatterns := []string{
		"timeout",
		"deadline exceeded",
		"context deadline",
		"timed out",
	}

	for _, pattern := range timeoutPatterns {
		if strings.Contains(errStr, pattern) {
			return ErrorTypeTimeout
		}
	}

	// Rate-limit patterns (upstream throttling, e.g. an external RPC returning
	// 429 or a gRPC service returning ResourceExhausted, like InfluxDB).
	// These are transient and self-healing — worth retrying, not worth paging on.
	rateLimitPatterns := []string{
		"rate limited",
		"too many requests",
		"status 429",
		"resourceexhausted",
		"resource exhausted",
	}

	for _, pattern := range rateLimitPatterns {
		if strings.Contains(errStr, pattern) {
			return ErrorTypeRateLimit
		}
	}

	// Auth patterns
	authPatterns := []string{
		"unauthorized",
		"authentication failed",
		"invalid credentials",
		"access denied",
		"permission denied",
	}

	for _, pattern := range authPatterns {
		if strings.Contains(errStr, pattern) {
			return ErrorTypeAuth
		}
	}

	// Query/syntax patterns
	queryPatterns := []string{
		"syntax error",
		"invalid query",
		"unknown column",
		"table not found",
		"unknown table",
		"invalid cypher",
	}

	for _, pattern := range queryPatterns {
		if strings.Contains(errStr, pattern) {
			return ErrorTypeQuery
		}
	}

	return ErrorTypeUnknown
}

// UserMessage returns a user-friendly error message based on the error type.
func UserMessage(err error) string {
	if err == nil {
		return ""
	}

	switch Classify(err) {
	case ErrorTypeConnectivity:
		return "Database temporarily unavailable. Please try again in a moment."
	case ErrorTypeTimeout:
		return "Request timed out. Please try again."
	case ErrorTypeRateLimit:
		return "Upstream is rate limiting requests. Please try again in a moment."
	case ErrorTypeAuth:
		return "Database authentication error. Please contact support."
	case ErrorTypeQuery:
		return "Invalid query. Please check your input."
	default:
		return "An unexpected error occurred. Please try again."
	}
}

// RetryConfig holds configuration for retry behavior.
type RetryConfig struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// DefaultRetryConfig returns sensible defaults for database retries.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseBackoff: 200 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
	}
}

// Retry executes fn with retries for transient errors.
// Returns the result and the last error if all attempts fail.
func Retry[T any](ctx context.Context, cfg RetryConfig, fn func() (T, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			backoff := calculateBackoff(cfg.BaseBackoff, cfg.MaxBackoff, attempt-1)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(backoff):
			}
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Don't retry if error is not transient
		if !IsTransient(err) {
			return zero, err
		}
	}

	return zero, lastErr
}

// calculateBackoff returns exponential backoff: base * 2^attempt, capped at max.
func calculateBackoff(base, maxBackoff time.Duration, attempt int) time.Duration {
	backoff := base * time.Duration(1<<uint(attempt))
	return min(backoff, maxBackoff)
}
