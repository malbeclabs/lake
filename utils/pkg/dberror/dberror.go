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

// awsRespErrRe matches the AWS SDK v2 "https response error StatusCode: 200"
// shape (lowercased by Classify): S3's documented "200 OK with an error
// mid-body" blip, which the SDK surfaces as a response error but does not
// retry internally — transient per S3's own guidance. The prefix keeps 200
// scoped to AWS SDK messages; a bare "status code 200" elsewhere stays
// non-transient. Retryable 5xx are handled shape-independently by
// httpStatusRe. The trailing \b prevents matching a longer digit run.
var awsRespErrRe = regexp.MustCompile(`https response error statuscode: 200\b`)

// httpStatusRe captures the first "status code NNN" mention in an error
// string — the failed request's own status, since wrapping prepends; a status
// quoted later (e.g. in a response body) must not classify. Classify treats
// the SDK-retryable set {500, 502, 503, 504} as transient; 501/505 are
// permanent endpoint failures and 4xx are actionable, so all keep paging.
var httpStatusRe = regexp.MustCompile(`status[ _]?code[:= ]?\s*(\d{3})\b`)

// ErrTransient is a sentinel that explicitly marks an error as transient for
// IsTransient, independent of its message. Wrap a return with it (e.g. via
// errors.Join or fmt.Errorf("...: %w", ErrTransient)) when the caller knows a
// failure is self-healing but its message wouldn't be classified as transient
// by Classify — for example an expected, retryable upstream miss.
//
// Two limits, both of which fail safe (page sooner rather than suppress):
//   - IsTransient checks context.Canceled/DeadlineExceeded before this marker,
//     so wrapping a context error with ErrTransient does not make it transient.
//   - The marker does not survive Temporal's failure serialization: after the
//     ErrorToFailure/FailureToError round-trip errors.Is(reconstructed,
//     ErrTransient) is false, so it cannot classify an error that reaches a
//     caller across an activity boundary. Use it where the wrapped error is
//     inspected in-process (e.g. before an activity returns nil to Temporal).
var ErrTransient = errors.New("transient error")

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

	// An explicit ErrTransient marker overrides message-based classification:
	// the caller already knows the failure is self-healing.
	if errors.Is(err, ErrTransient) {
		return true
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

	// AWS SDK v2 transient 200-with-embedded-error responses — self-healing
	// blips, not actionable.
	if awsRespErrRe.MatchString(errStr) {
		return ErrorTypeConnectivity
	}

	// Retryable 5xx from any HTTP upstream; a non-5xx first status falls
	// through to the remaining patterns.
	if m := httpStatusRe.FindStringSubmatch(errStr); m != nil {
		switch m[1] {
		case "500", "502", "503", "504":
			return ErrorTypeConnectivity
		}
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
		"request too large",
		"resourceexhausted",
		"resource exhausted",
		"resources exhausted",
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
