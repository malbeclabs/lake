package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimitError is returned when rate limit is exceeded.
type RateLimitError struct {
	Error      string `json:"error"`
	Message    string `json:"message"`
	RetryAfter int    `json:"retry_after"` // seconds
}

// RateLimiter provides per-IP rate limiting for database queries.
type RateLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rateLimiterEntry
	rate     rate.Limit
	burst    int
	cleanup  time.Duration
}

type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewRateLimiter creates a rate limiter with the specified rate (requests per second) and burst size.
// For example, NewRateLimiter(rate.Every(time.Minute/100), 10) allows 100 requests/minute with burst of 10.
func NewRateLimiter(r rate.Limit, burst int) *RateLimiter {
	rl := &RateLimiter{
		limiters: make(map[string]*rateLimiterEntry),
		rate:     r,
		burst:    burst,
		cleanup:  5 * time.Minute,
	}
	go rl.cleanupLoop()
	return rl
}

// Allow checks if a request from the given IP is allowed.
func (rl *RateLimiter) Allow(ip string) bool {
	allowed, _ := rl.AllowWithRetry(ip)
	return allowed
}

// AllowWithRetry checks if a request is allowed and returns time until next token if not.
func (rl *RateLimiter) AllowWithRetry(ip string) (allowed bool, retryAfter time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry, exists := rl.limiters[ip]
	if !exists {
		entry = &rateLimiterEntry{
			limiter:  rate.NewLimiter(rl.rate, rl.burst),
			lastSeen: time.Now(),
		}
		rl.limiters[ip] = entry
	}
	entry.lastSeen = time.Now()

	// Try to reserve a token
	reservation := entry.limiter.Reserve()
	if !reservation.OK() {
		return false, time.Minute // fallback
	}

	delay := reservation.Delay()
	if delay > 0 {
		// Can't get token now, cancel reservation and return delay
		reservation.Cancel()
		return false, delay
	}

	return true, 0
}

// cleanupLoop removes stale entries periodically.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanup)
	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-rl.cleanup)
		for ip, entry := range rl.limiters {
			if entry.lastSeen.Before(cutoff) {
				delete(rl.limiters, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// QueryRateLimiter is the shared rate limiter for database queries.
// Allows 100 queries per minute per IP with a burst of 20.
var QueryRateLimiter = NewRateLimiter(rate.Every(time.Minute/100), 20)

// RateLimitMiddleware creates HTTP middleware that rate limits requests using the given limiter.
func RateLimitMiddleware(limiter *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := GetIPFromRequest(r)
			allowed, retryAfter := limiter.AllowWithRetry(ip)
			if !allowed {
				retrySeconds := int(retryAfter.Seconds())
				if retrySeconds < 1 {
					retrySeconds = 1
				}

				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retrySeconds))
				w.WriteHeader(http.StatusTooManyRequests)

				_ = json.NewEncoder(w).Encode(RateLimitError{
					Error:      "rate_limit_exceeded",
					Message:    "Too many requests. Please slow down.",
					RetryAfter: retrySeconds,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// QueryRateLimitMiddleware is middleware that uses the shared query rate limiter.
var QueryRateLimitMiddleware = RateLimitMiddleware(QueryRateLimiter)

// CheckRateLimit checks the rate limit and returns an error message if exceeded.
// Returns empty string if allowed, or error message with retry time if not.
func CheckRateLimit(limiter *RateLimiter, ip string) string {
	allowed, retryAfter := limiter.AllowWithRetry(ip)
	if allowed {
		return ""
	}
	retrySeconds := int(retryAfter.Seconds())
	if retrySeconds < 1 {
		retrySeconds = 1
	}
	return fmt.Sprintf("rate limit exceeded, please try again in %d seconds", retrySeconds)
}
