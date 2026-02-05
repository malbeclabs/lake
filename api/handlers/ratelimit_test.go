package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestRateLimiter_Allow(t *testing.T) {
	// Create a limiter that allows 5 requests per second with burst of 5
	limiter := handlers.NewRateLimiter(rate.Limit(5), 5)

	ip := "192.168.1.1"

	// First 5 requests should be allowed (burst)
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow(ip), "request %d should be allowed", i+1)
	}

	// 6th request should be denied (burst exhausted)
	assert.False(t, limiter.Allow(ip), "request 6 should be denied")

	// Different IP should have its own limit
	otherIP := "192.168.1.2"
	assert.True(t, limiter.Allow(otherIP), "different IP should be allowed")
}

func TestRateLimiter_Refill(t *testing.T) {
	// Create a limiter that allows 10 requests per second with burst of 2
	limiter := handlers.NewRateLimiter(rate.Limit(10), 2)

	ip := "192.168.1.1"

	// Exhaust burst
	assert.True(t, limiter.Allow(ip))
	assert.True(t, limiter.Allow(ip))
	assert.False(t, limiter.Allow(ip))

	// Wait for token to refill (100ms = 1 token at 10/sec)
	time.Sleep(150 * time.Millisecond)

	// Should be allowed again
	assert.True(t, limiter.Allow(ip), "should be allowed after refill")
}

func TestQueryRateLimiter_Exists(t *testing.T) {
	// Verify the global rate limiter is initialized
	assert.NotNil(t, handlers.QueryRateLimiter)

	// Should allow requests (generous limit)
	assert.True(t, handlers.QueryRateLimiter.Allow("test-ip"))
}

func TestCheckRateLimit(t *testing.T) {
	limiter := handlers.NewRateLimiter(rate.Limit(5), 2)
	ip := "192.168.1.100"

	// First two requests should be allowed
	assert.Empty(t, handlers.CheckRateLimit(limiter, ip))
	assert.Empty(t, handlers.CheckRateLimit(limiter, ip))

	// Third request should be rate limited with retry message
	errMsg := handlers.CheckRateLimit(limiter, ip)
	assert.NotEmpty(t, errMsg)
	assert.Contains(t, errMsg, "rate limit exceeded")
	assert.Contains(t, errMsg, "try again in")
}

func TestRateLimitMiddleware_JSONResponse(t *testing.T) {
	limiter := handlers.NewRateLimiter(rate.Limit(1), 1)

	middleware := handlers.RateLimitMiddleware(limiter)
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request should pass
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "192.168.1.50:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Second request should be rate limited
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))

	// Verify JSON response
	var errResp handlers.RateLimitError
	err := json.NewDecoder(rec.Body).Decode(&errResp)
	assert.NoError(t, err)
	assert.Equal(t, "rate_limit_exceeded", errResp.Error)
	assert.NotEmpty(t, errResp.Message)
	assert.Greater(t, errResp.RetryAfter, 0)
}
