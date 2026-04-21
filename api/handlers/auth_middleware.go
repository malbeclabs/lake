package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
)

// InternalAuthDisabled controls whether RequireInternalDomain middleware is
// bypassed. Set from main() via SetInternalAuthDisabled after the build
// version and environment are known.
var InternalAuthDisabled bool

// SetInternalAuthDisabled determines whether internal auth should be bypassed.
// It is called from main() after the build version is known. Auth is bypassed
// when DISABLE_INTERNAL_AUTH=true, or when the build version indicates a
// non-release build (branch/PR deploys where OAuth redirects are not configured).
func SetInternalAuthDisabled(buildVersion string) {
	if override := os.Getenv("BUILD_VERSION"); override != "" {
		buildVersion = override
	}
	explicit := os.Getenv("DISABLE_INTERNAL_AUTH") == "true"
	isRelease := buildVersion == "staging" ||
		buildVersion == "main" ||
		(len(buildVersion) > 0 && buildVersion[0] == 'v')
	InternalAuthDisabled = explicit || !isRelease
	slog.Info("internal auth bypass decision",
		"disabled", InternalAuthDisabled,
		"explicit", explicit,
		"version", buildVersion,
		"is_release", isRelease,
	)
}

// Context keys for auth
type contextKey string

const (
	accountContextKey contextKey = "account"
	ipContextKey      contextKey = "client_ip"
)

// OptionalAuth middleware attaches user to context if authenticated, allows anonymous
func (a *API) OptionalAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Extract and store IP address
		ip := GetIPFromRequest(r)
		ctx = context.WithValue(ctx, ipContextKey, ip)

		// Try to authenticate
		token := extractBearerToken(r)
		if token != "" {
			account, err := a.GetAccountByToken(ctx, token)
			if err == nil && account != nil {
				ctx = context.WithValue(ctx, accountContextKey, account)
			} else if err != nil {
				slog.Debug("OptionalAuth: failed to get account by token", "error", err)
			}
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireAuth middleware returns 401 if not authenticated
func (a *API) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Extract and store IP address
		ip := GetIPFromRequest(r)
		ctx = context.WithValue(ctx, ipContextKey, ip)

		// Try to authenticate
		token := extractBearerToken(r)
		if token == "" {
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		account, err := a.GetAccountByToken(ctx, token)
		if err != nil || account == nil {
			http.Error(w, "Invalid or expired token", http.StatusUnauthorized)
			return
		}

		ctx = context.WithValue(ctx, accountContextKey, account)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireInternalDomain middleware returns 403 unless the user is authenticated
// with an email from an allowed domain (AUTH_ALLOWED_DOMAINS).
// Bypassed when InternalAuthDisabled is true (non-production only).
func RequireInternalDomain(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if InternalAuthDisabled {
			next.ServeHTTP(w, r)
			return
		}
		account := GetAccountFromContext(r.Context())
		if account == nil || !account.IsInternalUser {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// GetAccountFromContext returns the account from context, or nil if not authenticated
func GetAccountFromContext(ctx context.Context) *Account {
	account, ok := ctx.Value(accountContextKey).(*Account)
	if !ok {
		return nil
	}
	return account
}

// GetIPFromContext returns the IP from context
func GetIPFromContext(ctx context.Context) string {
	ip, ok := ctx.Value(ipContextKey).(string)
	if !ok {
		return ""
	}
	return ip
}

// GetIPFromRequest extracts the client IP from request
func GetIPFromRequest(r *http.Request) string {
	// Check X-Forwarded-For header (from proxies/load balancers)
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		// Take the first IP in the list
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}

	// Check X-Real-IP header
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// SetQuotaHeaders sets the rate limit headers on response
func SetQuotaHeaders(w http.ResponseWriter, quota *QuotaInfo) {
	if quota == nil {
		return
	}

	if quota.Limit != nil {
		w.Header().Set("X-RateLimit-Limit", itoa(*quota.Limit))
	}

	if quota.Remaining != nil {
		w.Header().Set("X-RateLimit-Remaining", itoa(*quota.Remaining))
	}

	// Parse ResetsAt and convert to Unix timestamp
	if quota.ResetsAt != "" {
		w.Header().Set("X-RateLimit-Reset", quota.ResetsAt)
	}
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

// SetAccountInContext is a test helper that adds an account to the context.
// This is used for testing handlers without going through the auth middleware.
func SetAccountInContext(ctx context.Context, account *Account) context.Context {
	return context.WithValue(ctx, accountContextKey, account)
}
