// Package redact removes credentials from URLs in logs and error messages.
//
// It targets three credential shapes that show up in RPC URLs:
//   - basic auth: https://user:pass@host/path
//   - sensitive query params: ?api-key=xxx, ?token=xxx, ?key=xxx, etc.
//   - path-embedded tokens: https://host/db336024-e7a8-46b1-80e5-352dd77060ab
//     (Triton/RPCPool style UUID, QuickNode-style long alphanumeric, etc.)
//
// Use [URL] for a single URL string, [String] for arbitrary text that may
// contain URLs (error messages, log lines), and [Error] as a convenience
// wrapper for errors.
package redact

import (
	"net/url"
	"regexp"
	"strings"
)

// sensitiveQueryKeys are query-parameter names whose values are redacted.
// Compared case-insensitively.
var sensitiveQueryKeys = map[string]struct{}{
	"api-key":   {},
	"api_key":   {},
	"apikey":    {},
	"auth":      {},
	"authkey":   {},
	"key":       {},
	"pass":      {},
	"password":  {},
	"secret":    {},
	"sig":       {},
	"signature": {},
	"token":     {},
	"x-api-key": {},
}

// tokenSegment matches path segments that look like API tokens. The intent is
// to catch UUIDs and long opaque hex/alphanumeric blobs while leaving normal
// path components (api, v1, mainnet-beta, getValidators) alone.
//
// Patterns:
//   - UUID:           [hex]{8}-[hex]{4}-[hex]{4}-[hex]{4}-[hex]{12}
//   - long hex:       [hex]{16,}      (no other characters)
//   - long alphanum:  [a-zA-Z0-9]{24,} (no dashes, underscores, or dots)
var tokenSegment = regexp.MustCompile(
	`^(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{16,}|[A-Za-z0-9]{24,})$`,
)

// urlFinder finds URL-shaped substrings inside arbitrary text. The scheme
// pattern matches anything URL-shaped (http, https, postgres, redis, mongodb,
// etc.), since credentials can ride on any of them. Trailing sentence
// punctuation is trimmed by the caller.
var urlFinder = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.\-]*://[^\s'"<>\\\[\]]+`)

// trailingPunct is stripped from the tail of regex matches before they are
// fed to [URL]. urlFinder accepts these characters mid-URL but they are
// almost always sentence punctuation when at the end.
const trailingPunct = ".,;:!?)]}>\"'"

// URL redacts credentials from a single URL string. Inputs that don't parse
// as a URL with a scheme and host are returned unchanged.
func URL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return raw
	}

	// Strip any user info (basic auth).
	if u.User != nil {
		u.User = url.User("redacted")
	}

	// Redact known-sensitive query parameters in place.
	if u.RawQuery != "" {
		q := u.Query()
		changed := false
		for k, vs := range q {
			if _, ok := sensitiveQueryKeys[strings.ToLower(k)]; !ok {
				continue
			}
			for i := range vs {
				if vs[i] != "" {
					vs[i] = "redacted"
					changed = true
				}
			}
		}
		if changed {
			u.RawQuery = q.Encode()
		}
	}

	// Redact token-shaped path segments (UUIDs, long opaque blobs).
	if u.Path != "" && u.Path != "/" {
		parts := strings.Split(u.Path, "/")
		for i, p := range parts {
			if p != "" && tokenSegment.MatchString(p) {
				parts[i] = "redacted"
			}
		}
		u.Path = strings.Join(parts, "/")
	}

	return u.String()
}

// String finds URLs in s and redacts credentials from each. Non-URL text is
// returned unchanged.
func String(s string) string {
	if !strings.Contains(s, "://") {
		return s
	}
	return urlFinder.ReplaceAllStringFunc(s, func(match string) string {
		// Peel off trailing sentence punctuation so it survives unredacted.
		tail := ""
		for len(match) > 0 && strings.ContainsRune(trailingPunct, rune(match[len(match)-1])) {
			tail = string(match[len(match)-1]) + tail
			match = match[:len(match)-1]
		}
		return URL(match) + tail
	})
}

// Error returns String(err.Error()) or "" if err is nil.
func Error(err error) string {
	if err == nil {
		return ""
	}
	return String(err.Error())
}
