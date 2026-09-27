package handlers

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// identifierPattern is the character set every code or key this API filters on
// is drawn from: metro/device/contributor/link codes, link types, statuses,
// user kinds, interface names (which carry "/"), and base58 pubkeys.
var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_.:/-]+$`)

// maxIdentifierLen bounds one value. The longest real code today is a link
// code at 32 characters; pubkeys are 44.
const maxIdentifierLen = 128

// identifierQueryParams are the query parameters that carry comma-separated
// codes or keys on the routes wrapped by RequireIdentifierParams. Free-text
// parameters (search) are deliberately absent.
var identifierQueryParams = []string{
	"metro", "device", "device_a", "device_z", "contributor", "link_type",
	"code", "status", "user_kind", "cyoa_type", "interface_type", "intf",
	"group", "device_pk", "pks",
}

// ValidateIdentifiers checks each value against the identifier allowlist.
// Values are trimmed and empty ones skipped, matching how the query builders
// treat them.
func ValidateIdentifiers(param string, vals []string) error {
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if len(v) > maxIdentifierLen {
			return fmt.Errorf("invalid %s: value longer than %d characters", param, maxIdentifierLen)
		}
		if !identifierPattern.MatchString(v) {
			return fmt.Errorf("invalid %s: unsupported characters", param)
		}
	}
	return nil
}

// RequireIdentifierParams rejects a request with 400 when any identifier query
// parameter holds a value outside the allowlist. It sits in front of handlers
// that still build SQL by string interpolation, as a second line behind
// escapeSingleQuote.
func RequireIdentifierParams(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for _, name := range identifierQueryParams {
			for _, raw := range q[name] {
				if err := ValidateIdentifiers(name, strings.Split(raw, ",")); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}
