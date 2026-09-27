package handlers

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// scanClickHouseLiteral reads a single-quoted ClickHouse string literal from
// the start of s and returns its decoded value and the byte length consumed.
// A backslash escapes the next byte and a doubled quote is a literal quote, as
// in ClickHouse.
func scanClickHouseLiteral(t *testing.T, s string) (string, int) {
	t.Helper()
	if !strings.HasPrefix(s, "'") {
		t.Fatalf("literal %q does not start with a quote", s)
	}
	var out strings.Builder
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 >= len(s) {
				t.Fatalf("literal %q ends inside an escape", s)
			}
			i++
			out.WriteByte(s[i])
		case '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				i++
				out.WriteByte('\'')
				continue
			}
			return out.String(), i + 1
		default:
			out.WriteByte(s[i])
		}
	}
	t.Fatalf("literal %q is never closed", s)
	return "", 0
}

func TestEscapeSingleQuote_LiteralCannotTerminateEarly(t *testing.T) {
	cases := map[string]string{
		"plain":                 "ams",
		"single quote":          "o'hare",
		"trailing backslash":    `ams\`,
		"backslash then quote":  `ams\'x`,
		"only backslashes":      `\\\`,
		"quote then backslash":  `'\`,
		"doubled quote":         "a''b",
		"backslash at both end": `\a\`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			lit := "'" + escapeSingleQuote(in) + "'"
			got, n := scanClickHouseLiteral(t, lit)
			if n != len(lit) {
				t.Fatalf("literal for %q closed at byte %d of %d: %q", in, n, len(lit), lit)
			}
			if got != in {
				t.Fatalf("literal for %q decodes to %q", in, got)
			}
		})
	}
}

func TestQuoteCSV_EachValueStaysInItsLiteral(t *testing.T) {
	in := []string{`a\`, "b", `c\'`}
	rest := quoteCSV(strings.Join(in, ","))
	for i, want := range in {
		got, n := scanClickHouseLiteral(t, rest)
		if got != want {
			t.Fatalf("value %d decodes to %q, want %q", i, got, want)
		}
		rest = rest[n:]
		if i < len(in)-1 {
			if !strings.HasPrefix(rest, ",") {
				t.Fatalf("value %d not followed by a separator: %q", i, rest)
			}
			rest = rest[1:]
		}
	}
	if rest != "" {
		t.Fatalf("unexpected trailing text %q", rest)
	}
}

func TestValidateIdentifiers(t *testing.T) {
	valid := []string{
		"ams", "ams001-dz001", "ams001-dz001:fra001-dz001", "Ethernet1/1", "Switch1/12/3",
		"soft-drained", "gre_over_dia", "ripe-atlas", "4uQeVj5tqViQh7yWWGStvkEG1Zmhx6uasJtWCJziofM",
		" fra ", "",
	}
	if err := ValidateIdentifiers("p", valid); err != nil {
		t.Fatalf("valid values rejected: %v", err)
	}

	invalid := map[string]string{
		"quote":              "ams'",
		"trailing backslash": `ams\`,
		"space inside":       "new york",
		"parenthesis":        "ams)",
		"too long":           strings.Repeat("a", maxIdentifierLen+1),
	}
	for name, v := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := ValidateIdentifiers("p", []string{"ams", v}); err == nil {
				t.Fatalf("value %q accepted", v)
			}
		})
	}
}

func TestRequireIdentifierParams(t *testing.T) {
	var reached bool
	h := RequireIdentifierParams(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))

	cases := []struct {
		name   string
		query  url.Values
		status int
	}{
		{"no params", url.Values{}, http.StatusOK},
		{"valid list", url.Values{"metro": {"ams,fra"}, "intf": {"Ethernet1/1"}}, http.StatusOK},
		{"free-text search is not checked", url.Values{"search": {"o'hare"}}, http.StatusOK},
		{"invalid element in list", url.Values{"device": {`ams001,ams002\`}}, http.StatusBadRequest},
		{"invalid repeated value", url.Values{"group": {"ok", "bad'"}}, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reached = false
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x?"+tc.query.Encode(), nil))
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d", rec.Code, tc.status)
			}
			if reached != (tc.status == http.StatusOK) {
				t.Fatalf("handler reached = %v", reached)
			}
		})
	}
}
