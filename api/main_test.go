package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSpaHandler(t *testing.T) {
	dir := t.TempDir()

	mustWrite := func(rel, body string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	mustWrite("index.html", "<html>spa</html>")
	mustWrite("robots.txt", "User-agent: *\n")
	mustWrite("assets/app.js", "console.log(1)")
	mustWrite(".well-known/mcp/server-card.json", `{"ok":true}`)

	h := spaHandler(dir, "")

	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{"existing file: robots.txt", "/robots.txt", 200, "User-agent: *\n"},
		{"existing file: hashed asset", "/assets/app.js", 200, "console.log(1)"},
		{"existing file: well-known JSON", "/.well-known/mcp/server-card.json", 200, `{"ok":true}`},
		{"SPA route falls back to index.html", "/status", 200, "<html>spa</html>"},
		{"deep SPA route falls back to index.html", "/dz/devices/ABC123", 200, "<html>spa</html>"},
		{"missing whitelisted asset 404s", "/assets/missing.js", 404, ""},
		{"missing well-known with extension 404s", "/.well-known/agent-skills/index.json", 404, ""},
		{"missing well-known WITHOUT extension 404s", "/.well-known/api-catalog", 404, ""},
		{"missing extensionless well-known sub-path 404s", "/.well-known/openid-configuration", 404, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			h(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %q)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}
