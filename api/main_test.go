package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// TestSPAHandlerAgentDiscovery guards the distinction between "this resource
// does not exist" and "this is an SPA route". Agent discovery clients probe for
// files like /robots.txt and /.well-known/mcp.json and treat any 200 as
// support, so falling through to index.html makes every probe look like a pass.
func TestSPAHandlerAgentDiscovery(t *testing.T) {
	dir := t.TempDir()

	index := filepath.Join(dir, "index.html")
	if err := os.WriteFile(index, []byte("<!doctype html><title>app</title>"), 0o644); err != nil {
		t.Fatalf("write index.html: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "robots.txt"), []byte("User-agent: *\nAllow: /\n"), 0o644); err != nil {
		t.Fatalf("write robots.txt: %v", err)
	}

	handler := spaHandler(dir, "")

	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "existing text file is served",
			path:       "/robots.txt",
			wantStatus: http.StatusOK,
			wantBody:   "User-agent: *\nAllow: /\n",
		},
		{
			name:       "missing text file 404s instead of serving the SPA",
			path:       "/llms.txt",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing xml file 404s instead of serving the SPA",
			path:       "/sitemap.xml",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing well-known path 404s instead of serving the SPA",
			path:       "/.well-known/oauth-protected-resource",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing well-known json 404s instead of serving the SPA",
			path:       "/.well-known/some-future-standard.json",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown extensionless route still falls back to the SPA",
			path:       "/topology/map",
			wantStatus: http.StatusOK,
			wantBody:   "<!doctype html><title>app</title>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()

			handler(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status for %s = %d, want %d", tt.path, rec.Code, tt.wantStatus)
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Errorf("body for %s = %q, want %q", tt.path, rec.Body.String(), tt.wantBody)
			}
		})
	}

	t.Run("HEAD on an existing file succeeds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/robots.txt", nil)
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("HEAD /robots.txt = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}

// TestIsDocumentRequest covers which responses carry the discovery Link
// headers. API and well-known paths must not, or agents would follow a Link
// header on a JSON response back to itself.
func TestIsDocumentRequest(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/", true},
		{"/status", true},
		{"/topology/map", true},
		{"/dz/shreds/scoreboard", true},
		{"/api/config", false},
		{"/api/mcp", false},
		{"/.well-known/mcp.json", false},
		{"/robots.txt", false},
		{"/llms.txt", false},
		{"/sitemap.xml", false},
		{"/assets/index-abc123.js", false},
		{"/favicon.ico", false},
	}

	for _, tt := range tests {
		if got := isDocumentRequest(tt.path); got != tt.want {
			t.Errorf("isDocumentRequest(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}
