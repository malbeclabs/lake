// Package v1 defines the public, versioned /api/v1 surface. Operations are
// declared with huma, so the OpenAPI spec is generated from handler
// signatures (drift-free). The v1 contract is intentionally decoupled from
// internal/UI types so it can evolve independently.
package v1

import (
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	"github.com/malbeclabs/lake/api/handlers"
)

// legacyRedirects maps deprecated v1 paths (relative to BasePath) to their
// current canonical paths. Entries should be removed once consumers have
// migrated. Keep this list short and well-commented — each entry is tech debt.
var legacyRedirects = map[string]string{
	// Pre-namespace path. Moved under /solana/ for consistency with other
	// Solana-scoped endpoints.
	"/validators-metadata": "/solana/validators-metadata",
}

// redocHTML is a minimal Redoc shell that loads the v1 OpenAPI spec.
// Redoc isn't bundled with huma, so we disable huma's docs route and serve
// our own page here. The spec URL is absolute so the page works regardless
// of how the host is reached.
const redocHTML = `<!DOCTYPE html>
<html>
<head>
<title>DoubleZero Data API</title>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body { margin: 0; padding: 0; }</style>
</head>
<body>
<redoc spec-url="` + BasePath + `/openapi.json"></redoc>
<script src="https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js"></script>
</body>
</html>`

// Version is the v1 API version reported in the OpenAPI spec.
const Version = "1.0.0"

// BasePath is the URL prefix under which all v1 operations are mounted.
const BasePath = "/api/v1"

// Mount mounts the v1 huma API (operations + docs + OpenAPI spec) on the
// given chi router under BasePath. The OpenAPI spec is served at
// {BasePath}/openapi.[json|yaml], docs at {BasePath}/docs, and JSON schemas
// at {BasePath}/schemas/{schema}.
func Mount(r chi.Router, api *handlers.API) huma.API {
	var humaAPI huma.API
	r.Route(BasePath, func(r chi.Router) {
		config := huma.DefaultConfig("DoubleZero Data API", Version)
		config.Info.Description = "Public API for DoubleZero network telemetry and the shred subscription program."
		config.OpenAPIPath = "/openapi"
		config.SchemasPath = "/schemas"
		// DocsPath is empty so huma doesn't register its built-in docs route —
		// we serve Redoc ourselves below.
		config.DocsPath = ""
		config.Servers = []*huma.Server{{URL: BasePath}}

		humaAPI = humachi.New(r, config)

		// Scoped CSP for the docs page: allows just what Redoc needs from
		// jsdelivr + Google Fonts. This intentionally overrides the app-wide
		// CSP set in main.go (which whitelists a different set of CDNs for
		// the web app) — huma's built-in renderers do the same trick.
		docsCSP := strings.Join([]string{
			"default-src 'none'",
			"base-uri 'none'",
			"connect-src 'self'",
			"script-src 'unsafe-eval' https://cdn.jsdelivr.net",
			"style-src 'unsafe-inline' https://fonts.googleapis.com",
			"font-src https://fonts.gstatic.com",
			"img-src 'self' data: blob: https:",
			"worker-src blob:",
			"frame-ancestors 'none'",
			"form-action 'none'",
		}, "; ")
		r.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Security-Policy", docsCSP)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(redocHTML))
		})

		registerShredsPublishers(humaAPI, api)
		registerShredsSubscribers(humaAPI, api)
		registerValidatorsMetadata(humaAPI, api)

		// Register 308 redirects from deprecated paths to their current
		// canonical paths. These keep existing consumers working while the
		// OpenAPI spec only advertises the new paths.
		for from, to := range legacyRedirects {
			target := BasePath + to
			r.Get(from, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target, http.StatusPermanentRedirect)
			})
		}
	})
	return humaAPI
}
