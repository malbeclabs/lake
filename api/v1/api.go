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

// Version is the v1 API version reported in the OpenAPI spec.
const Version = "1.0.0"

// BasePath is the URL prefix under which all v1 operations are mounted.
const BasePath = "/api/v1"

// scalarBundleURL is pinned to match the version huma bundles; keep in sync
// when upgrading huma. Integrity hash is taken from huma's Scalar renderer.
const scalarBundleURL = "https://unpkg.com/@scalar/api-reference@1.44.20/dist/browser/standalone.js"
const scalarBundleSRI = "sha384-tMz7GAo6dMy55x9tLFtH+sHtogji6Scmb+feBR31TAHmvSPRUTboK9H3M5NFaP4R"

// docsHTML is a minimal Scalar shell. We serve this ourselves (instead of
// huma's built-in docs renderer) so we can include an explicit favicon link
// and a CSP permissive enough to load it — huma's default CSP blocks images.
const docsHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="referrer" content="no-referrer">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" href="/favicon.ico">
    <title>DoubleZero Data API</title>
  </head>
  <body>
    <script id="api-reference" data-url="` + BasePath + `/openapi.json"></script>
    <script src="` + scalarBundleURL + `" crossorigin integrity="` + scalarBundleSRI + `"></script>
  </body>
</html>`

// Mount mounts the v1 huma API (operations + OpenAPI spec + docs) on the
// given chi router under BasePath. The OpenAPI spec is served at
// {BasePath}/openapi.[json|yaml], docs at {BasePath}/docs, and JSON schemas
// at {BasePath}/schemas/{schema}.
func Mount(r chi.Router, api *handlers.API) huma.API {
	var humaAPI huma.API
	r.Route(BasePath, func(r chi.Router) {
		config := huma.DefaultConfig("DoubleZero Data API", Version)
		config.Info.Description = "Public API for DoubleZero Data — the analytics platform for the DoubleZero network. Exposes data on network telemetry, Solana validators, and the shred subscription program."
		config.Tags = append(config.Tags, &huma.Tag{
			Name:        "Rate Limits",
			Description: "Requests are rate limited to 50 per minute per IP. On `429 Too Many Requests` responses, honor the `Retry-After` header.",
		})
		config.OpenAPIPath = "/openapi"
		config.SchemasPath = "/schemas"
		// DocsPath is empty so huma doesn't register its built-in docs route —
		// we serve Scalar ourselves below so we can include a favicon.
		config.DocsPath = ""
		config.Servers = []*huma.Server{{URL: BasePath}}

		humaAPI = humachi.New(r, config)

		// Scoped CSP for the docs page: mirrors huma's built-in Scalar CSP
		// but adds img-src so the favicon can load. Overrides the app-wide
		// CSP set in main.go — huma's built-in renderers do the same trick.
		docsCSP := strings.Join([]string{
			"default-src 'none'",
			"base-uri 'none'",
			"connect-src 'self'",
			"img-src 'self' data:",
			"form-action 'none'",
			"frame-ancestors 'none'",
			"sandbox allow-same-origin allow-scripts allow-popups allow-popups-to-escape-sandbox allow-downloads",
			"script-src 'unsafe-eval' " + scalarBundleURL,
			"style-src 'unsafe-inline'",
		}, "; ")
		r.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Security-Policy", docsCSP)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(docsHTML))
		})

		registerEdgeShredsPublishers(humaAPI, api)
		registerEdgeShredsSubscribers(humaAPI, api)
		registerValidatorsMetadata(humaAPI, api)
		registerDZLinksLatency(humaAPI, api)
		registerDZMetroPairsLatency(humaAPI, api)

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
