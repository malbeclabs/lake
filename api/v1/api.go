// Package v1 defines the public, versioned /api/v1 surface. Operations are
// declared with huma, so the OpenAPI spec is generated from handler
// signatures (drift-free). The v1 contract is intentionally decoupled from
// internal/UI types so it can evolve independently.
package v1

import (
	"net/http"

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

// Mount mounts the v1 huma API (operations + docs + OpenAPI spec) on the
// given chi router under BasePath. The OpenAPI spec is served at
// {BasePath}/openapi.[json|yaml], docs at {BasePath}/docs, and JSON schemas
// at {BasePath}/schemas/{schema}.
func Mount(r chi.Router, api *handlers.API) huma.API {
	var humaAPI huma.API
	r.Route(BasePath, func(r chi.Router) {
		config := huma.DefaultConfig("DoubleZero Data API", Version)
		config.Info.Description = "Public API for DoubleZero Data — the analytics platform for the DoubleZero network. Exposes data on network telemetry, Solana validators, and the shred subscription program."
		config.OpenAPIPath = "/openapi"
		config.DocsPath = "/docs"
		config.SchemasPath = "/schemas"
		config.DocsRenderer = huma.DocsRendererScalar
		config.Servers = []*huma.Server{{URL: BasePath}}

		humaAPI = humachi.New(r, config)

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
