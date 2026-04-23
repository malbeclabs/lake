package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apitesting "github.com/malbeclabs/lake/api/testing"
)

// TestV1OpenAPI_Spec verifies the OpenAPI spec is served and advertises
// every operation we expect. New v1 endpoints should be added here so a
// missing registration fails loudly.
func TestV1OpenAPI_Spec(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/openapi.json", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var spec struct {
		OpenAPI string                          `json:"openapi"`
		Info    struct{ Title, Version string } `json:"info"`
		Servers []struct{ URL string }          `json:"servers"`
		Paths   map[string]any                  `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &spec))

	assert.NotEmpty(t, spec.OpenAPI, "openapi version must be set")
	assert.Equal(t, "DoubleZero Data API", spec.Info.Title)
	require.Len(t, spec.Servers, 1)
	assert.Equal(t, "/api/v1", spec.Servers[0].URL)

	expected := []string{
		"/shreds/publishers/leaders",
		"/shreds/subscribers",
		"/solana/validators-metadata",
	}
	for _, p := range expected {
		_, ok := spec.Paths[p]
		assert.True(t, ok, "OpenAPI spec must advertise %s (all v1 ops must be registered)", p)
	}
}

func TestV1Docs_Served(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/docs", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "text/html")
}
