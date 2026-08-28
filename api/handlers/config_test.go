package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// GetConfig surfaces CARTO's basemap API key to the web bundle, and omits the
// field entirely when nothing is configured so an old bundle sees no change.
func TestGetConfig_CartoAPIKey(t *testing.T) {
	t.Run("surfaced when set", func(t *testing.T) {
		t.Setenv("CARTO_API_KEY", "test-key")

		var cfg PublicConfig
		if err := json.Unmarshal(getConfigBody(t), &cfg); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if cfg.CartoAPIKey != "test-key" {
			t.Errorf("cartoApiKey = %q, want %q", cfg.CartoAPIKey, "test-key")
		}
	})

	t.Run("omitted when unset", func(t *testing.T) {
		t.Setenv("CARTO_API_KEY", "")

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(getConfigBody(t), &raw); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := raw["cartoApiKey"]; ok {
			t.Error("cartoApiKey present with no CARTO_API_KEY set")
		}
	})
}

func getConfigBody(t *testing.T) []byte {
	t.Helper()
	a := &API{}
	rec := httptest.NewRecorder()
	a.GetConfig(rec, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	return rec.Body.Bytes()
}
