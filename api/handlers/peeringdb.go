package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

const peeringDBCacheTTL = 24 * time.Hour

type peeringDBFacility struct {
	OrgName string `json:"orgName"`
	Aka     string `json:"aka"`
	LogoURL string `json:"logoUrl"`
}

type peeringDBCacheEntry struct {
	facility  peeringDBFacility
	fetchedAt time.Time
}

var (
	peeringDBClient = &http.Client{Timeout: 10 * time.Second}
	peeringDBCache  sync.Map // map[string]peeringDBCacheEntry
)

func (a *API) GetPeeringDBFacility(w http.ResponseWriter, r *http.Request) {
	locID := chi.URLParam(r, "loc_id")

	if v, ok := peeringDBCache.Load(locID); ok {
		entry := v.(peeringDBCacheEntry)
		if time.Since(entry.fetchedAt) < peeringDBCacheTTL {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "public, max-age=86400")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(entry.facility); err != nil {
				logError("failed to encode peeringdb response", "error", err)
			}
			return
		}
	}

	resp, err := peeringDBClient.Get("https://www.peeringdb.com/api/fac/" + locID)
	if err != nil {
		http.Error(w, "failed to fetch from PeeringDB", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "failed to read PeeringDB response", http.StatusBadGateway)
		return
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		http.Error(w, "failed to parse PeeringDB response", http.StatusBadGateway)
		return
	}

	var fac map[string]any
	if data, ok := raw["data"].([]any); ok && len(data) > 0 {
		fac, _ = data[0].(map[string]any)
	}

	facility := peeringDBFacility{}
	if fac != nil {
		if org, ok := fac["org"].(map[string]any); ok {
			facility.OrgName, _ = org["name"].(string)
			facility.LogoURL, _ = org["logo"].(string)
		}
		facility.Aka, _ = fac["aka"].(string)
	}

	peeringDBCache.Store(locID, peeringDBCacheEntry{facility: facility, fetchedAt: time.Now()})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	if err := json.NewEncoder(w).Encode(facility); err != nil {
		logError("failed to encode peeringdb response", "error", err)
	}
}
