package handlers

import (
	"encoding/json"
	"net/http"
)

// VersionResponse contains the API build version info.
type VersionResponse struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

// GetVersion returns the current build version info.
func (a *API) GetVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(VersionResponse{
		Version: a.BuildVersion,
		Commit:  a.BuildCommit,
		Date:    a.BuildDate,
	})
}
