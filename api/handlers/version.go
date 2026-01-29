package handlers

import (
	"encoding/json"
	"net/http"
)

var (
	// BuildVersion, BuildCommit, BuildDate are set from main via SetBuildInfo.
	BuildVersion = "dev"
	BuildCommit  = "none"
	BuildDate    = "unknown"
)

// SetBuildInfo sets the build info from ldflags values in main.
func SetBuildInfo(version, commit, date string) {
	BuildVersion = version
	BuildCommit = commit
	BuildDate = date
}

// VersionResponse contains the API build version info.
type VersionResponse struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

// GetVersion returns the current build version info.
func GetVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(VersionResponse{
		Version: BuildVersion,
		Commit:  BuildCommit,
		Date:    BuildDate,
	})
}
