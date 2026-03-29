package server

import (
	"errors"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/indexer"
)

// VersionInfo contains build-time version information.
type VersionInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

type Config struct {
	Log               *slog.Logger
	ListenAddr        string
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
	VersionInfo       VersionInfo
	Indexer           *indexer.Indexer
}

func (cfg *Config) Validate() error {
	if cfg.ListenAddr == "" {
		return errors.New("listen addr is required")
	}
	if cfg.Indexer == nil {
		return errors.New("indexer is required")
	}
	return nil
}
