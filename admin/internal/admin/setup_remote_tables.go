package admin

import (
	"log/slog"

	"github.com/malbeclabs/lake/admin/remotetables"
)

// SetupRemoteTablesConfig is an alias for remotetables.Config.
type SetupRemoteTablesConfig = remotetables.Config

// SetupRemoteTables delegates to remotetables.Setup.
func SetupRemoteTables(log *slog.Logger, cfg SetupRemoteTablesConfig) error {
	return remotetables.Setup(log, cfg)
}
