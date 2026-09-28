package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A pooled connection older than ClickHouse Cloud's ~5m idle close is dead on reuse.
func TestNewOptionsCapsConnMaxLifetime(t *testing.T) {
	require.Equal(t, 3*time.Minute, newOptions("addr", "db", "user", "pass", true).ConnMaxLifetime)
}
