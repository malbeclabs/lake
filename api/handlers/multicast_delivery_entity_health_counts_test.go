package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The windowed health buckets must land exactly where addEntityStatusCount puts
// them, including an unrecognized status: Unknown is the remainder, not a
// counted bucket, so a status the SQL does not name still shows up.
func TestMulticastHealthWindowScanCountsMatchAddEntityStatusCount(t *testing.T) {
	t.Parallel()

	statuses := []string{"healthy", "healthy", "degraded", "unhealthy", "disconnected", "unknown", "", "invented"}

	var want MulticastEntityHealthStatusCounts
	for _, status := range statuses {
		addEntityStatusCount(&want, status, 1)
	}

	got := multicastHealthWindowScan{
		total:        uint64(len(statuses)),
		healthy:      2,
		degraded:     1,
		unhealthy:    1,
		disconnected: 1,
	}.counts()

	require.Equal(t, want, got)
	require.EqualValues(t, 3, got.Unknown, "unknown, empty, and unrecognized statuses all land in Unknown")
}
