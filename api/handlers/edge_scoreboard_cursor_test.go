package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCachedCoversCursor pins the guard that keeps the live tail from silently
// losing slots. The client seeds its cursor from the 24h blob, which is now on a
// refresh cadence, so the cursor can start further back than the fast-refreshed
// latest payload covers. Serving that payload anyway returns only the slots it
// happens to hold; the client advances past the rest and they are never delivered.
func TestCachedCoversCursor(t *testing.T) {
	t.Parallel()

	// Unsorted on purpose: the cached payload is not ordered.
	slots := []EdgeScoreboardSlotRace{{Slot: 1005}, {Slot: 1000}, {Slot: 1009}, {Slot: 1003}}

	require.False(t, cachedCoversCursor(slots, 998),
		"a cursor two slots behind the payload would skip 999 and 1000")
	require.True(t, cachedCoversCursor(slots, 999),
		"the boundary case: the next slot after the cursor is the payload's oldest")
	require.True(t, cachedCoversCursor(slots, 1004))
	require.True(t, cachedCoversCursor(slots, 1009), "caught up is covered")

	require.False(t, cachedCoversCursor(nil, 500),
		"an empty payload proves no coverage, so it must not answer a cursor poll")
}
