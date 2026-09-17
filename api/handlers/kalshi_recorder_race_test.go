package handlers_test

import (
	"testing"

	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorder race reads `kalshi_book_race`, a view this repository does not create — it is
// the recorder's own, three views deep over two tables in malbeclabs/kalshi. So the query
// itself is verified against the live store rather than a fixture, and what is pinned here is
// the behaviour an environment without it must have.

// **An absent view is an empty payload and not an error.** Every other leg of this page falls
// back to a live query; this one has nothing to fall back to, so a hard error here would take
// the whole scoreboard down in any environment where the recorder does not write — which is
// every local one, and every test that does not create the view.
func TestFetchKalshiRecorderRaceWithoutTheViewIsEmptyAndNotAnError(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	got, err := api.FetchKalshiRecorderRace(t.Context())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, got.Sites)
	// Stated even when empty: the consumer renders the window in its own caption, and a zero
	// there would read as "no window" rather than as "no sites".
	assert.Equal(t, 15, got.WindowMinutes)
	assert.False(t, got.GeneratedAt.IsZero())
}
