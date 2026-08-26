package handlers_test

import (
	"context"
	"testing"
	"time"

	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/require"
)

// TestPageCacheAges runs the query behind the page-cache refresh cadence against a
// real Postgres. The gate is driven entirely by this result and treats a missing key
// as due, so an array-binding or query fault reverts every cadenced entry to
// every-cycle refresh — the cost the cadence exists to remove.
//
// Must NOT run in parallel: SetupPostgresForTest gives no per-test isolation, hence
// the test-only keys.
func TestPageCacheAges(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)

	const (
		present1 = "test_page_cache_ages:a"
		present2 = "test_page_cache_ages:b"
		absent   = "test_page_cache_ages:missing"
	)
	t.Cleanup(func() {
		for _, key := range []string{present1, present2, absent} {
			// Single-key deletes, not the ANY($1) shape under test, so a broken
			// binding cannot also leak these rows into later tests. context.Background
			// because the test context is already canceled during cleanup.
			_, _ = api.PgPool.Exec(context.Background(), `DELETE FROM page_cache WHERE key = $1`, key)
		}
	})

	before := time.Now().UTC()
	require.NoError(t, api.WritePageCache(t.Context(), present1, map[string]int{"n": 1}))
	require.NoError(t, api.WritePageCache(t.Context(), present2, map[string]int{"n": 2}))

	ages, err := api.PageCacheAges(t.Context(), []string{present1, present2, absent})
	require.NoError(t, err)
	require.Len(t, ages, 2)
	require.NotContains(t, ages, absent,
		"a never-written key must be absent, not zero — absence is what makes an entry due")
	for _, key := range []string{present1, present2} {
		require.Contains(t, ages, key)
		require.WithinDuration(t, before, ages[key], time.Hour,
			"%s: updated_at must come back as the row's real write time", key)
	}

	// A key the caller did not ask for is never returned, so one entry's cadence
	// cannot be decided by another entry's age.
	ages, err = api.PageCacheAges(t.Context(), []string{present1})
	require.NoError(t, err)
	require.Len(t, ages, 1)
	require.Contains(t, ages, present1)

	// Empty input must not error: nothing in the signature stops a caller passing it.
	ages, err = api.PageCacheAges(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, ages)
}
