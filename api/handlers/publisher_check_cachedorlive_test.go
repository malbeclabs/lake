package handlers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFetchPublisherCheckCachedOrLive_LiveDeadline verifies the live fallback
// runs under a bounded deadline. With no PgPool the cache read fails, so both a
// default-shape and a non-default-shape request fall through to the live fetch —
// and the ctx handed to that fetch must carry a deadline (publisherCheckLiveTimeout).
func TestFetchPublisherCheckCachedOrLive_LiveDeadline(t *testing.T) {
	a := &API{} // nil PgPool → readPageCache errors → live path

	cases := []struct {
		name          string
		q             string
		epochs, slots int
	}{
		{"default shape, cache miss", "", 2, 0},
		{"non-default shape (filtered)", "dzuser1", 2, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotDeadline bool
			spy := func(ctx context.Context, _ string, _, _ int) (*PublisherCheckResponse, error) {
				_, gotDeadline = ctx.Deadline()
				return &PublisherCheckResponse{}, nil
			}
			_, err := a.fetchPublisherCheckCachedOrLive(context.Background(), tc.q, tc.epochs, tc.slots, spy)
			require.NoError(t, err)
			require.True(t, gotDeadline, "live fetch must run under a deadline")
		})
	}
}
