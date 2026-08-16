package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Cacheability is decided on the sanitized symbol, not the raw param: `symbol=all` is the
// all-symbols view the cache already holds, so treating it as a filtered request would push a
// multi-day scan onto the request path — exactly what the background refresher exists to
// prevent.
func TestKalshiScoreboardCacheKey(t *testing.T) {
	cases := []struct {
		name, query, want string
	}{
		{"default", "", kalshiScoreboardCacheBase},
		{"explicit 1h", "?window=1h", kalshiScoreboardCacheBase},
		{"heavy window", "?window=7d", kalshiScoreboardCacheBase + ":7d"},
		{"symbol=all", "?symbol=all", kalshiScoreboardCacheBase},
		{"symbol=ALL", "?symbol=ALL", kalshiScoreboardCacheBase},
		{"symbol=all on a heavy window", "?window=7d&symbol=all", kalshiScoreboardCacheBase + ":7d"},
		{"blank symbol", "?symbol=%20", kalshiScoreboardCacheBase},
		{"real symbol filter", "?symbol=KXBTCPERP", ""},
		{"real symbol filter on a heavy window", "?window=24h&symbol=KXBTCPERP", ""},
		{"unknown window", "?window=90d", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/dz/kalshi/scoreboard"+tc.query, nil)
			assert.Equal(t, tc.want, kalshiScoreboardCacheKey(r))
		})
	}
}
