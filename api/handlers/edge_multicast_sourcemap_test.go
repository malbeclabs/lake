package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The source→group rule is a naming convention, not a list, so these cases are the contract:
// break one and a live lane silently stops being attributed to its group, which on the fleet
// page reads as "no capture covers this" rather than as a bug.
func TestEdgeMulticastSourceMap(t *testing.T) {
	t.Parallel()

	groups := []MulticastDeliveryGroup{
		{PK: "pk-sports-mbp", Code: "edge-kalshi-sports-mbp"},
		{PK: "pk-sports-tob", Code: "edge-kalshi-sports-tob"},
		{PK: "pk-perps-mbp", Code: "edge-kalshi-perps-mbp"},
		{PK: "pk-perps-tob", Code: "edge-kalshi-perps-tob"},
		{PK: "pk-shreds", Code: "edge-solana-shreds"},
		{PK: "pk-root", Code: "edge-solana-root"},
		{PK: "pk-retrans-eu", Code: "edge-solana-retrans-eu"},
		{PK: "pk-mbone", Code: "mbone"},
	}
	m := newEdgeMulticastSourceMap(groups)

	for _, tc := range []struct {
		source string
		want   string
		why    string
	}{
		{"mbp_edge_kalshi_sports_nfl", "pk-sports-mbp", "league suffix on the market-by-price lane"},
		{"mbp_edge_kalshi_sports_pickleball", "pk-sports-mbp", "a league nobody has listed anywhere still maps"},
		{"tob_edge_kalshi_sports_nba", "pk-sports-tob", "the plane prefix picks the group, not the venue"},
		{"mbp_edge_kalshi_perps", "pk-perps-mbp", "perps has no league suffix: exact prefix match"},
		{"tob_edge_kalshi_perps_ws", "pk-perps-tob", "transport arms fold into their lane's group"},
		{"tob_edge_kalshi_perps_fix", "pk-perps-tob", "the other arm, same group"},
		{"edge-solana-root", "pk-root", "shred feeds name their group outright"},
		{"edge-solana-retrans-eu", "pk-retrans-eu", "so do the retransmit feeds"},
		{"dz", "pk-shreds", "the one alias that predates the convention"},
		{"jito", "", "a competitor feed is not a DoubleZero group"},
		{"turbine", "", "neither is the public path"},
		{"dz_rebop", "", "no evidence ties the per-validator rebroadcast to the rebop group"},
		{"mbp_edge_hyperliquid_perps", "", "a plausible-looking source with no group behind it is dropped"},
	} {
		assert.Equal(t, tc.want, m.resolve(tc.source), "%s: %s", tc.source, tc.why)
	}
}

// A source id that merely starts with a group's prefix, without the separator, belongs to a
// different lane and must not be captured by it.
func TestEdgeMulticastSourceMapPrefixBoundary(t *testing.T) {
	t.Parallel()

	m := newEdgeMulticastSourceMap([]MulticastDeliveryGroup{
		{PK: "pk-sports-mbp", Code: "edge-kalshi-sports-mbp"},
	})
	assert.Equal(t, "pk-sports-mbp", m.resolve("mbp_edge_kalshi_sports"))
	assert.Equal(t, "pk-sports-mbp", m.resolve("mbp_edge_kalshi_sports_nfl"))
	assert.Empty(t, m.resolve("mbp_edge_kalshi_sportsbook"), "no separator, different lane")
}

// Longest match wins, so a group whose code is a prefix of another's cannot swallow its lanes.
func TestEdgeMulticastSourceMapLongestPrefixWins(t *testing.T) {
	t.Parallel()

	m := newEdgeMulticastSourceMap([]MulticastDeliveryGroup{
		{PK: "pk-short", Code: "edge-kalshi-mbp"},
		{PK: "pk-long", Code: "edge-kalshi-sports-mbp"},
	})
	assert.Equal(t, "pk-long", m.resolve("mbp_edge_kalshi_sports_nfl"))
	assert.Equal(t, "pk-short", m.resolve("mbp_edge_kalshi_other"))
}
