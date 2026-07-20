package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHyperliquidEntries_InClause(t *testing.T) {
	var empty hyperliquidEntries
	assert.Equal(t, "", empty.inClause(), "no configured feeds must produce no predicate")

	e := hyperliquidEntries{ordered: []hyperliquidEntry{
		{Feed: "feed_a_bbo", Label: "Feed A"},
		{Feed: "feed_b_bbo", Label: "Feed B"},
	}}
	assert.Equal(t,
		"AND (feed IN ('feed_a_bbo', 'feed_b_bbo') OR loser_feed IN ('feed_a_bbo', 'feed_b_bbo'))",
		e.inClause())
}

func TestHyperliquidEntries_Display(t *testing.T) {
	e := hyperliquidEntries{labels: map[string]string{"feed_a_bbo": "Feed A"}}

	// Any tob_ feed is DoubleZero regardless of config.
	assert.Equal(t, "DoubleZero", e.display("tob_gcp_tyo_hl_mainnet1"))
	assert.Equal(t, "Feed A", e.display("feed_a_bbo"))
	// Unknown feeds fall back to the raw name.
	assert.Equal(t, "feed_z_bbo", e.display("feed_z_bbo"))
}

func TestHyperliquidEntries_Empty(t *testing.T) {
	var e hyperliquidEntries
	assert.True(t, e.empty())

	e.ordered = []hyperliquidEntry{{Feed: "feed_a_bbo", Label: "Feed A"}}
	assert.False(t, e.empty())
}

func TestHyperliquidFeedRe(t *testing.T) {
	assert.True(t, hyperliquidFeedRe.MatchString("feed_a_bbo"))
	assert.True(t, hyperliquidFeedRe.MatchString("tob_gcp_tyo_hl_mainnet1"))
	// Anything that could break out of a quoted SQL literal must be rejected.
	assert.False(t, hyperliquidFeedRe.MatchString("bad'; DROP TABLE x --"))
	assert.False(t, hyperliquidFeedRe.MatchString(""))
	assert.False(t, hyperliquidFeedRe.MatchString("has space"))
}
