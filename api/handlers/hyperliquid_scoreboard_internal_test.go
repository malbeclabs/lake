package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The matrix races every symbol in hyperliquidLiquidSymbols and the by-market table only shows
// the ones with a category, while the page renders each category as a delta against the
// headline. A symbol in the first set and not the second is therefore in the population every
// category is compared to and in none of the rows — which is what xyz:CL and xyz:BRENTOIL were
// until Commodities existed.
func TestHyperliquidScoreboard_EveryRacedSymbolHasACategory(t *testing.T) {
	categorised := map[string]string{}
	for _, g := range hyperliquidMarketGroups {
		for _, c := range g.Cats {
			for _, sym := range c.Symbols {
				require.Empty(t, categorised[sym], "symbol %q is in two categories", sym)
				categorised[sym] = c.Name
			}
		}
	}

	for _, sym := range hyperliquidLiquidSymbols {
		require.NotEmpty(t, categorised[sym],
			"%q is raced by the matrix but belongs to no market category, so it inflates the "+
				"population every category is measured against without appearing in the table", sym)
	}

	// And nothing categorised that the matrix never races, which would render an empty row.
	raced := map[string]bool{}
	for _, sym := range hyperliquidLiquidSymbols {
		raced[sym] = true
	}
	for sym := range categorised {
		require.True(t, raced[sym], "%q has a category but is not in hyperliquidLiquidSymbols", sym)
	}
}
