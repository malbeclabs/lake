import type { ShredFeedRevenue } from "@/lib/api";

// Which feed product the shreds economics page is about.
//
// The feed-subscription program is shared by every DoubleZero feed, so revenue
// rows arrive for kalshi feeds too. A feed's code names its product
// (solana-shreds-full, kalshi-sports-mbp) while its name carries the metro
// (solana-shreds-full-ams), so the product is a code prefix.
//
// Matches handlers.ShredsFeedCodePrefix, which is the same rule server-side.
export const SHREDS_FEED_CODE_PREFIX = "solana-shreds";

// isShredsFeedRow reports whether a revenue row belongs on a shreds page.
//
// This repeats what the API's code_prefix parameter already does. The parameter
// is what keeps the payload small and is the authoritative filter, but the page
// is what claims its tiles are shreds revenue, so it does not outsource that
// claim: an API build that predates the parameter, or a cached response from
// one, answers with every product and would fold kalshi revenue into these
// totals.
//
// A row with no code is kept, for the same reason the API keeps it: the label
// comes from a separate serviceability snapshot, and revenue must not disappear
// while that catches up.
export function isShredsFeedRow(row: Pick<ShredFeedRevenue, "code">): boolean {
  return !row.code || row.code.startsWith(SHREDS_FEED_CODE_PREFIX);
}
