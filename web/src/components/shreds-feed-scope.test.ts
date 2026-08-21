import { describe, it, expect } from "vitest";
import { isShredsFeedRow, SHREDS_FEED_CODE_PREFIX } from "./shreds-feed-scope";

describe("isShredsFeedRow", () => {
  it("keeps shreds feeds", () => {
    expect(isShredsFeedRow({ code: "solana-shreds-full" })).toBe(true);
  });

  it("drops another product's feeds", () => {
    // The feed-subscription program is shared, so these rows reach the page
    // whenever the API does not filter them out.
    expect(isShredsFeedRow({ code: "kalshi-sports-mbp" })).toBe(false);
    expect(isShredsFeedRow({ code: "kalshi-perps-tob" })).toBe(false);
  });

  it("keeps a feed whose label has not landed", () => {
    // The code comes from a serviceability snapshot that can lag the revenue
    // account. Dropping the row would hide revenue that was really collected.
    expect(isShredsFeedRow({ code: "" })).toBe(true);
  });

  it("matches the prefix the API is asked for", () => {
    expect(SHREDS_FEED_CODE_PREFIX).toBe("solana-shreds");
  });
});
