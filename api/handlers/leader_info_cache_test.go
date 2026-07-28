package handlers

import (
	"testing"
	"time"
)

func TestLeaderInfoCache(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	mkMap := func() map[string]leaderInfoEntry {
		return map[string]leaderInfoEntry{"acct-1": {name: "val-1", city: "nyc"}}
	}

	t.Run("miss on empty cache", func(t *testing.T) {
		var c leaderInfoCache
		if _, ok := c.get("mainnet-beta", base); ok {
			t.Fatal("expected miss")
		}
	})

	t.Run("hit within TTL, miss after TTL", func(t *testing.T) {
		var c leaderInfoCache
		c.set("mainnet-beta", mkMap(), time.Minute, base)
		if m, ok := c.get("mainnet-beta", base.Add(59*time.Second)); !ok || m["acct-1"].name != "val-1" {
			t.Fatalf("within TTL: ok=%v map=%v", ok, m)
		}
		if _, ok := c.get("mainnet-beta", base.Add(time.Minute)); ok {
			t.Fatal("expected miss at expiry boundary")
		}
	})

	t.Run("empty map is never cached", func(t *testing.T) {
		var c leaderInfoCache
		c.set("mainnet-beta", map[string]leaderInfoEntry{}, time.Minute, base)
		if _, ok := c.get("mainnet-beta", base); ok {
			t.Fatal("empty map must not be cached (cold/empty tables)")
		}
	})

	t.Run("envs are independent", func(t *testing.T) {
		var c leaderInfoCache
		c.set("mainnet-beta", map[string]leaderInfoEntry{"a": {name: "main"}}, time.Minute, base)
		c.set("testnet", map[string]leaderInfoEntry{"a": {name: "test"}}, time.Minute, base)
		if m, _ := c.get("mainnet-beta", base); m["a"].name != "main" {
			t.Fatalf("mainnet leaked: %v", m)
		}
		if m, _ := c.get("testnet", base); m["a"].name != "test" {
			t.Fatalf("testnet leaked: %v", m)
		}
	})
}
