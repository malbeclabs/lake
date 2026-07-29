package handlers

import (
	"testing"
	"time"
)

func TestScalarCache(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	t.Run("miss on empty cache", func(t *testing.T) {
		var c scalarCache
		if _, ok := c.get("mainnet-beta:total_stake", base); ok {
			t.Fatal("expected miss on empty cache")
		}
	})

	t.Run("hit within TTL, miss after TTL", func(t *testing.T) {
		var c scalarCache
		c.set("k", 42, time.Minute, base)

		if v, ok := c.get("k", base.Add(59*time.Second)); !ok || v != 42 {
			t.Fatalf("within TTL: got (%d, %v), want (42, true)", v, ok)
		}
		// Exactly at expiry is treated as expired (get requires now < expires).
		if _, ok := c.get("k", base.Add(time.Minute)); ok {
			t.Fatal("expected miss at expiry boundary")
		}
		if _, ok := c.get("k", base.Add(2*time.Minute)); ok {
			t.Fatal("expected miss after TTL")
		}
	})

	t.Run("zero value is never cached", func(t *testing.T) {
		var c scalarCache
		c.set("k", 0, time.Minute, base)
		if _, ok := c.get("k", base); ok {
			t.Fatal("zero value must not be cached (cold/empty table)")
		}
		// A real value later is cached as normal.
		c.set("k", 7, time.Minute, base)
		if v, ok := c.get("k", base); !ok || v != 7 {
			t.Fatalf("got (%d, %v), want (7, true)", v, ok)
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		var c scalarCache
		c.set("mainnet-beta:total_stake", 100, time.Minute, base)
		c.set("testnet:total_stake", 200, time.Minute, base)

		if v, _ := c.get("mainnet-beta:total_stake", base); v != 100 {
			t.Fatalf("mainnet got %d, want 100", v)
		}
		if v, _ := c.get("testnet:total_stake", base); v != 200 {
			t.Fatalf("testnet got %d, want 200", v)
		}
	})
}
