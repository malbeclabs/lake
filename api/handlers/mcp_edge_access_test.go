package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyEdgeAccess(t *testing.T) {
	t.Parallel()

	pubkey := "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb"
	ip := "103.106.58.157"

	t.Run("no pass is pending", func(t *testing.T) {
		t.Parallel()
		out := classifyEdgeAccess(pubkey, ip, nil)
		assert.Equal(t, "pending", out.Status)
		assert.Contains(t, out.Message, pubkey)
		assert.Contains(t, out.Message, ip)
		assert.Empty(t, out.PassPK)
	})

	t.Run("expired pass is pending", func(t *testing.T) {
		t.Parallel()
		out := classifyEdgeAccess(pubkey, ip, &edgeAccessPass{
			PK:       "pass1",
			ClientIP: ip,
			Status:   "expired",
			TypeTag:  "edge_seat",
		})
		assert.Equal(t, "pending", out.Status)
		assert.Equal(t, "pass1", out.PassPK)
		assert.Contains(t, out.Message, "expired")
	})

	t.Run("requested pass is pending", func(t *testing.T) {
		t.Parallel()
		out := classifyEdgeAccess(pubkey, ip, &edgeAccessPass{
			PK:       "pass1",
			ClientIP: ip,
			Status:   "requested",
			TypeTag:  "edge_seat",
		})
		assert.Equal(t, "pending", out.Status)
		assert.Contains(t, out.Message, "waiting to be approved")
	})

	t.Run("disconnected pass is pending", func(t *testing.T) {
		t.Parallel()
		out := classifyEdgeAccess(pubkey, ip, &edgeAccessPass{
			PK:       "pass1",
			ClientIP: ip,
			Status:   "Disconnected",
			TypeTag:  "edge_seat",
		})
		assert.Equal(t, "pending", out.Status)
		assert.Contains(t, out.Message, "Disconnected")
	})

	t.Run("connected exact IP is active", func(t *testing.T) {
		t.Parallel()
		out := classifyEdgeAccess(pubkey, ip, &edgeAccessPass{
			PK:       "pass1",
			ClientIP: ip,
			Status:   "connected",
			TypeTag:  "edge_seat",
		})
		assert.Equal(t, "active", out.Status)
		assert.Equal(t, "pass1", out.PassPK)
		assert.Equal(t, ip, out.ClientIP)
		assert.Equal(t, "edge_seat", out.TypeTag)
	})

	t.Run("any-IP pass is active", func(t *testing.T) {
		t.Parallel()
		out := classifyEdgeAccess(pubkey, ip, &edgeAccessPass{
			PK:       "pass-any",
			ClientIP: "0.0.0.0",
			Status:   "connected",
			TypeTag:  "prepaid",
		})
		assert.Equal(t, "active", out.Status)
		assert.Equal(t, "0.0.0.0", out.ClientIP)
	})
}
