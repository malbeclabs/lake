package sol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	solanarpc "github.com/gagliardetto/solana-go/rpc"
	soljsonrpc "github.com/gagliardetto/solana-go/rpc/jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestDecodeClientID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want *string
	}{
		{"empty", "", nil},
		{"null", "null", nil},
		{"string", `"abc123"`, ptr("abc123")},
		{"number", `42`, ptr("42")},
		{"large_number", `4294967295`, ptr("4294967295")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeClientID(json.RawMessage(tc.in))
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, *tc.want, *got)
		})
	}
}

func TestTolerantClient_GetClusterNodes_AcceptsNumericClientID(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"jsonrpc": "2.0",
			"id": 1,
			"result": [
				{"pubkey": "11111111111111111111111111111111", "gossip": "1.2.3.4:8001", "clientId": 7, "shredVersion": 50093},
				{"pubkey": "11111111111111111111111111111112", "gossip": "1.2.3.5:8001", "clientId": "agave", "shredVersion": 50093},
				{"pubkey": "11111111111111111111111111111113", "gossip": "1.2.3.6:8001", "shredVersion": 50093}
			]
		}`))
	}))
	defer srv.Close()

	rpcClient := soljsonrpc.NewClientWithOpts(srv.URL, nil)
	client := solanarpc.NewWithCustomRPCClient(rpcClient)
	tc := NewTolerantClient(client)

	nodes, err := tc.GetClusterNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, nodes, 3)

	require.NotNil(t, nodes[0].ClientID)
	require.Equal(t, "7", *nodes[0].ClientID)

	require.NotNil(t, nodes[1].ClientID)
	require.Equal(t, "agave", *nodes[1].ClientID)

	require.Nil(t, nodes[2].ClientID)

	require.NotNil(t, nodes[0].Gossip)
	require.Equal(t, "1.2.3.4:8001", *nodes[0].Gossip)
	require.Equal(t, uint16(50093), nodes[0].ShredVersion)
}

func ptr[T any](v T) *T { return &v }
