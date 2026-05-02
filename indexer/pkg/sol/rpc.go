package sol

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

// TolerantClient wraps a *solanarpc.Client and overrides GetClusterNodes to
// accept either a string or a number for the "clientId" field. The upstream
// gagliardetto/solana-go library types ClientID as *string (matching the Agave
// reference implementation), but some validators (e.g. Firedancer) return a
// numeric value, which fails strict JSON unmarshalling.
//
// We don't read ClientID anywhere, so we just decode it into a permissive
// container and stringify it for callers that might.
type TolerantClient struct {
	*solanarpc.Client
}

// NewTolerantClient returns a SolanaRPC implementation that tolerates numeric
// clientId values from getClusterNodes responses.
func NewTolerantClient(c *solanarpc.Client) *TolerantClient {
	return &TolerantClient{Client: c}
}

type clusterNodeFlex struct {
	Pubkey          solana.PublicKey `json:"pubkey"`
	Gossip          *string          `json:"gossip,omitempty"`
	TPU             *string          `json:"tpu,omitempty"`
	TPUQUIC         *string          `json:"tpuQuic,omitempty"`
	TPUForwards     *string          `json:"tpuForwards,omitempty"`
	TPUForwardsQUIC *string          `json:"tpuForwardsQuic,omitempty"`
	TPUVote         *string          `json:"tpuVote,omitempty"`
	ServeRepair     *string          `json:"serveRepair,omitempty"`
	PubSub          *string          `json:"pubsub,omitempty"`
	RPC             *string          `json:"rpc,omitempty"`
	Version         *string          `json:"version,omitempty"`
	FeatureSet      *uint32          `json:"featureSet,omitempty"`
	ShredVersion    uint16           `json:"shredVersion,omitempty"`
	ClientID        json.RawMessage  `json:"clientId,omitempty"`
}

func (t *TolerantClient) GetClusterNodes(ctx context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
	var raw []*clusterNodeFlex
	if err := t.Client.RPCCallForInto(ctx, &raw, "getClusterNodes", nil); err != nil {
		return nil, err
	}
	out := make([]*solanarpc.GetClusterNodesResult, 0, len(raw))
	for _, r := range raw {
		if r == nil {
			continue
		}
		out = append(out, &solanarpc.GetClusterNodesResult{
			Pubkey:          r.Pubkey,
			Gossip:          r.Gossip,
			TPU:             r.TPU,
			TPUQUIC:         r.TPUQUIC,
			TPUForwards:     r.TPUForwards,
			TPUForwardsQUIC: r.TPUForwardsQUIC,
			TPUVote:         r.TPUVote,
			ServeRepair:     r.ServeRepair,
			PubSub:          r.PubSub,
			RPC:             r.RPC,
			Version:         r.Version,
			FeatureSet:      r.FeatureSet,
			ShredVersion:    r.ShredVersion,
			ClientID:        decodeClientID(r.ClientID),
		})
	}
	return out, nil
}

// decodeClientID accepts either a JSON string or a JSON number and returns a
// stringified pointer. nil/empty/JSON-null is returned as nil.
func decodeClientID(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return &s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		v := n.String()
		return &v
	}
	v := fmt.Sprintf("%s", raw)
	return &v
}
