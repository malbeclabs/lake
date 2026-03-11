package solana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// DefaultRPCURL is the default Solana RPC endpoint
const DefaultRPCURL = "https://api.mainnet-beta.solana.com"

// GetRPCURL returns the configured Solana RPC URL
func GetRPCURL() string {
	url := os.Getenv("SOLANA_RPC_URL")
	if url == "" {
		return DefaultRPCURL
	}
	return url
}

// Client is a Solana-compatible JSON-RPC client that works with any
// Solana fork (including the DZ ledger).
type Client struct {
	rpcURL     string
	httpClient *http.Client
}

// NewClient creates a new RPC client for the given URL.
func NewClient(rpcURL string) *Client {
	return &Client{
		rpcURL:     rpcURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// rpcRequest represents a JSON-RPC 2.0 request
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

// rpcResponse represents a JSON-RPC 2.0 response
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// rpcError represents a JSON-RPC 2.0 error
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// call executes a JSON-RPC method and unmarshals the result.
func (c *Client) call(ctx context.Context, method string, params []any, result any) error {
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  method,
		Params:  params,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.rpcURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return fmt.Errorf("RPC error: %s (code %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	if err := json.Unmarshal(rpcResp.Result, result); err != nil {
		return fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return nil
}

// EpochInfo is the result of getEpochInfo.
type EpochInfo struct {
	AbsoluteSlot     uint64 `json:"absoluteSlot"`
	BlockHeight      uint64 `json:"blockHeight"`
	Epoch            uint64 `json:"epoch"`
	SlotIndex        uint64 `json:"slotIndex"`
	SlotsInEpoch     uint64 `json:"slotsInEpoch"`
	TransactionCount uint64 `json:"transactionCount"`
}

// GetEpochInfo returns information about the current epoch.
func (c *Client) GetEpochInfo(ctx context.Context) (*EpochInfo, error) {
	var result EpochInfo
	if err := c.call(ctx, "getEpochInfo", []any{map[string]string{"commitment": "finalized"}}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// PerformanceSample is a single entry from getRecentPerformanceSamples.
type PerformanceSample struct {
	Slot            uint64 `json:"slot"`
	NumTransactions uint64 `json:"numTransactions"`
	NumSlots        uint64 `json:"numSlots"`
	SamplePeriodSec uint64 `json:"samplePeriodSecs"`
}

// GetRecentPerformanceSamples returns recent TPS samples.
func (c *Client) GetRecentPerformanceSamples(ctx context.Context, limit int) ([]PerformanceSample, error) {
	var result []PerformanceSample
	if err := c.call(ctx, "getRecentPerformanceSamples", []any{limit}, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Supply is the result of getSupply.
type Supply struct {
	Context struct {
		Slot uint64 `json:"slot"`
	} `json:"context"`
	Value struct {
		Total          uint64 `json:"total"`
		Circulating    uint64 `json:"circulating"`
		NonCirculating uint64 `json:"nonCirculating"`
	} `json:"value"`
}

// GetSupply returns the current supply information.
func (c *Client) GetSupply(ctx context.Context) (*Supply, error) {
	var result Supply
	if err := c.call(ctx, "getSupply", []any{map[string]any{"commitment": "finalized", "excludeNonCirculatingAccountsList": true}}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// InflationRate is the result of getInflationRate.
type InflationRate struct {
	Total      float64 `json:"total"`
	Validator  float64 `json:"validator"`
	Foundation float64 `json:"foundation"`
	Epoch      uint64  `json:"epoch"`
}

// GetInflationRate returns the current inflation rate.
func (c *Client) GetInflationRate(ctx context.Context) (*InflationRate, error) {
	var result InflationRate
	if err := c.call(ctx, "getInflationRate", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Version is the result of getVersion.
type Version struct {
	SolanaCore string  `json:"solana-core"`
	FeatureSet *uint64 `json:"feature-set,omitempty"`
}

// GetVersion returns the node's software version.
func (c *Client) GetVersion(ctx context.Context) (*Version, error) {
	var result Version
	if err := c.call(ctx, "getVersion", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// VoteAccountsResult is the result of getVoteAccounts.
type VoteAccountsResult struct {
	Current    []VoteAccount `json:"current"`
	Delinquent []VoteAccount `json:"delinquent"`
}

// VoteAccount represents a single vote account entry.
type VoteAccount struct {
	ActivatedStake uint64 `json:"activatedStake"`
	Commission     uint8  `json:"commission"`
	VotePubkey     string `json:"votePubkey"`
	NodePubkey     string `json:"nodePubkey"`
}

// GetVoteAccounts returns active and delinquent vote accounts.
func (c *Client) GetVoteAccounts(ctx context.Context) (*VoteAccountsResult, error) {
	var result VoteAccountsResult
	if err := c.call(ctx, "getVoteAccounts", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// balanceResult represents the result of a getBalance call
type balanceResult struct {
	Context struct {
		Slot uint64 `json:"slot"`
	} `json:"context"`
	Value uint64 `json:"value"`
}

// GetBalance fetches the SOL balance for an address in lamports
func GetBalance(ctx context.Context, address string) (int64, error) {
	client := NewClient(GetRPCURL())
	var result balanceResult
	if err := client.call(ctx, "getBalance", []any{address}, &result); err != nil {
		return 0, err
	}
	return int64(result.Value), nil
}

// LamportsToSOL converts lamports to SOL
func LamportsToSOL(lamports int64) float64 {
	return float64(lamports) / 1_000_000_000
}

// SOLToLamports converts SOL to lamports
func SOLToLamports(sol float64) int64 {
	return int64(sol * 1_000_000_000)
}
