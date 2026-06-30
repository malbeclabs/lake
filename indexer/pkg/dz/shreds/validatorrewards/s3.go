package validatorrewards

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// S3BaseURL is the base URL for the DoubleZero Foundation's public
// validator leader-slot data export. Each Solana epoch's data is published
// as a JSON document at {S3BaseURL}/{solana_epoch}.json.
const S3BaseURL = "https://doublezero-foundation-public.s3.us-east-2.amazonaws.com/exports/multicast_validator_leader_slots"

// ValidatorLeaderSlotEntry is a single row from the S3 JSON export.
//
// The shape mirrors the Rust reference at
// `crates/shred-oracle/src/validator_rewards/s3.rs`.
type ValidatorLeaderSlotEntry struct {
	Epoch               uint64 `json:"epoch"`
	NodeIdentity        string `json:"node_identity"`
	ClientID            uint16 `json:"client_id"`
	NumberOfLeaderSlots uint32 `json:"number_of_leader_slots"`
}

// S3Client fetches the public DoubleZero Foundation leader-slot export. The
// HTTP client has an overall request timeout to avoid hangs on a stalled
// transfer or wedged TLS handshake.
type S3Client struct {
	http    *http.Client
	baseURL string
}

// NewS3Client returns an S3Client configured with a 30-second request
// timeout (matching the Rust reference).
func NewS3Client() *S3Client {
	return &S3Client{
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: S3BaseURL,
	}
}

// SetBaseURL overrides the base URL the client uses. Intended for tests
// pointing at an httptest.NewServer.
func (c *S3Client) SetBaseURL(url string) {
	c.baseURL = url
}

// FetchLeaderSlotData fetches the leader-slot JSON for a Solana epoch.
//
// Return shapes:
//   - 200: (entries, true, nil)
//   - 404 or 403: (nil, false, nil) — signals "not exported yet" without it
//     being an error condition. A public S3 bucket whose policy grants
//     s3:GetObject but not s3:ListBucket returns 403 (AccessDenied), not 404,
//     for a key that does not exist, to avoid leaking object existence. The
//     export only publishes a subset of epochs (e.g. testnet epochs are not
//     exported at all), so both statuses mean "no data for this epoch".
//   - any other status or transport/decode error: (nil, false, err)
func (c *S3Client) FetchLeaderSlotData(ctx context.Context, solanaEpoch uint64) ([]ValidatorLeaderSlotEntry, bool, error) {
	url := fmt.Sprintf("%s/%d.json", c.baseURL, solanaEpoch)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("build S3 request for epoch %d: %w", solanaEpoch, err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		metrics.ShredLeafFetchTotal.WithLabelValues("error").Inc()
		return nil, false, fmt.Errorf("HTTP request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	// 404, or 403 from a public bucket without s3:ListBucket, both mean the
	// object for this epoch does not exist — treat as "not exported yet". They
	// are recorded under distinct metric statuses ("not_found" vs "forbidden")
	// so a real access loss — a 403 where a 200 is expected — stays visible even
	// though neither is returned as an error.
	switch {
	case resp.StatusCode == http.StatusNotFound:
		metrics.ShredLeafFetchTotal.WithLabelValues("not_found").Inc()
		return nil, false, nil
	case resp.StatusCode == http.StatusForbidden:
		metrics.ShredLeafFetchTotal.WithLabelValues("forbidden").Inc()
		return nil, false, nil
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		metrics.ShredLeafFetchTotal.WithLabelValues("error").Inc()
		return nil, false, fmt.Errorf("S3 returned status %d for epoch %d", resp.StatusCode, solanaEpoch)
	}

	var entries []ValidatorLeaderSlotEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		metrics.ShredLeafFetchTotal.WithLabelValues("error").Inc()
		return nil, false, fmt.Errorf("decode leader-slot JSON for epoch %d: %w", solanaEpoch, err)
	}

	metrics.ShredLeafFetchTotal.WithLabelValues("ok").Inc()
	return entries, true, nil
}

// VerifiedLeaves is the output of FetchAndVerifyForEpoch and FetchForEpoch:
// the sorted leaf set with parallel base58 node-id strings and the
// aggregate counts that the caller will later persist alongside the merkle
// root. IsVerified is true only when the leaves were confirmed against the
// on-chain ShredDistribution.validator_rewards_merkle_root; FetchForEpoch
// returns IsVerified=false for indexing-before-finalization scenarios.
type VerifiedLeaves struct {
	SolanaEpoch               uint64
	Leaves                    []LeafBytes
	NodeIDStrings             []string // base58 strings, parallel to Leaves
	TotalPublishingValidators uint32
	TotalPublishedLeaderSlots uint32
	IsVerified                bool
}

// fetchVerifyLogger is the package-level logger used for debug-logging
// skipped malformed rows. It defaults to slog.Default() so callers don't
// have to thread a logger through; downstream callers that want
// scoped logging can use the higher-level store/view logger.
var fetchVerifyLogger = slog.Default()

// FetchAndVerifyForEpoch fetches the S3 JSON for a Solana epoch, decodes
// node-identity base58 pubkeys, computes the merkle root over the sorted
// leaves, and compares it to expectedRoot.
//
// Returns:
//   - On HTTP 200 and root match: (*VerifiedLeaves, true, nil).
//   - On HTTP 404: (nil, false, nil) — the export is not yet available
//     and the caller should treat this as a non-error "wait" condition.
//   - On root mismatch or any transport/decode error: (nil, false, err).
//
// Malformed rows (node_identity that cannot be decoded as base58) are
// skipped with a debug log, matching the Rust reference's
// `filter_map(Pubkey::from_str)`.
func FetchAndVerifyForEpoch(
	ctx context.Context,
	s3 *S3Client,
	solanaEpoch uint64,
	expectedRoot [32]byte,
) (*VerifiedLeaves, bool, error) {
	entries, ok, err := s3.FetchLeaderSlotData(ctx, solanaEpoch)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	leaves := make([]LeafBytes, 0, len(entries))
	nodeIDStrings := make([]string, 0, len(entries))
	var totalSlots uint64

	for _, e := range entries {
		pk, err := solana.PublicKeyFromBase58(e.NodeIdentity)
		if err != nil {
			fetchVerifyLogger.Debug("validatorrewards/s3: skipping malformed node_identity",
				"epoch", solanaEpoch,
				"node_identity", e.NodeIdentity,
				"err", err,
			)
			continue
		}

		var nid [32]byte
		copy(nid[:], pk[:])

		leaves = append(leaves, LeafBytes{
			NodeID:      nid,
			LeaderSlots: e.NumberOfLeaderSlots,
			ClientID:    e.ClientID,
		})
		nodeIDStrings = append(nodeIDStrings, e.NodeIdentity)
		totalSlots += uint64(e.NumberOfLeaderSlots)
	}

	// Sort leaves and produce a parallel sorted node-id slice. We sort the
	// node-id strings by indexing through SortedLeaves' permutation: build
	// an index slice and sort it the same way to keep the strings in
	// parallel with the leaves.
	sorted, sortedNodeIDs := sortLeavesWithNodeIDs(leaves, nodeIDStrings)

	root := MerkleRoot(sorted)
	if root != expectedRoot {
		return nil, false, fmt.Errorf("merkle root mismatch for epoch %d: computed %x, expected %x", solanaEpoch, root, expectedRoot)
	}

	if totalSlots > uint64(^uint32(0)) {
		return nil, false, fmt.Errorf("total published leader slots overflow uint32 for epoch %d: %d", solanaEpoch, totalSlots)
	}

	return &VerifiedLeaves{
		SolanaEpoch:               solanaEpoch,
		Leaves:                    sorted,
		NodeIDStrings:             sortedNodeIDs,
		TotalPublishingValidators: uint32(len(sorted)),
		TotalPublishedLeaderSlots: uint32(totalSlots),
		IsVerified:                true,
	}, true, nil
}

// FetchForEpoch fetches the S3 JSON for a Solana epoch and returns the
// sorted leaf set WITHOUT verifying against an on-chain merkle root.
// Intended for indexing leaves before the chain has posted a root — the
// returned IsVerified flag is false. Once the on-chain root is posted, the
// caller should re-fetch via FetchAndVerifyForEpoch and overwrite these
// entries.
//
// Returns:
//   - On HTTP 200: (*VerifiedLeaves with IsVerified=false, true, nil).
//   - On HTTP 404: (nil, false, nil) — the export is not yet available.
//   - On transport/decode errors: (nil, false, err).
//
// Malformed rows are skipped silently with a debug log, matching
// FetchAndVerifyForEpoch.
func FetchForEpoch(
	ctx context.Context,
	s3 *S3Client,
	solanaEpoch uint64,
) (*VerifiedLeaves, bool, error) {
	entries, ok, err := s3.FetchLeaderSlotData(ctx, solanaEpoch)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	leaves := make([]LeafBytes, 0, len(entries))
	nodeIDStrings := make([]string, 0, len(entries))
	var totalSlots uint64

	for _, e := range entries {
		pk, err := solana.PublicKeyFromBase58(e.NodeIdentity)
		if err != nil {
			fetchVerifyLogger.Debug("validatorrewards/s3: skipping malformed node_identity",
				"epoch", solanaEpoch,
				"node_identity", e.NodeIdentity,
				"err", err,
			)
			continue
		}

		var nid [32]byte
		copy(nid[:], pk[:])

		leaves = append(leaves, LeafBytes{
			NodeID:      nid,
			LeaderSlots: e.NumberOfLeaderSlots,
			ClientID:    e.ClientID,
		})
		nodeIDStrings = append(nodeIDStrings, e.NodeIdentity)
		totalSlots += uint64(e.NumberOfLeaderSlots)
	}

	sorted, sortedNodeIDs := sortLeavesWithNodeIDs(leaves, nodeIDStrings)

	if totalSlots > uint64(^uint32(0)) {
		return nil, false, fmt.Errorf("total published leader slots overflow uint32 for epoch %d: %d", solanaEpoch, totalSlots)
	}

	return &VerifiedLeaves{
		SolanaEpoch:               solanaEpoch,
		Leaves:                    sorted,
		NodeIDStrings:             sortedNodeIDs,
		TotalPublishingValidators: uint32(len(sorted)),
		TotalPublishedLeaderSlots: uint32(totalSlots),
		IsVerified:                false,
	}, true, nil
}

// sortLeavesWithNodeIDs sorts leaves by (NodeID, ClientID) and reorders
// nodeIDStrings in parallel. Callers rely on the two output slices having
// matching indices.
func sortLeavesWithNodeIDs(leaves []LeafBytes, nodeIDStrings []string) ([]LeafBytes, []string) {
	if len(leaves) != len(nodeIDStrings) {
		// Programmer error — keep things permissive and just sort the
		// leaves alone if the parallel slice is mis-sized.
		return SortedLeaves(leaves), nodeIDStrings
	}

	// Sort (leaf, nodeID) pairs together so the strings stay parallel to
	// the leaves. Uses the same comparator as SortedLeaves: NodeID asc,
	// then ClientID asc.
	type pair struct {
		leaf   LeafBytes
		nodeID string
	}
	pairs := make([]pair, len(leaves))
	for i := range leaves {
		pairs[i] = pair{leaf: leaves[i], nodeID: nodeIDStrings[i]}
	}

	sort.Slice(pairs, func(i, j int) bool {
		if c := bytes.Compare(pairs[i].leaf.NodeID[:], pairs[j].leaf.NodeID[:]); c != 0 {
			return c < 0
		}
		return pairs[i].leaf.ClientID < pairs[j].leaf.ClientID
	})

	outLeaves := make([]LeafBytes, len(pairs))
	outNodeIDs := make([]string, len(pairs))
	for i, p := range pairs {
		outLeaves[i] = p.leaf
		outNodeIDs[i] = p.nodeID
	}
	return outLeaves, outNodeIDs
}
