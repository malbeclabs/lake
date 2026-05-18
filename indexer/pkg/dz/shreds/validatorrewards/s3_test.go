package validatorrewards

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestServer returns an httptest server that serves the given entries
// as JSON for any path that ends in `.json`. statusCode is the response
// status for non-empty bodies. If overridePath is non-empty, only that
// path returns the body — other paths return 404.
func newTestServer(t *testing.T, statusCode int, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		w.WriteHeader(statusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newClientFor(t *testing.T, srv *httptest.Server) *S3Client {
	t.Helper()
	c := NewS3Client()
	c.SetBaseURL(srv.URL)
	return c
}

// makeValidEntries returns a deterministic set of valid leader-slot entries
// using on-curve / system pubkeys.
func makeValidEntries() []ValidatorLeaderSlotEntry {
	return []ValidatorLeaderSlotEntry{
		{
			Epoch:               951,
			NodeIdentity:        "11111111111111111111111111111112", // system program +1
			ClientID:            1,
			NumberOfLeaderSlots: 100,
		},
		{
			Epoch:               951,
			NodeIdentity:        "Vote111111111111111111111111111111111111111",
			ClientID:            2,
			NumberOfLeaderSlots: 200,
		},
		{
			Epoch:               951,
			NodeIdentity:        "Stake11111111111111111111111111111111111111",
			ClientID:            3,
			NumberOfLeaderSlots: 300,
		},
	}
}

// rootFromEntries computes the expected merkle root the same way
// FetchAndVerifyForEpoch does: decode pubkeys, sort, MerkleRoot.
func rootFromEntries(t *testing.T, entries []ValidatorLeaderSlotEntry) [32]byte {
	t.Helper()
	leaves := make([]LeafBytes, 0, len(entries))
	for _, e := range entries {
		pk, err := solana.PublicKeyFromBase58(e.NodeIdentity)
		if err != nil {
			continue
		}
		var nid [32]byte
		copy(nid[:], pk[:])
		leaves = append(leaves, LeafBytes{
			NodeID:      nid,
			LeaderSlots: e.NumberOfLeaderSlots,
			ClientID:    e.ClientID,
		})
	}
	return MerkleRoot(SortedLeaves(leaves))
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestS3Client_Fetch_OK(t *testing.T) {
	entries := makeValidEntries()
	srv := newTestServer(t, http.StatusOK, mustJSON(t, entries))
	c := newClientFor(t, srv)

	got, ok, err := c.FetchLeaderSlotData(context.Background(), 951)
	require.NoError(t, err)
	assert.True(t, ok)
	require.Len(t, got, len(entries))
	assert.Equal(t, entries[0].NodeIdentity, got[0].NodeIdentity)
	assert.Equal(t, entries[1].ClientID, got[1].ClientID)
	assert.Equal(t, entries[2].NumberOfLeaderSlots, got[2].NumberOfLeaderSlots)
}

func TestS3Client_Fetch_NotFound(t *testing.T) {
	srv := newTestServer(t, http.StatusNotFound, []byte(""))
	c := newClientFor(t, srv)

	got, ok, err := c.FetchLeaderSlotData(context.Background(), 951)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestS3Client_Fetch_ServerError(t *testing.T) {
	srv := newTestServer(t, http.StatusInternalServerError, []byte("boom"))
	c := newClientFor(t, srv)

	got, ok, err := c.FetchLeaderSlotData(context.Background(), 951)
	require.Error(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "500")
}

func TestFetchAndVerify_RootMatch_Accepted(t *testing.T) {
	entries := makeValidEntries()
	expected := rootFromEntries(t, entries)
	srv := newTestServer(t, http.StatusOK, mustJSON(t, entries))
	c := newClientFor(t, srv)

	verified, ok, err := FetchAndVerifyForEpoch(context.Background(), c, 951, expected)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, verified)

	assert.Equal(t, uint64(951), verified.SolanaEpoch)
	assert.Equal(t, uint32(len(entries)), verified.TotalPublishingValidators)
	assert.Equal(t, uint32(100+200+300), verified.TotalPublishedLeaderSlots)
	assert.Len(t, verified.Leaves, len(entries))
	assert.Len(t, verified.NodeIDStrings, len(entries))

	// Verify leaves and node-id strings are parallel: decoding each
	// nodeIDString should equal the leaf's NodeID.
	for i, leaf := range verified.Leaves {
		pk, err := solana.PublicKeyFromBase58(verified.NodeIDStrings[i])
		require.NoError(t, err)
		var nid [32]byte
		copy(nid[:], pk[:])
		assert.Equal(t, leaf.NodeID, nid, "leaf %d nodeID does not match parallel string", i)
	}
}

func TestFetchAndVerify_RootMismatch_Rejected(t *testing.T) {
	entries := makeValidEntries()
	var wrongRoot [32]byte
	for i := range wrongRoot {
		wrongRoot[i] = 0xAB
	}
	srv := newTestServer(t, http.StatusOK, mustJSON(t, entries))
	c := newClientFor(t, srv)

	verified, ok, err := FetchAndVerifyForEpoch(context.Background(), c, 951, wrongRoot)
	require.Error(t, err)
	assert.False(t, ok)
	assert.Nil(t, verified)
	assert.True(t, strings.Contains(err.Error(), "merkle root mismatch"), "want merkle-root-mismatch error, got: %v", err)
}

func TestFetchAndVerify_404_NotErr(t *testing.T) {
	srv := newTestServer(t, http.StatusNotFound, []byte(""))
	c := newClientFor(t, srv)

	var anyRoot [32]byte
	verified, ok, err := FetchAndVerifyForEpoch(context.Background(), c, 951, anyRoot)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, verified)
}

func TestFetchAndVerify_SkipsMalformedPubkey(t *testing.T) {
	valid := makeValidEntries()

	// One bad row plus all the valid rows. Expected root is computed from
	// the valid rows only (matching the actual filter behavior).
	withBad := append([]ValidatorLeaderSlotEntry{
		{
			Epoch:               951,
			NodeIdentity:        "not-a-valid-base58-pubkey-zzz",
			ClientID:            9,
			NumberOfLeaderSlots: 999,
		},
	}, valid...)

	expected := rootFromEntries(t, valid)

	srv := newTestServer(t, http.StatusOK, mustJSON(t, withBad))
	c := newClientFor(t, srv)

	verified, ok, err := FetchAndVerifyForEpoch(context.Background(), c, 951, expected)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, verified)

	// Only the valid rows survived.
	assert.Equal(t, uint32(len(valid)), verified.TotalPublishingValidators)
	assert.Equal(t, uint32(100+200+300), verified.TotalPublishedLeaderSlots)
	assert.Len(t, verified.Leaves, len(valid))
	assert.Len(t, verified.NodeIDStrings, len(valid))
}
