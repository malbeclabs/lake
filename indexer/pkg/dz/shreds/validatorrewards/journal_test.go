package validatorrewards

import (
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildJournalAccount synthesises a ShredDistributionJournal account byte
// blob for tests. The layout mirrors the bytemuck Pod struct documented in
// programs/shred-subscription/src/state/shred_distribution_journal.rs.
//
// `bitmap` is appended as the publisher accumulation bitmap and the bitmap
// indices are filled in pointing at it (start=0, end=len(bitmap)). The
// client bitmap range is left zeroed (unallocated).
func buildJournalAccount(
	t *testing.T,
	subscriptionEpoch uint64,
	mintKey solana.PublicKey,
	rewardMintKey solana.PublicKey,
	accumulatedPublisherLeafCount uint32,
	distributedPublisherLeafCount uint32,
	bitmap []byte,
) []byte {
	t.Helper()

	const headerSize = journalFixedHeader // 296
	buf := make([]byte, headerSize)

	// discriminator
	copy(buf[0:8], JournalDiscriminator[:])

	// subscription_epoch (u64 LE)
	binary.LittleEndian.PutUint64(buf[8:16], subscriptionEpoch)

	// mint_key (32B)
	copy(buf[16:48], mintKey[:])

	// reward_mint_key (32B)
	copy(buf[48:80], rewardMintKey[:])

	// _flags (8B), usdc_swapped_amount (8B): leave zero.
	// tokens_received_amount (8B) at offset 96 — a recognizable sentinel so
	// the decode of the 2Z reward pool is exercised.
	binary.LittleEndian.PutUint64(buf[96:104], 7_000_000)

	// publisher_accumulation_bitmap_start_index = 0
	// publisher_accumulation_bitmap_end_index = len(bitmap)
	binary.LittleEndian.PutUint32(buf[104:108], 0)
	binary.LittleEndian.PutUint32(buf[108:112], uint32(len(bitmap)))

	// client bitmap left zero (unallocated)

	// validator_pool (8B) at 120 left zero; total_leader_slots (u32) at 128 —
	// a recognizable sentinel so the decode of the denominator is exercised.
	binary.LittleEndian.PutUint32(buf[128:132], 4242)

	// padding (4B) zero

	// accumulated_publisher_slots_scaled (8B), accumulated_client_slots_scaled (8B) zero

	// accumulated_publisher_leaf_count
	binary.LittleEndian.PutUint32(buf[152:156], accumulatedPublisherLeafCount)
	binary.LittleEndian.PutUint32(buf[156:160], distributedPublisherLeafCount)

	// distributed_amount (8B), accumulated_client_leaf_count (4B),
	// distributed_client_leaf_count (4B), padding (16B),
	// first_distribute_timestamp (8B), gap (96B) — all zero.

	return append(buf, bitmap...)
}

func TestDecodeJournalAccount_ValidLayout(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	reward := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	bitmap := []byte{0b00000101, 0b00000000}

	data := buildJournalAccount(t, 951, mint, reward, 5, 2, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)
	require.NotNil(t, view)

	assert.Equal(t, uint64(951), view.SubscriptionEpoch)
	assert.Equal(t, mint, view.MintKey)
	assert.Equal(t, uint64(7_000_000), view.TokensReceivedAmount)
	assert.Equal(t, uint32(0), view.PublisherAccumulationBitmapStartIndex)
	assert.Equal(t, uint32(len(bitmap)), view.PublisherAccumulationBitmapEndIndex)
	assert.Equal(t, uint32(4242), view.TotalLeaderSlots)
	assert.Equal(t, uint32(5), view.AccumulatedPublisherLeafCount)
	assert.Equal(t, uint32(2), view.DistributedPublisherLeafCount)
	assert.Equal(t, bitmap, view.RemainingData)
}

func TestDecodeJournalAccount_BadDiscriminator(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	data := buildJournalAccount(t, 1, mint, mint, 0, 0, nil)
	// Corrupt discriminator.
	data[0] ^= 0xFF

	_, err := DecodeJournalAccount(data)
	require.Error(t, err)
}

func TestDecodeJournalAccount_TooShort(t *testing.T) {
	_, err := DecodeJournalAccount(make([]byte, journalFixedHeader-1))
	require.Error(t, err)
}

func TestIsClaimableForLeafIndex_LSBFirst(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Bits 0 and 2 set within the single byte (LSB-first).
	bitmap := []byte{0b00000101}
	data := buildJournalAccount(t, 1, mint, mint, 3, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	claimable, ok := view.IsClaimableForLeafIndex(0)
	require.True(t, ok)
	assert.True(t, claimable)

	claimable, ok = view.IsClaimableForLeafIndex(1)
	require.True(t, ok)
	assert.False(t, claimable)

	claimable, ok = view.IsClaimableForLeafIndex(2)
	require.True(t, ok)
	assert.True(t, claimable)

	// Bit 7 in the same byte is clear.
	claimable, ok = view.IsClaimableForLeafIndex(7)
	require.True(t, ok)
	assert.False(t, claimable)

	// Out-of-range leaf index returns ok=false.
	_, ok = view.IsClaimableForLeafIndex(8)
	assert.False(t, ok)
}

func TestIsClaimableForLeafIndex_BitmapUnallocated(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Build a journal with both bitmap indices zero (i.e., end_index = 0),
	// which means the bitmap is not yet allocated.
	data := buildJournalAccount(t, 1, mint, mint, 0, 0, nil)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	_, ok := view.IsClaimableForLeafIndex(0)
	assert.False(t, ok)
}

func TestProjectStatuses_OnlyAccumulatedLeaves(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Bitmap has 3 bits set (indices 0, 1, 2) but accumulated count is 2,
	// so only the first two leaves should emit rows.
	bitmap := []byte{0b00000111}
	data := buildJournalAccount(t, 951, mint, mint, 2, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	leafIndexToNodeID := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		1: {NodeID: "node-b", ClientID: 2},
		2: {NodeID: "node-c", ClientID: 1},
	}

	rows := ProjectStatuses(view, 951, leafIndexToNodeID)
	require.Len(t, rows, 2)

	byNode := make(map[string]LeafDistributionStatusRow, len(rows))
	for _, r := range rows {
		byNode[r.NodeID] = r
	}

	rowA, ok := byNode["node-a"]
	require.True(t, ok)
	assert.Equal(t, uint64(951), rowA.SubscriptionEpoch)
	assert.Equal(t, uint16(1), rowA.ClientID)
	assert.Equal(t, LeafDistributionStatusPK(951, "node-a", 1), rowA.PK)
	assert.Equal(t, uint8(1), rowA.IsClaimable)
	assert.Equal(t, DoubleZeroMintKey, rowA.JournalMintKey)

	rowB, ok := byNode["node-b"]
	require.True(t, ok)
	assert.Equal(t, uint8(1), rowB.IsClaimable)

	// node-c is at leaf index 2 — beyond accumulated count — excluded.
	_, ok = byNode["node-c"]
	assert.False(t, ok)
}

func TestProjectStatuses_MissingLeafInMap(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	bitmap := []byte{0b00000101}
	data := buildJournalAccount(t, 951, mint, mint, 3, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	// Only leaf index 0 and 2 have a node-id mapping. Leaf 1 has no
	// mapping and must be silently skipped.
	leafIndexToNodeID := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		2: {NodeID: "node-c", ClientID: 1},
	}

	rows := ProjectStatuses(view, 951, leafIndexToNodeID)
	require.Len(t, rows, 2)

	byNode := make(map[string]LeafDistributionStatusRow, len(rows))
	for _, r := range rows {
		byNode[r.NodeID] = r
	}

	rowA, ok := byNode["node-a"]
	require.True(t, ok)
	assert.Equal(t, uint8(1), rowA.IsClaimable)

	rowC, ok := byNode["node-c"]
	require.True(t, ok)
	assert.Equal(t, uint8(1), rowC.IsClaimable)
}

func TestProjectStatuses_ClearedBitEmitsZero(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Bit 0 set, bit 1 clear, bit 2 set.
	bitmap := []byte{0b00000101}
	data := buildJournalAccount(t, 951, mint, mint, 3, 1, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	leafIndexToNodeID := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		1: {NodeID: "node-b", ClientID: 2},
		2: {NodeID: "node-c", ClientID: 1},
	}

	rows := ProjectStatuses(view, 951, leafIndexToNodeID)
	require.Len(t, rows, 3)

	byNode := make(map[string]LeafDistributionStatusRow, len(rows))
	for _, r := range rows {
		byNode[r.NodeID] = r
	}

	assert.Equal(t, uint8(1), byNode["node-a"].IsClaimable)
	// bit 1 is clear → already distributed (or never accumulated).
	assert.Equal(t, uint8(0), byNode["node-b"].IsClaimable)
	assert.Equal(t, uint8(1), byNode["node-c"].IsClaimable)
}

// TestProjectStatuses_MultiClientSameNode is the regression test for the bug
// where a validator publishing under more than one software client in a single
// epoch was collapsed to one row. The two leaves share a node_id but differ by
// client_id, so they must produce two distinct rows with distinct PKs and
// independent claimable bits.
func TestProjectStatuses_MultiClientSameNode(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Two accumulated leaves: bit 0 set (claimable), bit 1 clear.
	bitmap := []byte{0b00000001}
	data := buildJournalAccount(t, 951, mint, mint, 2, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	// Same node_id, two different client_ids — distinct leaves.
	leafIndexToIdentity := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		1: {NodeID: "node-a", ClientID: 2},
	}

	rows := ProjectStatuses(view, 951, leafIndexToIdentity)
	require.Len(t, rows, 2, "each (node, client) leaf must produce its own row")

	byClient := make(map[uint16]LeafDistributionStatusRow, len(rows))
	for _, r := range rows {
		assert.Equal(t, "node-a", r.NodeID)
		byClient[r.ClientID] = r
	}

	require.Contains(t, byClient, uint16(1))
	require.Contains(t, byClient, uint16(2))
	assert.NotEqual(t, byClient[1].PK, byClient[2].PK, "PKs must differ by client_id")
	assert.Equal(t, LeafDistributionStatusPK(951, "node-a", 1), byClient[1].PK)
	assert.Equal(t, LeafDistributionStatusPK(951, "node-a", 2), byClient[2].PK)
	// Independent claimable bits: client 1 claimable, client 2 not.
	assert.Equal(t, uint8(1), byClient[1].IsClaimable)
	assert.Equal(t, uint8(0), byClient[2].IsClaimable)
}

func TestJournalPDA_IsDeterministic(t *testing.T) {
	programID := solana.MustPublicKeyFromBase58("11111111111111111111111111111112")
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)

	pdaA, bumpA, err := JournalPDA(programID, 951, mint)
	require.NoError(t, err)

	pdaB, bumpB, err := JournalPDA(programID, 951, mint)
	require.NoError(t, err)

	assert.Equal(t, pdaA, pdaB)
	assert.Equal(t, bumpA, bumpB)

	pdaC, _, err := JournalPDA(programID, 952, mint)
	require.NoError(t, err)
	assert.NotEqual(t, pdaA, pdaC, "different epoch must yield different PDA")
}

func TestJournalDiscriminator_MatchesSha256Prefix(t *testing.T) {
	// Hardcoded from sha256("dz::account::shred_distribution_journal")[:8].
	// If this fails, the upstream Rust seed string has changed.
	expected := [8]byte{0x2c, 0x00, 0x81, 0x59, 0x1d, 0x13, 0x66, 0x94}
	assert.Equal(t, expected, JournalDiscriminator)
}
