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

	// accumulated_publisher_slots_scaled (8B) at 136 and
	// accumulated_client_slots_scaled (8B) at 144 — recognizable sentinels so the
	// per-token scaled-slot denominator decode is exercised (sum = 1_538_000).
	binary.LittleEndian.PutUint64(buf[136:144], 1_000_000)
	binary.LittleEndian.PutUint64(buf[144:152], 538_000)

	// accumulated_publisher_leaf_count
	binary.LittleEndian.PutUint32(buf[152:156], accumulatedPublisherLeafCount)
	binary.LittleEndian.PutUint32(buf[156:160], distributedPublisherLeafCount)

	// distributed_amount (8B) at 160 — sentinel so the decode is exercised.
	binary.LittleEndian.PutUint64(buf[160:168], 4_242_000)

	// accumulated_client_leaf_count (4B),
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
	assert.Equal(t, reward, view.RewardMintKey)
	assert.Equal(t, uint64(7_000_000), view.TokensReceivedAmount)
	assert.Equal(t, uint32(0), view.PublisherAccumulationBitmapStartIndex)
	assert.Equal(t, uint32(len(bitmap)), view.PublisherAccumulationBitmapEndIndex)
	assert.Equal(t, uint32(4242), view.TotalLeaderSlots)
	assert.Equal(t, uint64(1_000_000), view.AccumulatedPublisherSlotsScaled)
	assert.Equal(t, uint64(538_000), view.AccumulatedClientSlotsScaled)
	assert.Equal(t, uint64(1_538_000), view.AccumulatedSlotsScaledDenominator())
	assert.Equal(t, uint32(5), view.AccumulatedPublisherLeafCount)
	assert.Equal(t, uint32(2), view.DistributedPublisherLeafCount)
	assert.Equal(t, uint64(4_242_000), view.DistributedAmount)
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

// indexByNode keys status rows by node_id for convenient assertions.
func indexByNode(rows []LeafDistributionStatusRow) map[string]LeafDistributionStatusRow {
	m := make(map[string]LeafDistributionStatusRow, len(rows))
	for _, r := range rows {
		m[r.NodeID] = r
	}
	return m
}

func TestProjectEpochStatuses_PartialAccumulationPending(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Bits 0 and 1 set (claimable), bit 2 clear; accumulated count is 2 but the
	// map has 3 leaves, so the epoch is NOT fully accumulated. Leaf 2 hasn't been
	// accumulated yet → pending (no row), not distributed.
	bitmap := []byte{0b00000011}
	data := buildJournalAccount(t, 951, mint, mint, 2, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	leafMap := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		1: {NodeID: "node-b", ClientID: 2},
		2: {NodeID: "node-c", ClientID: 1},
	}

	rows := ProjectEpochStatuses(951, []*JournalView{view}, leafMap, nil)
	require.Len(t, rows, 2)

	byNode := indexByNode(rows)
	rowA, ok := byNode["node-a"]
	require.True(t, ok)
	assert.Equal(t, uint64(951), rowA.SubscriptionEpoch)
	assert.Equal(t, uint16(1), rowA.ClientID)
	assert.Equal(t, LeafDistributionStatusPK(951, "node-a", 1), rowA.PK)
	assert.Equal(t, uint8(1), rowA.IsClaimable)
	assert.Equal(t, DoubleZeroMintKey, rowA.JournalMintKey)

	assert.Equal(t, uint8(1), byNode["node-b"].IsClaimable)

	_, ok = byNode["node-c"]
	assert.False(t, ok, "leaf 2 not yet accumulated → pending, no row")
}

func TestProjectEpochStatuses_FullAccumulationDistributed(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// Bits 0 and 2 set, bit 1 clear; accumulated == leaf count (3), so the epoch
	// is fully accumulated and leaf 1's cleared bit ⇒ distributed — derived from
	// the live bitmap with no observation history.
	bitmap := []byte{0b00000101}
	data := buildJournalAccount(t, 951, mint, mint, 3, 1, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	leafMap := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		1: {NodeID: "node-b", ClientID: 2},
		2: {NodeID: "node-c", ClientID: 1},
	}

	rows := ProjectEpochStatuses(951, []*JournalView{view}, leafMap, nil)
	require.Len(t, rows, 3)

	byNode := indexByNode(rows)
	assert.Equal(t, uint8(1), byNode["node-a"].IsClaimable)
	assert.Equal(t, uint8(0), byNode["node-b"].IsClaimable, "cleared bit in fully-accumulated epoch ⇒ distributed")
	assert.Equal(t, DoubleZeroMintKey, byNode["node-b"].JournalMintKey, "pre-968 distributed defaults to 2Z")
	assert.Equal(t, uint8(1), byNode["node-c"].IsClaimable)
}

// TestProjectEpochStatuses_NonPublisherTokenClaimable covers the multi-token
// era: a non-2Z journal (here USDC) owns a sparse, high global leaf index. Its
// set bit attributes the leaf to USDC and marks it claimable. The other leaf is
// clear but the epoch is not fully accumulated, so it stays pending.
func TestProjectEpochStatuses_NonPublisherTokenClaimable(t *testing.T) {
	usdc := solana.MustPublicKeyFromBase58(USDCMintKey)
	bitmap := make([]byte, 3) // covers indices 0..23
	bitmap[17/8] = 1 << (17 % 8)
	data := buildJournalAccount(t, 970, usdc, usdc, 1, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)
	require.Equal(t, usdc, view.RewardMintKey)

	leafMap := map[uint32]LeafIdentity{
		3:  {NodeID: "node-x", ClientID: 1},
		17: {NodeID: "node-y", ClientID: 2},
	}

	rows := ProjectEpochStatuses(970, []*JournalView{view}, leafMap, nil)
	require.Len(t, rows, 1, "set-bit leaf claimable; the other isn't accumulated yet (pending)")
	assert.Equal(t, "node-y", rows[0].NodeID)
	assert.Equal(t, uint16(2), rows[0].ClientID)
	assert.Equal(t, uint8(1), rows[0].IsClaimable)
	assert.Equal(t, USDCMintKey, rows[0].JournalMintKey, "leaf attributed to USDC")
}

func TestProjectEpochStatuses_MissingLeafInMap(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	bitmap := []byte{0b00000101}
	data := buildJournalAccount(t, 951, mint, mint, 3, 0, bitmap)

	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	// Only leaf index 0 and 2 have a mapping. Leaf 1 has none → silently skipped.
	leafMap := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		2: {NodeID: "node-c", ClientID: 1},
	}

	rows := ProjectEpochStatuses(951, []*JournalView{view}, leafMap, nil)
	require.Len(t, rows, 2)
	byNode := indexByNode(rows)
	assert.Equal(t, uint8(1), byNode["node-a"].IsClaimable)
	assert.Equal(t, uint8(1), byNode["node-c"].IsClaimable)
}

// TestProjectEpochStatuses_MultiToken covers a fully-accumulated multi-token
// epoch: a 2Z journal and a USDC journal. A set bit identifies the owning token
// (claimable); a leaf cleared in EVERY journal is distributed, with its token
// taken from the claimable-observation map.
func TestProjectEpochStatuses_MultiToken(t *testing.T) {
	twoZ := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	usdc := solana.MustPublicKeyFromBase58(USDCMintKey)
	// 2Z journal owns leaf 0 (bit 0 set), accumulated 1.
	zView, err := DecodeJournalAccount(buildJournalAccount(t, 970, twoZ, twoZ, 1, 0, []byte{0b00000001}))
	require.NoError(t, err)
	// USDC journal: leaf 1 set (claimable), leaf 2 clear (distributed). accumulated 2.
	uView, err := DecodeJournalAccount(buildJournalAccount(t, 970, usdc, usdc, 2, 1, []byte{0b00000010}))
	require.NoError(t, err)

	views := []*JournalView{zView, uView}
	leafMap := map[uint32]LeafIdentity{
		0: {NodeID: "node-z", ClientID: 1},
		1: {NodeID: "node-u", ClientID: 1},
		2: {NodeID: "node-w", ClientID: 1},
	}
	// Σ accumulated = 3 = leaf count → fully accumulated.
	tokenByIdentity := map[LeafIdentity]string{}
	CollectClaimableTokens(views, leafMap, tokenByIdentity)
	// node-w is distributed (not claimable this epoch); mimic a prior claimable
	// observation in USDC so its distributed leaf attributes to USDC.
	tokenByIdentity[LeafIdentity{NodeID: "node-w", ClientID: 1}] = USDCMintKey

	rows := ProjectEpochStatuses(970, views, leafMap, tokenByIdentity)
	require.Len(t, rows, 3)
	byNode := indexByNode(rows)
	assert.Equal(t, uint8(1), byNode["node-z"].IsClaimable)
	assert.Equal(t, DoubleZeroMintKey, byNode["node-z"].JournalMintKey)
	assert.Equal(t, uint8(1), byNode["node-u"].IsClaimable)
	assert.Equal(t, USDCMintKey, byNode["node-u"].JournalMintKey)
	assert.Equal(t, uint8(0), byNode["node-w"].IsClaimable, "cleared everywhere + fully accumulated ⇒ distributed")
	assert.Equal(t, USDCMintKey, byNode["node-w"].JournalMintKey, "distributed token from claimable-observation map")
}

// TestProjectEpochStatuses_SingleJournalDistributedTokenUnambiguous: when an
// epoch has a single journal, a distributed leaf takes that journal's token and
// the cross-epoch token map is ignored. This is what keeps pre-multi-token
// epochs (always one 2Z journal) correctly 2Z even for a validator that later
// switched to USDC — without any hardcoded launch epoch.
func TestProjectEpochStatuses_SingleJournalDistributedTokenUnambiguous(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// bit 0 clear, accumulated 1 == leaf count → fully accumulated → distributed.
	data := buildJournalAccount(t, 960, mint, mint, 1, 1, []byte{0b00000000})
	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	leafMap := map[uint32]LeafIdentity{0: {NodeID: "node-a", ClientID: 1}}
	// Map says USDC, but the single 2Z journal wins (unambiguous).
	tokenByIdentity := map[LeafIdentity]string{{NodeID: "node-a", ClientID: 1}: USDCMintKey}

	rows := ProjectEpochStatuses(960, []*JournalView{view}, leafMap, tokenByIdentity)
	require.Len(t, rows, 1)
	assert.Equal(t, uint8(0), rows[0].IsClaimable)
	assert.Equal(t, DoubleZeroMintKey, rows[0].JournalMintKey, "single-journal epoch ⇒ that journal's token")
}

// TestProjectEpochStatuses_MultiClientSameNode is the regression test for the
// bug where a validator publishing under more than one software client in a
// single epoch was collapsed to one row. The leaves share a node_id but differ
// by client_id, so they produce two distinct rows with independent bits.
func TestProjectEpochStatuses_MultiClientSameNode(t *testing.T) {
	mint := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	// bit 0 set (claimable), bit 1 clear (distributed); accumulated == leaf count 2.
	data := buildJournalAccount(t, 951, mint, mint, 2, 1, []byte{0b00000001})
	view, err := DecodeJournalAccount(data)
	require.NoError(t, err)

	leafMap := map[uint32]LeafIdentity{
		0: {NodeID: "node-a", ClientID: 1},
		1: {NodeID: "node-a", ClientID: 2},
	}

	rows := ProjectEpochStatuses(951, []*JournalView{view}, leafMap, nil)
	require.Len(t, rows, 2, "each (node, client) leaf must produce its own row")

	byClient := make(map[uint16]LeafDistributionStatusRow, len(rows))
	for _, r := range rows {
		assert.Equal(t, "node-a", r.NodeID)
		byClient[r.ClientID] = r
	}
	assert.NotEqual(t, byClient[1].PK, byClient[2].PK, "PKs must differ by client_id")
	assert.Equal(t, LeafDistributionStatusPK(951, "node-a", 1), byClient[1].PK)
	assert.Equal(t, LeafDistributionStatusPK(951, "node-a", 2), byClient[2].PK)
	assert.Equal(t, uint8(1), byClient[1].IsClaimable)
	assert.Equal(t, uint8(0), byClient[2].IsClaimable)
}

func TestProjectEpochStatuses_NoViews(t *testing.T) {
	rows := ProjectEpochStatuses(951, nil, map[uint32]LeafIdentity{0: {NodeID: "n", ClientID: 1}}, nil)
	assert.Empty(t, rows)
}

func TestCollectClaimableTokens(t *testing.T) {
	twoZ := solana.MustPublicKeyFromBase58(DoubleZeroMintKey)
	usdc := solana.MustPublicKeyFromBase58(USDCMintKey)
	zView, err := DecodeJournalAccount(buildJournalAccount(t, 970, twoZ, twoZ, 1, 0, []byte{0b00000001}))
	require.NoError(t, err)
	uView, err := DecodeJournalAccount(buildJournalAccount(t, 970, usdc, usdc, 1, 0, []byte{0b00000010}))
	require.NoError(t, err)

	leafMap := map[uint32]LeafIdentity{
		0: {NodeID: "node-z", ClientID: 1}, // set in 2Z
		1: {NodeID: "node-u", ClientID: 1}, // set in USDC
		2: {NodeID: "node-x", ClientID: 1}, // set in neither
	}
	dst := map[LeafIdentity]string{}
	CollectClaimableTokens([]*JournalView{zView, uView}, leafMap, dst)

	assert.Equal(t, DoubleZeroMintKey, dst[LeafIdentity{NodeID: "node-z", ClientID: 1}])
	assert.Equal(t, USDCMintKey, dst[LeafIdentity{NodeID: "node-u", ClientID: 1}])
	_, ok := dst[LeafIdentity{NodeID: "node-x", ClientID: 1}]
	assert.False(t, ok, "leaf set in no journal is not recorded")
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
