package validatorrewards

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// DoubleZeroMintKey is the base58 of the canonical 2Z mint on mainnet. The
// shred-distribution journal seeded by this mint is the "publisher" journal
// — its publisher accumulation bitmap tracks which leaves have been
// accumulated but not yet distributed (i.e., immediately claimable).
//
// Reference:
//
//	doublezero-shreds/crates/shred-oracle/src/main.rs (mainnet feature flag
//	path) — uses
//	`doublezero_revenue_distribution::env::mainnet::DOUBLEZERO_MINT_KEY`.
const DoubleZeroMintKey = "J6pQQ3FAcJQeWPPGppWRb4nM8jU3wLyYbRrLh7feMfvd"

// journalSeedPrefix is the PDA seed prefix for the ShredDistributionJournal
// account, matching `ShredDistributionJournal::SEED_PREFIX` upstream.
const journalSeedPrefix = "shred_distribution_journal"

// journalFixedHeader is the byte length of the account discriminator plus
// the bytemuck-Pod struct body. The publisher / client bitmaps live in
// `remaining_data = account_data[journalFixedHeader:]`.
//
// Layout (verified against
// doublezero-shreds/programs/shred-subscription/src/state/shred_distribution_journal.rs
// lines 55-157, where `Flags = U64` is 8 bytes and `StorageGap<3>` is 96
// bytes):
//
//	offset  size  field
//	  0       8   account_discriminator
//	  8       8   subscription_epoch (u64 LE)
//	 16      32   mint_key (Pubkey)
//	 48      32   reward_mint_key (Pubkey)
//	 80       8   _flags (Flags = U64)
//	 88       8   usdc_swapped_amount (u64)
//	 96       8   tokens_received_amount (u64)
//	104       4   publisher_accumulation_bitmap_start_index (u32 LE)
//	108       4   publisher_accumulation_bitmap_end_index (u32 LE)
//	112       4   client_accumulation_bitmap_start_index (u32 LE)
//	116       4   client_accumulation_bitmap_end_index (u32 LE)
//	120       8   validator_pool (u64)
//	128       4   total_leader_slots (u32)
//	132       4   _padding_0
//	136       8   accumulated_publisher_slots_scaled (u64)
//	144       8   accumulated_client_slots_scaled (u64)
//	152       4   accumulated_publisher_leaf_count (u32)
//	156       4   distributed_publisher_leaf_count (u32)
//	160       8   distributed_amount (u64)
//	168       4   accumulated_client_leaf_count (u32)
//	172       4   distributed_client_leaf_count (u32)
//	176      16   _padding_1
//	192       8   first_distribute_timestamp (i64)
//	200      96   _gap (StorageGap<3>)
//	296    var.   remaining_data (publisher & client bitmaps)
const journalFixedHeader = 296

// JournalDiscriminator is the first 8 bytes of
// `sha256("dz::account::shred_distribution_journal")`. Verified by:
//
//	python3 -c "import hashlib; \
//	  print(hashlib.sha256(b'dz::account::shred_distribution_journal') \
//	    .digest()[:8].hex())"
//	→ 2c0081591d136694
//
// Exported so callers (e.g., the shreds batch RPC demuxer) can switch on
// this discriminator without copy-pasting the byte sequence.
var JournalDiscriminator = [8]byte{0x2c, 0x00, 0x81, 0x59, 0x1d, 0x13, 0x66, 0x94}

// Byte offsets into the account data for each decoded field. Centralised so
// changes in the upstream Rust struct (e.g., a new field inserted) update
// in one place and are visibly diffable.
const (
	offSubscriptionEpoch                     = 8
	offMintKey                               = 16
	offRewardMintKey                         = 48
	offTokensReceivedAmount                  = 96
	offPublisherAccumulationBitmapStartIndex = 104
	offPublisherAccumulationBitmapEndIndex   = 108
	offClientAccumulationBitmapStartIndex    = 112
	offClientAccumulationBitmapEndIndex      = 116
	offTotalLeaderSlots                      = 128
	offAccumulatedPublisherLeafCount         = 152
	offDistributedPublisherLeafCount         = 156
)

// JournalView captures the subset of ShredDistributionJournal state we
// project per (subscription_epoch, node_id). Other struct fields (flags,
// swapped amounts, slot scalars, padding, gap) are skipped — they're not
// needed for the claimability projection but the byte layout above
// documents where they live in the account.
type JournalView struct {
	SubscriptionEpoch uint64
	MintKey           solana.PublicKey
	// TokensReceivedAmount is the journal's post-Jupiter-swap balance of
	// reward_mint_key tokens (`rewards_amount()` upstream for a non-bypassed
	// journal). For the 2Z journal this is the validator-reward pool that
	// per-leaf publisher shares are drawn from.
	TokensReceivedAmount                  uint64
	PublisherAccumulationBitmapStartIndex uint32
	PublisherAccumulationBitmapEndIndex   uint32
	// TotalLeaderSlots is the journal's authoritative count of all leader slots
	// in the epoch. It is the denominator the on-chain distribution uses to
	// split the pool by leader slots — NOT the sum of the leaves we happen to
	// have indexed (which can be incomplete and would over-credit each present
	// validator).
	TotalLeaderSlots              uint32
	AccumulatedPublisherLeafCount uint32
	DistributedPublisherLeafCount uint32
	// RemainingData is `account_data[journalFixedHeader:]`. Bitmaps are
	// referenced into it via the start/end indices above.
	RemainingData []byte
}

// DecodeJournalAccount parses raw account bytes into a JournalView.
// Returns an error if the discriminator does not match or the data is
// shorter than the fixed header.
func DecodeJournalAccount(data []byte) (*JournalView, error) {
	if len(data) < journalFixedHeader {
		return nil, fmt.Errorf("journal account data too short: got %d bytes, need at least %d",
			len(data), journalFixedHeader)
	}

	var disc [8]byte
	copy(disc[:], data[0:8])
	if disc != JournalDiscriminator {
		return nil, fmt.Errorf("journal account discriminator mismatch: got %x, want %x",
			disc, JournalDiscriminator)
	}

	view := &JournalView{
		SubscriptionEpoch:                     binary.LittleEndian.Uint64(data[offSubscriptionEpoch : offSubscriptionEpoch+8]),
		TokensReceivedAmount:                  binary.LittleEndian.Uint64(data[offTokensReceivedAmount : offTokensReceivedAmount+8]),
		PublisherAccumulationBitmapStartIndex: binary.LittleEndian.Uint32(data[offPublisherAccumulationBitmapStartIndex : offPublisherAccumulationBitmapStartIndex+4]),
		PublisherAccumulationBitmapEndIndex:   binary.LittleEndian.Uint32(data[offPublisherAccumulationBitmapEndIndex : offPublisherAccumulationBitmapEndIndex+4]),
		TotalLeaderSlots:                      binary.LittleEndian.Uint32(data[offTotalLeaderSlots : offTotalLeaderSlots+4]),
		AccumulatedPublisherLeafCount:         binary.LittleEndian.Uint32(data[offAccumulatedPublisherLeafCount : offAccumulatedPublisherLeafCount+4]),
		DistributedPublisherLeafCount:         binary.LittleEndian.Uint32(data[offDistributedPublisherLeafCount : offDistributedPublisherLeafCount+4]),
		RemainingData:                         data[journalFixedHeader:],
	}
	copy(view.MintKey[:], data[offMintKey:offMintKey+32])

	return view, nil
}

// publisherBitmap returns the slice of RemainingData that holds the
// publisher accumulation bitmap, or nil when the bitmap is not yet
// allocated (the upstream `checked_publisher_accumulation_bitmap_range`
// returns None iff end_index == 0).
func (j *JournalView) publisherBitmap() []byte {
	if j == nil {
		return nil
	}
	if j.PublisherAccumulationBitmapEndIndex == 0 {
		return nil
	}
	start := int(j.PublisherAccumulationBitmapStartIndex)
	end := int(j.PublisherAccumulationBitmapEndIndex)
	if start < 0 || end < start || end > len(j.RemainingData) {
		return nil
	}
	return j.RemainingData[start:end]
}

// IsClaimableForLeafIndex reports whether the publisher accumulation bit
// for `leafIndex` is set. Returns (claimable, true) on success, or
// (false, false) when the bitmap is unallocated or `leafIndex` is outside
// the bitmap.
//
// Bit ordering: LSB-first within each byte, matching
// `try_clear_remaining_data_leaf_index` in
// programs/shred-subscription/src/processor/common.rs
// (`leaf_byte_index = leaf_index / 8`, `leaf_bit = leaf_index % 8`).
func (j *JournalView) IsClaimableForLeafIndex(leafIndex uint32) (bool, bool) {
	bitmap := j.publisherBitmap()
	if bitmap == nil {
		return false, false
	}
	byteIdx := int(leafIndex / 8)
	if byteIdx >= len(bitmap) {
		return false, false
	}
	bitPos := leafIndex % 8
	set := (bitmap[byteIdx]>>bitPos)&1 == 1
	return set, true
}

// ProjectStatuses emits one LeafDistributionStatusRow per leaf in
// `[0, AccumulatedPublisherLeafCount)`. Leaves with no (node_id, client_id)
// mapping in `leafIndexToIdentity` are silently skipped. The journal's mint
// key is recorded on every row as the source of the bitmap. Rows are keyed
// per (node, client) so a validator running multiple clients keeps an
// independent claimable bit per client.
func ProjectStatuses(view *JournalView, subscriptionEpoch uint64, leafIndexToIdentity map[uint32]LeafIdentity) []LeafDistributionStatusRow {
	if view == nil {
		return nil
	}
	count := view.AccumulatedPublisherLeafCount
	if count == 0 {
		return nil
	}
	rows := make([]LeafDistributionStatusRow, 0, count)
	mintB58 := view.MintKey.String()
	for leafIndex := uint32(0); leafIndex < count; leafIndex++ {
		id, ok := leafIndexToIdentity[leafIndex]
		if !ok {
			continue
		}
		claimable, ok := view.IsClaimableForLeafIndex(leafIndex)
		if !ok {
			continue
		}
		var bit uint8
		if claimable {
			bit = 1
		}
		rows = append(rows, LeafDistributionStatusRow{
			PK:                LeafDistributionStatusPK(subscriptionEpoch, id.NodeID, id.ClientID),
			SubscriptionEpoch: subscriptionEpoch,
			NodeID:            id.NodeID,
			ClientID:          id.ClientID,
			IsClaimable:       bit,
			JournalMintKey:    mintB58,
		})
	}
	return rows
}

// JournalPDA derives the (PDA, bump) for the ShredDistributionJournal
// owned by `programID` for `(subscriptionEpoch, mintKey)`. The seeds match
// `ShredDistributionJournal::find_address` upstream:
//
//	[SEED_PREFIX, subscription_epoch.to_le_bytes(), mint_key.as_ref()]
func JournalPDA(programID solana.PublicKey, subscriptionEpoch uint64, mintKey solana.PublicKey) (solana.PublicKey, uint8, error) {
	if programID.IsZero() {
		return solana.PublicKey{}, 0, errors.New("programID is required")
	}
	epochBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochBytes, subscriptionEpoch)
	seeds := [][]byte{
		[]byte(journalSeedPrefix),
		epochBytes,
		mintKey[:],
	}
	return solana.FindProgramAddress(seeds, programID)
}
