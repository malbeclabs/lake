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
	offAccumulatedPublisherSlotsScaled       = 136
	offAccumulatedClientSlotsScaled          = 144
	offAccumulatedPublisherLeafCount         = 152
	offDistributedPublisherLeafCount         = 156
	offDistributedAmount                     = 160
)

// JournalView captures the subset of ShredDistributionJournal state we
// project per (subscription_epoch, node_id). Other struct fields (flags,
// swapped amounts, slot scalars, padding, gap) are skipped — they're not
// needed for the claimability projection but the byte layout above
// documents where they live in the account.
type JournalView struct {
	SubscriptionEpoch uint64
	// MintKey is the journal's seed mint — the token validators selected to be
	// rewarded in. Each (subscription_epoch, mint_key) has its own journal, and
	// a journal owns only the leaves of validators who picked that token.
	MintKey solana.PublicKey
	// RewardMintKey is the mint of the tokens actually held and paid out by this
	// journal (TokensReceivedAmount is denominated in it). For the supported
	// reward tokens (2Z, USDC, wSOL) it equals MintKey.
	RewardMintKey solana.PublicKey
	// TokensReceivedAmount is the journal's post-Jupiter-swap balance of
	// reward_mint_key tokens (`rewards_amount()` upstream for a non-bypassed
	// journal). This is the validator-reward pool that per-leaf publisher shares
	// are drawn from, in base units of RewardMintKey.
	TokensReceivedAmount                  uint64
	PublisherAccumulationBitmapStartIndex uint32
	PublisherAccumulationBitmapEndIndex   uint32
	// TotalLeaderSlots is the journal's authoritative count of all leader slots
	// in the epoch. It is the denominator the on-chain distribution uses to
	// split the pool by leader slots — NOT the sum of the leaves we happen to
	// have indexed (which can be incomplete and would over-credit each present
	// validator).
	TotalLeaderSlots uint32
	// AccumulatedPublisherSlotsScaled and AccumulatedClientSlotsScaled are the
	// running sums of leader_slots × proportion (in basis points) over the
	// leaves accumulated into THIS journal — i.e. only the validators who picked
	// this journal's token. Their sum equals (this journal's leader slots) ×
	// 10000 and is the authoritative per-token reward denominator: in the
	// multi-token era a validator's share is split over its journal's slots, not
	// the epoch-wide total.
	AccumulatedPublisherSlotsScaled uint64
	AccumulatedClientSlotsScaled    uint64
	AccumulatedPublisherLeafCount   uint32
	DistributedPublisherLeafCount   uint32
	// DistributedAmount is the running total of reward-token base units already
	// paid out by this journal. For non-2Z tokens (USDC, wSOL) the journal does
	// not pre-hold a swapped balance (TokensReceivedAmount is 0), so this is the
	// reward pool the page splits; for a finalized epoch it is the full pool.
	// (The 2Z journal instead splits TokensReceivedAmount, less the 10% burn.)
	DistributedAmount uint64
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
		AccumulatedPublisherSlotsScaled:       binary.LittleEndian.Uint64(data[offAccumulatedPublisherSlotsScaled : offAccumulatedPublisherSlotsScaled+8]),
		AccumulatedClientSlotsScaled:          binary.LittleEndian.Uint64(data[offAccumulatedClientSlotsScaled : offAccumulatedClientSlotsScaled+8]),
		AccumulatedPublisherLeafCount:         binary.LittleEndian.Uint32(data[offAccumulatedPublisherLeafCount : offAccumulatedPublisherLeafCount+4]),
		DistributedPublisherLeafCount:         binary.LittleEndian.Uint32(data[offDistributedPublisherLeafCount : offDistributedPublisherLeafCount+4]),
		DistributedAmount:                     binary.LittleEndian.Uint64(data[offDistributedAmount : offDistributedAmount+8]),
		RemainingData:                         data[journalFixedHeader:],
	}
	copy(view.MintKey[:], data[offMintKey:offMintKey+32])
	copy(view.RewardMintKey[:], data[offRewardMintKey:offRewardMintKey+32])

	return view, nil
}

// AccumulatedSlotsScaledDenominator returns the per-token reward denominator:
// accumulated_publisher_slots_scaled + accumulated_client_slots_scaled. This
// equals (the journal's leader slots) × 10000, so dividing
// tokens_received × leader_slots × (10000 − client_proportion) by it yields the
// validator's share of this journal's pool.
func (j *JournalView) AccumulatedSlotsScaledDenominator() uint64 {
	if j == nil {
		return 0
	}
	return j.AccumulatedPublisherSlotsScaled + j.AccumulatedClientSlotsScaled
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

// ProjectStatuses emits per-leaf status rows attributing each leaf to this
// journal's reward token and recording its claimable bit. Leaves with no
// (node_id, client_id) mapping in `leafIndexToIdentity` are silently skipped.
// Rows are keyed per (node, client) so a validator running multiple clients
// keeps an independent bit per client.
//
// In the multi-token era each journal owns only the leaves of validators who
// picked its token, scattered across the epoch's global leaf indices. A SET
// publisher bit is the authoritative ownership signal: it means this journal
// accumulated the leaf and has not yet distributed it (immediately claimable),
// and the row's JournalMintKey then identifies the validator's reward token.
//
// Detecting an already-distributed leaf (accumulated, then paid → bit cleared)
// cannot be done from a cleared bit alone, because a cleared bit is
// indistinguishable from "not this journal's leaf" — especially in the
// multi-token era, where each journal owns only a sparse subset of the epoch's
// global leaf indices (so the leaf index is NOT a contiguous accumulation
// prefix). We resolve it with prior attribution: `existingMint` maps each leaf
// index to the reward mint already recorded for it (from the persisted status
// table). A leaf whose bit is now clear is marked distributed for THIS journal
// only if it was previously attributed to this journal's token. A leaf never
// captured while its bit was set (e.g. distributed before we first observed it)
// has no prior attribution and is left out — its token is unrecoverable.
func ProjectStatuses(view *JournalView, subscriptionEpoch uint64, leafIndexToIdentity map[uint32]LeafIdentity, existingMint map[uint32]string) []LeafDistributionStatusRow {
	if view == nil {
		return nil
	}
	rewardMint := view.RewardMintKey.String()

	rows := make([]LeafDistributionStatusRow, 0, len(leafIndexToIdentity))
	for leafIndex, id := range leafIndexToIdentity {
		set, _ := view.IsClaimableForLeafIndex(leafIndex)
		var bit uint8
		switch {
		case set:
			// Bit set ⇒ this journal owns the leaf and it is claimable.
			bit = 1
		case rewardMint != "" && existingMint[leafIndex] == rewardMint:
			// Previously attributed to this token, bit now clear ⇒ distributed.
		default:
			// Not owned by this journal (bit unset, no prior attribution to this
			// token) ⇒ skip so we never mistag another token's (or an
			// unrecoverable) leaf.
			continue
		}
		rows = append(rows, LeafDistributionStatusRow{
			PK:                LeafDistributionStatusPK(subscriptionEpoch, id.NodeID, id.ClientID),
			SubscriptionEpoch: subscriptionEpoch,
			NodeID:            id.NodeID,
			ClientID:          id.ClientID,
			IsClaimable:       bit,
			JournalMintKey:    rewardMint,
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
