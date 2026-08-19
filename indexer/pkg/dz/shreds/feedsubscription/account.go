// Package feedsubscription indexes the DoubleZero feed-subscription program's
// FeedDistribution accounts: how much USDC each feed collected in each calendar
// month.
//
// The program has no Go bindings yet, so this package decodes the account at
// fixed byte offsets. The same call was made for ShredDistributionJournal (see
// dzshreds.KeyedJournal). malbeclabs/infra#2313 asks for generated bindings; when
// they land, DecodeFeedDistribution is the one place to replace.
package feedsubscription

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// ProgramID is the feed-subscription program. declare_id! sets one address for
// every cluster, so this is not per-environment configuration. The program is
// deployed on Solana mainnet-beta only; on a cluster without it,
// getProgramAccounts returns an empty list rather than an error.
var ProgramID = solana.MustPublicKeyFromBase58("J9gupbyffs4XAoKn5NrJ4hrbdqW5ZfvMDaaas3FtH8yC")

// discriminatorSeed is the string the program hashes for FeedDistribution's
// discriminator. The ::v2 suffix matters: v2 replaced the per-month vault with a
// per-feed vault, which orphaned every v1 account. Reading a v1 account as v2
// would report a wrong amount, so a mismatch is an error, never a fallback.
const discriminatorSeed = "dz::account::feed_distribution::v2"

// DiscriminatorFeedDistribution is the first 8 bytes of sha256(discriminatorSeed).
var DiscriminatorFeedDistribution = func() [8]byte {
	sum := sha256.Sum256([]byte(discriminatorSeed))
	var d [8]byte
	copy(d[:], sum[:8])
	return d
}()

// Field offsets on the wire: an 8-byte discriminator followed by the 120-byte
// #[repr(C, align(8))] struct. The struct has no interior padding before
// collected_usdc_amount, so every offset below is the struct offset plus 8.
const (
	offsetFeedKey       = 8
	offsetYear          = 40
	offsetMonth         = 42
	offsetCollectedUSDC = 48
	// minAccountLen is the end of collected_usdc_amount. Everything after it is
	// reserved flags and a storage gap this package does not read, so a shorter
	// account is the only length worth rejecting.
	minAccountLen = 56
)

// FeedDistribution is the part of the on-chain account this package indexes.
// publisher_rewards_proportion_bps is deliberately absent: settlement records
// it and settlement does not run yet, so it is zero on every account today.
type FeedDistribution struct {
	FeedKey       solana.PublicKey
	Year          uint16
	Month         uint8
	CollectedUSDC uint64
}

// DecodeFeedDistribution reads a FeedDistribution from raw account data.
func DecodeFeedDistribution(data []byte) (*FeedDistribution, error) {
	if len(data) < minAccountLen {
		return nil, fmt.Errorf("feed distribution account too short: %d bytes, want at least %d", len(data), minAccountLen)
	}
	if [8]byte(data[:8]) != DiscriminatorFeedDistribution {
		return nil, fmt.Errorf("feed distribution discriminator mismatch: got %x, want %x", data[:8], DiscriminatorFeedDistribution)
	}
	return &FeedDistribution{
		FeedKey:       solana.PublicKeyFromBytes(data[offsetFeedKey : offsetFeedKey+32]),
		Year:          binary.LittleEndian.Uint16(data[offsetYear : offsetYear+2]),
		Month:         data[offsetMonth],
		CollectedUSDC: binary.LittleEndian.Uint64(data[offsetCollectedUSDC : offsetCollectedUSDC+8]),
	}, nil
}

// hexDiscriminator renders the discriminator for the "nothing matched" error in
// fetchFeedDistributions, and lets a test pin the constant to a literal.
func hexDiscriminator() string {
	return hex.EncodeToString(DiscriminatorFeedDistribution[:])
}
