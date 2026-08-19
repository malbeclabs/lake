package feedsubscription

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realAccountBase64 is mainnet account crW8HCYDpQVyCxYG7m3hXeC42rAnjoLroGGfgGLLXM2,
// read on 2026-08-18 and truncated to the first 56 bytes by a dataSlice. Real
// bytes rather than synthesised ones are what make a wrong offset fail here.
const realAccountBase64 = "OGd+UVWaSNwwUMWSdfRNaNAxqeB5i3T95mrqWd7yZShRKdBh7irZu+oHCP8AAP3/JyAEfAAAAAA="

func TestDecodeFeedDistribution_RealMainnetAccount(t *testing.T) {
	data, err := base64.StdEncoding.DecodeString(realAccountBase64)
	require.NoError(t, err)

	got, err := DecodeFeedDistribution(data)
	require.NoError(t, err)

	assert.Equal(t, uint16(2026), got.Year)
	assert.Equal(t, uint8(8), got.Month)
	assert.Equal(t, uint64(2080645159), got.CollectedUSDC)
	assert.Equal(t, "4Fc1Fyd1x8BoWYPWN8vFhbP6fpgayybQuLUSPRwfE7Wi", got.FeedKey.String())
}

func TestDecodeFeedDistribution_RejectsWrongDiscriminator(t *testing.T) {
	data, err := base64.StdEncoding.DecodeString(realAccountBase64)
	require.NoError(t, err)
	// Flip one discriminator byte. A v1 account would land here, and reading it
	// as v2 would report a wrong number rather than an error.
	data[0] ^= 0xff

	_, err = DecodeFeedDistribution(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "discriminator")
}

func TestDecodeFeedDistribution_RejectsShortAccount(t *testing.T) {
	data, err := base64.StdEncoding.DecodeString(realAccountBase64)
	require.NoError(t, err)

	_, err = DecodeFeedDistribution(data[:47])
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestDiscriminatorMatchesOnchainSeed(t *testing.T) {
	// Pins the constant to the string the program hashes, so a v3 bump cannot
	// pass silently.
	assert.Equal(t, "38677e51559a48dc", hexDiscriminator())
}
