package permissionevents

import (
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Permission flag bit values, mirrored from the on-chain program for test fixtures.
const (
	flagPermissionAdmin uint64 = 1 << 1
	flagNetworkAdmin    uint64 = 1 << 3
	flagActivator       uint64 = 1 << 7
	flagSentinel        uint64 = 1 << 8
	flagUserAdmin       uint64 = 1 << 9
	flagQA              uint64 = 1 << 12
)

// u128LE encodes a low/high pair into 16 little-endian bytes.
func u128LE(lo, hi uint64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], lo)
	binary.LittleEndian.PutUint64(b[8:16], hi)
	return b
}

func TestLake_PermissionEvents_Decode_Create(t *testing.T) {
	target := solana.PublicKeyFromBytes([]byte{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
	})

	data := []byte{variantCreatePermission}
	data = append(data, target.Bytes()...)
	data = append(data, u128LE(flagNetworkAdmin|flagActivator, 0)...)

	ix, ok, err := DecodePermissionInstruction(data)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, EventCreate, ix.EventType)
	require.Equal(t, target.String(), ix.TargetUserPayer)
	require.Equal(t, flagNetworkAdmin|flagActivator, ix.Added.Lo)
	require.Equal(t, uint64(0), ix.Added.Hi)
	require.True(t, ix.Removed.IsZero())
	require.Equal(t, "network-admin, activator", FlagNames(ix.Added))
}

func TestLake_PermissionEvents_Decode_Update(t *testing.T) {
	data := []byte{variantUpdatePermission}
	data = append(data, u128LE(flagUserAdmin, 0)...) // add
	data = append(data, u128LE(flagQA, 0)...)        // remove

	ix, ok, err := DecodePermissionInstruction(data)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, EventUpdate, ix.EventType)
	require.Empty(t, ix.TargetUserPayer)
	require.Equal(t, flagUserAdmin, ix.Added.Lo)
	require.Equal(t, flagQA, ix.Removed.Lo)
	require.Equal(t, "user-admin", FlagNames(ix.Added))
	require.Equal(t, "qa", FlagNames(ix.Removed))
}

func TestLake_PermissionEvents_Decode_StatusVariants(t *testing.T) {
	cases := []struct {
		variant uint8
		want    string
	}{
		{variantSuspendPermission, EventSuspend},
		{variantResumePermission, EventResume},
		{variantDeletePermission, EventDelete},
	}
	for _, c := range cases {
		ix, ok, err := DecodePermissionInstruction([]byte{c.variant})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, c.want, ix.EventType)
		require.True(t, ix.Added.IsZero())
		require.True(t, ix.Removed.IsZero())
	}
}

func TestLake_PermissionEvents_Decode_NotAPermissionInstruction(t *testing.T) {
	// Variant 36 = CreateUser — must be ignored, not misdecoded.
	ix, ok, err := DecodePermissionInstruction([]byte{36, 0, 0, 0})
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, ix)

	// Empty data.
	ix, ok, err = DecodePermissionInstruction(nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, ix)
}

func TestLake_PermissionEvents_Decode_MalformedArgsError(t *testing.T) {
	// Recognized as CreatePermission but the arg payload is truncated.
	_, ok, err := DecodePermissionInstruction([]byte{variantCreatePermission, 1, 2, 3})
	require.Error(t, err)
	require.False(t, ok)

	_, ok, err = DecodePermissionInstruction([]byte{variantUpdatePermission, 1, 2, 3})
	require.Error(t, err)
	require.False(t, ok)
}

func TestLake_PermissionEvents_FlagNames_OrderAndEmpty(t *testing.T) {
	// Mirrors the Rust bitmask_to_names roundtrip test: activator, sentinel, qa.
	require.Equal(t, "activator, sentinel, qa",
		FlagNames(u128{Lo: flagActivator | flagSentinel | flagQA}))
	require.Equal(t, "", FlagNames(u128{}))
	require.Equal(t, "permission-admin", FlagNames(u128{Lo: flagPermissionAdmin}))
}

func TestLake_PermissionEvents_FlagNames_UnknownBitsSurfaced(t *testing.T) {
	// A bit with no known name must not be silently dropped.
	names := FlagNames(u128{Lo: flagNetworkAdmin | (1 << 40), Hi: 1})
	require.Contains(t, names, "network-admin")
	require.Contains(t, names, "unknown-lo:")
	require.Contains(t, names, "unknown-hi:")
}

func TestLake_PermissionEvents_Mask_Hex(t *testing.T) {
	require.Equal(t, "0x0", u128{}.Hex())
	require.Equal(t, "0x88", u128{Lo: 0x88}.Hex())
	require.Equal(t, "0x10000000000000001", u128{Lo: 1, Hi: 1}.Hex())
}
