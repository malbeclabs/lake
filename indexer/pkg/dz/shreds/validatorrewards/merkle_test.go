package validatorrewards

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPodBytes_Layout(t *testing.T) {
	l := LeafBytes{
		NodeID:      [32]byte{1, 2, 3},
		LeaderSlots: 0x0000_BEEF,
		ClientID:    0x00CA,
	}
	b := l.PodBytes()
	require.Equal(t, 40, len(b))
	assert.Equal(t, byte(1), b[0])
	assert.Equal(t, byte(2), b[1])
	assert.Equal(t, byte(3), b[2])
	assert.Equal(t, byte(0xEF), b[32])
	assert.Equal(t, byte(0xBE), b[33])
	assert.Equal(t, byte(0x00), b[34])
	assert.Equal(t, byte(0x00), b[35])
	assert.Equal(t, byte(0xCA), b[36])
	assert.Equal(t, byte(0x00), b[37])
	assert.Equal(t, byte(0x00), b[38])
	assert.Equal(t, byte(0x00), b[39])
}

func TestSortedLeaves_OrderByNodeIDThenClient(t *testing.T) {
	in := []LeafBytes{
		{NodeID: [32]byte{0x02}, ClientID: 5},
		{NodeID: [32]byte{0x01}, ClientID: 7},
		{NodeID: [32]byte{0x01}, ClientID: 3},
	}
	out := SortedLeaves(in)
	assert.Equal(t, byte(0x01), out[0].NodeID[0])
	assert.Equal(t, uint16(3), out[0].ClientID)
	assert.Equal(t, byte(0x01), out[1].NodeID[0])
	assert.Equal(t, uint16(7), out[1].ClientID)
	assert.Equal(t, byte(0x02), out[2].NodeID[0])
}

func TestMerkleRoot_OneLeaf_PinnedFromRust(t *testing.T) {
	var nid [32]byte
	nid[0] = 0xAA
	leaves := []LeafBytes{{NodeID: nid, LeaderSlots: 100, ClientID: 1}}
	root := MerkleRoot(SortedLeaves(leaves))
	expected := mustHex(t, "3a4f3d885981df3ef58aeb8b3d4527465da3ad7ca1d331a763d19d0c2e3bec4a")
	assert.Equal(t, expected, root)
}

func TestMerkleRoot_TwoLeaf_PinnedFromRust(t *testing.T) {
	var nid1, nid2 [32]byte
	for i := 0; i < 32; i++ {
		nid1[i] = 1
		nid2[i] = 2
	}
	leaves := []LeafBytes{
		{NodeID: nid1, LeaderSlots: 100, ClientID: 1},
		{NodeID: nid2, LeaderSlots: 200, ClientID: 2},
	}
	root := MerkleRoot(SortedLeaves(leaves))
	expected := mustHex(t, "a5260cfe9ef131080fb3f264e2569adb78db32836ea26b7cd22166ab3d7d72a3")
	assert.Equal(t, expected, root)
}

func TestMerkleRoot_ThreeLeaf_OddTail_PinnedFromRust(t *testing.T) {
	var n1, n2, n3 [32]byte
	n1[0], n2[0], n3[0] = 1, 2, 3
	leaves := []LeafBytes{
		{NodeID: n1, LeaderSlots: 10, ClientID: 1},
		{NodeID: n2, LeaderSlots: 20, ClientID: 2},
		{NodeID: n3, LeaderSlots: 30, ClientID: 3},
	}
	root := MerkleRoot(SortedLeaves(leaves))
	expected := mustHex(t, "d27efcf66dd8d10d3019bae60771794b6ea113b99dfae77b7b165d66089876ba")
	assert.Equal(t, expected, root)
}

func mustHex(t *testing.T, s string) [32]byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	require.Equal(t, 32, len(b))
	var out [32]byte
	copy(out[:], b)
	return out
}
