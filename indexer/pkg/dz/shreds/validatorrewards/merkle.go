package validatorrewards

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// LeafPrefix is the first-hash domain-separation prefix used when hashing
// validator rewards leaves. It must match the prefix used by the on-chain
// shred-subscription program.
const LeafPrefix = "dz::validator_rewards"

// nodePrefix is the domain-separation prefix used for internal (non-leaf)
// nodes. Matches svm-hash's NODE_PREFIX (a single 0x01 byte).
var nodePrefix = []byte{0x01}

// indexSeparator is the trailing byte in the second_prefix used for indexed
// leaves (svm-hash's INDEX_SEPARATOR).
const indexSeparator byte = 0xFF

// LeafBytes is the unhashed per-validator pod data that goes into the merkle
// tree. The on-wire layout is fixed and little-endian; see PodBytes.
type LeafBytes struct {
	NodeID      [32]byte
	LeaderSlots uint32
	ClientID    uint16
}

// PodBytes returns the 40-byte little-endian representation of the leaf:
//
//	node_id[32] || leader_slots[u32_le] || client_id[u16_le] || _reserved[2]
//
// The two trailing reserved bytes are always zero.
func (l LeafBytes) PodBytes() [40]byte {
	var b [40]byte
	copy(b[0:32], l.NodeID[:])
	binary.LittleEndian.PutUint32(b[32:36], l.LeaderSlots)
	binary.LittleEndian.PutUint16(b[36:38], l.ClientID)
	// b[38], b[39] left as zero (reserved padding).
	return b
}

// SortedLeaves returns a copy of in sorted by (NodeID ascending, ClientID
// ascending). The original slice is not modified.
func SortedLeaves(in []LeafBytes) []LeafBytes {
	out := make([]LeafBytes, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool {
		if c := bytes.Compare(out[i].NodeID[:], out[j].NodeID[:]); c != 0 {
			return c < 0
		}
		return out[i].ClientID < out[j].ClientID
	})
	return out
}

// MerkleRoot computes the indexed-pod-leaf merkle root over the already-sorted
// leaves. The result is byte-identical to svm-hash's
// merkle_root_from_indexed_pod_leaves with leaf_prefix = LeafPrefix.
//
// MerkleRoot assumes the caller has already sorted the input via SortedLeaves;
// it does NOT re-sort.
//
// For an empty input, MerkleRoot returns the zero hash.
func MerkleRoot(sorted []LeafBytes) [32]byte {
	if len(sorted) == 0 {
		return [32]byte{}
	}

	nodes := make([][32]byte, len(sorted))
	for i, leaf := range sorted {
		pod := leaf.PodBytes()
		nodes[i] = hashIndexedLeaf(uint32(i), pod[:])
	}

	length := len(nodes)
	for length > 1 {
		writeIdx := 0
		for readIdx := 0; readIdx < length; readIdx += 2 {
			left := nodes[readIdx]
			var right [32]byte
			rightIdx := readIdx + 1
			if rightIdx < length {
				right = nodes[rightIdx]
			} else {
				right = dummyRightLeaf(uint32(rightIdx), left)
			}
			nodes[writeIdx] = hashPair(left, right)
			writeIdx++
		}
		length = writeIdx
	}

	return nodes[0]
}

// hashIndexedLeaf computes the indexed-leaf hash:
//
//	first  = sha256(LeafPrefix || pod)
//	prefix = [0x00, idx_le_u32 (4 bytes), 0xFF]
//	leaf   = sha256(prefix || first)
func hashIndexedLeaf(idx uint32, pod []byte) [32]byte {
	h1 := sha256.New()
	h1.Write([]byte(LeafPrefix))
	h1.Write(pod)
	first := h1.Sum(nil)

	var second [6]byte
	second[0] = 0x00 // DEFAULT_LEAF_PREFIX[0]
	binary.LittleEndian.PutUint32(second[1:5], idx)
	second[5] = indexSeparator

	h2 := sha256.New()
	h2.Write(second[:])
	h2.Write(first)
	var out [32]byte
	copy(out[:], h2.Sum(nil))
	return out
}

// hashPair computes sha256(NODE_PREFIX || left || right).
func hashPair(left, right [32]byte) [32]byte {
	h := sha256.New()
	h.Write(nodePrefix)
	h.Write(left[:])
	h.Write(right[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// dummyRightLeaf computes sha256(right_index_le_u32 || left). This is a single
// hash with no domain-separation prefix; matches svm-hash's dummy_right_leaf.
func dummyRightLeaf(rightIndex uint32, left [32]byte) [32]byte {
	var idx [4]byte
	binary.LittleEndian.PutUint32(idx[:], rightIndex)

	h := sha256.New()
	h.Write(idx[:])
	h.Write(left[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
