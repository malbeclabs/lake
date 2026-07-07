// Package permissionevents indexes DoubleZero serviceability Permission-management
// transactions (grant / change / suspend / resume / revoke) into an append-only
// ClickHouse audit trail.
//
// The on-chain program keeps no audit trail: Permission accounts store only current
// state and the permission processors emit no logs/events. So — unlike escrowevents,
// which parses program log messages — this source decodes the *instruction data bytes*
// (leading variant byte + Borsh args) fetched via getTransaction.
//
// The decode contract below is reimplemented from the Rust source of truth (kept in
// sync manually; there is no Go SDK decoder). See:
//   - smartcontract/programs/doublezero-serviceability/src/instructions.rs (variants 97-101)
//   - smartcontract/programs/doublezero-serviceability/src/processors/permission/{create,update}.rs (args)
//   - smartcontract/programs/doublezero-serviceability/src/state/permission.rs (flag bits)
//   - smartcontract/cli/src/permission/flags.rs (bitmask -> role names + emit order)
package permissionevents

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/gagliardetto/solana-go"
)

// Serviceability instruction variant tags for the Permission-management instructions.
// These are the first byte of the instruction data (instructions.rs:228-232 / 372-376).
const (
	variantCreatePermission  uint8 = 97
	variantUpdatePermission  uint8 = 98
	variantSuspendPermission uint8 = 99
	variantResumePermission  uint8 = 100
	variantDeletePermission  uint8 = 101
)

// Event type labels stored in the fact table.
const (
	EventCreate  = "Create"
	EventUpdate  = "Update"
	EventSuspend = "Suspend"
	EventResume  = "Resume"
	EventDelete  = "Delete"
)

// u128 is a 128-bit little-endian unsigned integer split into low/high 64-bit halves,
// mirroring how the Go SDK stores the Permission account bitmask (PermissionsLo/Hi).
// The permission flags only occupy bits 0-17 today (all in Lo), but we decode the full
// 16 bytes so a future flag in the high word is preserved rather than truncated.
type u128 struct {
	Lo uint64
	Hi uint64
}

// readU128LE decodes a little-endian u128 from the first 16 bytes of b.
func readU128LE(b []byte) u128 {
	return u128{
		Lo: binary.LittleEndian.Uint64(b[0:8]),
		Hi: binary.LittleEndian.Uint64(b[8:16]),
	}
}

// IsZero reports whether the mask has no bits set.
func (m u128) IsZero() bool { return m.Lo == 0 && m.Hi == 0 }

// Hex renders the mask as a canonical 0x-prefixed hex string.
func (m u128) Hex() string {
	if m.Hi == 0 {
		return fmt.Sprintf("0x%x", m.Lo)
	}
	return fmt.Sprintf("0x%x%016x", m.Hi, m.Lo)
}

// flagName pairs a bit mask with its role name. Ordered to match the Rust
// bitmask_to_names() emit order (flags.rs:117-136) so audit output is identical.
var flagNames = []struct {
	bit  uint64
	name string
}{
	{1 << 0, "foundation"},
	{1 << 1, "permission-admin"},
	{1 << 13, "globalstate-admin"},
	{1 << 14, "contributor-admin"},
	{1 << 2, "infra-admin"},
	{1 << 3, "network-admin"},
	{1 << 4, "tenant-admin"},
	{1 << 5, "multicast-admin"},
	{1 << 6, "feed-authority"},
	{1 << 7, "activator"},
	{1 << 8, "sentinel"},
	{1 << 9, "user-admin"},
	{1 << 10, "access-pass-admin"},
	{1 << 11, "health-oracle"},
	{1 << 12, "qa"},
	{1 << 15, "topology-admin"},
	{1 << 16, "resource-admin"},
	{1 << 17, "index-admin"},
}

// knownFlagBits is the union of all named flag bits (all in the low 64 bits). Computed
// once from flagNames so FlagNames doesn't recompute the constant on every call.
var knownFlagBits = func() uint64 {
	var known uint64
	for _, f := range flagNames {
		known |= f.bit
	}
	return known
}()

// FlagNames returns the comma-separated role names set in the mask, in the same order
// as the Rust CLI. Known flags all live in the low 64 bits; if any unknown high bits are
// set they are surfaced as "unknown-hi:0x..." so the audit never silently drops a grant.
func FlagNames(m u128) string {
	var out []string
	for _, f := range flagNames {
		if m.Lo&f.bit != 0 {
			out = append(out, f.name)
		}
	}
	// Surface any bits we don't have a name for (future flags) rather than hiding them.
	if unknownLo := m.Lo &^ knownFlagBits; unknownLo != 0 {
		out = append(out, fmt.Sprintf("unknown-lo:0x%x", unknownLo))
	}
	if m.Hi != 0 {
		out = append(out, fmt.Sprintf("unknown-hi:0x%x", m.Hi))
	}
	return strings.Join(out, ", ")
}

// DecodedPermissionIx is a decoded Permission-management instruction.
type DecodedPermissionIx struct {
	Variant   uint8
	EventType string

	// TargetUserPayer is the grantee, base58-encoded. Only CreatePermission carries it
	// in its args; for the other variants it is empty and must be resolved by the caller
	// (from the Permission PDA account, or by joining the indexed create row).
	TargetUserPayer string

	// Added/Removed are the permission bitmask deltas. For Create, Added is the granted
	// mask and Removed is zero. For Update, both apply. For Suspend/Resume/Delete both
	// are zero (they only change status / close the account).
	Added   u128
	Removed u128
}

// DecodePermissionInstruction decodes a single serviceability instruction's data bytes.
// It returns (decoded, true, nil) for a Permission-management instruction, (nil, false, nil)
// for any other (non-permission) instruction, and (nil, false, err) when a permission
// variant is recognized but its argument payload is malformed.
func DecodePermissionInstruction(data []byte) (*DecodedPermissionIx, bool, error) {
	if len(data) == 0 {
		return nil, false, nil
	}

	switch data[0] {
	case variantCreatePermission:
		// PermissionCreateArgs { user_payer: Pubkey(32), permissions: u128(16) }
		const want = 1 + 32 + 16
		if len(data) < want {
			return nil, false, fmt.Errorf("CreatePermission: data too short: got %d want >= %d", len(data), want)
		}
		return &DecodedPermissionIx{
			Variant:         data[0],
			EventType:       EventCreate,
			TargetUserPayer: solana.PublicKeyFromBytes(data[1:33]).String(),
			Added:           readU128LE(data[33:49]),
		}, true, nil

	case variantUpdatePermission:
		// PermissionUpdateArgs { add: u128(16), remove: u128(16) }
		const want = 1 + 16 + 16
		if len(data) < want {
			return nil, false, fmt.Errorf("UpdatePermission: data too short: got %d want >= %d", len(data), want)
		}
		return &DecodedPermissionIx{
			Variant:   data[0],
			EventType: EventUpdate,
			Added:     readU128LE(data[1:17]),
			Removed:   readU128LE(data[17:33]),
		}, true, nil

	case variantSuspendPermission:
		return &DecodedPermissionIx{Variant: data[0], EventType: EventSuspend}, true, nil
	case variantResumePermission:
		return &DecodedPermissionIx{Variant: data[0], EventType: EventResume}, true, nil
	case variantDeletePermission:
		return &DecodedPermissionIx{Variant: data[0], EventType: EventDelete}, true, nil
	}

	return nil, false, nil
}
