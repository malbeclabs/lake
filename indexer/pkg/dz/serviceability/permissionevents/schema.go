package permissionevents

import (
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// PermissionEventRow is a single decoded Permission-management instruction — one row
// in the append-only audit trail. Column order in ToRow must match the migration DDL
// (event_ts first, as the TimeColumn).
type PermissionEventRow struct {
	EventTS                time.Time
	IngestedAt             time.Time
	TxSignature            string
	Slot                   uint64
	InstructionIndex       uint16
	Signer                 string // acting admin = transaction fee-payer
	PermissionPK           string // instruction account[0] = the Permission PDA
	TargetPubkey           string // grantee (user_payer); set for Create, resolved best-effort otherwise
	EventType              string // Create | Update | Suspend | Resume | Delete
	PermissionsAdded       string // decoded role names, comma-separated
	PermissionsRemoved     string // decoded role names, comma-separated
	PermissionsAddedMask   string // raw bitmask, hex (no truncation)
	PermissionsRemovedMask string // raw bitmask, hex
	Success                uint8  // 1 if the transaction succeeded, 0 if it failed on-chain
}

type permissionEventSchema struct{}

func (s *permissionEventSchema) Name() string { return "dz_permission_events" }

func (s *permissionEventSchema) UniqueKeyColumns() []string {
	return []string{"permission_pk", "slot", "tx_signature", "instruction_index"}
}

func (s *permissionEventSchema) Columns() []string {
	return []string{
		"ingested_at:TIMESTAMP",
		"tx_signature:VARCHAR",
		"slot:BIGINT",
		"instruction_index:INTEGER",
		"signer:VARCHAR",
		"permission_pk:VARCHAR",
		"target_pubkey:VARCHAR",
		"event_type:VARCHAR",
		"permissions_added:VARCHAR",
		"permissions_removed:VARCHAR",
		"permissions_added_mask:VARCHAR",
		"permissions_removed_mask:VARCHAR",
		"success:INTEGER",
	}
}

func (s *permissionEventSchema) TimeColumn() string           { return "event_ts" }
func (s *permissionEventSchema) PartitionByTime() bool        { return true }
func (s *permissionEventSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }
func (s *permissionEventSchema) DedupVersionColumn() string   { return "ingested_at" }

func (s *permissionEventSchema) ToRow(row PermissionEventRow) []any {
	return []any{
		row.EventTS.UTC(),          // event_ts
		row.IngestedAt,             // ingested_at
		row.TxSignature,            // tx_signature
		row.Slot,                   // slot
		row.InstructionIndex,       // instruction_index
		row.Signer,                 // signer
		row.PermissionPK,           // permission_pk
		row.TargetPubkey,           // target_pubkey
		row.EventType,              // event_type
		row.PermissionsAdded,       // permissions_added
		row.PermissionsRemoved,     // permissions_removed
		row.PermissionsAddedMask,   // permissions_added_mask
		row.PermissionsRemovedMask, // permissions_removed_mask
		row.Success,                // success
	}
}

var schema = &permissionEventSchema{}

func newDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, schema)
}
