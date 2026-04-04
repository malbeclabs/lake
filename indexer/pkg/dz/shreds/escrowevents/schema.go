package escrowevents

import (
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// EscrowEventRow represents a single parsed escrow event from on-chain
// transaction logs.
type EscrowEventRow struct {
	EventTS          time.Time
	IngestedAt       time.Time
	EscrowPK         string
	ClientSeatPK     string
	TxSignature      string
	Slot             uint64
	EventType        string
	AmountUSDC       *int64
	BalanceAfterUSDC *int64
	Epoch            *uint64
	Status           string
	Signer           string
}

// EscrowInfo identifies a payment escrow account and its associated client seat.
type EscrowInfo struct {
	EscrowPK     string
	ClientSeatPK string
}

type escrowEventSchema struct{}

func (s *escrowEventSchema) Name() string { return "dz_shred_escrow_events" }

func (s *escrowEventSchema) UniqueKeyColumns() []string {
	return []string{"escrow_pk", "slot", "tx_signature", "event_type"}
}

func (s *escrowEventSchema) Columns() []string {
	return []string{
		"ingested_at:TIMESTAMP",
		"escrow_pk:VARCHAR",
		"client_seat_pk:VARCHAR",
		"tx_signature:VARCHAR",
		"slot:BIGINT",
		"event_type:VARCHAR",
		"amount_usdc:BIGINT",
		"balance_after_usdc:BIGINT",
		"epoch:BIGINT",
		"status:VARCHAR",
		"signer:VARCHAR",
	}
}

func (s *escrowEventSchema) TimeColumn() string           { return "event_ts" }
func (s *escrowEventSchema) PartitionByTime() bool        { return true }
func (s *escrowEventSchema) DedupMode() dataset.DedupMode { return dataset.DedupReplacing }
func (s *escrowEventSchema) DedupVersionColumn() string   { return "ingested_at" }

func (s *escrowEventSchema) ToRow(row EscrowEventRow) []any {
	return []any{
		row.EventTS.UTC(),    // event_ts
		row.IngestedAt,       // ingested_at
		row.EscrowPK,         // escrow_pk
		row.ClientSeatPK,     // client_seat_pk
		row.TxSignature,      // tx_signature
		row.Slot,             // slot
		row.EventType,        // event_type
		row.AmountUSDC,       // amount_usdc
		row.BalanceAfterUSDC, // balance_after_usdc
		row.Epoch,            // epoch
		row.Status,           // status
		row.Signer,           // signer
	}
}

var schema = &escrowEventSchema{}

func newDataset(log *slog.Logger) (*dataset.FactDataset, error) {
	return dataset.NewFactDataset(log, schema)
}
