package escrowevents

import (
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// unknownActionsSeen dedupes the unknown-instruction log per action string: a
// new on-chain instruction type is actionable (missing decoder) and logs
// ERROR once, then WARN for every further matching transaction so the backlog
// doesn't re-page until the decoder catches up. Action strings come from the
// escrow program's own logs, so cardinality is bounded by its instruction set.
var unknownActionsSeen sync.Map

// Event type constants.
const (
	EventTypeFund             = "fund"
	EventTypeAllocateSeat     = "allocate_seat"
	EventTypeWithdrawSeat     = "withdraw_seat"
	EventTypeClose            = "close"
	EventTypeBatchAllocate    = "batch_allocate"
	EventTypeBatchSettle      = "batch_settle"
	EventTypeInitializeSeat   = "initialize_seat"
	EventTypeInitializeEscrow = "initialize_escrow"
	EventTypeAckAllocate      = "ack_allocate"
	EventTypeRejectAllocate   = "reject_allocate"
	EventTypeAckWithdraw      = "ack_withdraw"
	EventTypeSetPriceOverride = "set_price_override"
	EventTypeUnknown          = "unknown"
)

// parsedEvent holds the result of parsing a single instruction's logs.
type parsedEvent struct {
	EventType string
	Amount    *int64
	Balance   *int64
	Epoch     *uint64
}

// ParseTransactionLogs parses Solana transaction log messages into escrow events.
// The parsing logic mirrors the Rust CLI's summarize_tx_logs function.
func ParseTransactionLogs(
	log *slog.Logger,
	escrowPK string,
	clientSeatPK string,
	txSig string,
	slot uint64,
	blockTime time.Time,
	logs []string,
	failed bool,
	programID string,
	signer string,
) []EscrowEventRow {
	groups := splitIntoInstructionGroups(logs, programID)

	var events []EscrowEventRow
	for _, g := range groups {
		pe := parseInstructionGroup(g.action, g.details, clientSeatPK)
		if pe == nil {
			continue
		}
		if pe.EventType == EventTypeUnknown && log != nil {
			args := []any{"action", g.action, "escrow_pk", escrowPK, "tx_signature", txSig}
			if _, seen := unknownActionsSeen.LoadOrStore(g.action, struct{}{}); seen {
				log.Warn("shreds/escrow-events: unknown instruction action", args...)
			} else {
				log.Error("shreds/escrow-events: unknown instruction action", args...)
			}
		}

		status := "ok"
		if failed {
			status = "failed"
		}

		events = append(events, EscrowEventRow{
			EventTS:          blockTime,
			EscrowPK:         escrowPK,
			ClientSeatPK:     clientSeatPK,
			TxSignature:      txSig,
			Slot:             slot,
			EventType:        pe.EventType,
			AmountUSDC:       pe.Amount,
			BalanceAfterUSDC: pe.Balance,
			Epoch:            pe.Epoch,
			Status:           status,
			Signer:           signer,
		})
	}

	fillAllocateChargeFromBalanceDelta(events)

	return events
}

// fillAllocateChargeFromBalanceDelta sets amount_usdc for instant-allocation
// events that don't carry a logged charge. Programs from before the
// "Charged {N} for instant seat allocation" log was added only emit the escrow
// balance after the allocation, not the amount charged. Since an instant
// allocation is bundled after a fund (and escrow balances chain across the
// transaction's instructions), the charge is the drop from the nearest
// preceding event with a known escrow balance. Events already populated by the
// log path are left untouched.
func fillAllocateChargeFromBalanceDelta(events []EscrowEventRow) {
	for i := range events {
		e := &events[i]
		if e.EventType != EventTypeAllocateSeat || e.AmountUSDC != nil || e.BalanceAfterUSDC == nil {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			if events[j].BalanceAfterUSDC == nil {
				continue
			}
			if charge := *events[j].BalanceAfterUSDC - *e.BalanceAfterUSDC; charge >= 0 {
				e.AmountUSDC = &charge
			}
			break
		}
	}
}

// instructionGroup represents a single instruction's logs within a transaction.
type instructionGroup struct {
	action  string
	details []string
}

// splitIntoInstructionGroups splits transaction logs into per-instruction groups
// based on "invoke [1]" boundaries for the given program.
func splitIntoInstructionGroups(logs []string, programID string) []instructionGroup {
	var groups []instructionGroup

	for _, log := range logs {
		if strings.Contains(log, programID) && strings.Contains(log, "invoke [1]") {
			groups = append(groups, instructionGroup{})
		} else if msg, ok := strings.CutPrefix(log, "Program log: "); ok {
			if len(groups) == 0 {
				continue
			}
			g := &groups[len(groups)-1]
			if g.action == "" {
				g.action = msg
			} else {
				g.details = append(g.details, msg)
			}
		}
	}

	return groups
}

// parseInstructionGroup parses a single instruction group into a parsedEvent.
func parseInstructionGroup(action string, details []string, clientSeatPK string) *parsedEvent {
	switch action {
	case "Fund payment escrow with USDC":
		return parseFund(details)
	case "Request instant seat allocation":
		return parseInstantAllocate(details)
	case "Request instant seat withdrawal":
		return &parsedEvent{EventType: EventTypeWithdrawSeat}
	case "Request prorated instant seat withdrawal":
		return parseProratedWithdraw(details)
	case "Close payment escrow":
		return parseClose(details)
	case "Batch allocate seats":
		return parseBatchAllocate(details, clientSeatPK)
	case "Batch settle devices":
		return &parsedEvent{EventType: EventTypeBatchSettle}
	case "Initialize client seat":
		return &parsedEvent{EventType: EventTypeInitializeSeat}
	case "Initialize payment escrow":
		return &parsedEvent{EventType: EventTypeInitializeEscrow}
	case "Ack instant seat allocation":
		return parseAckAllocate(details)
	case "Reject instant seat allocation":
		return &parsedEvent{EventType: EventTypeRejectAllocate}
	case "Ack instant seat withdrawal":
		return &parsedEvent{EventType: EventTypeAckWithdraw}
	case "Set client seat price override":
		return &parsedEvent{EventType: EventTypeSetPriceOverride}
	case "Check CLI version":
		return nil
	default:
		return &parsedEvent{EventType: EventTypeUnknown}
	}
}

// parseChargedAmount parses the charged amount (micro-USDC) from a "Charged ..."
// log line, accepting both the legacy "Charged: {n}" form (older batch-allocate
// logs) and the newer "Charged {n} for {batch,instant} seat allocation" form.
func parseChargedAmount(d string) (int64, bool) {
	if after, ok := strings.CutPrefix(d, "Charged: "); ok {
		n, err := strconv.ParseInt(after, 10, 64)
		return n, err == nil
	}
	if after, ok := strings.CutPrefix(d, "Charged "); ok {
		numStr := after
		if idx := strings.Index(after, " for "); idx >= 0 {
			numStr = after[:idx]
		}
		n, err := strconv.ParseInt(numStr, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func parseFund(details []string) *parsedEvent {
	pe := &parsedEvent{EventType: EventTypeFund}

	for _, d := range details {
		// Amount: "Funded payment escrow for client seat ... with {N} USDC"
		if after, ok := strings.CutSuffix(d, " USDC"); ok {
			if idx := strings.LastIndex(after, " with "); idx >= 0 {
				if n, err := strconv.ParseInt(after[idx+6:], 10, 64); err == nil {
					pe.Amount = &n
				}
			}
		}
		// Balance: "USDC balance after funding: {N}"
		if after, ok := strings.CutPrefix(d, "USDC balance after funding: "); ok {
			if n, err := strconv.ParseInt(after, 10, 64); err == nil {
				pe.Balance = &n
			}
		}
	}

	return pe
}

func parseInstantAllocate(details []string) *parsedEvent {
	pe := &parsedEvent{EventType: EventTypeAllocateSeat}

	for _, d := range details {
		pe.Epoch = parseEpochFromTenure(d, pe.Epoch)
		// Amount: "Charged {N} for instant seat allocation" (N in micro-USDC).
		// Older program versions didn't log the charge; ParseTransactionLogs
		// recovers it from the escrow balance delta as a fallback.
		if n, ok := parseChargedAmount(d); ok {
			pe.Amount = &n
		}
		if after, ok := strings.CutPrefix(d, "Escrow balance: "); ok {
			if n, err := strconv.ParseInt(after, 10, 64); err == nil {
				pe.Balance = &n
			}
		}
	}

	return pe
}

func parseClose(details []string) *parsedEvent {
	pe := &parsedEvent{EventType: EventTypeClose}
	zero := int64(0)
	pe.Balance = &zero

	for _, d := range details {
		if after, ok := strings.CutPrefix(d, "Withdrew "); ok {
			parts := strings.SplitN(after, " ", 2)
			if n, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				pe.Amount = &n
			}
		}
	}

	return pe
}

// parseProratedWithdraw parses logs from RequestProratedInstantSeatWithdrawal.
// We keep EventType=withdraw_seat so the unique key matches the older
// non-prorated variant (dedup replaces on re-ingest); the discriminator at
// query time is amount_usdc IS NOT NULL, which only the prorated path sets.
// The prorated variant emits "Refunded {N} USDC" — N is in micro-USDC.
func parseProratedWithdraw(details []string) *parsedEvent {
	pe := &parsedEvent{EventType: EventTypeWithdrawSeat}

	for _, d := range details {
		if after, ok := strings.CutPrefix(d, "Refunded "); ok {
			if before, ok := strings.CutSuffix(after, " USDC"); ok {
				if n, err := strconv.ParseInt(before, 10, 64); err == nil {
					pe.Amount = &n
				}
			}
		}
	}

	return pe
}

func parseAckAllocate(details []string) *parsedEvent {
	pe := &parsedEvent{EventType: EventTypeAckAllocate}

	for _, d := range details {
		pe.Epoch = parseEpochFromTenure(d, pe.Epoch)
		if after, ok := strings.CutPrefix(d, "Escrow balance: "); ok {
			if n, err := strconv.ParseInt(after, 10, 64); err == nil {
				pe.Balance = &n
			}
		}
	}

	return pe
}

func parseBatchAllocate(details []string, clientSeatPK string) *parsedEvent {
	pe := &parsedEvent{EventType: EventTypeBatchAllocate}

	// Check for new-style logs with "Client seat:" delimiters.
	hasClientSeatLogs := false
	for _, d := range details {
		if strings.HasPrefix(d, "Client seat: ") {
			hasClientSeatLogs = true
			break
		}
	}

	if hasClientSeatLogs {
		// Split details into per-seat groups.
		type seatGroup struct {
			key     string
			details []string
		}
		var groups []seatGroup
		for _, d := range details {
			if after, ok := strings.CutPrefix(d, "Client seat: "); ok {
				groups = append(groups, seatGroup{key: after})
			} else if len(groups) > 0 {
				g := &groups[len(groups)-1]
				g.details = append(g.details, d)
			}
		}

		// Find matching seat group.
		for _, g := range groups {
			if g.key != clientSeatPK {
				continue
			}
			for _, d := range g.details {
				pe.Epoch = parseEpochFromTenure(d, pe.Epoch)
				if n, ok := parseChargedAmount(d); ok {
					pe.Amount = &n
				}
				if after, ok := strings.CutPrefix(d, "Escrow balance: "); ok {
					if n, err := strconv.ParseInt(after, 10, 64); err == nil {
						pe.Balance = &n
					}
				}
			}
			return pe
		}
	}

	// Fallback: old-style logs or no matching seat. Extract epoch only.
	for _, d := range details {
		pe.Epoch = parseEpochFromTenure(d, pe.Epoch)
	}

	return pe
}

// parseEpochFromTenure extracts the active_epoch from a "Tenure epochs: ..." log line.
func parseEpochFromTenure(detail string, current *uint64) *uint64 {
	after, ok := strings.CutPrefix(detail, "Tenure epochs: ")
	if !ok {
		return current
	}
	parts := strings.SplitN(after, "active_epoch: ", 2)
	if len(parts) < 2 {
		return current
	}
	if n, err := strconv.ParseUint(parts[1], 10, 64); err == nil {
		return &n
	}
	return current
}
