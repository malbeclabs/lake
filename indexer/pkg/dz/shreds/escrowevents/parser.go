package escrowevents

import (
	"log/slog"
	"strconv"
	"strings"
	"time"
)

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
			log.Error("shreds/escrow-events: unknown instruction action",
				"action", g.action,
				"escrow_pk", escrowPK,
				"tx_signature", txSig,
			)
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

	return events
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
	default:
		return &parsedEvent{EventType: EventTypeUnknown}
	}
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
				if after, ok := strings.CutPrefix(d, "Charged: "); ok {
					if n, err := strconv.ParseInt(after, 10, 64); err == nil {
						pe.Amount = &n
					}
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
