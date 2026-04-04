package escrowevents

import (
	"testing"
	"time"
)

const (
	testProgramID    = "ShredXYZ111111111111111111111111111111111"
	testEscrowPK     = "EscrowABC"
	testClientSeatPK = "SeatDEF"
	testTxSig        = "txSig123"
	testSigner       = "SignerPQR"
)

var testBlockTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

func TestParseFundEvent(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Fund payment escrow with USDC",
		"Program log: Funded payment escrow for client seat SeatDEF with 1000000 USDC",
		"Program log: USDC balance after funding: 2000000",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 100, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventType != EventTypeFund {
		t.Errorf("expected event type %q, got %q", EventTypeFund, e.EventType)
	}
	if e.AmountUSDC == nil || *e.AmountUSDC != 1000000 {
		t.Errorf("expected amount 1000000, got %v", e.AmountUSDC)
	}
	if e.BalanceAfterUSDC == nil || *e.BalanceAfterUSDC != 2000000 {
		t.Errorf("expected balance 2000000, got %v", e.BalanceAfterUSDC)
	}
	if e.Status != "ok" {
		t.Errorf("expected status ok, got %q", e.Status)
	}
	if e.EscrowPK != testEscrowPK {
		t.Errorf("expected escrow pk %q, got %q", testEscrowPK, e.EscrowPK)
	}
}

func TestParseInstantAllocateEvent(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Request instant seat allocation",
		"Program log: Tenure epochs: 3, active_epoch: 42",
		"Program log: Escrow balance: 500000",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 200, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventType != EventTypeAllocateSeat {
		t.Errorf("expected event type %q, got %q", EventTypeAllocateSeat, e.EventType)
	}
	if e.Epoch == nil || *e.Epoch != 42 {
		t.Errorf("expected epoch 42, got %v", e.Epoch)
	}
	if e.BalanceAfterUSDC == nil || *e.BalanceAfterUSDC != 500000 {
		t.Errorf("expected balance 500000, got %v", e.BalanceAfterUSDC)
	}
}

func TestParseInstantWithdrawEvent(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Request instant seat withdrawal",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 300, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventType != EventTypeWithdrawSeat {
		t.Errorf("expected event type %q, got %q", EventTypeWithdrawSeat, e.EventType)
	}
	if e.AmountUSDC != nil {
		t.Errorf("expected nil amount, got %v", e.AmountUSDC)
	}
}

func TestParseCloseEvent(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Close payment escrow",
		"Program log: Withdrew 1500000 USDC from payment escrow to refund account",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 400, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventType != EventTypeClose {
		t.Errorf("expected event type %q, got %q", EventTypeClose, e.EventType)
	}
	if e.AmountUSDC == nil || *e.AmountUSDC != 1500000 {
		t.Errorf("expected amount 1500000, got %v", e.AmountUSDC)
	}
	if e.BalanceAfterUSDC == nil || *e.BalanceAfterUSDC != 0 {
		t.Errorf("expected balance 0, got %v", e.BalanceAfterUSDC)
	}
}

func TestParseBatchAllocateNewStyle(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Batch allocate seats",
		"Program log: Client seat: OtherSeat",
		"Program log: Tenure epochs: 2, active_epoch: 10",
		"Program log: Charged: 200000",
		"Program log: Escrow balance: 800000",
		"Program log: Client seat: " + testClientSeatPK,
		"Program log: Tenure epochs: 5, active_epoch: 42",
		"Program log: Charged: 100000",
		"Program log: Escrow balance: 900000",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 500, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventType != EventTypeBatchAllocate {
		t.Errorf("expected event type %q, got %q", EventTypeBatchAllocate, e.EventType)
	}
	if e.AmountUSDC == nil || *e.AmountUSDC != 100000 {
		t.Errorf("expected amount 100000 (matching seat), got %v", e.AmountUSDC)
	}
	if e.BalanceAfterUSDC == nil || *e.BalanceAfterUSDC != 900000 {
		t.Errorf("expected balance 900000, got %v", e.BalanceAfterUSDC)
	}
	if e.Epoch == nil || *e.Epoch != 42 {
		t.Errorf("expected epoch 42, got %v", e.Epoch)
	}
}

func TestParseBatchAllocateOldStyle(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Batch allocate seats",
		"Program log: Tenure epochs: 3, active_epoch: 10",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 600, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventType != EventTypeBatchAllocate {
		t.Errorf("expected event type %q, got %q", EventTypeBatchAllocate, e.EventType)
	}
	if e.Epoch == nil || *e.Epoch != 10 {
		t.Errorf("expected epoch 10, got %v", e.Epoch)
	}
	// Old-style logs don't have per-seat amounts.
	if e.AmountUSDC != nil {
		t.Errorf("expected nil amount for old-style, got %v", e.AmountUSDC)
	}
}

func TestParseBatchSettle(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Batch settle devices",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 700, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event for batch settle, got %d", len(events))
	}
	if events[0].EventType != "batch_settle" {
		t.Errorf("expected event type batch_settle, got %q", events[0].EventType)
	}
}

func TestParseFailedTransaction(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Fund payment escrow with USDC",
		"Program log: Funded payment escrow for client seat SeatDEF with 500000 USDC",
		"Program " + testProgramID + " failed: custom error",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 800, testBlockTime, logs, true, testProgramID, testSigner)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Status != "failed" {
		t.Errorf("expected status failed, got %q", events[0].Status)
	}
}

func TestParseMultipleInstructionsInOneTx(t *testing.T) {
	logs := []string{
		"Program " + testProgramID + " invoke [1]",
		"Program log: Fund payment escrow with USDC",
		"Program log: Funded payment escrow for client seat SeatDEF with 1000000 USDC",
		"Program log: USDC balance after funding: 1000000",
		"Program " + testProgramID + " success",
		"Program " + testProgramID + " invoke [1]",
		"Program log: Request instant seat allocation",
		"Program log: Tenure epochs: 1, active_epoch: 5",
		"Program log: Escrow balance: 900000",
		"Program " + testProgramID + " success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 900, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	if events[0].EventType != EventTypeFund {
		t.Errorf("first event: expected %q, got %q", EventTypeFund, events[0].EventType)
	}
	if events[1].EventType != EventTypeAllocateSeat {
		t.Errorf("second event: expected %q, got %q", EventTypeAllocateSeat, events[1].EventType)
	}
}

func TestParseNoRelevantLogs(t *testing.T) {
	logs := []string{
		"Program SomeOtherProgram invoke [1]",
		"Program log: Transfer",
		"Program SomeOtherProgram success",
	}

	events := ParseTransactionLogs(nil, testEscrowPK, testClientSeatPK, testTxSig, 1000, testBlockTime, logs, false, testProgramID, testSigner)

	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}
