package handlers

import (
	"strings"
	"testing"
)

func TestSeatAlertMessage(t *testing.T) {
	t.Parallel()
	alert := SeatAlert{TriggerType: "epochs_left", ThresholdValue: 3}
	seat := ShredSubscriberRow{
		PK:                   "5i6ajSK8s7f3nQ2xW9pR4tYcVbNmLdOeAqXsZxs8",
		DeviceCode:           "AMS-CORE-01",
		MetroCode:            "AMS",
		ClientIP:             "203.0.113.7",
		TenureEpochs:         12,
		TotalUSDCBalance:     25_000_000, // 25 USDC
		PricePerEpochDollars: 10,
	}

	msg := seatAlertMessage(alert, seat)

	if !strings.Contains(msg, "AMS-CORE-01") {
		t.Errorf("message missing device code:\n%s", msg)
	}
	if !strings.Contains(msg, "25.00 USDC") {
		t.Errorf("message missing balance:\n%s", msg)
	}
	wantPay := "doublezero-solana shreds pay --device-code AMS-CORE-01 --client-ip 203.0.113.7"
	if !strings.Contains(msg, wantPay) {
		t.Errorf("message missing pay command:\n%s", msg)
	}
	if !strings.Contains(msg, "/topup") {
		t.Errorf("message missing /topup hint:\n%s", msg)
	}
}

// TestSeatAlertMessage_NoPriceGuards covers the price<=0 and missing
// device-code/client-ip fallbacks, since a device can be misconfigured or the
// caller (an internal user) may not see the client IP.
func TestSeatAlertMessage_NoPriceGuards(t *testing.T) {
	t.Parallel()
	alert := SeatAlert{TriggerType: "balance_below_usdc", ThresholdValue: 5}
	seat := ShredSubscriberRow{
		TotalUSDCBalance:     0,
		PricePerEpochDollars: 0,
	}
	msg := seatAlertMessage(alert, seat)
	if strings.Contains(msg, "This device costs") {
		t.Errorf("expected cost line to be dropped when price <= 0:\n%s", msg)
	}
	if !strings.Contains(msg, "--amount <USDC>") {
		t.Errorf("expected placeholder amount when price <= 0:\n%s", msg)
	}
	if !strings.Contains(msg, "<device-code>") || !strings.Contains(msg, "<your-ip>") {
		t.Errorf("expected placeholders for missing device code / client ip:\n%s", msg)
	}
}
