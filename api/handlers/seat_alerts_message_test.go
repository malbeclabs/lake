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
	if !strings.Contains(msg, "<b>Seat:</b>") {
		t.Errorf("message missing bold seat label:\n%s", msg)
	}
	wantPay := "doublezero-solana shreds pay --device-code AMS-CORE-01 --client-ip 203.0.113.7"
	if !strings.Contains(msg, "<pre>"+wantPay) {
		t.Errorf("message missing pay command inside <pre>:\n%s", msg)
	}
	if !strings.Contains(msg, "</pre>") {
		t.Errorf("message missing closing </pre>:\n%s", msg)
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
	if !strings.Contains(msg, "--amount [USDC]") {
		t.Errorf("expected placeholder amount when price <= 0:\n%s", msg)
	}
	if !strings.Contains(msg, "[device-code]") || !strings.Contains(msg, "[your-ip]") {
		t.Errorf("expected placeholders for missing device code / client ip:\n%s", msg)
	}
}

// TestSeatAlertMessage_EscapesHTML ensures a dynamic value containing HTML
// metacharacters can't break the Telegram HTML markup or be misread as a tag.
func TestSeatAlertMessage_EscapesHTML(t *testing.T) {
	t.Parallel()
	alert := SeatAlert{TriggerType: "epochs_left", ThresholdValue: 3}
	seat := ShredSubscriberRow{
		PK:                   "pk",
		DeviceCode:           "AMS<CORE>&1",
		MetroCode:            "AMS",
		ClientIP:             "203.0.113.7",
		PricePerEpochDollars: 10,
	}
	msg := seatAlertMessage(alert, seat)
	if strings.Contains(msg, "AMS<CORE>&1") {
		t.Errorf("device code was not escaped:\n%s", msg)
	}
	if !strings.Contains(msg, "AMS&lt;CORE&gt;&amp;1") {
		t.Errorf("expected escaped device code in message:\n%s", msg)
	}
	// The pay command must still be enclosed in a single <pre>...</pre> block.
	start := strings.Index(msg, "<pre>")
	end := strings.Index(msg, "</pre>")
	if start == -1 || end == -1 || end < start {
		t.Fatalf("expected a <pre>...</pre> block:\n%s", msg)
	}
}
