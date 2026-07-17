package handlers

import (
	"context"
	"errors"
	"fmt"
	"html"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultSeatAlertSweepInterval = 30 * time.Minute

// seatAlertSweepInterval resolves the sweep interval, allowing an operator
// override via the SEAT_ALERT_SWEEP_INTERVAL env var (a Go duration string,
// e.g. "1m", "30s", "30m"). Falls back to the default if unset or invalid.
func seatAlertSweepInterval() time.Duration {
	v := os.Getenv("SEAT_ALERT_SWEEP_INTERVAL")
	if v == "" {
		return defaultSeatAlertSweepInterval
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return defaultSeatAlertSweepInterval
	}
	return d
}

// RunSeatAlertSweep evaluates every active alert once and sends a Telegram
// message for those that meet their trigger and have not been notified this epoch.
func (a *API) RunSeatAlertSweep(ctx context.Context) error {
	if a.TelegramSender == nil {
		return nil // telegram not configured; nothing to do
	}
	overview := a.FetchShredsOverview(ctx)
	if overview.CurrentSolanaEpoch == 0 {
		return fmt.Errorf("unknown current solana epoch; skipping sweep")
	}
	epoch := int64(overview.CurrentSolanaEpoch)

	alerts, err := a.ListActiveAlertsWithContacts(ctx)
	if err != nil {
		return err
	}
	if len(alerts) == 0 {
		return nil
	}

	// Fetch all seats once, index by pk (no per-seat fetch function exists).
	seats, _, err := a.FetchShredSubscribers(ctx, "", 100000, 0)
	if err != nil {
		return err
	}
	bySeat := make(map[string]ShredSubscriberRow, len(seats))
	for _, s := range seats {
		bySeat[s.PK] = s
	}

	for _, awc := range alerts {
		if awc.Alert.LastNotifiedEpoch != nil && *awc.Alert.LastNotifiedEpoch == epoch {
			continue // already notified this epoch
		}
		s, ok := bySeat[awc.Alert.SeatPK]
		if !ok {
			continue // seat no longer present
		}
		m := SeatMetrics{
			TotalUSDCBalance:     s.TotalUSDCBalance,
			PricePerEpochDollars: s.PricePerEpochDollars,
			ActiveEpoch:          s.ActiveEpoch,
			EscrowCount:          s.EscrowCount,
		}
		if !AlertConditionMet(awc.Alert.TriggerType, awc.Alert.ThresholdValue, m, overview.CurrentSolanaEpoch) {
			continue
		}
		text := seatAlertMessage(awc.Alert, s)
		if err := a.TelegramSender.SendMessage(ctx, awc.ChatID, text); err != nil {
			logError("seat alert send failed", "alert_id", awc.Alert.ID, "error", err)
			continue // leave last_notified_epoch unset so we retry next sweep
		}
		if err := a.MarkAlertNotified(ctx, awc.Alert.ID, epoch); err != nil {
			logError("failed to mark alert notified", "alert_id", awc.Alert.ID, "error", err)
		}
	}
	return nil
}

// htmlEscape escapes &, <, and > so a dynamic value (seat pk, device code,
// metro code, trigger text, etc.) can be safely interpolated into an
// HTML-parse-mode Telegram message without being misread as markup.
func htmlEscape(s string) string {
	return html.EscapeString(s)
}

// seatAlertMessage renders the low-balance warning sent to Telegram. It is a
// pure function (no I/O) so it is unit tested directly (see
// seat_alerts_message_test.go). The message uses Telegram's HTML parse mode
// (bold labels, a <pre> block around the copy-pasteable command); every
// dynamic value is passed through htmlEscape before interpolation, and static
// text uses square-bracket placeholders like [device-code] instead of angle
// brackets so nothing in the template itself needs escaping.
func seatAlertMessage(alert SeatAlert, s ShredSubscriberRow) string {
	epochsLeft := PrepaidEpochs(s.TotalUSDCBalance, s.PricePerEpochDollars)
	balance := float64(s.TotalUSDCBalance) / 1_000_000
	price := s.PricePerEpochDollars
	days := epochsLeft * 2

	deviceCode := s.DeviceCode
	if deviceCode == "" {
		deviceCode = "[device-code]"
	}
	clientIP := s.ClientIP
	if clientIP == "" {
		clientIP = "[your-ip]"
	}

	lines := []string{
		"⚠️ <b>Seat running low</b>",
		"",
		fmt.Sprintf("<b>Seat:</b> %s", htmlEscape(shortSeatPK(s.PK))),
		fmt.Sprintf("<b>Device:</b> %s (%s)", htmlEscape(deviceCode), htmlEscape(s.MetroCode)),
		fmt.Sprintf("<b>Escrow:</b> %.2f USDC left", balance),
		fmt.Sprintf("<b>Runway:</b> ~%d epoch(s) (~%d days)", epochsLeft, days),
		fmt.Sprintf("<b>Tenure at stake:</b> %d epoch(s)", s.TenureEpochs),
		fmt.Sprintf("<b>Trigger:</b> %s", htmlEscape(triggerText(alert))),
		"",
		"If the escrow runs out you lose the seat and its tenure.",
		"",
	}

	// A device with no configured price (price <= 0) can't offer a suggested
	// top-up amount, so the cost line and the suggested-amount line are
	// dropped and the pay command falls back to a placeholder amount.
	var suggested int64
	if price > 0 {
		suggested = price * 15 // ~15 epochs of runway
		lines = append(lines,
			fmt.Sprintf("This device costs ~%d USDC/epoch (~2 days). Top up (adds to your escrow — more is better):", price),
			"",
			fmt.Sprintf("<pre>doublezero-solana shreds pay --device-code %s --client-ip %s --amount %d</pre>", htmlEscape(deviceCode), htmlEscape(clientIP), suggested),
			"",
			fmt.Sprintf("%d USDC ≈ ~%d days here. Minimum %d = one epoch.", suggested, suggested/price*2, price),
		)
	} else {
		lines = append(lines,
			"Top up (adds to your escrow — more is better):",
			"",
			fmt.Sprintf("<pre>doublezero-solana shreds pay --device-code %s --client-ip %s --amount [USDC]</pre>", htmlEscape(deviceCode), htmlEscape(clientIP)),
		)
	}
	lines = append(lines, "", "Reply /topup for details, /help for all commands.")

	return strings.Join(lines, "\n")
}

// StartSeatAlertWorker runs RunSeatAlertSweep on a ticker, single-flighted
// across replicas via a Postgres row claim so only one replica sweeps per tick.
func (a *API) StartSeatAlertWorker(ctx context.Context, serverID string) {
	interval := seatAlertSweepInterval()

	// A claim older than this is reclaimable. It is kept strictly shorter than
	// the sweep interval so that at each tick exactly one replica wins the
	// claim (the previous tick's claim is definitely stale), while a second
	// replica ticking seconds later sees a fresh claim and skips.
	stale := interval * 2 / 3

	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if a.claimSeatAlertSweep(ctx, serverID, stale) {
					if err := a.RunSeatAlertSweep(ctx); err != nil {
						logError("seat alert sweep failed", "error", err)
					}
				}
			}
		}
	}()
}

// claimSeatAlertSweep returns true if this replica won the right to sweep now.
// The claim is stale-reclaimable after the given duration so a crashed
// claimant does not block sweeps forever.
func (a *API) claimSeatAlertSweep(ctx context.Context, serverID string, stale time.Duration) bool {
	var claimed bool
	err := a.PgPool.QueryRow(ctx, `
		UPDATE seat_alert_worker_lock
		SET claimed_by = $1, claimed_at = NOW()
		WHERE id = 1 AND (claimed_at IS NULL OR claimed_at < NOW() - make_interval(secs => $2))
		RETURNING true`,
		serverID, stale.Seconds()).Scan(&claimed)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			logError("seat alert sweep claim failed", "error", err)
		}
		return false // pgx.ErrNoRows => another replica holds a fresh claim
	}
	return claimed
}
