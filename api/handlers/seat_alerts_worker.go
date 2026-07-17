package handlers

import (
	"context"
	"errors"
	"fmt"
	"os"
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

func seatAlertMessage(alert SeatAlert, s ShredSubscriberRow) string {
	if alert.TriggerType == "balance_below_usdc" {
		return fmt.Sprintf(
			"Heads up: your seat %s (%s) escrow is about %.2f USDC, at or below your %.2f USDC alert threshold. Top up to keep the seat and its tenure.",
			s.PK, s.MetroCode, float64(s.TotalUSDCBalance)/1_000_000, alert.ThresholdValue)
	}
	return fmt.Sprintf(
		"Heads up: your seat %s (%s) has about %d epoch(s) of runway left. Top up the escrow to keep the seat and its tenure.",
		s.PK, s.MetroCode, PrepaidEpochs(s.TotalUSDCBalance, s.PricePerEpochDollars))
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
