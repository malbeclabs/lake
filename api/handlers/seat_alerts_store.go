package handlers

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type SeatAlert struct {
	ID                   uuid.UUID
	ContactID            *uuid.UUID
	CreatedByAccountID   *uuid.UUID
	SeatPK               string
	TriggerType          string
	ThresholdValue       float64
	DesiredAnnouncements bool
	ActivationToken      string
	Status               string
	ConsecutiveFailures  int
	LastNotifiedEpoch    *int64
	CreatedAt            time.Time
}

type AlertWithContact struct {
	Alert  SeatAlert
	ChatID int64
}

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

const seatAlertColumns = `id, contact_id, created_by_account_id, seat_pk,
	trigger_type, threshold_value, desired_announcements_opt_in, activation_token,
	status, consecutive_failures, last_notified_epoch, created_at`

func scanSeatAlert(row rowScanner) (SeatAlert, error) {
	var a SeatAlert
	var trigger, status string
	err := row.Scan(&a.ID, &a.ContactID, &a.CreatedByAccountID, &a.SeatPK,
		&trigger, &a.ThresholdValue, &a.DesiredAnnouncements, &a.ActivationToken,
		&status, &a.ConsecutiveFailures, &a.LastNotifiedEpoch, &a.CreatedAt)
	a.TriggerType = trigger
	a.Status = status
	return a, err
}

func (a *API) CreateSeatAlert(ctx context.Context, accountID uuid.UUID, seatPK, triggerType string, threshold float64, announcements bool) (SeatAlert, error) {
	token := uuid.NewString()
	return scanSeatAlert(a.PgPool.QueryRow(ctx, `
		INSERT INTO seat_alerts
			(created_by_account_id, seat_pk, trigger_type, threshold_value,
			 desired_announcements_opt_in, activation_token, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending_activation')
		RETURNING `+seatAlertColumns,
		accountID, seatPK, triggerType, threshold, announcements, token))
}

func (a *API) GetSeatAlertByToken(ctx context.Context, token string) (SeatAlert, error) {
	return scanSeatAlert(a.PgPool.QueryRow(ctx,
		`SELECT `+seatAlertColumns+` FROM seat_alerts WHERE activation_token = $1`, token))
}

func (a *API) ActivateSeatAlertByToken(ctx context.Context, token string, chatID int64, username string) (SeatAlert, error) {
	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		return SeatAlert{}, err
	}
	defer tx.Rollback(ctx)

	// Only a still-pending alert can be activated. A token that was already
	// activated or stopped matches no row (pgx.ErrNoRows), making tokens one-time.
	alert, err := scanSeatAlert(tx.QueryRow(ctx,
		`SELECT `+seatAlertColumns+` FROM seat_alerts WHERE activation_token = $1 AND status = 'pending_activation' FOR UPDATE`, token))
	if err != nil {
		return SeatAlert{}, err
	}

	// Upsert a contact for this chat. Union the announcement preference.
	var contactID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO telegram_contacts (chat_id, username, announcements_opt_in, status, activated_at)
		VALUES ($1, $2, $3, 'active', NOW())
		ON CONFLICT (chat_id) WHERE chat_id IS NOT NULL
		DO UPDATE SET username = EXCLUDED.username,
		              status = 'active',
		              announcements_opt_in = telegram_contacts.announcements_opt_in OR EXCLUDED.announcements_opt_in
		RETURNING id`,
		chatID, username, alert.DesiredAnnouncements).Scan(&contactID)
	if err != nil {
		return SeatAlert{}, err
	}

	updated, err := scanSeatAlert(tx.QueryRow(ctx, `
		UPDATE seat_alerts
		SET contact_id = $2, status = 'active', activated_at = NOW(), updated_at = NOW()
		WHERE id = $1
		RETURNING `+seatAlertColumns, alert.ID, contactID))
	if err != nil {
		return SeatAlert{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SeatAlert{}, err
	}
	return updated, nil
}

func (a *API) ListActiveAlertsWithContacts(ctx context.Context) ([]AlertWithContact, error) {
	rows, err := a.PgPool.Query(ctx, `
		SELECT `+prefixed("s", seatAlertColumns)+`, c.chat_id
		FROM seat_alerts s
		JOIN telegram_contacts c ON c.id = s.contact_id
		WHERE s.status = 'active' AND c.status = 'active' AND c.chat_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AlertWithContact{}
	for rows.Next() {
		var awc AlertWithContact
		var trigger, status string
		if err := rows.Scan(&awc.Alert.ID, &awc.Alert.ContactID, &awc.Alert.CreatedByAccountID,
			&awc.Alert.SeatPK, &trigger, &awc.Alert.ThresholdValue, &awc.Alert.DesiredAnnouncements,
			&awc.Alert.ActivationToken, &status, &awc.Alert.ConsecutiveFailures,
			&awc.Alert.LastNotifiedEpoch, &awc.Alert.CreatedAt, &awc.ChatID); err != nil {
			return nil, err
		}
		awc.Alert.TriggerType = trigger
		awc.Alert.Status = status
		out = append(out, awc)
	}
	return out, rows.Err()
}

func (a *API) ListAlertsByChatID(ctx context.Context, chatID int64) ([]SeatAlert, error) {
	rows, err := a.PgPool.Query(ctx, `
		SELECT `+prefixed("s", seatAlertColumns)+`
		FROM seat_alerts s JOIN telegram_contacts c ON c.id = s.contact_id
		WHERE c.chat_id = $1 AND s.status = 'active'`, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SeatAlert{}
	for rows.Next() {
		alert, err := scanSeatAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, alert)
	}
	return out, rows.Err()
}

func (a *API) StopAlertsByChatID(ctx context.Context, chatID int64) (int64, error) {
	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE seat_alerts SET status = 'stopped', updated_at = NOW()
		WHERE status = 'active' AND contact_id IN (SELECT id FROM telegram_contacts WHERE chat_id = $1)`, chatID)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE telegram_contacts SET status = 'stopped' WHERE chat_id = $1`, chatID); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (a *API) MarkAlertNotified(ctx context.Context, alertID uuid.UUID, epoch int64) error {
	_, err := a.PgPool.Exec(ctx,
		`UPDATE seat_alerts SET last_notified_epoch = $2, consecutive_failures = 0, updated_at = NOW() WHERE id = $1`,
		alertID, epoch)
	return err
}

func (a *API) ListSeatAlertsByAccount(ctx context.Context, accountID uuid.UUID) ([]SeatAlert, error) {
	rows, err := a.PgPool.Query(ctx,
		`SELECT `+seatAlertColumns+` FROM seat_alerts WHERE created_by_account_id = $1 ORDER BY created_at DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SeatAlert{}
	for rows.Next() {
		alert, err := scanSeatAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, alert)
	}
	return out, rows.Err()
}

func (a *API) DeleteSeatAlert(ctx context.Context, id, accountID uuid.UUID) error {
	tag, err := a.PgPool.Exec(ctx, `DELETE FROM seat_alerts WHERE id = $1 AND created_by_account_id = $2`, id, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// chatIDForAlert looks up the Telegram chat_id for an alert, scoped to the
// account that created it. Returns (0, false, nil) when the alert doesn't
// exist, isn't owned by accountID, or has no activated contact yet.
func (a *API) chatIDForAlert(ctx context.Context, alertID, accountID uuid.UUID) (int64, bool, error) {
	var chatID *int64
	err := a.PgPool.QueryRow(ctx, `
		SELECT c.chat_id
		FROM seat_alerts s JOIN telegram_contacts c ON c.id = s.contact_id
		WHERE s.id = $1 AND s.created_by_account_id = $2`, alertID, accountID).Scan(&chatID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if chatID == nil {
		return 0, false, nil
	}
	return *chatID, true, nil
}

// prefixed qualifies a comma-separated column list with a table alias, e.g.
// prefixed("s", "id, name") -> "s.id, s.name". Used to disambiguate joins.
func prefixed(alias, columns string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}
