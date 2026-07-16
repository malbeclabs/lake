-- +goose Up

CREATE TYPE contact_status AS ENUM ('pending_activation', 'active', 'blocked', 'stopped');
CREATE TYPE alert_status AS ENUM ('pending_activation', 'active', 'stopped', 'failing');
CREATE TYPE alert_trigger AS ENUM ('epochs_left', 'balance_below_usdc');

CREATE TABLE telegram_contacts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_id              BIGINT,
    username             VARCHAR(255),
    announcements_opt_in BOOLEAN NOT NULL DEFAULT TRUE,
    status               contact_status NOT NULL DEFAULT 'pending_activation',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at         TIMESTAMPTZ
);

-- One contact row per Telegram chat once activated; nulls (pre-activation) are not constrained.
CREATE UNIQUE INDEX idx_telegram_contacts_chat_id ON telegram_contacts(chat_id) WHERE chat_id IS NOT NULL;

CREATE TABLE seat_alerts (
    id                           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_id                   UUID REFERENCES telegram_contacts(id) ON DELETE SET NULL,
    created_by_account_id        UUID REFERENCES accounts(id),
    seat_pk                      TEXT NOT NULL,
    trigger_type                 alert_trigger NOT NULL,
    threshold_value              DOUBLE PRECISION NOT NULL,
    desired_announcements_opt_in BOOLEAN NOT NULL DEFAULT TRUE,
    activation_token             TEXT NOT NULL,
    status                       alert_status NOT NULL DEFAULT 'pending_activation',
    consecutive_failures         INTEGER NOT NULL DEFAULT 0,
    last_notified_epoch          BIGINT,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at                 TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_seat_alerts_activation_token ON seat_alerts(activation_token);
CREATE INDEX idx_seat_alerts_active ON seat_alerts(status) WHERE status = 'active';
CREATE INDEX idx_seat_alerts_account ON seat_alerts(created_by_account_id);

-- +goose Down
DROP TABLE IF EXISTS seat_alerts;
DROP TABLE IF EXISTS telegram_contacts;
DROP TYPE IF EXISTS alert_trigger;
DROP TYPE IF EXISTS alert_status;
DROP TYPE IF EXISTS contact_status;
