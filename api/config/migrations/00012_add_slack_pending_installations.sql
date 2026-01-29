-- +goose Up
CREATE TABLE IF NOT EXISTS slack_pending_installations (
    id VARCHAR(64) PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    team_id VARCHAR(20) NOT NULL,
    team_name VARCHAR(255),
    bot_token VARCHAR(255) NOT NULL,
    bot_user_id VARCHAR(20) NOT NULL,
    scope TEXT,
    previous_account_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes'
);

-- +goose Down
DROP TABLE IF EXISTS slack_pending_installations;
