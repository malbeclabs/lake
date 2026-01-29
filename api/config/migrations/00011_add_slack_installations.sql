-- +goose Up
CREATE TABLE IF NOT EXISTS slack_oauth_states (
    state VARCHAR(64) PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes'
);

CREATE TABLE IF NOT EXISTS slack_installations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id VARCHAR(20) NOT NULL UNIQUE,
    team_name VARCHAR(255),
    bot_token VARCHAR(255) NOT NULL,
    bot_user_id VARCHAR(20) NOT NULL,
    scope TEXT,
    installed_by UUID REFERENCES accounts(id) ON DELETE SET NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS slack_installations;
DROP TABLE IF EXISTS slack_oauth_states;
