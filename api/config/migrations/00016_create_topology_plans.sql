-- +goose Up

-- Topology planner: shared, env-scoped planning documents.
-- No updated_at trigger: migration 00005 removed update_updated_at_column();
-- handlers set updated_at explicitly (matches sessions/workflow_runs convention).

CREATE TYPE plan_status AS ENUM ('draft', 'approved', 'done', 'archived');
CREATE TYPE plan_op_type AS ENUM ('add_device', 'remove_device', 'add_link', 'remove_link', 'move_link_end');
CREATE TYPE plan_change_state AS ENUM ('pending', 'done', 'skipped', 'superseded');

CREATE TABLE topology_plans (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                  VARCHAR(255) NOT NULL,
    description           TEXT,
    status                plan_status NOT NULL DEFAULT 'draft',
    environment           VARCHAR(20) NOT NULL,
    baseline_as_of        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version               INTEGER NOT NULL DEFAULT 1,
    created_by_account_id UUID REFERENCES accounts(id),
    created_by_email      VARCHAR(255),
    updated_by_account_id UUID REFERENCES accounts(id),
    updated_by_email      VARCHAR(255),
    forked_from_plan_id   UUID REFERENCES topology_plans(id),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at            TIMESTAMPTZ
);

-- Name uniqueness is per environment and ignores soft-deleted rows.
CREATE UNIQUE INDEX idx_topology_plans_env_name_active
    ON topology_plans(environment, name)
    WHERE deleted_at IS NULL;

-- Listing is env-scoped and hides soft-deleted plans.
CREATE INDEX idx_topology_plans_env_active
    ON topology_plans(environment)
    WHERE deleted_at IS NULL;

CREATE TABLE topology_plan_changes (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id               UUID NOT NULL REFERENCES topology_plans(id) ON DELETE CASCADE,
    seq                   INTEGER NOT NULL,
    op_type               plan_op_type NOT NULL,
    ref_device_pk         VARCHAR(64),
    ref_link_pk           VARCHAR(64),
    new_device_pk         VARCHAR(64),
    local_ref             VARCHAR(64),
    payload               JSONB NOT NULL DEFAULT '{}',
    ref_snapshot          JSONB NOT NULL DEFAULT '{}',
    target_date           DATE,
    assignee_note         TEXT,
    state                 plan_change_state NOT NULL DEFAULT 'pending',
    version               INTEGER NOT NULL DEFAULT 1,
    created_by_account_id UUID REFERENCES accounts(id),
    created_by_email      VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT topology_plan_changes_plan_seq_unique UNIQUE (plan_id, seq),
    CONSTRAINT topology_plan_changes_shape CHECK (
        (op_type = 'add_device'    AND local_ref     IS NOT NULL) OR
        (op_type = 'add_link'      AND local_ref     IS NOT NULL) OR
        (op_type = 'remove_device' AND ref_device_pk IS NOT NULL) OR
        (op_type = 'remove_link'   AND ref_link_pk   IS NOT NULL) OR
        (op_type = 'move_link_end' AND ref_link_pk   IS NOT NULL)
    )
);

-- Reverse drift lookups: "which change touches this pk?".
CREATE INDEX idx_topology_plan_changes_ref_device
    ON topology_plan_changes(ref_device_pk) WHERE ref_device_pk IS NOT NULL;
CREATE INDEX idx_topology_plan_changes_ref_link
    ON topology_plan_changes(ref_link_pk) WHERE ref_link_pk IS NOT NULL;
CREATE INDEX idx_topology_plan_changes_new_device
    ON topology_plan_changes(new_device_pk) WHERE new_device_pk IS NOT NULL;

CREATE TABLE topology_plan_events (
    id               BIGSERIAL PRIMARY KEY,
    plan_id          UUID NOT NULL,
    change_id        UUID,
    actor_account_id UUID REFERENCES accounts(id),
    actor_email      VARCHAR(255),
    action           VARCHAR(64) NOT NULL,
    before           JSONB,
    after            JSONB,
    at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Activity feed reads newest-first per plan.
CREATE INDEX idx_topology_plan_events_plan_at
    ON topology_plan_events(plan_id, at DESC);

CREATE TABLE topology_plan_issues (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plan_id          UUID NOT NULL REFERENCES topology_plans(id) ON DELETE CASCADE,
    contributor_pk   VARCHAR(64),
    contributor_code VARCHAR(64),
    github_repo      VARCHAR(128) NOT NULL,
    issue_number     INTEGER NOT NULL,
    issue_url        TEXT NOT NULL,
    is_parent        BOOLEAN NOT NULL DEFAULT false,
    last_synced_at   TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT topology_plan_issues_plan_contributor_unique UNIQUE (plan_id, contributor_pk)
);

-- One parent tracking issue per plan (contributor_pk is NULL for parents, so the
-- table UNIQUE above does not constrain it; this partial index does).
CREATE UNIQUE INDEX idx_topology_plan_issues_parent
    ON topology_plan_issues(plan_id) WHERE is_parent;

-- +goose Down
DROP TABLE IF EXISTS topology_plan_issues;
DROP TABLE IF EXISTS topology_plan_events;
DROP TABLE IF EXISTS topology_plan_changes;
DROP TABLE IF EXISTS topology_plans;
DROP TYPE IF EXISTS plan_change_state;
DROP TYPE IF EXISTS plan_op_type;
DROP TYPE IF EXISTS plan_status;
