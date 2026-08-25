-- +goose Up
ALTER TABLE topology_plan_issues
    ADD COLUMN IF NOT EXISTS issue_kind VARCHAR(20) NOT NULL DEFAULT 'contributor',
    ADD COLUMN IF NOT EXISTS entity_pk  VARCHAR(64);

-- Per-entity decom issues (device_decom / link_decom) are idempotent per (plan, kind, entity).
-- Contributor rows keep entity_pk NULL and stay governed by UNIQUE(plan_id, contributor_pk).
CREATE UNIQUE INDEX IF NOT EXISTS idx_topology_plan_issues_entity
    ON topology_plan_issues(plan_id, issue_kind, entity_pk)
    WHERE entity_pk IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_topology_plan_issues_entity;
ALTER TABLE topology_plan_issues DROP COLUMN IF EXISTS entity_pk;
ALTER TABLE topology_plan_issues DROP COLUMN IF EXISTS issue_kind;
