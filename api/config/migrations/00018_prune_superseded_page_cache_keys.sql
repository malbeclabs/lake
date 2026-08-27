-- +goose Up
-- Drop page_cache rows whose keys nothing writes or reads any more.
--
-- Both were first-page-only entries replaced by a complete-set entry under a NEW
-- key rather than in place: the page-cache worker runs inside the api pod, so
-- writing the whole set under the old key would have had old pods still serving
-- it verbatim mid-rollout, returning every row and ignoring the request's limit.
-- The rename is what makes the rollout safe; it also leaves the old row behind,
-- refreshed by nothing and read by nothing, holding its last payload forever.
--
--   validators     -> validators:all      (handlers.ValidatorsPageCacheKey)
--   shreds_rewards -> shreds_rewards:all  (handlers.ShredsRewardsPageCacheKey)
--
-- Safe at any point in a rollout: a pod that still reads one of these keys falls
-- through to its live query, which is what it does before the first refresh
-- populates a new key anyway.
--
-- Wrapped so that a role without DELETE on page_cache skips the cleanup instead
-- of failing the migration. The asymmetry is the whole reason: api/main.go exits
-- non-zero when LoadPostgres fails, and POSTGRES_RUN_MIGRATIONS is set in the
-- deployed config, so a permission error here would crashloop every API replica
-- on deploy — to reclaim two dead cache rows. Every other statement in this
-- directory is schema the API genuinely cannot run without; this one is
-- housekeeping, and it should degrade to "the rows stay" rather than take the
-- service down.
--
-- The role is expected to have it: there is one connection string
-- (api/config/postgres.go builds POSTGRES_USER's connStr and hands it to both
-- runMigrations and the runtime pool), and that role has already run CREATE
-- TABLE page_cache (00014) and ALTER TABLE page_cache SET (...) (00015), which
-- needs ownership rather than a grantable privilege — and an owner has DELETE.
-- Only insufficient_privilege is caught, so a genuinely broken statement still
-- fails loudly.
-- +goose StatementBegin
DO $$
BEGIN
    DELETE FROM page_cache WHERE key IN ('validators', 'shreds_rewards');
EXCEPTION WHEN insufficient_privilege THEN
    RAISE WARNING 'page_cache cleanup skipped: role % lacks DELETE on page_cache. The superseded rows (validators, shreds_rewards) are inert; remove them by hand when convenient.', current_user;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- Nothing to restore: these rows are cache payloads, rebuilt on demand by the
-- live query path and never again by any refresh.
SELECT 1;
