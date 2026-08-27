-- +goose Up
-- Drop page_cache rows nothing writes or reads any more. Both were first-page-only
-- entries superseded by a complete-set entry under a new key:
--
--   validators     -> validators:all      (handlers.ValidatorsPageCacheKey)
--   shreds_rewards -> shreds_rewards:all  (handlers.ShredsRewardsPageCacheKey)
--
-- Wrapped so a role without DELETE on page_cache skips the cleanup instead of
-- failing the migration: a migration error exits the API process, and crashlooping
-- every replica to reclaim two dead cache rows is the wrong trade. Only
-- insufficient_privilege is caught, so a genuinely broken statement still fails.
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
