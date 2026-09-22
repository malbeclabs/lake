-- +goose Up
-- Drop the page_cache row nothing writes or reads any more. The scoreboard split left it
-- orphaned under a name both boards would otherwise have answered to:
--
--   hyperliquid_scoreboard -> hyperliquid_internal_scoreboard  (the 1h internal view)
--                          -> hyperliquid_public_scoreboard    (handlers.HyperliquidScoreboardCacheKey)
--
-- Deleting it is not housekeeping. readPageCache serves the stored bytes without
-- unmarshalling them, so a row left behind under the old name is an internal-shaped payload
-- that a rollback would feed to the public page.
--
-- Guarded like 00018: a role without DELETE on page_cache skips the cleanup rather than
-- failing the migration, which would exit the API process.
-- +goose StatementBegin
DO $$
BEGIN
    DELETE FROM page_cache WHERE key = 'hyperliquid_scoreboard';
EXCEPTION WHEN insufficient_privilege THEN
    RAISE WARNING 'page_cache cleanup skipped: role % lacks DELETE on page_cache. The superseded row (hyperliquid_scoreboard) is inert; remove it by hand when convenient.', current_user;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- Nothing to restore: a cache payload, rebuilt on demand by the live query path.
SELECT 1;
