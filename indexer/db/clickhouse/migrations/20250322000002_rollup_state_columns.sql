-- +goose Up

-- Link rollup: add entity state columns resolved from history tables at write time.
ALTER TABLE link_rollup_5m
    ADD COLUMN IF NOT EXISTS status String DEFAULT '' AFTER z_samples,
    ADD COLUMN IF NOT EXISTS provisioning Bool DEFAULT false AFTER status,
    ADD COLUMN IF NOT EXISTS isis_down Bool DEFAULT false AFTER provisioning;

-- Device interface rollup: add context and entity state columns.
ALTER TABLE device_interface_rollup_5m
    ADD COLUMN IF NOT EXISTS link_pk String DEFAULT '' AFTER intf,
    ADD COLUMN IF NOT EXISTS link_side String DEFAULT '' AFTER link_pk,
    ADD COLUMN IF NOT EXISTS user_tunnel_id Nullable(Int64) AFTER link_side,
    ADD COLUMN IF NOT EXISTS user_pk String DEFAULT '' AFTER user_tunnel_id,
    ADD COLUMN IF NOT EXISTS status String DEFAULT '' AFTER max_out_pps,
    ADD COLUMN IF NOT EXISTS isis_overload Bool DEFAULT false AFTER status,
    ADD COLUMN IF NOT EXISTS isis_unreachable Bool DEFAULT false AFTER isis_overload;

-- +goose Down

ALTER TABLE link_rollup_5m
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS provisioning,
    DROP COLUMN IF EXISTS isis_down;

ALTER TABLE device_interface_rollup_5m
    DROP COLUMN IF EXISTS link_pk,
    DROP COLUMN IF EXISTS link_side,
    DROP COLUMN IF EXISTS user_tunnel_id,
    DROP COLUMN IF EXISTS user_pk,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS isis_overload,
    DROP COLUMN IF EXISTS isis_unreachable;
