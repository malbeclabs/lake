package handlers

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/malbeclabs/lake/api/config"
)

var errNoPgPool = errors.New("postgres not configured")

// ReadPageCache reads a cached JSON value from Postgres.
// Returns an error if PgPool is nil or the key does not exist.
func ReadPageCache(ctx context.Context, key string) (json.RawMessage, error) {
	if config.PgPool == nil {
		return nil, errNoPgPool
	}
	var data json.RawMessage
	err := config.PgPool.QueryRow(ctx,
		`SELECT data FROM page_cache WHERE key = $1`, key,
	).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WritePageCache upserts a cache entry in Postgres, serializing value as JSON.
func WritePageCache(ctx context.Context, key string, value any) error {
	if config.PgPool == nil {
		return errNoPgPool
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = config.PgPool.Exec(ctx,
		`INSERT INTO page_cache (key, data, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (key) DO UPDATE SET data = $2, updated_at = NOW()`,
		key, data,
	)
	return err
}
