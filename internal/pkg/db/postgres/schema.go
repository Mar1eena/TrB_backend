package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS hct_scheduler_target (
    uid         text        NOT NULL,
    interval    integer     NOT NULL,
    enabled     boolean     NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (uid, interval)
);

CREATE INDEX IF NOT EXISTS hct_scheduler_target_enabled_idx
    ON hct_scheduler_target (enabled)
    WHERE enabled;
`

func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}
