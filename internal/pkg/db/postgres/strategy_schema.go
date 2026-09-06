package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// strategySchemaSQL — схема домена стратегий (strategy-manage).
// Только CREATE ... IF NOT EXISTS / ALTER ... ADD COLUMN IF NOT EXISTS —
// миграционного инструмента в проекте нет, эволюция структуры идёт через jsonb.
const strategySchemaSQL = `
CREATE TABLE IF NOT EXISTS strategy (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name         text        NOT NULL,
    description  text        NOT NULL DEFAULT '',
    spec         jsonb       NOT NULL,
    spec_hash    bigint      NOT NULL,
    spec_version integer     NOT NULL DEFAULT 1,
    archived     boolean     NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS strategy_active_idx    ON strategy (created_at DESC) WHERE NOT archived;
CREATE INDEX IF NOT EXISTS strategy_spec_hash_idx ON strategy (spec_hash);

CREATE TABLE IF NOT EXISTS backtest_run (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id    uuid        REFERENCES strategy(id) ON DELETE SET NULL,
    spec           jsonb       NOT NULL,
    spec_hash      bigint      NOT NULL,
    uid            text        NOT NULL,
    interval       integer     NOT NULL,
    period_start   timestamptz NOT NULL,
    period_end     timestamptz NOT NULL,
    config         jsonb       NOT NULL,
    status         text        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    error          text        NOT NULL DEFAULT '',
    engine_version text        NOT NULL DEFAULT '',
    search_run_id  uuid,
    dedup_key      text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    finished_at    timestamptz
);
CREATE INDEX IF NOT EXISTS backtest_run_strategy_idx ON backtest_run (strategy_id, created_at DESC);
CREATE INDEX IF NOT EXISTS backtest_run_active_idx   ON backtest_run (status) WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS backtest_run_search_idx   ON backtest_run (search_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS backtest_run_dedup_idx ON backtest_run (dedup_key) WHERE dedup_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS backtest_result (
    run_id        uuid   PRIMARY KEY REFERENCES backtest_run(id) ON DELETE CASCADE,
    total_return  double precision,
    cagr          double precision,
    sharpe        double precision,
    sortino       double precision,
    max_drawdown  double precision,
    win_rate      double precision,
    profit_factor double precision,
    sqn           double precision,
    trades_count  integer,
    exposure      double precision,
    final_equity  double precision,
    metrics       jsonb  NOT NULL DEFAULT '{}',
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS backtest_result_sharpe_idx ON backtest_result (sharpe DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS backtest_result_cagr_idx   ON backtest_result (cagr   DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS search_run (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    base_strategy_id uuid        REFERENCES strategy(id) ON DELETE SET NULL,
    base_spec        jsonb       NOT NULL DEFAULT '{}',
    name             text        NOT NULL DEFAULT '',
    method           text        NOT NULL DEFAULT 'genetic' CHECK (method IN ('genetic')),
    search_space     jsonb       NOT NULL DEFAULT '[]',
    structure        jsonb       NOT NULL DEFAULT '{}',
    objective        jsonb       NOT NULL,
    budget           jsonb       NOT NULL,
    uid              text        NOT NULL,
    interval         integer     NOT NULL,
    period_start     timestamptz NOT NULL,
    period_end       timestamptz NOT NULL,
    config           jsonb       NOT NULL,
    status           text        NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    progress         jsonb       NOT NULL DEFAULT '{}',
    error            text        NOT NULL DEFAULT '',
    engine_version   text        NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz
);
CREATE INDEX IF NOT EXISTS search_run_active_idx ON search_run (status) WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS search_run_created_idx ON search_run (created_at DESC);

CREATE TABLE IF NOT EXISTS search_candidate (
    id              uuid   PRIMARY KEY DEFAULT gen_random_uuid(),
    search_run_id   uuid   NOT NULL REFERENCES search_run(id) ON DELETE CASCADE,
    spec            jsonb  NOT NULL,
    spec_hash       bigint NOT NULL,
    params          jsonb  NOT NULL DEFAULT '{}',
    backtest_run_id uuid   REFERENCES backtest_run(id) ON DELETE SET NULL,
    score           double precision NOT NULL,
    metrics         jsonb  NOT NULL DEFAULT '{}',
    rank            integer,
    generation      integer NOT NULL DEFAULT 0,
    status          text    NOT NULL DEFAULT 'evaluated'
                    CHECK (status IN ('evaluated','failed','pruned')),
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS search_candidate_score_idx ON search_candidate (search_run_id, score DESC);
CREATE UNIQUE INDEX IF NOT EXISTS search_candidate_dedup_idx ON search_candidate (search_run_id, spec_hash);
`

// EnsureStrategySchema создаёт таблицы домена стратегий, если их ещё нет.
func EnsureStrategySchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, strategySchemaSQL)
	return err
}
