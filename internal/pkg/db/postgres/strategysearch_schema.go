package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// strategySearchSchemaSQL — схема домена Optuna-поиска (strategysearch-manage).
// Только CREATE ... IF NOT EXISTS — миграционного инструмента в проекте нет,
// эволюция структуры идёт через jsonb. Independent от домена strategy
// (trb.strategysearch.v1 не зависит от trb.strategy.v1).
const strategySearchSchemaSQL = `
CREATE TABLE IF NOT EXISTS strategysearch_run (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name           text        NOT NULL DEFAULT '',
    base_spec      jsonb       NOT NULL,
    search_space   jsonb       NOT NULL DEFAULT '[]',
    study          jsonb       NOT NULL,
    uid            text        NOT NULL,
    interval       integer     NOT NULL,
    period_start   timestamptz NOT NULL,
    period_end     timestamptz NOT NULL,
    config         jsonb       NOT NULL,
    status         text        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    progress       jsonb       NOT NULL DEFAULT '{}',
    error          text        NOT NULL DEFAULT '',
    engine_version text        NOT NULL DEFAULT '',
    created_at     timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    finished_at    timestamptz
);
CREATE INDEX IF NOT EXISTS strategysearch_run_active_idx  ON strategysearch_run (status) WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS strategysearch_run_created_idx ON strategysearch_run (created_at DESC);

CREATE TABLE IF NOT EXISTS strategysearch_trial (
    id                uuid    PRIMARY KEY DEFAULT gen_random_uuid(),
    search_run_id     uuid    NOT NULL REFERENCES strategysearch_run(id) ON DELETE CASCADE,
    trial_number      integer NOT NULL,
    spec              jsonb   NOT NULL,
    spec_hash         bigint  NOT NULL,
    params            jsonb   NOT NULL DEFAULT '{}',
    values            jsonb   NOT NULL DEFAULT '{}',
    state             text    NOT NULL DEFAULT 'running'
                      CHECK (state IN ('running','waiting','complete','pruned','fail')),
    metrics           jsonb   NOT NULL DEFAULT '{}',
    is_pareto_optimal boolean NOT NULL DEFAULT false,
    backtest_run_id   text    NOT NULL DEFAULT '',
    created_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS strategysearch_trial_dedup_idx ON strategysearch_trial (search_run_id, trial_number);
CREATE INDEX IF NOT EXISTS strategysearch_trial_run_idx ON strategysearch_trial (search_run_id, state);

-- Глобальный кэш оценок трайлов: (spec_hash + инструмент + период + fidelity +
-- параметры брокера + версия движка) -> метрики. Тот же формат ключа, что и
-- у search_eval_cache домена strategy, независимая таблица/жизненный цикл.
CREATE TABLE IF NOT EXISTS strategysearch_eval_cache (
    eval_key       text        PRIMARY KEY,
    spec_hash      bigint      NOT NULL,
    metrics        jsonb       NOT NULL,
    data_fraction  double precision NOT NULL DEFAULT 1.0,
    engine_version text        NOT NULL DEFAULT '',
    hits           integer     NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS strategysearch_eval_cache_gc_idx ON strategysearch_eval_cache (created_at);
`

// EnsureStrategySearchSchema создаёт таблицы домена strategysearch, если их ещё нет.
func EnsureStrategySearchSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, strategySearchSchemaSQL)
	return err
}
