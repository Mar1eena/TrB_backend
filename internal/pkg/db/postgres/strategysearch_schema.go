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
    finished_at    timestamptz,
    -- template задан => структурный поиск по палитре индикаторов (StrategyTemplate),
    -- вместо/вместе с тюнингом base_spec через search_space.
    template           jsonb NOT NULL DEFAULT '{}',
    -- market_space задан => uid/interval/период — тоже измерение поиска;
    -- market_candidates — резолв market_space (HistoricCandle/Instruments),
    -- сделанный один раз при SubmitSearch (см. manage/server/search.go).
    market_space       jsonb NOT NULL DEFAULT '{}',
    market_candidates  jsonb NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS strategysearch_run_active_idx  ON strategysearch_run (status) WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS strategysearch_run_created_idx ON strategysearch_run (created_at DESC);

-- CREATE TABLE IF NOT EXISTS не добавляет колонки в уже существующую таблицу —
-- для развёрнутых окружений с более старой strategysearch_run добавляем явно.
ALTER TABLE strategysearch_run ADD COLUMN IF NOT EXISTS template          jsonb NOT NULL DEFAULT '{}';
ALTER TABLE strategysearch_run ADD COLUMN IF NOT EXISTS market_space      jsonb NOT NULL DEFAULT '{}';
ALTER TABLE strategysearch_run ADD COLUMN IF NOT EXISTS market_candidates jsonb NOT NULL DEFAULT '[]';

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

-- Именованный снимок настроек формы поиска (base_spec/search_space/study/config) —
-- чтобы не заполнять форму заново. Не связан со strategysearch_run: пресет не
-- запускает поиск сам по себе, только хранит конфигурацию для последующего SubmitSearch.
CREATE TABLE IF NOT EXISTS strategysearch_preset (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name         text        NOT NULL,
    base_spec    jsonb       NOT NULL,
    search_space jsonb       NOT NULL DEFAULT '[]',
    study        jsonb       NOT NULL,
    config       jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    -- Снимок структурного/рыночного поиска формы (см. strategysearch_run) —
    -- market_candidates намеренно не хранится: это резолв на момент
    -- SubmitSearch, а не часть конфигурации, которую имеет смысл сохранять.
    template     jsonb       NOT NULL DEFAULT '{}',
    market_space jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS strategysearch_preset_created_idx ON strategysearch_preset (created_at DESC);

ALTER TABLE strategysearch_preset ADD COLUMN IF NOT EXISTS template     jsonb NOT NULL DEFAULT '{}';
ALTER TABLE strategysearch_preset ADD COLUMN IF NOT EXISTS market_space jsonb NOT NULL DEFAULT '{}';

-- Каталог именованных стратегий (составление стратегии руками, SpecBuilder) —
-- независим от strategysearch_run/trial: сохранённый StrategySearchSpec, а не
-- прогон поиска.
CREATE TABLE IF NOT EXISTS strategysearch_strategy (
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
CREATE INDEX IF NOT EXISTS strategysearch_strategy_active_idx    ON strategysearch_strategy (created_at DESC) WHERE NOT archived;
CREATE INDEX IF NOT EXISTS strategysearch_strategy_spec_hash_idx ON strategysearch_strategy (spec_hash);

-- Отдельные (не связанные с поиском) прогоны бэктеста — submit одного спека
-- на фиксированном инструменте/периоде, без Optuna.
CREATE TABLE IF NOT EXISTS strategysearch_backtest_run (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id    uuid        REFERENCES strategysearch_strategy(id) ON DELETE SET NULL,
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
    dedup_key      text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    finished_at    timestamptz
);
CREATE INDEX IF NOT EXISTS strategysearch_backtest_run_strategy_idx ON strategysearch_backtest_run (strategy_id, created_at DESC);
CREATE INDEX IF NOT EXISTS strategysearch_backtest_run_active_idx   ON strategysearch_backtest_run (status) WHERE status IN ('queued','running');
CREATE UNIQUE INDEX IF NOT EXISTS strategysearch_backtest_run_dedup_idx ON strategysearch_backtest_run (dedup_key) WHERE dedup_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS strategysearch_backtest_result (
    run_id        uuid   PRIMARY KEY REFERENCES strategysearch_backtest_run(id) ON DELETE CASCADE,
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
CREATE INDEX IF NOT EXISTS strategysearch_backtest_result_sharpe_idx ON strategysearch_backtest_result (sharpe DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS strategysearch_backtest_result_cagr_idx   ON strategysearch_backtest_result (cagr   DESC NULLS LAST);
`

// EnsureStrategySearchSchema создаёт таблицы домена strategysearch, если их ещё нет.
func EnsureStrategySearchSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, strategySearchSchemaSQL)
	return err
}
