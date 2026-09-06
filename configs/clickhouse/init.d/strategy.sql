-- Домен стратегий: кривая капитала, сделки и оценки поиска по прогонам бэктеста.
--   clickhouse-client --multiquery < configs/clickhouse/init.d/strategy.sql
-- init.d применяется только на свежем volume — на работающем кластере выполнить вручную.

CREATE DATABASE IF NOT EXISTS TrB_strategy;

-- Кривая капитала по барам. Ключ — run_id (= backtest_run.id в Postgres).
-- Движок делает ALTER TABLE ... DELETE WHERE run_id = ... перед перезаписью полного прогона.
CREATE TABLE IF NOT EXISTS TrB_strategy.equity_curve
(
    run_id UUID,
    time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    equity Float64 CODEC(ZSTD(1)),
    cash Float64 CODEC(ZSTD(1)),
    position_value Float64 CODEC(ZSTD(1)),
    drawdown Float64 CODEC(ZSTD(1)),
    ret Float64 CODEC(ZSTD(1)),
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (run_id, time)
SETTINGS index_granularity = 8192;

-- Закрытые сделки прогона.
CREATE TABLE IF NOT EXISTS TrB_strategy.trades
(
    run_id UUID,
    trade_id UInt32,
    is_long UInt8,
    entry_time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    entry_price Float64 CODEC(ZSTD(1)),
    exit_time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    exit_price Float64 CODEC(ZSTD(1)),
    size Float64 CODEC(ZSTD(1)),
    pnl Float64 CODEC(ZSTD(1)),
    pnl_pct Float64 CODEC(ZSTD(1)),
    bars_held UInt32,
    mae Float64 CODEC(ZSTD(1)),
    mfe Float64 CODEC(ZSTD(1)),
    entry_reason LowCardinality(String),
    exit_reason LowCardinality(String),
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (run_id, trade_id)
SETTINGS index_granularity = 8192;

-- Ряды индикаторов, использованных стратегией в прогоне (по выбранному output_key).
-- Одна строка = (индикатор, выход, бар). Движок перезаписывает через
-- ALTER TABLE ... DELETE WHERE run_id = ... перед записью нового прогона.
CREATE TABLE IF NOT EXISTS TrB_strategy.indicator_series
(
    run_id UUID,
    indicator_id LowCardinality(String),
    indicator LowCardinality(String),
    output_key LowCardinality(String),
    overlay UInt8,
    time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    value Float64 CODEC(ZSTD(1)),
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (run_id, indicator_id, output_key, time)
SETTINGS index_granularity = 8192;

-- Каждая оценка кандидата в ходе поиска (сходимость/скаттер). Дёшево, TTL 90 дней.
CREATE TABLE IF NOT EXISTS TrB_strategy.search_evals
(
    search_run_id UUID,
    candidate_id UUID,
    generation UInt16,
    score Float64,
    metrics Map(LowCardinality(String), Float64),
    evaluated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (search_run_id, evaluated_at)
TTL toDateTime(evaluated_at) + toIntervalDay(90)
SETTINGS index_granularity = 8192;
