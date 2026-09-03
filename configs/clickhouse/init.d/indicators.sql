-- Индикаторы: реестр параметров + значения (indicator_values) + агрегаты.
--   clickhouse-client --multiquery < configs/clickhouse/init.d/indicators.sql

CREATE DATABASE IF NOT EXISTS TrB;

CREATE TABLE IF NOT EXISTS TrB.indicator_param_registry
(
    param_hash UInt64,
    indicator LowCardinality(String),
    params String CODEC(ZSTD(3)),
    value_keys Array(LowCardinality(String)) CODEC(ZSTD(1)),
    created_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (indicator, param_hash);

CREATE TABLE IF NOT EXISTS TrB.indicator_values
(
    interval UInt8 CODEC(DoubleDelta, ZSTD(1)),
    indicator LowCardinality(String) CODEC(ZSTD(1)),
    uid String CODEC(ZSTD(1)),
    param_hash UInt64 CODEC(ZSTD(1)),
    time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    metrics Map(LowCardinality(String), Float64) CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (interval, indicator, uid, param_hash, time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS TrB.indicator_values_agg
(
    interval UInt8 CODEC(Delta(1), ZSTD(1)),
    indicator LowCardinality(String) CODEC(ZSTD(1)),
    uid String CODEC(ZSTD(1)),
    param_hash UInt64 CODEC(ZSTD(1)),
    log_date Date CODEC(DoubleDelta, ZSTD(1)),
    min_time AggregateFunction(min, DateTime64(3)) CODEC(ZSTD(1)),
    max_time AggregateFunction(max, DateTime64(3)) CODEC(ZSTD(1)),
    count_indicators AggregateFunction(count, UInt64) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(log_date)
ORDER BY (interval, indicator, uid, param_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS TrB.indicator_values_agg_mv
TO TrB.indicator_values_agg
AS
SELECT
    interval,
    indicator,
    uid,
    param_hash,
    toDate(time) AS log_date,
    minState(time) AS min_time,
    maxState(time) AS max_time,
    countState() AS count_indicators
FROM TrB.indicator_values
GROUP BY interval, indicator, uid, param_hash, log_date;

CREATE DATABASE IF NOT EXISTS TrB_indicators;

CREATE TABLE IF NOT EXISTS TrB_indicators.indicator_assignments
(
    param_hash UInt64,
    request String CODEC(ZSTD(3)),
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (param_hash)
SETTINGS index_granularity = 8192;
