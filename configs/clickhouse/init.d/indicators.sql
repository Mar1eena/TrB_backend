-- Индикаторы: настройки (JSON) + реестр параметров + значения (indicator_values_v2).
--   clickhouse-client --multiquery < configs/clickhouse/init.d/indicators.sql

CREATE DATABASE IF NOT EXISTS TrB;

CREATE TABLE IF NOT EXISTS TrB.indicator_settings
(
    uid String CODEC(ZSTD(1)),
    interval Int32 CODEC(Delta, ZSTD(1)),
    indicator LowCardinality(String),
    params String COMMENT 'JSON params, e.g. {"period":14.0}' CODEC(ZSTD(1)),
    enabled UInt8,
    updated_at DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (uid, interval, indicator, params);

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

CREATE TABLE IF NOT EXISTS TrB.indicator_values_v2
(
    uid String CODEC(ZSTD(1)),
    interval UInt8 CODEC(Delta, ZSTD(1)),
    indicator LowCardinality(String),
    param_hash UInt64 CODEC(ZSTD(1)),
    time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    v0 Float32 CODEC(Gorilla, ZSTD(1)),
    v1 Float32 CODEC(Gorilla, ZSTD(1)),
    v2 Float32 CODEC(Gorilla, ZSTD(1)),
    v3 Float32 CODEC(Gorilla, ZSTD(1)),
    v4 Float32 CODEC(Gorilla, ZSTD(1)),
    calculated_at DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYear(time)
ORDER BY (uid, interval, indicator, param_hash, time)
SETTINGS index_granularity = 8192;
