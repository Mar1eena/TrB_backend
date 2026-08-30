-- Индикаторы: настройки (JSON) + значения (массивы value_keys / value_data).
--   clickhouse-client --multiquery < configs/clickhouse/init.d/indicators.sql

CREATE DATABASE IF NOT EXISTS TrB;

DROP TABLE IF EXISTS TrB.indicator_values_flat;
DROP TABLE IF EXISTS TrB.indicator_values;

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

CREATE TABLE IF NOT EXISTS TrB.indicator_values
(
    uid String CODEC(ZSTD(1)),
    interval Int32 CODEC(DoubleDelta, ZSTD(1)),
    indicator LowCardinality(String),
    params String CODEC(ZSTD(1)),
    time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    value_keys Array(LowCardinality(String)) CODEC(ZSTD(1)),
    value_data Array(Int64) CODEC(ZSTD(1)),
    values Map(LowCardinality(String), Float64) ALIAS mapFromArrays(
        value_keys,
        arrayMap(x -> toFloat64(x) / 1000000, value_data)
    ),
    calculated_at DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (uid, interval, indicator, params, time)
SETTINGS index_granularity = 8192;
