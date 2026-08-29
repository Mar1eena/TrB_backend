CREATE DATABASE IF NOT EXISTS techlog;

CREATE TABLE IF NOT EXISTS techlog.events
(
    `timestamp` DateTime64(6),
    `duration_us` UInt64,
    `duration_ms` Float64,
    `event` LowCardinality(String),
    `severity` LowCardinality(String),
    `level` UInt8,
    `process` LowCardinality(String),
    `process_name` LowCardinality(String),
    `os_thread` UInt32,
    `client_id` UInt32,
    `application_name` LowCardinality(String),
    `computer_name` LowCardinality(String),
    `connect_id` UInt32,
    `session_id` UInt32,
    `usr` LowCardinality(String),
    `app_id` LowCardinality(String),
    `dbpid` UInt32,
    `func` LowCardinality(String),
    `regions` String,
    `locks` String,
    `wait_connections` String,
    `context` String,
    `descr` String,
    `exception` String,
    `sql_text` String,
    `lka` String,
    `lkp` String,
    `lkpid` String,
    `lksrc` String,
    `lkpto` String,
    `lkato` String,
    `file` String,
    `raw` String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (event, process, connect_id, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(14)
SETTINGS index_granularity = 8192;
