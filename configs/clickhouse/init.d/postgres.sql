CREATE DATABASE IF NOT EXISTS postgres;

CREATE TABLE IF NOT EXISTS postgres.logs
(
    `timestamp` DateTime64(3),
    `service` LowCardinality(String),
    `source` LowCardinality(String),
    `severity` LowCardinality(String),
    `severity_pg` LowCardinality(String),
    `user` LowCardinality(String),
    `dbname` LowCardinality(String),
    `pid` UInt32,
    `application_name` LowCardinality(String),
    `backend_type` LowCardinality(String),
    `remote_host` LowCardinality(String),
    `command_tag` LowCardinality(String),
    `query_id` String,
    `sql_state` LowCardinality(String),
    `sql_text` String,
    `message` String,
    `duration_ms` Nullable(Float64),
    `event` LowCardinality(String),
    `query_plan` String,
    `plan_node` LowCardinality(String),
    `detail` String,
    `hint` String,
    `context` String,
    `object_kind` LowCardinality(String) ALIAS multiIf(positionCaseInsensitive(sql_text, '_accumrg') > 0, 'Регистр накопления', positionCaseInsensitive(sql_text, '_inforg') > 0, 'Регистр сведений', positionCaseInsensitive(sql_text, '_accrg') > 0, 'Регистр бухгалтерии', positionCaseInsensitive(sql_text, '_calcrg') > 0, 'Регистр расчета', positionCaseInsensitive(sql_text, '_documentjn') > 0, 'Журнал документов', positionCaseInsensitive(sql_text, '_documentjourn') > 0, 'Журнал документов', positionCaseInsensitive(sql_text, '_document') > 0, 'Документ', positionCaseInsensitive(sql_text, '_reference') > 0, 'Справочник', positionCaseInsensitive(sql_text, '_enum') > 0, 'Перечисление', positionCaseInsensitive(sql_text, '_chrc') > 0, 'План видов характеристик', match(sql_text, '(?i)_Acc[0-9]'), 'План счетов', positionCaseInsensitive(sql_text, '_cacl') > 0, 'План видов расчета', positionCaseInsensitive(sql_text, '_bpr') > 0, 'Бизнес-процесс', positionCaseInsensitive(sql_text, '_task') > 0, 'Задача', positionCaseInsensitive(sql_text, '_const') > 0, 'Константа', positionCaseInsensitive(sql_text, '_node') > 0, 'План обмена', match(sql_text, '(?i)\\b(configsave|config|params|files|dbschema|schemastorage|ibversion)\\b'), 'Платформа 1С', 'Прочее'),
    `object_table` String ALIAS extract(sql_text, '(?i)(_(?:InfoRg|AccumRg|AccRg|CalcRg|DocumentJn|DocumentJourn|Document|Reference|Enum|Chrc|Cacl|BPr|Task|Const|Node)[A-Za-z0-9]+)'),
    `sql_hash` String ALIAS hex(cityHash64(sql_text))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service, dbname, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS postgres.query_plans
(
    `timestamp` DateTime64(3),
    `service` LowCardinality(String),
    `user` LowCardinality(String),
    `dbname` LowCardinality(String),
    `pid` UInt32,
    `application_name` LowCardinality(String),
    `command_tag` LowCardinality(String),
    `query_id` String,
    `sql_text` String,
    `duration_ms` Float64,
    `plan_node` LowCardinality(String),
    `query_plan` String,
    `object_kind` LowCardinality(String) ALIAS multiIf(positionCaseInsensitive(sql_text, '_accumrg') > 0, 'Регистр накопления', positionCaseInsensitive(sql_text, '_inforg') > 0, 'Регистр сведений', positionCaseInsensitive(sql_text, '_accrg') > 0, 'Регистр бухгалтерии', positionCaseInsensitive(sql_text, '_calcrg') > 0, 'Регистр расчета', positionCaseInsensitive(sql_text, '_documentjn') > 0, 'Журнал документов', positionCaseInsensitive(sql_text, '_documentjourn') > 0, 'Журнал документов', positionCaseInsensitive(sql_text, '_document') > 0, 'Документ', positionCaseInsensitive(sql_text, '_reference') > 0, 'Справочник', positionCaseInsensitive(sql_text, '_enum') > 0, 'Перечисление', positionCaseInsensitive(sql_text, '_chrc') > 0, 'План видов характеристик', match(sql_text, '(?i)_Acc[0-9]'), 'План счетов', positionCaseInsensitive(sql_text, '_cacl') > 0, 'План видов расчета', positionCaseInsensitive(sql_text, '_bpr') > 0, 'Бизнес-процесс', positionCaseInsensitive(sql_text, '_task') > 0, 'Задача', positionCaseInsensitive(sql_text, '_const') > 0, 'Константа', positionCaseInsensitive(sql_text, '_node') > 0, 'План обмена', match(sql_text, '(?i)\\b(configsave|config|params|files|dbschema|schemastorage|ibversion)\\b'), 'Платформа 1С', 'Прочее'),
    `object_table` String ALIAS extract(sql_text, '(?i)(_(?:InfoRg|AccumRg|AccRg|CalcRg|DocumentJn|DocumentJourn|Document|Reference|Enum|Chrc|Cacl|BPr|Task|Const|Node)[A-Za-z0-9]+)'),
    `sql_hash` String ALIAS hex(cityHash64(sql_text))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service, dbname, duration_ms, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS postgres.errors
(
    `timestamp` DateTime64(3),
    `service` LowCardinality(String),
    `source` LowCardinality(String),
    `severity` LowCardinality(String),
    `severity_pg` LowCardinality(String),
    `user` LowCardinality(String),
    `dbname` LowCardinality(String),
    `pid` UInt32,
    `application_name` LowCardinality(String),
    `command_tag` LowCardinality(String),
    `query_id` String,
    `sql_state` LowCardinality(String),
    `sql_text` String,
    `message` String,
    `detail` String,
    `hint` String,
    `context` String,
    `object_kind` LowCardinality(String) ALIAS multiIf(positionCaseInsensitive(sql_text, '_accumrg') > 0, 'Регистр накопления', positionCaseInsensitive(sql_text, '_inforg') > 0, 'Регистр сведений', positionCaseInsensitive(sql_text, '_accrg') > 0, 'Регистр бухгалтерии', positionCaseInsensitive(sql_text, '_calcrg') > 0, 'Регистр расчета', positionCaseInsensitive(sql_text, '_documentjn') > 0, 'Журнал документов', positionCaseInsensitive(sql_text, '_documentjourn') > 0, 'Журнал документов', positionCaseInsensitive(sql_text, '_document') > 0, 'Документ', positionCaseInsensitive(sql_text, '_reference') > 0, 'Справочник', positionCaseInsensitive(sql_text, '_enum') > 0, 'Перечисление', positionCaseInsensitive(sql_text, '_chrc') > 0, 'План видов характеристик', match(sql_text, '(?i)_Acc[0-9]'), 'План счетов', positionCaseInsensitive(sql_text, '_cacl') > 0, 'План видов расчета', positionCaseInsensitive(sql_text, '_bpr') > 0, 'Бизнес-процесс', positionCaseInsensitive(sql_text, '_task') > 0, 'Задача', positionCaseInsensitive(sql_text, '_const') > 0, 'Константа', positionCaseInsensitive(sql_text, '_node') > 0, 'План обмена', match(sql_text, '(?i)\\b(configsave|config|params|files|dbschema|schemastorage|ibversion)\\b'), 'Платформа 1С', 'Прочее'),
    `object_table` String ALIAS extract(sql_text, '(?i)(_(?:InfoRg|AccumRg|AccRg|CalcRg|DocumentJn|DocumentJourn|Document|Reference|Enum|Chrc|Cacl|BPr|Task|Const|Node)[A-Za-z0-9]+)'),
    `sql_hash` String ALIAS hex(cityHash64(sql_text))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service, sql_state, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(90)
SETTINGS index_granularity = 8192;
