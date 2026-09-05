# indicators / calculation

Python-воркер: читает задания из NATS JetStream (`TrB.indicators.tasks`, JSONEachRow с
`param_hash`), по каждому достаёт protobuf `Settings` из
`TrB_indicators.indicator_assignments`, берёт свечи из `TrB.hct`, считает индикатор
через TA-Lib и пишет точки в `TrB_indicators.indicator_values`.

## Поток обработки (`worker.process_row`)

1. `param_hash` → `indicator_assignments` → `Settings` (`settings_codec.decode_request`).
2. `indicator_values_agg` → `maxMerge(max_time)`; если `settings.end <= max_time` — пропуск
   (`outcome=up_to_date`).
3. `TrB.hct` → OHLCV. Первый расчёт (`max_time is None`) читает весь диапазон
   `start..end`; инкрементальный — только хвост `lookback(индикатор) + INDICATORS_WARMUP_MARGIN_BARS`
   баров, чтобы не перечитывать всю историю.
4. `registry` + TA-Lib → значения; строки с не-`finite` (разогрев) отбрасываются.
5. `insert` только точек новее `max_time` (идемпотентность).

Каждый исход инкрементит счётчик `indicators_calculation_rows_total{outcome=...}`.

## Конкурентность

`INDICATORS_CONCURRENCY > 1` поднимает пул из N ClickHouse-клиентов (клиент
`clickhouse-connect` не потокобезопасен — по клиенту на обработчик) и обрабатывает
батч сообщений параллельно; сам расчёт TA-Lib/NumPy идёт в отдельных потоках
(`asyncio.to_thread`), event loop не блокируется.

> При нескольких репликах с одним durable-консьюмером чтение `max_time` и `insert`
> не атомарны — полагаемся на движок таблицы `indicator_values` (Replacing/Aggregating
> MergeTree схлопывает дубли по `ORDER BY`).

## Переменные окружения

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `INDICATORS_NATS_STREAM` | `indicators_task` | стрим JetStream |
| `INDICATORS_NATS_SUBJECT` | `TrB.indicators.tasks` | subject |
| `INDICATORS_NATS_CONSUMER` | `indicators_calculation` | durable-консьюмер |
| `INDICATORS_CONCURRENCY` | `1` | число параллельных обработчиков (= клиентов CH) |
| `INDICATORS_FETCH_BATCH` | `= CONCURRENCY` | сколько сообщений тянуть за раз |
| `INDICATORS_FETCH_TIMEOUT_SEC` | `5` | таймаут `fetch` |
| `INDICATORS_NAK_DELAY_SEC` | `5` | задержка перед повторной доставкой при сбое (`nak(delay=)`) |
| `INDICATORS_WARMUP_MARGIN_BARS` | `250` | запас баров сверх lookback для инкрементального расчёта |
| `INDICATORS_METRICS_ADDR` | `:9105` | адрес HTTP метрик; `:0` — выключить |
| `INDICATORS_CH_CONNECT_RETRIES` | `30` | попыток подключения к ClickHouse на старте (`-1` — бесконечно) |
| `INDICATORS_CH_CONNECT_BACKOFF_SEC` | `2` | пауза между попытками |
| `NATS_URL` / `NATS_URL_DOCKER` | `nats://localhost:4222` | адрес NATS |
| `CLICKHOUSE_URL` / `CLICKHOUSE_URL_DOCKER` | `localhost:8123` | адрес ClickHouse (HTTP) |
| `CLICKHOUSE_DATABASE` | `TrB` | база |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / `default` | учётка |

## HTTP-эндпоинты (`INDICATORS_METRICS_ADDR`)

- `GET /metrics` — Prometheus text (счётчики исходов, сообщений, записанных точек,
  несопоставленных параметров, `uptime`).
- `GET /healthz` — жив ли процесс.
- `GET /readyz` — доступен ли ClickHouse (503, если недоступен дольше 30 с).

## Тесты

```bash
cd internal/services/indicators/calculation
python -m pytest tests/ -q
```

`tests/conftest.py` гоняет `async def test_*` без плагинов. Тесты `test_calc.py`
требуют нативную libta-lib.
