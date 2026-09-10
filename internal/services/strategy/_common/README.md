# strategy/_common

Общий код сервисов [`../engine`](../engine) (координатор) и
[`../eval-worker`](../eval-worker). Не самостоятельный сервис.

Каждый Docker-образ копирует `_common/*` и код своего сервиса **в один плоский
`/app`** — поэтому импорты остаются без префикса (`import hct`,
`from specmod import load`, `import btcore`). Тестовые `conftest.py` в сервисах
добавляют `../_common` (и `../TrB_proto/gen/python`, если `trb-proto` не
установлен) в `sys.path`.

| Модуль | Назначение |
|---|---|
| `btcore.py` | чистое ядро бэктеста (`run_backtest_inproc`, `_cerebro_once`, `ENGINE_VERSION`) — без PG/CH/NATS |
| `specmod/` | интерпретация `StrategySpec` → `bt.Strategy`, индикаторы, анализаторы, хэш |
| `hct.py` | загрузка свечей `TrB.hct` → `pandas.DataFrame` |
| `clickhouse_client.py` | подключение к ClickHouse (HTTP) |
| `natsloop.py` | обвязка JetStream pull-консьюмера: fetch-loop, ack/nak, heartbeat, `TransientError` |
| `metrics.py` | HTTP `/metrics` `/healthz` `/readyz` (Prometheus text) |
| `envutil.py` | `.env` + выбор host/docker URL |
| `jsonutil.py` | JSON-сериализация для jsonb (NaN/Inf → null) |
| `tasks_subjects.py` | имена NATS-субъектов/стримов/консьюмеров (зеркало Go `tasks.go`) |

Тесты ядра: `pytest tests/` (`test_interpreter`, `test_analyzers`, `test_ch_indicators`).
