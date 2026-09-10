# strategy-eval-worker

Пул stateless-воркеров: оценка одного кандидата генетического поиска
(`EvalTask` → полный бэктест → `EvalResult`). Масштабируется числом реплик:

```bash
docker compose up -d --scale strategy-eval-worker=8
```

Раскладка кода: `eval-worker/` (этот сервис) + [`../_common`](../_common) (общее
ядро: `btcore`, `specmod/`, `hct`, `clickhouse_client`, `metrics`, `natsloop`,
`tasks_subjects`).

## Поток

| Subject | Consumer | |
|---|---|---|
| `TrB.strategy.eval.tasks` | `strategy_eval_worker` (JetStream, WorkQueue, shared) | `EvalTask` |
| `TrB.strategy.eval.results.<search_id>` | core-NATS | `EvalResult` координатору |
| `TrB.strategy.eval.ping` | core-NATS, queue-group `eval-workers` | ответ на пробу `EvalDispatcher.available()` |

`handle_eval_task`: `EvalTask` → свечи из ClickHouse (LRU-кэш по
`(uid, interval, период)` в дочернем процессе) → срез до `data_fraction` →
`btcore.run_backtest_inproc` → `EvalResult`. Ошибка спеки/движка → `EvalResult` с
пустыми `metrics` (кандидат отсеивается координатором, задача ACK-ается).

Внутри реплики — `ProcessPoolExecutor` (`STRATEGY_EVAL_WORKER_CONCURRENCY`).
**Postgres не нужен** — кандидаты/кэш/прогресс пишет координатор.

## Переменные окружения

`NATS_URL(_DOCKER)`, `CLICKHOUSE_URL(_DOCKER)`, `CLICKHOUSE_DATABASE/USER/PASSWORD`,
`STRATEGY_METRICS_ADDR` (`:9106`), `STRATEGY_ENGINE_VERSION`.

| Переменная | Дефолт | Смысл |
|---|---|---|
| `STRATEGY_EVAL_WORKER_CONCURRENCY` | `min(cpu, 8)` | процессов в пуле |
| `STRATEGY_EVAL_TASK_TIMEOUT_SEC` | `120` | дедлайн одного кандидата |
| `STRATEGY_CANDLE_CACHE_SIZE` | `4` | LRU наборов свечей в дочернем процессе |

## Локальный запуск

```bash
pip install -r requirements.txt
python main.py
```

Тесты: `pytest tests/`.
