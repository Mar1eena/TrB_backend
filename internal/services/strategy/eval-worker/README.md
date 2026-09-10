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
`btcore.run_backtest_inproc(lean=True)` → `EvalResult`. Ошибка спеки/движка →
`EvalResult` с пустыми `metrics` (кандидат отсеивается координатором, ACK).

**Postgres не нужен** — кандидаты/кэш/прогресс пишет координатор.

## Память

Пик ~1–1.5 ГБ на дочерний процесс на длинных сериях. Против утечки:

- **lean-режим** оценки: без `EquityRecorder` (dict на каждый бар) и
  `TradeRecorder` — только сводные метрики. На многолетних минутках это сотни МБ
  на прогон.
- **перезапуск дочернего процесса** каждые `STRATEGY_EVAL_MAX_TASKS_PER_CHILD`
  (деф. 40) — backtrader копит память между прогонами.
- **кэш свечей = 1** на процесс по умолчанию.
- `gc.collect()` каждые 5 прогонов.
- **авто-параллелизм из cgroup-лимита**: `min(cpu, mem_limit / 1.2ГБ, 8)`, если
  `STRATEGY_EVAL_WORKER_CONCURRENCY` не задан. При OOM — понизьте параллелизм или
  поднимите `mem_limit` (~1.5 ГБ на процесс + ~0.3 ГБ базы).
- gauge `..._process_rss_bytes` в `/metrics` — следить за ростом RSS.

## Переменные окружения

`NATS_URL(_DOCKER)`, `CLICKHOUSE_URL(_DOCKER)`, `CLICKHOUSE_DATABASE/USER/PASSWORD`,
`STRATEGY_METRICS_ADDR` (`:9106`), `STRATEGY_ENGINE_VERSION`.

| Переменная | Дефолт | Смысл |
|---|---|---|
| `STRATEGY_EVAL_WORKER_CONCURRENCY` | `min(cpu, mem_limit/1.2ГБ, 8)` | процессов в пуле |
| `STRATEGY_EVAL_MAX_TASKS_PER_CHILD` | `40` | перезапуск дочернего процесса каждые N задач |
| `STRATEGY_EVAL_TASK_TIMEOUT_SEC` | `120` | дедлайн одного кандидата |
| `STRATEGY_CANDLE_CACHE_SIZE` | `1` | LRU наборов свечей в дочернем процессе |

## Локальный запуск

```bash
pip install -r requirements.txt
python main.py
```

Тесты: `pytest tests/`.
