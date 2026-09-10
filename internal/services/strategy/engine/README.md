# strategy-engine (координатор)

Python-сервис на **backtrader**: слушает NATS JetStream и выполняет single-бэктесты
и оркестрацию генетического поиска стратегий домена `trb.strategy.v1`. Тяжёлую
оценку кандидатов поиска раздаёт пулу [`../eval-worker`](../eval-worker).

Раскладка кода: `engine/` (этот сервис) + [`../_common`](../_common) (общее ядро,
раскладывается плоско в тот же образ).

## Потоки

| Subject | Consumer | Что делает |
|---|---|---|
| `TrB.strategy.backtest.tasks` | `strategy_backtest_engine` | один прогон `BacktestTask.run_id` |
| `TrB.strategy.search.tasks` | `strategy_search_engine` | оркестрация поиска `SearchTask.search_id` |

Задачи публикует Go-сервис `strategy-manage` (стримы/консьюмеры создаёт он же).
Оценку кандидатов координатор публикует как `EvalTask` в `TrB.strategy.eval.tasks`
(JetStream), ответы `EvalResult` собирает по core-NATS
`TrB.strategy.eval.results.<search_id>`.

## Пайплайн бэктеста (`backtest.run_backtest`)

1. `pg.fetch_backtest_run` → spec (JSONB) + config; статус `running`.
2. `hct.load_candles` — OHLCV из `TrB.hct FINAL` за `[start, end]` → `pandas.DataFrame`.
3. `specmod.interpreter.build_strategy_class` — spec → подкласс `bt.Strategy`
   (дерево правил компилируется в замыкания, без `eval`).
   Индикаторы (`STRATEGY_INDICATOR_SOURCE`):
   - `clickhouse` (по умолчанию) — `resolve_indicator_lines` заказывает расчёт через
     indicators-manage, ждёт `TrB_indicators.indicator_values` (до
     `STRATEGY_INDICATOR_WAIT_SEC`) и читает готовый ряд.
   - `talib` (или фолбэк при таймауте) — считается в движке через `bt.talib`.
   Генетический поиск всегда считает индикаторы в движке (`bt.talib`).
4. `btcore.run_backtest_inproc` → `Cerebro.run` (детерминированный прогон).
5. `specmod.analyzers.extract` — Sharpe/Sortino/CAGR/MaxDD/PF/SQN/… + кривая капитала + сделки.
6. `pg.write_backtest_result` + `TrB_strategy.equity_curve` / `trades` в ClickHouse.

Битая спецификация → `failed` + ACK (poison-proof). CH/PG недоступны → NAK с ретраем
(`natsloop.TransientError`).

## Пайплайн поиска (`search/runner.run_search`)

- `search/genetic.GeneticSearch`: поколение 0 — random-выборка из `search_space`
  (+ опц. мутация структуры), далее турнирный отбор → кроссовер → мутация, элитизм.
  Дефолты: `population=40`, `generations=8`; при заданном `max_evaluations` без
  `population` — `population = max(40, concurrency*4)`,
  `generations = ceil(max_evaluations / population)`.
- `search/genome`: точечные пути в `StrategySpec` через protobuf-reflection, без `eval`.
- **Оценка кандидатов** (на поколение):
  1. глобальный кэш `search_eval_cache` (Postgres) — ключ включает `spec_hash`,
     инструмент/период, `data_fraction`, комиссию/слиппедж/кэш, `ENGINE_VERSION`.
     `budget.disable_cache` выключает.
  2. промахи → `search/dispatch.EvalDispatcher.evaluate_batch`: `EvalTask` в
     JetStream, пул воркеров считает и отвечает `EvalResult`; не ответившие →
     `score=-inf`, один ретрай.
  3. если воркеров нет (`STRATEGY_EVAL_MODE=auto`, проба пуста) или пул молчит →
     локальный `ProcessPoolExecutor` (`budget.concurrency`), свечи грузятся один
     раз и передаются в пул как есть (не копия-список), процесс перезапускается
     каждые 50 задач.
  Оценка идёт в lean-режиме (`btcore.run_backtest_inproc(lean=True)`) — только
  метрики, без кривой капитала и списка сделок: экономит память на длинных сериях.
- **Successive halving** (`budget.halving_eta >= 2`): нижняя ступень — вся популяция
  на префиксе периода (`low_fidelity_frac`, деф. 0.5), топ `1/eta` → полный период;
  проигравшие → `status='pruned'`. `evaluated` считает каждый бэктест обеих ступеней.
- Кандидаты пишутся **батчем на поколение**: `search_candidate` (Postgres, `id`
  генерится клиентом) + один `insert` в `TrB_strategy.search_evals` (ClickHouse).
- Лимиты: `max_evaluations`, `max_seconds`, `population × generations`. Отмена — между поколениями.

## Переменные окружения

`NATS_URL(_DOCKER)`, `CLICKHOUSE_URL(_DOCKER)`, `CLICKHOUSE_DATABASE/USER/PASSWORD`,
`POSTGRES_URL(_DOCKER)`, `POSTGRES_USER/PASSWORD/DB`,
`STRATEGY_METRICS_ADDR` (`:9106`), `STRATEGY_PG_POOL` (`4`), `STRATEGY_ENGINE_VERSION`,
`STRATEGY_INDICATOR_SOURCE` (`clickhouse`|`talib`), `STRATEGY_INDICATOR_WAIT_SEC` (`180`),
`STRATEGY_BACKTEST_CONCURRENCY` (`3`).

| Переменная | Дефолт | Смысл |
|---|---|---|
| `STRATEGY_EVAL_MODE` | `auto` | `auto` (проба воркеров, иначе локальный пул) / `distributed` / `local` |
| `STRATEGY_EVAL_TASK_TIMEOUT_SEC` | `120` | дедлайн оценки одного кандидата |
| `STRATEGY_EVAL_CACHE_TTL_DAYS` | `30` | GC `search_eval_cache` в начале поиска; 0 = не чистить |

## Локальный запуск

```bash
pip install -r requirements.txt              # нужна нативная libta-lib
python main.py                               # PYTHONPATH подхватывает ../_common и ../TrB_proto/gen/python
```

Для распределённой оценки поднять рядом `../eval-worker` (или оставить
`STRATEGY_EVAL_MODE` по умолчанию — движок посчитает локальным пулом).

Тесты: `pytest tests/` (conftest сам добавляет `../_common` и proto в `sys.path`).
