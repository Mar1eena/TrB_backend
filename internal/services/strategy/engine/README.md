# strategy-engine

Python-воркер на **backtrader**: слушает NATS JetStream и выполняет
бэктесты и генетический поиск стратегий домена `trb.strategy.v1`.

## Потоки

| Subject | Consumer | Что делает |
|---|---|---|
| `TrB.strategy.backtest.tasks` | `strategy_backtest_engine` | один прогон `BacktestTask.run_id` |
| `TrB.strategy.search.tasks` | `strategy_search_engine` | генетический поиск `SearchTask.search_id` |

Задачи публикует Go-сервис `strategy-manage`, тело — сериализованный protobuf.

## Пайплайн бэктеста (`runner.run_backtest`)

1. `pg.fetch_backtest_run` → spec (JSONB) + config; статус `running`.
2. `hct.load_candles` — OHLCV из `TrB.hct FINAL` за `[start, end]` → `pandas.DataFrame`.
3. `specmod.interpreter.build_strategy_class` — spec → подкласс `bt.Strategy`
   (дерево правил компилируется в замыкания, без `eval`).
   Индикаторы (`STRATEGY_INDICATOR_SOURCE`):
   - `clickhouse` (по умолчанию) — `resolve_indicator_lines` по каждому `IndicatorRef`
     кладёт `indicators.Settings` в `TrB_indicators.indicator_assignments`, публикует
     задачу в `TrB.indicators.tasks`, ждёт появления значений в
     `TrB_indicators.indicator_values` (до `STRATEGY_INDICATOR_WAIT_SEC`) и читает
     готовый ряд (`param_hash` включает окно `[start,end]` — расчёт всегда полный).
     `_ArrayLine` подаёт его в backtrader как линию индикатора.
   - `talib` (или фолбэк при таймауте/ошибке) — считается в движке через `bt.talib`.
   Генетический поиск всегда считает индикаторы в движке (`bt.talib`) — тысячи
   прогонов через NATS+CH были бы медленнее.
4. `Cerebro.run(runonce=False, coc=False)` — детерминированный прогон.
5. `specmod.analyzers.extract` — Sharpe/Sortino/CAGR/MaxDD/PF/SQN/… + кривая капитала + сделки.
6. `pg.write_backtest_result` (промо-колонки + `metrics` JSONB) и
   `TrB_strategy.equity_curve` / `TrB_strategy.trades` в ClickHouse
   (delete-by-`run_id` → insert). Статус `succeeded`.

Битая спецификация → `failed` + ACK (poison-proof). CH/PG недоступны → NAK с ретраем.

## Пайплайн поиска (`search/runner.run_search`)

- `search/genetic.GeneticSearch`: поколение 0 — random-выборка из `search_space`
  (+ опц. мутация структуры), далее турнирный отбор → кроссовер параметров →
  мутация параметров/структуры, элитизм.
- `search/genome`: точечные пути в `StrategySpec` (`indicators.<id>.settings.rsi.period`,
  `risk.stop_loss_pct`, `entry_long.all.operands.0.compare.right.constant`) —
  разбор через protobuf-reflection, без `eval`.
- Оценка кандидатов — `ProcessPoolExecutor` (`budget.concurrency`), свечи грузятся один раз.
- `search/objective.score` — метрика → скаляр; жёсткие ворота `min_trades` / `max_drawdown_limit`.
- Каждый кандидат → `search_candidate` (Postgres) + `TrB_strategy.search_evals` (ClickHouse).
- Лимиты: `max_evaluations`, `max_seconds`, `population × generations`. Отмена — между поколениями.

## Переменные окружения

`NATS_URL(_DOCKER)`, `CLICKHOUSE_URL(_DOCKER)`, `CLICKHOUSE_DATABASE/USER/PASSWORD`,
`POSTGRES_URL(_DOCKER)`, `POSTGRES_USER/PASSWORD/DB`,
`STRATEGY_METRICS_ADDR` (`:9106`), `STRATEGY_PG_POOL` (`4`), `STRATEGY_ENGINE_VERSION`,
`STRATEGY_INDICATOR_SOURCE` (`clickhouse`|`talib`), `STRATEGY_INDICATOR_WAIT_SEC` (`180`).

## Локальный запуск

```bash
pip install -r requirements.txt          # нужна нативная libta-lib
pip install -e ../../../..               # либо PYTHONPATH на ../TrB_proto/gen/python
python main.py
```

Тесты: `PYTHONPATH=../../../../../TrB_proto/gen/python:. pytest tests/`
