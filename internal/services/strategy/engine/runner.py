"""Прогон одного бэктеста: NATS-задача -> свечи -> Cerebro -> PG/CH."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any

import backtrader as bt
import numpy as np
import pandas as pd

import envutil
import hct
import pg
from specmod import analyzers as an
from specmod import ch_indicators
from specmod import indicators as ind_mod
from specmod import load as specload
from specmod.interpreter import build_strategy_class

log = logging.getLogger(__name__)


def _indicator_source() -> str:
    return (envutil.get("STRATEGY_INDICATOR_SOURCE") or "clickhouse").strip().lower()


def _indicator_wait_sec() -> float:
    raw = envutil.get("STRATEGY_INDICATOR_WAIT_SEC")
    try:
        return float(raw) if raw else 60.0
    except ValueError:
        return 60.0


def _default_version() -> str:
    try:
        import talib

        tav = talib.__version__
    except Exception:  # noqa: BLE001
        tav = "?"
    return f"bt{bt.__version__}+talib{tav}+engine2"


ENGINE_VERSION = os.environ.get("STRATEGY_ENGINE_VERSION") or _default_version()


class BacktestError(Exception):
    """Транзиентная ошибка (CH/PG недоступны) — NAK с ретраем."""


def run_backtest_inproc(spec, df: pd.DataFrame, config, precomputed: dict | None = None) -> dict[str, Any]:
    """Чистый прогон: без PG/CH/NATS. Возвращает {'metrics','equity','trades'}.

    precomputed: {IndicatorRef.id: np.ndarray выровненный по барам df} — значения
    индикаторов из общего пайплайна (ClickHouse). Отсутствующие считаются в движке.
    """
    if df.empty:
        raise specload.SpecError("нет свечей в диапазоне")
    if len(df) <= max(int(spec.warmup_bars), 0) + 2:
        raise specload.SpecError("недостаточно баров для warmup")

    cerebro = bt.Cerebro(stdstats=False, runonce=False)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))

    cash = config.initial_cash or 100000.0
    cerebro.broker.setcash(cash)
    if config.commission_pct:
        cerebro.broker.setcommission(commission=config.commission_pct)
    if config.slippage_pct:
        cerebro.broker.set_slippage_perc(perc=config.slippage_pct)
    cerebro.broker.set_coc(False)

    strat_cls = build_strategy_class(spec, long_only=config.long_only, precomputed=precomputed)
    cerebro.addstrategy(strat_cls)
    an.attach(cerebro)

    results = cerebro.run()
    strat = results[0]
    return an.extract(strat, cash)


def resolve_indicator_lines(
    ch_client,
    publish,
    spec,
    df: pd.DataFrame,
    uid: str,
    interval: int,
    start: datetime,
    end: datetime,
) -> dict[str, np.ndarray]:
    """Заказывает расчёт индикаторов в calculation и читает готовые ряды из ClickHouse.

    Индикаторы, которые не удалось получить (таймаут/ошибка), опускаются —
    интерпретатор посчитает их в движке через bt.talib.
    """
    if _indicator_source() != "clickhouse" or publish is None:
        return {}

    candle_times = [ts.to_pydatetime() for ts in df.index]
    wait_sec = _indicator_wait_sec()
    out: dict[str, np.ndarray] = {}

    # 1. заказываем расчёт всех индикаторов сразу — calculation считает их
    #    параллельно, а не по одному после каждого ожидания.
    pending: list[tuple[Any, str, int]] = []
    for ref in spec.indicators:
        name = ind_mod.indicator_type_name(ref.settings)
        if not name:
            continue
        try:
            h = ch_indicators.request_indicator(
                ch_client, publish, uid=uid, interval=interval,
                indicator_settings=ref.settings, start=start, end=end,
            )
            pending.append((ref, name, h))
        except Exception as exc:  # noqa: BLE001
            log.warning("индикатор %s: ошибка заказа (%s) — движок посчитает сам", ref.id, exc)

    # 2. ждём покрытия с ОБЩИМ дедлайном — ожидания перекрываются.
    deadline = time.monotonic() + wait_sec
    for ref, name, h in pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            log.warning("индикатор %s: бюджет ожидания исчерпан — движок посчитает сам", ref.id)
            continue
        try:
            ch_indicators.wait_for_coverage(ch_client, h, start, end, timeout_sec=remaining)
            keys = ch_indicators.output_keys_for(name)
            series = ch_indicators.load_series(
                ch_client, h, ref.output_key, keys, candle_times, start, end,
            )
            if np.isfinite(series).any():
                out[ref.id] = series
                log.info("индикатор %s (%s) взят из ClickHouse, hash=%s", ref.id, name, h)
            else:
                log.warning("индикатор %s: в ClickHouse пусто — движок посчитает сам", ref.id)
        except ch_indicators.IndicatorTimeout as exc:
            log.warning("индикатор %s: %s — движок посчитает сам", ref.id, exc)
        except Exception as exc:  # noqa: BLE001
            log.warning("индикатор %s: ошибка запроса (%s) — движок посчитает сам", ref.id, exc)
    return out


def run_backtest(ch_client, task_run_id: str, publish=None) -> None:
    row = pg.fetch_backtest_run(task_run_id)
    if row is None:
        log.warning("нет backtest_run id=%s", task_run_id)
        return
    if row["status"] not in ("queued", "running"):
        log.info("backtest_run %s уже в статусе %s — пропуск", task_run_id, row["status"])
        return

    pg.mark_run_running(task_run_id)
    try:
        spec = specload.parse_spec(row["spec"])
        config = specload.parse_config(row["config"])
    except specload.SpecError as exc:
        log.warning("run %s: битая спецификация: %s", task_run_id, exc)
        pg.mark_run_status(task_run_id, "failed", error=str(exc), engine_version=ENGINE_VERSION)
        return

    try:
        df = hct.load_candles(ch_client, row["uid"], int(row["interval"]),
                              row["period_start"], row["period_end"])
    except Exception as exc:  # noqa: BLE001
        raise BacktestError(f"выборка свечей: {exc}") from exc

    precomputed = resolve_indicator_lines(
        ch_client, publish, spec, df, row["uid"], int(row["interval"]),
        row["period_start"], row["period_end"],
    )

    try:
        result = run_backtest_inproc(spec, df, config, precomputed=precomputed)
    except specload.SpecError as exc:
        pg.mark_run_status(task_run_id, "failed", error=str(exc), engine_version=ENGINE_VERSION)
        return
    except Exception as exc:  # noqa: BLE001
        log.exception("run %s: ошибка Cerebro", task_run_id)
        pg.mark_run_status(task_run_id, "failed", error=str(exc), engine_version=ENGINE_VERSION)
        return

    try:
        _write_result(ch_client, task_run_id, result)
    except Exception as exc:  # noqa: BLE001
        raise BacktestError(f"запись результата: {exc}") from exc

    pg.write_backtest_result(task_run_id, result["metrics"])
    pg.mark_run_status(task_run_id, "succeeded", engine_version=ENGINE_VERSION)
    log.info("run %s: готово, trades=%s sharpe=%.3f",
             task_run_id, result["metrics"]["trades_count"], result["metrics"]["sharpe"])


def _write_result(ch_client, run_id: str, result: dict[str, Any]) -> None:
    ch_client.command(f"ALTER TABLE TrB_strategy.equity_curve DELETE WHERE run_id = '{run_id}'")
    ch_client.command(f"ALTER TABLE TrB_strategy.trades DELETE WHERE run_id = '{run_id}'")

    eq = result["equity"]
    if eq:
        ch_client.insert(
            "TrB_strategy.equity_curve",
            [[run_id, p["time"], p["equity"], p["cash"], p["position_value"], p["drawdown"], p["ret"]] for p in eq],
            column_names=["run_id", "time", "equity", "cash", "position_value", "drawdown", "ret"],
        )
    tr = result["trades"]
    if tr:
        ch_client.insert(
            "TrB_strategy.trades",
            [[run_id, t["trade_id"], t["is_long"], t["entry_time"], t["entry_price"],
              t["exit_time"], t["exit_price"], t["size"], t["pnl"], t["pnl_pct"],
              t["bars_held"], t["mae"], t["mfe"], t["entry_reason"], t["exit_reason"]] for t in tr],
            column_names=["run_id", "trade_id", "is_long", "entry_time", "entry_price",
                          "exit_time", "exit_price", "size", "pnl", "pnl_pct",
                          "bars_held", "mae", "mfe", "entry_reason", "exit_reason"],
        )
