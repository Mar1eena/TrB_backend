"""Прогон одного бэктеста: NATS-задача -> свечи -> Cerebro -> PG/CH."""

from __future__ import annotations

import logging
import os
from typing import Any

import backtrader as bt
import pandas as pd

import hct
import pg
from specmod import analyzers as an
from specmod import load as specload
from specmod.interpreter import build_strategy_class

log = logging.getLogger(__name__)


def _default_version() -> str:
    try:
        import talib

        tav = talib.__version__
    except Exception:  # noqa: BLE001
        tav = "?"
    return f"bt{bt.__version__}+talib{tav}+engine1"


ENGINE_VERSION = os.environ.get("STRATEGY_ENGINE_VERSION") or _default_version()


class BacktestError(Exception):
    """Транзиентная ошибка (CH/PG недоступны) — NAK с ретраем."""


def run_backtest_inproc(spec, df: pd.DataFrame, config) -> dict[str, Any]:
    """Чистый прогон: без PG/CH/NATS. Возвращает {'metrics','equity','trades'}."""
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

    strat_cls = build_strategy_class(spec, long_only=config.long_only)
    cerebro.addstrategy(strat_cls)
    an.attach(cerebro)

    results = cerebro.run()
    strat = results[0]
    return an.extract(strat, cash)


def run_backtest(ch_client, task_run_id: str) -> None:
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

    try:
        result = run_backtest_inproc(spec, df, config)
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
