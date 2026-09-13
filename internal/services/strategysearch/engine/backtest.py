"""Полный прогон одного отдельного (не связанного с Optuna-поиском) бэктеста:
NATS-задача -> свечи -> Cerebro -> PG/CH.

Чистое ядро прогона — в `_common/btcore.py` (общее с search/eval-worker).
Индикаторы считаются целиком в движке (TA-Lib), без обращения к indicators-manage —
в отличие от старого internal/services/strategy/engine/backtest.py, здесь нет
предвыборки через indicator_gateway (не портировали, см. план восстановления).
"""

from __future__ import annotations

import logging
from typing import Any

import hct
import pg
from btcore import ENGINE_VERSION, run_backtest_inproc
from natsloop import TransientError
from specmod import load as specload

log = logging.getLogger(__name__)

_CH_DATABASE = "TrB_strategysearch"
_TABLES_ENSURED = False


def _ensure_tables(ch_client: Any) -> None:
    """Self-provisions equity_curve/trades/indicator_series, тем же паттерном,
    что и search/chsink.py для trials/studies — manage о них ничего не знает."""
    global _TABLES_ENSURED
    if _TABLES_ENSURED:
        return
    ch_client.command(f"CREATE DATABASE IF NOT EXISTS {_CH_DATABASE}")
    ch_client.command(f"""
        CREATE TABLE IF NOT EXISTS {_CH_DATABASE}.equity_curve
        (
            run_id UUID,
            time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
            equity Float64 CODEC(ZSTD(1)),
            cash Float64 CODEC(ZSTD(1)),
            position_value Float64 CODEC(ZSTD(1)),
            drawdown Float64 CODEC(ZSTD(1)),
            ret Float64 CODEC(ZSTD(1)),
            inserted_at DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(inserted_at)
        ORDER BY (run_id, time)
        SETTINGS index_granularity = 8192
    """)
    ch_client.command(f"""
        CREATE TABLE IF NOT EXISTS {_CH_DATABASE}.trades
        (
            run_id UUID,
            trade_id UInt32,
            is_long UInt8,
            entry_time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
            entry_price Float64 CODEC(ZSTD(1)),
            exit_time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
            exit_price Float64 CODEC(ZSTD(1)),
            size Float64 CODEC(ZSTD(1)),
            pnl Float64 CODEC(ZSTD(1)),
            pnl_pct Float64 CODEC(ZSTD(1)),
            bars_held UInt32,
            mae Float64 CODEC(ZSTD(1)),
            mfe Float64 CODEC(ZSTD(1)),
            entry_reason LowCardinality(String),
            exit_reason LowCardinality(String),
            inserted_at DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(inserted_at)
        ORDER BY (run_id, trade_id)
        SETTINGS index_granularity = 8192
    """)
    ch_client.command(f"""
        CREATE TABLE IF NOT EXISTS {_CH_DATABASE}.indicator_series
        (
            run_id UUID,
            indicator_id LowCardinality(String),
            indicator LowCardinality(String),
            output_key LowCardinality(String),
            overlay UInt8,
            time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
            value Float64 CODEC(ZSTD(1)),
            inserted_at DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(inserted_at)
        ORDER BY (run_id, indicator_id, output_key, time)
        SETTINGS index_granularity = 8192
    """)
    _TABLES_ENSURED = True


class BacktestError(TransientError):
    """CH/PG недоступны — NAK с ретраем."""


def run_backtest(ch_client, task_run_id: str) -> None:
    row = pg.fetch_backtest_run(task_run_id)
    if row is None:
        log.warning("нет strategysearch_backtest_run id=%s", task_run_id)
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
        result = run_backtest_inproc(spec, df, config, with_indicators=True, lean=False)
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
    _ensure_tables(ch_client)
    ch_client.command(f"ALTER TABLE TrB_strategysearch.equity_curve DELETE WHERE run_id = '{run_id}'")
    ch_client.command(f"ALTER TABLE TrB_strategysearch.trades DELETE WHERE run_id = '{run_id}'")
    ch_client.command(f"ALTER TABLE TrB_strategysearch.indicator_series DELETE WHERE run_id = '{run_id}'")

    ind_rows = [
        [run_id, s["indicator_id"], s["indicator"], s["output_key"], 1 if s["overlay"] else 0, t, v]
        for s in result.get("indicators", [])
        for (t, v) in s["points"]
    ]
    if ind_rows:
        ch_client.insert(
            "TrB_strategysearch.indicator_series",
            ind_rows,
            column_names=["run_id", "indicator_id", "indicator", "output_key", "overlay", "time", "value"],
        )

    eq = result["equity"]
    if eq:
        ch_client.insert(
            "TrB_strategysearch.equity_curve",
            [[run_id, p["time"], p["equity"], p["cash"], p["position_value"], p["drawdown"], p["ret"]] for p in eq],
            column_names=["run_id", "time", "equity", "cash", "position_value", "drawdown", "ret"],
        )
    tr = result["trades"]
    if tr:
        ch_client.insert(
            "TrB_strategysearch.trades",
            [[run_id, t["trade_id"], t["is_long"], t["entry_time"], t["entry_price"],
              t["exit_time"], t["exit_price"], t["size"], t["pnl"], t["pnl_pct"],
              t["bars_held"], t["mae"], t["mfe"], t["entry_reason"], t["exit_reason"]] for t in tr],
            column_names=["run_id", "trade_id", "is_long", "entry_time", "entry_price",
                          "exit_time", "exit_price", "size", "pnl", "pnl_pct",
                          "bars_held", "mae", "mfe", "entry_reason", "exit_reason"],
        )
