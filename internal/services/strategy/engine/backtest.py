"""Полный прогон одного бэктеста: NATS-задача -> свечи -> Cerebro -> PG/CH.

Чистое ядро прогона — в `_common/btcore.py` (общее с eval-worker).
"""

from __future__ import annotations

import logging
import queue
import time
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

import envutil
import hct
import pg
from btcore import ENGINE_VERSION, run_backtest_inproc
from natsloop import TransientError
from specmod import ch_indicators
from specmod import indicators as ind_mod
from specmod import load as specload

log = logging.getLogger(__name__)


def _indicator_source() -> str:
    return (envutil.get("STRATEGY_INDICATOR_SOURCE") or "clickhouse").strip().lower()


def _indicator_wait_sec() -> float:
    raw = envutil.get("STRATEGY_INDICATOR_WAIT_SEC")
    try:
        return float(raw) if raw else 60.0
    except ValueError:
        return 60.0


class BacktestError(TransientError):
    """CH/PG недоступны — NAK с ретраем."""


def resolve_indicator_lines(
    ch_client,
    gateway,
    spec,
    df: pd.DataFrame,
    uid: str,
    interval: int,
    start: datetime,
    end: datetime,
) -> dict[str, np.ndarray]:
    """Заказывает расчёт индикаторов через indicators-manage и ждёт статус по NATS.

    Поток: manage.UpdateSettings → param_hash → подписка на
    TrB.indicators.status.<hash> → calculation публикует "done" → читаем ряд из
    TrB_indicators.indicator_values по хэшу. Что не успели/ошиблись — опускаем,
    интерпретатор посчитает сам (векторно, runonce=True).
    """
    if _indicator_source() != "clickhouse" or gateway is None or not gateway.enabled:
        return {}

    candle_times = [ts.to_pydatetime() for ts in df.index]
    wait_sec = _indicator_wait_sec()
    out: dict[str, np.ndarray] = {}

    def _take(ref, name: str, h: int) -> bool:
        keys = ch_indicators.output_keys_for(name)
        series = ch_indicators.load_series(
            ch_client, h, ref.output_key, keys, candle_times, start, end,
        )
        if np.isfinite(series).any():
            out[ref.id] = series
            return True
        return False

    # 1. RPC upsert (assignment + задача расчёта) и подписка на статус по хэшу.
    #    Подписываемся сразу после upsert; сам upsert публикует задачу, calc
    #    считает секунды — гонка «успел посчитать раньше подписки» закрыта
    #    финальным чтением из ClickHouse на шаге 3.
    pending: list[tuple[Any, str, int, Any]] = []
    for ref in spec.indicators:
        name = ind_mod.indicator_type_name(ref.settings)
        if not name:
            continue
        try:
            settings = ch_indicators.build_settings_message(uid, interval, ref.settings, start, end)
            h = gateway.upsert(settings)
            # уже посчитано (кэш прошлых прогонов) — не ждём статус вовсе
            if ch_indicators.coverage_reached(ch_client, h, end) and _take(ref, name, h):
                log.info("индикатор %s (%s) уже в ClickHouse hash=%s", ref.id, name, h)
                continue
            waiter = gateway.subscribe_status(h)
            pending.append((ref, name, h, waiter))
        except Exception as exc:  # noqa: BLE001
            log.warning("индикатор %s: заказ через manage не удался (%s) — движок посчитает сам", ref.id, exc)

    # 2. ждём статус "done" с общим дедлайном (ожидания перекрываются).
    deadline = time.monotonic() + wait_sec
    for ref, name, h, waiter in pending:
        try:
            remaining = deadline - time.monotonic()
            st: dict | None = None
            if remaining > 0:
                try:
                    st = waiter.wait(remaining)
                except queue.Empty:
                    st = None
            if st is not None and st.get("status") == "error":
                log.warning("индикатор %s: calculation error=%s — движок посчитает сам", ref.id, st.get("error"))
                continue
            # "done" либо таймаут: пробуем прочитать (вдруг расчёт завершился раньше подписки).
            status_label = (st or {}).get("status", "timeout")
            if _take(ref, name, h):
                log.info("индикатор %s (%s) из ClickHouse hash=%s статус=%s", ref.id, name, h, status_label)
            else:
                log.warning("индикатор %s: в ClickHouse пусто (статус=%s) — движок посчитает сам", ref.id, status_label)
        except Exception as exc:  # noqa: BLE001
            log.warning("индикатор %s: %s — движок посчитает сам", ref.id, exc)
        finally:
            try:
                waiter.close()
            except Exception:  # noqa: BLE001
                pass
    return out


def run_backtest(ch_client, task_run_id: str, gateway=None) -> None:
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
        ch_client, gateway, spec, df, row["uid"], int(row["interval"]),
        row["period_start"], row["period_end"],
    )

    try:
        result = run_backtest_inproc(spec, df, config, precomputed=precomputed, with_indicators=True)
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
    ch_client.command(f"ALTER TABLE TrB_strategy.indicator_series DELETE WHERE run_id = '{run_id}'")

    ind_rows = [
        [run_id, s["indicator_id"], s["indicator"], s["output_key"], 1 if s["overlay"] else 0, t, v]
        for s in result.get("indicators", [])
        for (t, v) in s["points"]
    ]
    if ind_rows:
        ch_client.insert(
            "TrB_strategy.indicator_series",
            ind_rows,
            column_names=["run_id", "indicator_id", "indicator", "output_key", "overlay", "time", "value"],
        )

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
